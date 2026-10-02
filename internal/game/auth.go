package game

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"servergame/internal/errs"
	"servergame/internal/model"
	"servergame/internal/pkg/logger"
	"servergame/internal/server"
	"servergame/internal/store"
	gamepb "servergame/pb/game/v1"
)

// 用户名与设备号的合法性约束。
var (
	usernamePattern = regexp.MustCompile(`^[A-Za-z0-9_]{3,24}$`)
	deviceIDPattern = regexp.MustCompile(`^[\x21-\x7e]{8,64}$`) // 8~64 个可见 ASCII 字符
)

// onGuestLogin 游客登录：设备号首次出现则自动建号。
func (g *Game) onGuestLogin(c *server.Conn, req *gamepb.GuestLoginReq) error {
	deviceID := strings.TrimSpace(req.DeviceId)
	if !deviceIDPattern.MatchString(deviceID) {
		return errs.BadRequest("deviceId 需为 8~64 位可见字符")
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.MongoTimeout())
	defer cancel()

	now := time.Now()
	acc, err := g.accounts.FindByDeviceID(ctx, deviceID)
	switch {
	case err == nil:
		// 已有游客账号
	case errors.Is(err, store.ErrNotFound):
		acc = &model.Account{
			Username:    "guest_" + shortHash(deviceID),
			IsGuest:     true,
			DeviceID:    deviceID,
			CreatedAt:   now,
			LastLoginAt: now,
		}
		if err := g.accounts.Create(ctx, acc); err != nil {
			if errors.Is(err, store.ErrDuplicate) {
				// 极小概率的用户名冲突：直接按用户名再查一次
				acc, err = g.accounts.FindByUsername(ctx, acc.Username)
				if err != nil {
					return errs.Internal("登录失败，请稍后再试")
				}
			} else {
				return errs.Internal("创建游客账号失败")
			}
		} else {
			logger.L.Infof("新游客建号: %s (device=%s)", acc.Username, deviceID)
		}
	default:
		return errs.Internal("登录失败，请稍后再试")
	}

	if err := g.accounts.TouchLogin(ctx, acc.UID, now); err != nil {
		logger.L.Warnf("更新登录时间失败 uid=%d: %v", acc.UID, err)
	}
	p, isNew, err := g.loadOrCreatePlayer(ctx, acc, now)
	if err != nil {
		return errs.Internal("加载玩家数据失败")
	}

	s := g.buildSession(c, acc, p)
	return c.SendMsg(uint16(gamepb.MsgID_GUEST_LOGIN_RES), &gamepb.LoginRes{
		IsNew:   isNew,
		Profile: playerToPb(p, s.Username),
	})
}

// onAccountRegister 账号注册（成功即登录）。
func (g *Game) onAccountRegister(c *server.Conn, req *gamepb.AccountRegisterReq) error {
	username := strings.TrimSpace(req.Username)
	if !usernamePattern.MatchString(username) {
		return errs.BadRequest("用户名需为 3~24 位字母、数字或下划线")
	}
	if len(req.Password) < 6 || len(req.Password) > 32 {
		return errs.BadRequest("密码长度需为 6~32 位")
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.MongoTimeout())
	defer cancel()

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return errs.Internal("密码加密失败")
	}

	now := time.Now()
	acc := &model.Account{
		Username:     username,
		PasswordHash: string(hash),
		CreatedAt:    now,
		LastLoginAt:  now,
	}
	if err := g.accounts.Create(ctx, acc); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			return errs.New(gamepb.ErrCode_ERR_ALREADY_EXISTS, "用户名已被注册")
		}
		return errs.Internal("注册失败，请稍后再试")
	}
	logger.L.Infof("新账号注册: %s (uid=%d)", username, acc.UID)

	p, _, err := g.loadOrCreatePlayer(ctx, acc, now)
	if err != nil {
		return errs.Internal("初始化玩家数据失败")
	}

	s := g.buildSession(c, acc, p)
	return c.SendMsg(uint16(gamepb.MsgID_ACCOUNT_REGISTER_RES), &gamepb.AccountRegisterRes{
		Uid:      acc.UID,
		Username: acc.Username,
		Profile:  playerToPb(p, s.Username),
	})
}

// onAccountLogin 账号登录。
func (g *Game) onAccountLogin(c *server.Conn, req *gamepb.AccountLoginReq) error {
	username := strings.TrimSpace(req.Username)
	if username == "" || req.Password == "" {
		return errs.BadRequest("请输入用户名和密码")
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.MongoTimeout())
	defer cancel()

	acc, err := g.accounts.FindByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return errs.New(gamepb.ErrCode_ERR_NOT_FOUND, "账号不存在")
		}
		return errs.Internal("登录失败，请稍后再试")
	}
	if acc.PasswordHash == "" || bcrypt.CompareHashAndPassword([]byte(acc.PasswordHash), []byte(req.Password)) != nil {
		return errs.New(gamepb.ErrCode_ERR_PASSWORD_WRONG, "密码错误")
	}

	now := time.Now()
	if err := g.accounts.TouchLogin(ctx, acc.UID, now); err != nil {
		logger.L.Warnf("更新登录时间失败 uid=%d: %v", acc.UID, err)
	}
	p, _, err := g.loadOrCreatePlayer(ctx, acc, now)
	if err != nil {
		return errs.Internal("加载玩家数据失败")
	}

	s := g.buildSession(c, acc, p)
	return c.SendMsg(uint16(gamepb.MsgID_ACCOUNT_LOGIN_RES), &gamepb.LoginRes{
		IsNew:   false,
		Profile: playerToPb(p, s.Username),
	})
}

// shortHash 生成 12 位十六进制短摘要（用于游客用户名）。
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:12]
}
