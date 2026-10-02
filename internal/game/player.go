package game

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"servergame/internal/errs"
	"servergame/internal/model"
	"servergame/internal/store"
	gamepb "servergame/pb/game/v1"
)

// pbProfile 简短别名。
type pbProfile = gamepb.PlayerProfile

// playerToPb 把玩家模型转换为 pb 资料。
func playerToPb(p *model.Player, username string) *pbProfile {
	return &pbProfile{
		Uid:         p.UID,
		Username:    username,
		Nickname:    p.Nickname,
		Level:       p.Level,
		Exp:         p.Exp,
		Gold:        p.Gold,
		Diamond:     p.Diamond,
		WinCount:    p.WinCount,
		LoseCount:   p.LoseCount,
		DrawCount:   p.DrawCount,
		CreatedAt:   p.CreatedAt.UnixMilli(),
		LastLoginAt: p.LastLoginAt.UnixMilli(),
	}
}

// onGetProfile 查询自己的资料。
func (g *Game) onGetProfile(s *Session, _ *gamepb.Empty) error {
	snap := s.PlayerSnapshot()
	return s.Conn.SendMsg(uint16(gamepb.MsgID_GET_PROFILE_RES), &gamepb.GetProfileRes{
		Profile: playerToPb(&snap, s.Username),
	})
}

// onRename 修改昵称（全局唯一）。
func (g *Game) onRename(s *Session, req *gamepb.RenameReq) error {
	name := strings.TrimSpace(req.Nickname)
	if n := utf8.RuneCountInString(name); n < 2 || n > 16 {
		return errs.New(gamepb.ErrCode_ERR_NAME_INVALID, "昵称需为 2~16 个字符")
	}

	snap := s.PlayerSnapshot()
	snap.Nickname = name
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.players.Save(ctx, &snap); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			return errs.New(gamepb.ErrCode_ERR_NAME_TAKEN, "昵称已被占用")
		}
		return errs.Internal("保存昵称失败")
	}

	s.SetNickname(name)
	return s.Conn.SendMsg(uint16(gamepb.MsgID_RENAME_RES), &gamepb.RenameRes{Nickname: name})
}
