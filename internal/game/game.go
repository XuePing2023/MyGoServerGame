// Package game 组装游戏服务器的全部业务：会话、账号、玩家、聊天、房间。
package game

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"servergame/internal/config"
	"servergame/internal/errs"
	"servergame/internal/model"
	"servergame/internal/pkg/logger"
	"servergame/internal/protocol"
	"servergame/internal/server"
	"servergame/internal/store"
	gamepb "servergame/pb/game/v1"
)

// 等级与经验换算：每 levelStepExp 点经验提升 1 级。
const (
	levelStepExp = int64(100)
	maxLevel     = int32(99)
)

// Game 游戏服务器聚合根。
type Game struct {
	cfg      *config.Config
	accounts store.AccountStore
	players  store.PlayerStore

	router   *protocol.Router
	conns    *server.Manager
	sessions *SessionManager
	rooms    *RoomManager

	onlineDirty atomic.Bool // 在线人数变化待广播
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

// New 创建游戏主体并注册全部消息路由。
func New(cfg *config.Config, accounts store.AccountStore, players store.PlayerStore) *Game {
	g := &Game{
		cfg:      cfg,
		accounts: accounts,
		players:  players,
		router:   protocol.NewRouter(),
		conns:    server.NewManager(),
		sessions: NewSessionManager(),
		stopCh:   make(chan struct{}),
	}
	g.rooms = NewRoomManager(cfg.Game.MaxRooms)
	g.registerRoutes()
	return g
}

// Router 消息路由器（网络层接入时使用）。
func (g *Game) Router() *protocol.Router { return g.router }

// ConnConfig 生成网络层所需的连接参数。
func (g *Game) ConnConfig() *server.ConnConfig {
	return &server.ConnConfig{
		Router:         g.router,
		SendChanSize:   g.cfg.App.SendChanSize,
		MaxPacketBytes: g.cfg.App.MaxPacketBytes,
		OnOpen:         g.OnConnOpen,
		OnClose:        g.onConnClosed,
	}
}

// OnConnOpen 新连接建立：登记并推送服务器信息与在线人数。
func (g *Game) OnConnOpen(c *server.Conn) {
	g.conns.Add(c)
	_ = c.SendMsg(uint16(gamepb.MsgID_SERVER_INFO_NOTIFY), &gamepb.ServerInfoNotify{
		Name:                g.cfg.App.Name,
		Version:             g.cfg.App.Version,
		HeartbeatIntervalMs: int32(g.cfg.Game.HeartbeatIntervalMs),
		IdleTimeoutMs:       int32(g.cfg.Game.IdleTimeoutSeconds * 1000),
		ServerTime:          nowMs(),
	})
	_ = c.SendMsg(uint16(gamepb.MsgID_ONLINE_COUNT_NOTIFY), &gamepb.OnlineCountNotify{
		Online: int32(g.sessions.Count()),
	})
}

// onConnClosed 连接关闭：清理会话、退房、落盘、广播在线人数。
func (g *Game) onConnClosed(c *server.Conn, reason string) {
	g.conns.Remove(c)
	logger.L.Infof("连接 #%d (%s) 关闭: %s", c.ID(), c.RemoteAddr(), reason)

	s, _ := c.Data().(*Session)
	if s == nil {
		return // 从未登录，无需处理
	}
	g.sessions.Remove(s)
	g.rooms.RemoveMember(s)
	g.saveSession(s, "连接关闭")
	g.markOnlineDirty()
}

// markOnlineDirty 标记在线人数变化，由后台协程合并广播。
func (g *Game) markOnlineDirty() { g.onlineDirty.Store(true) }

// Start 启动后台协程：空闲踢线、自动落盘、在线人数广播。
func (g *Game) Start() {
	g.wg.Add(3)
	go g.idleKickLoop()
	go g.autosaveLoop()
	go g.onlineBroadcastLoop()
	logger.L.Info("游戏后台协程已启动")
}

// Stop 停止后台协程并保存全部会话数据（网络层与注册中心由 main 负责停）。
func (g *Game) Stop() {
	g.stopOnce.Do(func() { close(g.stopCh) })
	g.wg.Wait()

	// 兜底保存所有仍在会话中的玩家数据
	sessions := g.sessions.All()
	for _, s := range sessions {
		g.saveSession(s, "停机保存")
	}
	logger.L.Infof("停机保存完成，共 %d 个会话", len(sessions))
}

// idleKickLoop 周期扫描连接，踢掉超过空闲阈值的连接。
func (g *Game) idleKickLoop() {
	defer g.wg.Done()
	interval := g.cfg.IdleTimeout() / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
		}
		deadline := time.Now().Add(-g.cfg.IdleTimeout())
		for _, c := range g.conns.List() {
			if c.LastActive().Before(deadline) {
				_ = c.SendMsg(uint16(gamepb.MsgID_KICK_NOTIFY), &gamepb.KickNotify{Reason: "长时间未活动，请重新连接"})
				c.CloseAfterDrain("空闲超时", 200*time.Millisecond)
			}
		}
	}
}

// autosaveLoop 周期保存脏会话数据。
func (g *Game) autosaveLoop() {
	defer g.wg.Done()
	interval := time.Duration(g.cfg.Game.AutosaveSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
		}
		saved := 0
		for _, s := range g.sessions.All() {
			if !s.dirty.CompareAndSwap(true, false) {
				continue
			}
			snap := s.PlayerSnapshot()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := g.players.Save(ctx, &snap)
			cancel()
			if err != nil {
				s.dirty.Store(true) // 保存失败恢复脏标记
				logger.L.Warnf("自动保存玩家 %d 失败: %v", s.UID, err)
				continue
			}
			saved++
		}
		if saved > 0 {
			logger.L.Debugf("自动保存完成: %d 个玩家", saved)
		}
	}
}

// onlineBroadcastLoop 在线人数变化合并广播（最多 2 秒一次）。
func (g *Game) onlineBroadcastLoop() {
	defer g.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
		}
		if g.onlineDirty.CompareAndSwap(true, false) {
			_ = g.conns.BroadcastMsg(uint16(gamepb.MsgID_ONLINE_COUNT_NOTIFY), &gamepb.OnlineCountNotify{
				Online: int32(g.sessions.Count()),
			})
		}
	}
}

// saveSession 保存单个会话的玩家数据（同步小超时，供下线/停机路径使用）。
func (g *Game) saveSession(s *Session, cause string) {
	if !s.dirty.CompareAndSwap(true, false) {
		return
	}
	snap := s.PlayerSnapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.players.Save(ctx, &snap); err != nil {
		s.dirty.Store(true)
		logger.L.Warnf("保存玩家 %d 失败（%s）: %v", s.UID, cause, err)
	}
}

// KickAll 向全部连接下发踢下线通知并断开（停机时使用）。
func (g *Game) KickAll(reason string) {
	for _, c := range g.conns.List() {
		_ = c.SendMsg(uint16(gamepb.MsgID_KICK_NOTIFY), &gamepb.KickNotify{Reason: reason})
		c.CloseAfterDrain(reason, 500*time.Millisecond)
	}
}

// BroadcastAnnounce 全服公告（调试端点使用）。
func (g *Game) BroadcastAnnounce(text string) {
	_ = g.conns.BroadcastMsg(uint16(gamepb.MsgID_SERVER_BROADCAST_NOTIFY), &gamepb.ServerBroadcastNotify{
		Text: text,
		Ts:   nowMs(),
	})
}

// Stats 暴露运行统计（调试端点使用）。
type Stats struct {
	ConnCount    int               `json:"connCount"`
	SessionCount int               `json:"sessionCount"`
	RoomCount    int               `json:"roomCount"`
	Routes       map[uint16]string `json:"routes"`
}

// Stats 当前运行统计。
func (g *Game) Stats() Stats {
	return Stats{
		ConnCount:    g.conns.Count(),
		SessionCount: g.sessions.Count(),
		RoomCount:    g.rooms.Count(),
		Routes:       g.router.Types(),
	}
}

// ---------------------------------------------------------------------------
// 路由注册与消息处理辅助
// ---------------------------------------------------------------------------

// regPublic 注册无需登录的消息处理。
func regPublic[T any, PT interface {
	*T
	proto.Message
}](g *Game, id uint16, fn func(conn *server.Conn, req PT) error) {
	var sample PT // 零值即 (*T)(nil)，仅用于路由表登记类型名
	g.router.Handle(id, sample, func(conn protocol.ConnHandler, m *protocol.Message) {
		c, ok := conn.(*server.Conn)
		if !ok {
			return
		}
		req := reflect.New(reflect.TypeFor[T]()).Interface().(PT)
		if err := m.Unmarshal(req); err != nil {
			protocol.SendError(conn, id, errs.BadRequest("请求体解析失败"))
			return
		}
		if err := fn(c, req); err != nil {
			protocol.SendError(conn, id, err)
		}
	})
}

// regAuth 注册需要登录的消息处理：未登录返回 ERR_UNAUTHORIZED。
func (g *Game) regAuth[T any, PT interface {
	*T
	proto.Message
}](id uint16, fn func(s *Session, req PT) error) {
	var sample PT
	g.router.Handle(id, sample, func(conn protocol.ConnHandler, m *protocol.Message) {
		s := g.sessionOf(conn)
		if s == nil {
			protocol.SendError(conn, id, errs.New(gamepb.ErrCode_ERR_UNAUTHORIZED, "请先登录"))
			return
		}
		req := reflect.New(reflect.TypeFor[T]()).Interface().(PT)
		if err := m.Unmarshal(req); err != nil {
			protocol.SendError(conn, id, errs.BadRequest("请求体解析失败"))
			return
		}
		if err := fn(s, req); err != nil {
			protocol.SendError(conn, id, err)
		}
	})
}

// sessionOf 从连接上取出会话。
func (g *Game) sessionOf(conn protocol.ConnHandler) *Session {
	s, _ := conn.Data().(*Session)
	return s
}

// registerRoutes 注册全部业务消息。
func (g *Game) registerRoutes() {
	// 系统
	regPublic(g, uint16(gamepb.MsgID_HEARTBEAT_REQ), g.onHeartbeat)
	// 账号
	regPublic(g, uint16(gamepb.MsgID_GUEST_LOGIN_REQ), g.onGuestLogin)
	regPublic(g, uint16(gamepb.MsgID_ACCOUNT_REGISTER_REQ), g.onAccountRegister)
	regPublic(g, uint16(gamepb.MsgID_ACCOUNT_LOGIN_REQ), g.onAccountLogin)
	// 玩家
	g.regAuth(uint16(gamepb.MsgID_GET_PROFILE_REQ), g.onGetProfile)
	g.regAuth(uint16(gamepb.MsgID_RENAME_REQ), g.onRename)
	// 聊天
	g.regAuth(uint16(gamepb.MsgID_WORLD_CHAT_REQ), g.onWorldChat)
	// 房间
	g.regAuth(uint16(gamepb.MsgID_CREATE_ROOM_REQ), g.onCreateRoom)
	g.regAuth(uint16(gamepb.MsgID_JOIN_ROOM_REQ), g.onJoinRoom)
	g.regAuth(uint16(gamepb.MsgID_LEAVE_ROOM_REQ), g.onLeaveRoom)
	g.regAuth(uint16(gamepb.MsgID_ROOM_CHAT_REQ), g.onRoomChat)
	g.regAuth(uint16(gamepb.MsgID_RPS_PLAY_REQ), g.onRpsPlay)
}

// onHeartbeat 心跳。
func (g *Game) onHeartbeat(c *server.Conn, req *gamepb.HeartbeatReq) error {
	return c.SendMsg(uint16(gamepb.MsgID_HEARTBEAT_ACK), &gamepb.HeartbeatAck{
		ClientTime: req.ClientTime,
		ServerTime: nowMs(),
	})
}

// ---------------------------------------------------------------------------
// 通用小工具
// ---------------------------------------------------------------------------

func nowMs() int64 { return time.Now().UnixMilli() }

// buildSession 登录成功后的公共流程：顶号处理 + 会话登记 + 绑定连接。
// 登录结果消息由各登录 handler 自行下发（游客/账号/注册的应答不同）。
func (g *Game) buildSession(c *server.Conn, acc *model.Account, p *model.Player) *Session {
	// 同账号旧会话：先落盘其脏数据，再踢下线
	if old := g.sessions.Get(acc.UID); old != nil {
		g.saveSession(old, "顶号保存")
		_ = old.Conn.SendMsg(uint16(gamepb.MsgID_KICK_NOTIFY), &gamepb.KickNotify{Reason: "账号在其他设备登录"})
		old.Conn.CloseAfterDrain("顶号下线", 200*time.Millisecond)
		logger.L.Infof("玩家 %d(%s) 被顶号: 旧连接 #%d", acc.UID, acc.Username, old.Conn.ID())
	}

	s := NewSession(c, acc, p)
	g.sessions.Add(s)
	c.SetData(s)
	g.markOnlineDirty()
	return s
}

// loadOrCreatePlayer 登录时加载玩家数据；不存在则按默认值建档。
// 返回值 isNew 表示本次是否新建。
func (g *Game) loadOrCreatePlayer(ctx context.Context, acc *model.Account, now time.Time) (p *model.Player, isNew bool, err error) {
	p, err = g.players.FindByUID(ctx, acc.UID)
	switch {
	case err == nil:
		p.LastLoginAt = now
		return p, false, nil
	case errors.Is(err, store.ErrNotFound):
		p = &model.Player{
			UID:         acc.UID,
			Nickname:    fmt.Sprintf("玩家%d", acc.UID),
			Level:       1,
			Gold:        1000,
			Diamond:     10,
			CreatedAt:   now,
			LastLoginAt: now,
		}
		if err := g.players.Create(ctx, p); err != nil {
			return nil, false, err
		}
		return p, true, nil
	default:
		return nil, false, err
	}
}
