package game

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"servergame/internal/config"
	"servergame/internal/errs"
	"servergame/internal/pkg/logger"
	"servergame/internal/protocol"
	"servergame/internal/server"
	gamepb "servergame/pb/game/v1"
)

// ---------------------------------------------------------------------------
// 测试基建
// ---------------------------------------------------------------------------

// fakeConn 测试用连接：记录发送的消息，读协程永久阻塞。
type fakeConn struct {
	*server.Conn
	sent chan *protocol.Message
}

func newFakeConn(g *Game) *fakeConn {
	fc := &fakeConn{sent: make(chan *protocol.Message, 128)}
	readBlock := make(chan struct{})
	fc.Conn = server.StartConn(server.ConnOpts{
		Kind: server.KindTCP,
		Addr: "test-conn",
		Read: func() (*protocol.Message, error) {
			<-readBlock
			return nil, errors.New("test closed")
		},
		Write: func(m *protocol.Message) error {
			fc.sent <- m
			return nil
		},
		RawClose:     func() error { close(readBlock); return nil },
		Router:       g.Router(),
		OnClose:      g.ConnConfig().OnClose, // 复用 onConnClosed，保证掉线流程生效
		SendChanSize: 128,
	})
	return fc
}

// wait 等待连接发出指定 ID 的消息（跳过其他消息），超时返回 nil。
func (fc *fakeConn) wait(t *testing.T, want gamepb.MsgID) *protocol.Message {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case m := <-fc.sent:
			if gamepb.MsgID(m.ID) == want {
				return m
			}
			// 其他消息（如在线人数推送）直接忽略
		case <-deadline:
			t.Fatalf("等待消息 %v 超时", want)
			return nil
		}
	}
}

// newTestGame 构造带内存存储的游戏实例。
func newTestGame(t *testing.T) (*Game, *fakeAccounts, *fakePlayers) {
	t.Helper()
	logger.InitForTest()
	cfg := &config.Config{}
	cfg.App.Name = "TestGame"
	cfg.App.Version = "test"
	cfg.App.SendChanSize = 128
	cfg.App.MaxPacketBytes = 65536
	cfg.Mongo.TimeoutSeconds = 1
	cfg.Game.IdleTimeoutSeconds = 60
	cfg.Game.AutosaveSeconds = 3600
	cfg.Game.ChatIntervalMs = 1000
	cfg.Game.MaxContentLength = 200
	cfg.Game.MaxRooms = 10
	cfg.Game.RewardWinExp = 50
	cfg.Game.RewardWinGold = 100
	cfg.Game.RewardLoseExp = 10
	cfg.Game.RewardLoseGold = 10
	cfg.Game.RewardDrawExp = 25
	cfg.Game.RewardDrawGold = 30

	accounts := newFakeAccounts()
	players := newFakePlayers()
	g := New(cfg, accounts, players)
	t.Cleanup(func() { g.Stop() })
	return g, accounts, players
}

// send 把 pb 请求直接塞进路由（模拟已解码的网络消息）。
// 注意必须传入内嵌的 *server.Conn，与生产环境一致。
func send(t *testing.T, g *Game, conn *fakeConn, id gamepb.MsgID, req proto.Message) {
	t.Helper()
	m, err := protocol.NewMessage(uint16(id), req)
	if err != nil {
		t.Fatalf("构造消息 %v: %v", id, err)
	}
	if err := g.Router().Dispatch(conn.Conn, m); err != nil {
		t.Fatalf("分发消息 %v: %v", id, err)
	}
}

// decode 把收到的消息反序列化为指定类型。
func decode[T any, PT interface {
	*T
	proto.Message
}](t *testing.T, m *protocol.Message) PT {
	t.Helper()
	req := PT(new(T))
	if err := m.Unmarshal(req); err != nil {
		t.Fatalf("反序列化 %d: %v", m.ID, err)
	}
	return req
}

// loginGuest 游客登录并返回会话绑定的连接。
func loginGuest(t *testing.T, g *Game, device string) *fakeConn {
	t.Helper()
	conn := newFakeConn(g)
	g.OnConnOpen(conn.Conn)
	send(t, g, conn, gamepb.MsgID_GUEST_LOGIN_REQ, &gamepb.GuestLoginReq{DeviceId: device})
	res := decode[gamepb.LoginRes](t, conn.wait(t, gamepb.MsgID_GUEST_LOGIN_RES))
	if res.Profile == nil || res.Profile.Uid == 0 {
		t.Fatalf("登录应答缺少资料: %+v", res)
	}
	return conn
}

// drain 清空连接中未读的消息，避免影响后续断言。
func drain(conn *fakeConn) {
	for {
		select {
		case <-conn.sent:
		default:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// 测试用例
// ---------------------------------------------------------------------------

// TestGuestLoginFirstIsNew 游客首次登录 IsNew=true 且有默认资产。
func TestGuestLoginFirstIsNew(t *testing.T) {
	g, _, _ := newTestGame(t)

	conn := newFakeConn(g)
	g.OnConnOpen(conn.Conn)
	send(t, g, conn, gamepb.MsgID_GUEST_LOGIN_REQ, &gamepb.GuestLoginReq{DeviceId: "device-abcdef-02"})
	res := decode[gamepb.LoginRes](t, conn.wait(t, gamepb.MsgID_GUEST_LOGIN_RES))
	if !res.IsNew {
		t.Fatal("首次登录应 IsNew=true")
	}
	p := res.Profile
	if p.Level != 1 || p.Gold != 1000 || p.Diamond != 10 {
		t.Fatalf("默认资料不符: level=%d gold=%d diamond=%d", p.Level, p.Gold, p.Diamond)
	}
	if p.Nickname == "" {
		t.Fatal("应自动生成昵称")
	}

	// 同设备二次登录：同一账号，IsNew=false
	conn2 := newFakeConn(g)
	g.OnConnOpen(conn2.Conn)
	send(t, g, conn2, gamepb.MsgID_GUEST_LOGIN_REQ, &gamepb.GuestLoginReq{DeviceId: "device-abcdef-02"})
	res2 := decode[gamepb.LoginRes](t, conn2.wait(t, gamepb.MsgID_GUEST_LOGIN_RES))
	if res2.IsNew {
		t.Fatal("二次登录应 IsNew=false")
	}
	if res2.Profile.Uid != p.Uid {
		t.Fatalf("应为同一账号: %d vs %d", res2.Profile.Uid, p.Uid)
	}
}

// TestGuestLoginValidation 设备号非法时返回错误。
func TestGuestLoginValidation(t *testing.T) {
	g, _, _ := newTestGame(t)
	conn := newFakeConn(g)
	g.OnConnOpen(conn.Conn)

	send(t, g, conn, gamepb.MsgID_GUEST_LOGIN_REQ, &gamepb.GuestLoginReq{DeviceId: "short"})
	notify := decode[gamepb.ErrorNotify](t, conn.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify.Code != gamepb.ErrCode_ERR_BAD_REQUEST {
		t.Fatalf("应返回 BAD_REQUEST, got %v", notify.Code)
	}
}

// TestUnauthorized 未登录调用需登录的接口返回 ERR_UNAUTHORIZED。
func TestUnauthorized(t *testing.T) {
	g, _, _ := newTestGame(t)
	conn := newFakeConn(g)
	g.OnConnOpen(conn.Conn)

	send(t, g, conn, gamepb.MsgID_GET_PROFILE_REQ, &gamepb.Empty{})
	notify := decode[gamepb.ErrorNotify](t, conn.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify.Code != gamepb.ErrCode_ERR_UNAUTHORIZED {
		t.Fatalf("应返回 UNAUTHORIZED, got %v", notify.Code)
	}
}

// TestAccountRegisterAndLogin 注册、重名、登录、密码错误。
func TestAccountRegisterAndLogin(t *testing.T) {
	g, _, _ := newTestGame(t)

	// 注册（即登录）
	reg := newFakeConn(g)
	g.OnConnOpen(reg.Conn)
	send(t, g, reg, gamepb.MsgID_ACCOUNT_REGISTER_REQ, &gamepb.AccountRegisterReq{Username: "alice", Password: "secret1"})
	regRes := decode[gamepb.AccountRegisterRes](t, reg.wait(t, gamepb.MsgID_ACCOUNT_REGISTER_RES))
	if regRes.Username != "alice" || regRes.Profile == nil {
		t.Fatalf("注册应答异常: %+v", regRes)
	}

	// 重复注册
	dup := newFakeConn(g)
	g.OnConnOpen(dup.Conn)
	send(t, g, dup, gamepb.MsgID_ACCOUNT_REGISTER_REQ, &gamepb.AccountRegisterReq{Username: "alice", Password: "secret1"})
	notify := decode[gamepb.ErrorNotify](t, dup.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify.Code != gamepb.ErrCode_ERR_ALREADY_EXISTS {
		t.Fatalf("重复注册应返回 ALREADY_EXISTS, got %v", notify.Code)
	}

	// 正常登录
	login := newFakeConn(g)
	g.OnConnOpen(login.Conn)
	send(t, g, login, gamepb.MsgID_ACCOUNT_LOGIN_REQ, &gamepb.AccountLoginReq{Username: "alice", Password: "secret1"})
	loginRes := decode[gamepb.LoginRes](t, login.wait(t, gamepb.MsgID_ACCOUNT_LOGIN_RES))
	if loginRes.Profile.Uid != regRes.Uid {
		t.Fatalf("登录 UID 不一致: %d vs %d", loginRes.Profile.Uid, regRes.Uid)
	}

	// 密码错误
	bad := newFakeConn(g)
	g.OnConnOpen(bad.Conn)
	send(t, g, bad, gamepb.MsgID_ACCOUNT_LOGIN_REQ, &gamepb.AccountLoginReq{Username: "alice", Password: "wrong!"})
	notify2 := decode[gamepb.ErrorNotify](t, bad.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify2.Code != gamepb.ErrCode_ERR_PASSWORD_WRONG {
		t.Fatalf("应返回 PASSWORD_WRONG, got %v", notify2.Code)
	}
}

// TestRename 昵称修改与占用校验。
func TestRename(t *testing.T) {
	g, _, _ := newTestGame(t)
	c1 := loginGuest(t, g, "device-rename-a")
	c2 := loginGuest(t, g, "device-rename-b")

	send(t, g, c1, gamepb.MsgID_RENAME_REQ, &gamepb.RenameReq{Nickname: "小明"})
	res := decode[gamepb.RenameRes](t, c1.wait(t, gamepb.MsgID_RENAME_RES))
	if res.Nickname != "小明" {
		t.Fatalf("昵称应已修改: %q", res.Nickname)
	}

	// 与小明同名 → 冲突
	send(t, g, c2, gamepb.MsgID_RENAME_REQ, &gamepb.RenameReq{Nickname: "小明"})
	notify := decode[gamepb.ErrorNotify](t, c2.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify.Code != gamepb.ErrCode_ERR_NAME_TAKEN {
		t.Fatalf("应返回 NAME_TAKEN, got %v", notify.Code)
	}

	// 过短昵称 → NAME_INVALID
	send(t, g, c2, gamepb.MsgID_RENAME_REQ, &gamepb.RenameReq{Nickname: "A"})
	notify2 := decode[gamepb.ErrorNotify](t, c2.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify2.Code != gamepb.ErrCode_ERR_NAME_INVALID {
		t.Fatalf("应返回 NAME_INVALID, got %v", notify2.Code)
	}
}

// TestWorldChat 世界聊天广播与限流。
func TestWorldChat(t *testing.T) {
	g, _, _ := newTestGame(t)
	c1 := loginGuest(t, g, "device-chat-a")
	c2 := loginGuest(t, g, "device-chat-b")
	drain(c1)
	drain(c2)

	send(t, g, c1, gamepb.MsgID_WORLD_CHAT_REQ, &gamepb.WorldChatReq{Content: "大家好"})
	_ = decode[gamepb.WorldChatRes](t, c1.wait(t, gamepb.MsgID_WORLD_CHAT_RES))
	chat := decode[gamepb.WorldChatNotify](t, c1.wait(t, gamepb.MsgID_WORLD_CHAT_NOTIFY))
	if chat.Content != "大家好" {
		t.Fatalf("广播内容不符: %q", chat.Content)
	}
	// 另一玩家也能收到
	chat2 := decode[gamepb.WorldChatNotify](t, c2.wait(t, gamepb.MsgID_WORLD_CHAT_NOTIFY))
	if chat2.Content != "大家好" {
		t.Fatalf("玩家2广播内容不符: %q", chat2.Content)
	}

	// 立刻再发：限流
	send(t, g, c1, gamepb.MsgID_WORLD_CHAT_REQ, &gamepb.WorldChatReq{Content: "再来一条"})
	notify := decode[gamepb.ErrorNotify](t, c1.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify.Code != gamepb.ErrCode_ERR_RATE_LIMITED {
		t.Fatalf("应返回 RATE_LIMITED, got %v", notify.Code)
	}
}

// TestRoomAndRPS 房间全流程：创建/加入/出招/结算/离开。
func TestRoomAndRPS(t *testing.T) {
	g, _, _ := newTestGame(t)
	c1 := loginGuest(t, g, "device-room-a")
	c2 := loginGuest(t, g, "device-room-b")
	drain(c1)
	drain(c2)

	// 创建房间
	send(t, g, c1, gamepb.MsgID_CREATE_ROOM_REQ, &gamepb.Empty{})
	create := decode[gamepb.CreateRoomRes](t, c1.wait(t, gamepb.MsgID_CREATE_ROOM_RES))
	if create.RoomId < 100000 || create.RoomId > 999999 {
		t.Fatalf("房间号应为 6 位: %d", create.RoomId)
	}

	// 重复创建 → IN_ROOM
	send(t, g, c1, gamepb.MsgID_CREATE_ROOM_REQ, &gamepb.Empty{})
	notify := decode[gamepb.ErrorNotify](t, c1.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify.Code != gamepb.ErrCode_ERR_IN_ROOM {
		t.Fatalf("重复建房应返回 IN_ROOM, got %v", notify.Code)
	}

	// c2 加入 → c1 收到 START 事件，c2 收到 JOIN 结果
	send(t, g, c2, gamepb.MsgID_JOIN_ROOM_REQ, &gamepb.JoinRoomReq{RoomId: create.RoomId})
	joinRes := decode[gamepb.JoinRoomRes](t, c2.wait(t, gamepb.MsgID_JOIN_ROOM_RES))
	if len(joinRes.Members) != 2 {
		t.Fatalf("加入后应有 2 名成员, got %d", len(joinRes.Members))
	}
	startEvent := decode[gamepb.RoomEventNotify](t, c1.wait(t, gamepb.MsgID_ROOM_EVENT_NOTIFY))
	if startEvent.Type != gamepb.RoomEventNotify_START {
		t.Fatalf("应收到 START 事件, got %v", startEvent.Type)
	}

	// 人数不足时（临时用 c3 直接触招）→ NOT_IN_ROOM
	c3 := loginGuest(t, g, "device-room-c")
	drain(c3)
	send(t, g, c3, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_ROCK})
	notify3 := decode[gamepb.ErrorNotify](t, c3.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notify3.Code != gamepb.ErrCode_ERR_NOT_IN_ROOM {
		t.Fatalf("应返回 NOT_IN_ROOM, got %v", notify3.Code)
	}

	// 猜拳：c1 出石头，c2 出剪刀 → c1 胜
	send(t, g, c1, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_ROCK})
	_ = decode[gamepb.RpsPlayRes](t, c1.wait(t, gamepb.MsgID_RPS_PLAY_RES)) // 对方未出招，仅确认

	send(t, g, c2, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_SCISSORS})
	_ = decode[gamepb.RpsPlayRes](t, c2.wait(t, gamepb.MsgID_RPS_PLAY_RES))

	round1 := decode[gamepb.RpsRoundNotify](t, c1.wait(t, gamepb.MsgID_RPS_ROUND_NOTIFY))
	if round1.Outcome != gamepb.RpsRoundNotify_WIN || round1.MyMove != gamepb.RpsPlayReq_ROCK || round1.OppMove != gamepb.RpsPlayReq_SCISSORS {
		t.Fatalf("c1 应判胜: %+v", round1)
	}
	if round1.Profile.WinCount != 1 || round1.Profile.Gold != 1100 || round1.Profile.Exp != 50 {
		t.Fatalf("胜者奖励不符: %+v", round1.Profile)
	}
	round2 := decode[gamepb.RpsRoundNotify](t, c2.wait(t, gamepb.MsgID_RPS_ROUND_NOTIFY))
	if round2.Outcome != gamepb.RpsRoundNotify_LOSE {
		t.Fatalf("c2 应判负: %+v", round2)
	}
	if round2.Profile.LoseCount != 1 || round2.Profile.Gold != 1010 {
		t.Fatalf("败者奖励不符: %+v", round2.Profile)
	}

	// 平局：双方都出布
	send(t, g, c1, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_PAPER})
	send(t, g, c2, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_PAPER})
	draw1 := decode[gamepb.RpsRoundNotify](t, c1.wait(t, gamepb.MsgID_RPS_ROUND_NOTIFY))
	draw2 := decode[gamepb.RpsRoundNotify](t, c2.wait(t, gamepb.MsgID_RPS_ROUND_NOTIFY))
	if draw1.Outcome != gamepb.RpsRoundNotify_DRAW || draw2.Outcome != gamepb.RpsRoundNotify_DRAW {
		t.Fatalf("应判平局: %v / %v", draw1.Outcome, draw2.Outcome)
	}

	// 重复出招（未清空前）应报错——上面结算已清空，这里验证对方已出招时不能重复出
	send(t, g, c1, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_ROCK})
	_ = decode[gamepb.RpsPlayRes](t, c1.wait(t, gamepb.MsgID_RPS_PLAY_RES))
	send(t, g, c1, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_PAPER})
	notifyDup := decode[gamepb.ErrorNotify](t, c1.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notifyDup.Code != gamepb.ErrCode_ERR_GAME_STATE {
		t.Fatalf("重复出招应返回 GAME_STATE, got %v", notifyDup.Code)
	}
	// c2 出招完成本局，避免影响后续用例
	send(t, g, c2, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_SCISSORS})
	_ = decode[gamepb.RpsRoundNotify](t, c1.wait(t, gamepb.MsgID_RPS_ROUND_NOTIFY))
	_ = decode[gamepb.RpsRoundNotify](t, c2.wait(t, gamepb.MsgID_RPS_ROUND_NOTIFY))

	// 房间聊天
	send(t, g, c1, gamepb.MsgID_ROOM_CHAT_REQ, &gamepb.RoomChatReq{Content: "打得不错"})
	roomChat := decode[gamepb.RoomChatNotify](t, c2.wait(t, gamepb.MsgID_ROOM_CHAT_NOTIFY))
	if roomChat.Content != "打得不错" {
		t.Fatalf("房间聊天不符: %q", roomChat.Content)
	}

	// c2 离开 → c1 收到 LEAVE 事件
	send(t, g, c2, gamepb.MsgID_LEAVE_ROOM_REQ, &gamepb.Empty{})
	leaveRes := decode[gamepb.LeaveRoomRes](t, c2.wait(t, gamepb.MsgID_LEAVE_ROOM_RES))
	if leaveRes.RoomId != create.RoomId {
		t.Fatalf("离开房间号不符: %d", leaveRes.RoomId)
	}
	leaveEvent := decode[gamepb.RoomEventNotify](t, c1.wait(t, gamepb.MsgID_ROOM_EVENT_NOTIFY))
	if leaveEvent.Type != gamepb.RoomEventNotify_LEAVE || len(leaveEvent.Members) != 1 {
		t.Fatalf("离开事件不符: %+v", leaveEvent)
	}

	// c1 此时可以正常出招校验：不在房间 → c2 已不在；c1 还在（1人）→ 出招报 GAME_STATE
	send(t, g, c1, gamepb.MsgID_RPS_PLAY_REQ, &gamepb.RpsPlayReq{Move: gamepb.RpsPlayReq_ROCK})
	notifySingle := decode[gamepb.ErrorNotify](t, c1.wait(t, gamepb.MsgID_ERROR_NOTIFY))
	if notifySingle.Code != gamepb.ErrCode_ERR_GAME_STATE {
		t.Fatalf("单人在房出招应返回 GAME_STATE, got %v", notifySingle.Code)
	}
}

// TestDisconnectLeavesRoom 掉线自动退房并通知对手。
func TestDisconnectLeavesRoom(t *testing.T) {
	g, _, _ := newTestGame(t)
	c1 := loginGuest(t, g, "device-disc-a")
	c2 := loginGuest(t, g, "device-disc-b")
	drain(c1)
	drain(c2)

	send(t, g, c1, gamepb.MsgID_CREATE_ROOM_REQ, &gamepb.Empty{})
	create := decode[gamepb.CreateRoomRes](t, c1.wait(t, gamepb.MsgID_CREATE_ROOM_RES))
	send(t, g, c2, gamepb.MsgID_JOIN_ROOM_REQ, &gamepb.JoinRoomReq{RoomId: create.RoomId})
	_ = decode[gamepb.JoinRoomRes](t, c2.wait(t, gamepb.MsgID_JOIN_ROOM_RES))
	_ = decode[gamepb.RoomEventNotify](t, c1.wait(t, gamepb.MsgID_ROOM_EVENT_NOTIFY))

	// c2 连接断开（模拟掉线）→ onConnClosed → 自动退房
	c2.Conn.Close("模拟掉线")
	leaveEvent := decode[gamepb.RoomEventNotify](t, c1.wait(t, gamepb.MsgID_ROOM_EVENT_NOTIFY))
	if leaveEvent.Type != gamepb.RoomEventNotify_LEAVE {
		t.Fatalf("对手掉线应收到 LEAVE 事件, got %v", leaveEvent.Type)
	}
	if g.sessions.Count() != 1 {
		t.Fatalf("掉线后会话数应为 1, got %d", g.sessions.Count())
	}
}

// TestKickOnDuplicateLogin 同账号二次登录会顶掉旧连接。
func TestKickOnDuplicateLogin(t *testing.T) {
	g, _, _ := newTestGame(t)

	c1 := loginGuest(t, g, "device-kick-a")
	drain(c1)

	// 同一设备再登录 → 新会话；旧连接被踢
	c2 := loginGuest(t, g, "device-kick-a")
	kick := decode[gamepb.KickNotify](t, c1.wait(t, gamepb.MsgID_KICK_NOTIFY))
	if kick.Reason == "" {
		t.Fatal("被踢原因不应为空")
	}
	if g.sessions.Count() != 1 {
		t.Fatalf("顶号后会话数应为 1, got %d", g.sessions.Count())
	}
	_ = c2
}

// TestHeartbeat 心跳请求返回应答并回传时间戳。
func TestHeartbeat(t *testing.T) {
	g, _, _ := newTestGame(t)
	conn := newFakeConn(g)
	g.OnConnOpen(conn.Conn)
	drain(conn)

	send(t, g, conn, gamepb.MsgID_HEARTBEAT_REQ, &gamepb.HeartbeatReq{ClientTime: 12345})
	ack := decode[gamepb.HeartbeatAck](t, conn.wait(t, gamepb.MsgID_HEARTBEAT_ACK))
	if ack.ClientTime != 12345 || ack.ServerTime == 0 {
		t.Fatalf("心跳应答不符: %+v", ack)
	}
}

// TestJudgeTable 猜拳规则表全覆盖。
func TestJudgeTable(t *testing.T) {
	cases := []struct {
		a, b gamepb.RpsPlayReq_Move
		want gamepb.RpsRoundNotify_Outcome
	}{
		{gamepb.RpsPlayReq_ROCK, gamepb.RpsPlayReq_SCISSORS, gamepb.RpsRoundNotify_WIN},
		{gamepb.RpsPlayReq_SCISSORS, gamepb.RpsPlayReq_ROCK, gamepb.RpsRoundNotify_LOSE},
		{gamepb.RpsPlayReq_PAPER, gamepb.RpsPlayReq_ROCK, gamepb.RpsRoundNotify_WIN},
		{gamepb.RpsPlayReq_ROCK, gamepb.RpsPlayReq_PAPER, gamepb.RpsRoundNotify_LOSE},
		{gamepb.RpsPlayReq_SCISSORS, gamepb.RpsPlayReq_PAPER, gamepb.RpsRoundNotify_WIN},
		{gamepb.RpsPlayReq_PAPER, gamepb.RpsPlayReq_SCISSORS, gamepb.RpsRoundNotify_LOSE},
		{gamepb.RpsPlayReq_ROCK, gamepb.RpsPlayReq_ROCK, gamepb.RpsRoundNotify_DRAW},
		{gamepb.RpsPlayReq_PAPER, gamepb.RpsPlayReq_PAPER, gamepb.RpsRoundNotify_DRAW},
		{gamepb.RpsPlayReq_SCISSORS, gamepb.RpsPlayReq_SCISSORS, gamepb.RpsRoundNotify_DRAW},
	}
	for _, c := range cases {
		if got := judge(c.a, c.b); got != c.want {
			t.Fatalf("judge(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// TestLevelUp 连胜触发升级。
func TestLevelUp(t *testing.T) {
	g, _, _ := newTestGame(t)
	conn := loginGuest(t, g, "device-levelup")
	drain(conn)

	// 直接构造会话验证奖励与升级逻辑
	s := g.sessionOf(conn)
	if s == nil {
		t.Fatal("登录后应有会话")
	}
	// 默认 0 经验 → 赢 2 局 = 100 经验 → Lv.3? 0/100+1=1 → +50 = Lv1 → +50=100 → Lv2
	profile, leveled1 := s.applyReward(50, 100, 1, 0, 0)
	if leveled1 || profile.Level != 1 {
		t.Fatalf("第一局不应升级: level=%d up=%v", profile.Level, leveled1)
	}
	profile, leveled2 := s.applyReward(50, 100, 1, 0, 0)
	if !leveled2 || profile.Level != 2 {
		t.Fatalf("第二局应升到 Lv.2: level=%d up=%v", profile.Level, leveled2)
	}
	if profile.WinCount != 2 || profile.Gold != 1200 {
		t.Fatalf("战绩/金币不符: win=%d gold=%d", profile.WinCount, profile.Gold)
	}
}

// TestErrsFrom 非业务错误统一包装。
func TestErrsFrom(t *testing.T) {
	e := errs.From(fmt.Errorf("db down"))
	if e.Code != gamepb.ErrCode_ERR_INTERNAL {
		t.Fatalf("非业务错误应映射为 INTERNAL, got %v", e.Code)
	}
}
