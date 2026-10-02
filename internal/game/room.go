package game

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"servergame/internal/errs"
	"servergame/internal/pkg/randx"
	gamepb "servergame/pb/game/v1"
)

// 房间容量：内置猜拳游戏为双人对战。
const roomCapacity = 2

// Room 房间：最多 roomCapacity 名成员的猜拳对局。
type Room struct {
	ID int64

	mu      sync.Mutex
	members []*Session
	moves   map[int64]gamepb.RpsPlayReq_Move // 本局已出的招（uid -> move）
}

// memberPbOf 从会话列表构造成员 pb（会话昵称读取自带锁保护）。
func memberPbOf(members []*Session) []*gamepb.RoomMember {
	out := make([]*gamepb.RoomMember, 0, len(members))
	for _, m := range members {
		out = append(out, &gamepb.RoomMember{Uid: m.UID, Nickname: m.Nickname()})
	}
	return out
}

// sendToAll 向成员快照逐一推送（发送不阻塞，可在任意协程调用）。
func sendToAll(members []*Session, id uint16, msg proto.Message) {
	for _, m := range members {
		_ = m.Conn.SendMsg(id, msg)
	}
}

// ---------------------------------------------------------------------------
// RoomManager
// ---------------------------------------------------------------------------

// RoomManager 房间容器：房间 ID -> 房间，及 uid -> 房间 ID 的反向索引。
// 锁顺序：先 m.mu 再 r.mu。
type RoomManager struct {
	mu       sync.Mutex
	rooms    map[int64]*Room
	byUID    map[int64]int64
	maxRooms int
}

// NewRoomManager 创建房间管理器。
func NewRoomManager(maxRooms int) *RoomManager {
	return &RoomManager{
		rooms:    make(map[int64]*Room),
		byUID:    make(map[int64]int64),
		maxRooms: maxRooms,
	}
}

// Count 当前房间数。
func (m *RoomManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}

// Create 创建房间并让创建者入房。
func (m *RoomManager) Create(s *Session) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byUID[s.UID]; ok {
		return 0, errs.New(gamepb.ErrCode_ERR_IN_ROOM, "你已在房间中，请先退出")
	}
	if len(m.rooms) >= m.maxRooms {
		return 0, errs.GameState("房间数已达上限（%d）", m.maxRooms)
	}

	// 随机 6 位房间号，撞号时重试
	var id int64
	for retry := 0; ; retry++ {
		id = int64(randx.RandomInt(100000, 999999))
		if _, exists := m.rooms[id]; !exists {
			break
		}
		if retry > 64 {
			return 0, errs.GameState("房间号分配失败，请重试")
		}
	}

	r := &Room{ID: id, moves: make(map[int64]gamepb.RpsPlayReq_Move)}
	r.members = append(r.members, s)
	m.rooms[id] = r
	m.byUID[s.UID] = id
	s.SetRoomID(id)
	return id, nil
}

// Join 加入房间；返回加入后的成员列表。
func (m *RoomManager) Join(s *Session, roomID int64) ([]*gamepb.RoomMember, error) {
	m.mu.Lock()
	if _, ok := m.byUID[s.UID]; ok {
		m.mu.Unlock()
		return nil, errs.New(gamepb.ErrCode_ERR_IN_ROOM, "你已在房间中，请先退出")
	}
	r, ok := m.rooms[roomID]
	if !ok {
		m.mu.Unlock()
		return nil, errs.New(gamepb.ErrCode_ERR_ROOM_NOT_FOUND, "房间不存在或已解散")
	}

	r.mu.Lock()
	if len(r.members) >= roomCapacity {
		r.mu.Unlock()
		m.mu.Unlock()
		return nil, errs.New(gamepb.ErrCode_ERR_ROOM_FULL, "房间已满")
	}
	r.members = append(r.members, s)
	m.byUID[s.UID] = roomID
	s.SetRoomID(roomID)
	members := r.members
	memberPb := memberPbOf(members)
	started := len(members) == roomCapacity
	r.mu.Unlock()
	m.mu.Unlock()

	// 锁外广播：人齐开局，否则成员加入事件
	event := &gamepb.RoomEventNotify{
		RoomId:  roomID,
		Type:    gamepb.RoomEventNotify_JOIN,
		Text:    s.Nickname() + " 加入了房间",
		Members: memberPb,
		Ts:      nowMs(),
	}
	if started {
		event.Type = gamepb.RoomEventNotify_START
		event.Text = "双方就位，可以出招了！"
	}
	sendToAll(members, uint16(gamepb.MsgID_ROOM_EVENT_NOTIFY), event)
	return memberPb, nil
}

// Leave 主动离开房间。返回房间 ID 与是否确实在房间中。
func (m *RoomManager) Leave(s *Session) (int64, bool) {
	m.mu.Lock()
	roomID, ok := m.byUID[s.UID]
	if !ok {
		m.mu.Unlock()
		return 0, false
	}
	r := m.rooms[roomID]

	r.mu.Lock()
	remaining := m.removeMemberLocked(r, s)
	memberPb := memberPbOf(remaining)
	r.mu.Unlock()
	m.mu.Unlock()

	if len(remaining) > 0 {
		sendToAll(remaining, uint16(gamepb.MsgID_ROOM_EVENT_NOTIFY), &gamepb.RoomEventNotify{
			RoomId:  roomID,
			Type:    gamepb.RoomEventNotify_LEAVE,
			Text:    s.Nickname() + " 离开了房间",
			Members: memberPb,
			Ts:      nowMs(),
		})
	}
	return roomID, true
}

// RemoveMember 玩家掉线时调用：强制退房并通知剩余成员。
func (m *RoomManager) RemoveMember(s *Session) {
	m.Leave(s)
}

// removeMemberLocked 从房间移除成员并清理索引；房间为空则销毁。
// 调用方需同时持有 m.mu 与 r.mu；返回剩余成员。
func (m *RoomManager) removeMemberLocked(r *Room, s *Session) []*Session {
	kept := make([]*Session, 0, len(r.members))
	for _, mem := range r.members {
		if mem.UID != s.UID {
			kept = append(kept, mem)
		}
	}
	r.members = kept
	delete(r.moves, s.UID)
	delete(m.byUID, s.UID)
	s.SetRoomID(0)

	if len(r.members) == 0 {
		delete(m.rooms, r.ID)
	}
	return r.members
}

// RoomChat 房间内发言。
func (m *RoomManager) RoomChat(s *Session, content string) error {
	m.mu.Lock()
	roomID, ok := m.byUID[s.UID]
	if !ok {
		m.mu.Unlock()
		return errs.New(gamepb.ErrCode_ERR_NOT_IN_ROOM, "你不在房间中")
	}
	r := m.rooms[roomID]
	r.mu.Lock()
	members := r.members
	r.mu.Unlock()
	m.mu.Unlock()

	sendToAll(members, uint16(gamepb.MsgID_ROOM_CHAT_NOTIFY), &gamepb.RoomChatNotify{
		Uid:      s.UID,
		Nickname: s.Nickname(),
		Content:  content,
		Ts:       nowMs(),
	})
	return nil
}

// Play 猜拳出招。双方都出招后立即结算，并向双方推送各自视角的结果。
func (m *RoomManager) Play(g *Game, s *Session, move gamepb.RpsPlayReq_Move) error {
	if move < gamepb.RpsPlayReq_ROCK || move > gamepb.RpsPlayReq_SCISSORS {
		return errs.BadRequest("出招无效")
	}

	m.mu.Lock()
	roomID, ok := m.byUID[s.UID]
	if !ok {
		m.mu.Unlock()
		return errs.New(gamepb.ErrCode_ERR_NOT_IN_ROOM, "你不在房间中")
	}
	r := m.rooms[roomID]

	defer m.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.members) < roomCapacity {
		return errs.GameState("等待其他玩家加入后才能出招")
	}
	if _, dup := r.moves[s.UID]; dup {
		return errs.GameState("本局已出招，等待对方")
	}
	r.moves[s.UID] = move

	// 对方尚未出招：仅确认
	if len(r.moves) < roomCapacity {
		_ = s.Conn.SendMsg(uint16(gamepb.MsgID_RPS_PLAY_RES), &gamepb.RpsPlayRes{Accepted: true})
		return nil
	}

	// 双方齐了：结算本局并清空出招，准备下一局
	var moves [roomCapacity]gamepb.RpsPlayReq_Move
	for i, mem := range r.members {
		moves[i] = r.moves[mem.UID]
	}
	r.moves = make(map[int64]gamepb.RpsPlayReq_Move)

	_ = s.Conn.SendMsg(uint16(gamepb.MsgID_RPS_PLAY_RES), &gamepb.RpsPlayRes{Accepted: true})

	outcome0 := judge(moves[0], moves[1])
	for i, mem := range r.members {
		out := gamepb.RpsRoundNotify_DRAW
		switch {
		case outcome0 == gamepb.RpsRoundNotify_WIN && i == 0,
			outcome0 == gamepb.RpsRoundNotify_LOSE && i == 1:
			out = gamepb.RpsRoundNotify_WIN
		case outcome0 != gamepb.RpsRoundNotify_DRAW:
			out = gamepb.RpsRoundNotify_LOSE
		}

		profile, leveledUp := applyOutcome(g, mem, out)
		text := ""
		if leveledUp {
			text = "恭喜升级！"
		}
		_ = mem.Conn.SendMsg(uint16(gamepb.MsgID_RPS_ROUND_NOTIFY), &gamepb.RpsRoundNotify{
			MyMove:   moves[i],
			OppMove:  moves[1-i],
			Outcome:  out,
			Profile:  profile,
			ExtraMsg: text,
		})
	}
	return nil
}

// judge 以 a 的视角判定胜负。
func judge(a, b gamepb.RpsPlayReq_Move) gamepb.RpsRoundNotify_Outcome {
	if a == b {
		return gamepb.RpsRoundNotify_DRAW
	}
	aWins := (a == gamepb.RpsPlayReq_ROCK && b == gamepb.RpsPlayReq_SCISSORS) ||
		(a == gamepb.RpsPlayReq_SCISSORS && b == gamepb.RpsPlayReq_PAPER) ||
		(a == gamepb.RpsPlayReq_PAPER && b == gamepb.RpsPlayReq_ROCK)
	if aWins {
		return gamepb.RpsRoundNotify_WIN
	}
	return gamepb.RpsRoundNotify_LOSE
}

// applyOutcome 结算奖励：按配置加经验/金币与战绩。
func applyOutcome(g *Game, s *Session, outcome gamepb.RpsRoundNotify_Outcome) (*pbProfile, bool) {
	cfg := g.cfg.Game
	switch outcome {
	case gamepb.RpsRoundNotify_WIN:
		return s.applyReward(cfg.RewardWinExp, cfg.RewardWinGold, 1, 0, 0)
	case gamepb.RpsRoundNotify_LOSE:
		return s.applyReward(cfg.RewardLoseExp, cfg.RewardLoseGold, 0, 1, 0)
	default:
		return s.applyReward(cfg.RewardDrawExp, cfg.RewardDrawGold, 0, 0, 1)
	}
}

// ShutdownAll 停机时解散全部房间：通知成员后清空。
func (m *RoomManager) ShutdownAll(reason string) {
	m.mu.Lock()
	rooms := make([]*Room, 0, len(m.rooms))
	for _, r := range m.rooms {
		rooms = append(rooms, r)
	}
	m.rooms = make(map[int64]*Room)
	m.byUID = make(map[int64]int64)
	for _, r := range rooms {
		r.mu.Lock()
		for _, mem := range r.members {
			mem.SetRoomID(0)
			_ = mem.Conn.SendMsg(uint16(gamepb.MsgID_ROOM_EVENT_NOTIFY), &gamepb.RoomEventNotify{
				RoomId: r.ID,
				Type:   gamepb.RoomEventNotify_CLOSE,
				Text:   reason,
				Ts:     time.Now().UnixMilli(),
			})
		}
		r.mu.Unlock()
	}
	m.mu.Unlock()
}
