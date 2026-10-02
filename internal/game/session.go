package game

import (
	"sync"
	"sync/atomic"

	"servergame/internal/model"
	"servergame/internal/server"
)

// Session 一次登录会话：连接 + 玩家内存态。
// 玩家数据先改内存（打脏标记），由自动落盘协程与下线时机批量写回 MongoDB。
type Session struct {
	Conn *server.Conn

	UID      int64
	Username string
	IsGuest  bool

	mu     sync.Mutex
	player *model.Player

	roomID     atomic.Int64 // 所在房间 ID，0 表示不在房间
	lastChatAt atomic.Int64 // 上次发言毫秒时间戳（限流）
	dirty      atomic.Bool  // 玩家数据是否有未落盘修改
}

// NewSession 创建会话。
func NewSession(conn *server.Conn, acc *model.Account, p *model.Player) *Session {
	return &Session{
		Conn:     conn,
		UID:      acc.UID,
		Username: acc.Username,
		IsGuest:  acc.IsGuest,
		player:   p,
	}
}

// PlayerSnapshot 返回玩家数据的拷贝（线程安全）。
func (s *Session) PlayerSnapshot() model.Player {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.player
}

// Nickname 玩家昵称（线程安全）。
func (s *Session) Nickname() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.player.Nickname
}

// SetNickname 修改昵称并打脏标记。
func (s *Session) SetNickname(name string) {
	s.mu.Lock()
	s.player.Nickname = name
	s.mu.Unlock()
	s.dirty.Store(true)
}

// RoomID 会话所在房间 ID。
func (s *Session) RoomID() int64 { return s.roomID.Load() }

// SetRoomID 更新所在房间 ID。
func (s *Session) SetRoomID(id int64) { s.roomID.Store(id) }

// ChatAllowed 判断是否满足发言间隔；满足则记录本次时间。
func (s *Session) ChatAllowed(intervalMs int64) bool {
	now := nowMs()
	last := s.lastChatAt.Load()
	if last != 0 && now-last < intervalMs {
		return false
	}
	s.lastChatAt.Store(now)
	return true
}

// applyReward 结算奖励：加经验/金币与战绩，返回升级后的玩家 pb 快照。
// 返回值 leveledUp 表示本次是否升级。
func (s *Session) applyReward(exp, gold int64, win, lose, draw int64) (profile *pbProfile, leveledUp bool) {
	s.mu.Lock()
	p := s.player
	p.Exp += exp
	p.Gold += gold
	p.WinCount += win
	p.LoseCount += lose
	p.DrawCount += draw

	newLevel := int32(p.Exp/levelStepExp + 1)
	if newLevel > maxLevel {
		newLevel = maxLevel
	}
	leveledUp = newLevel > p.Level
	p.Level = newLevel
	snapshot := *p
	s.mu.Unlock()

	s.dirty.Store(true)
	return playerToPb(&snapshot, s.Username), leveledUp
}

// ---------------------------------------------------------------------------
// SessionManager
// ---------------------------------------------------------------------------

// SessionManager 会话注册表：UID 与连接两个维度的索引。
type SessionManager struct {
	mu     sync.RWMutex
	byUID  map[int64]*Session
	byConn map[uint64]*Session
}

// NewSessionManager 创建会话管理器。
func NewSessionManager() *SessionManager {
	return &SessionManager{
		byUID:  make(map[int64]*Session),
		byConn: make(map[uint64]*Session),
	}
}

// Get 按 UID 查会话。
func (m *SessionManager) Get(uid int64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byUID[uid]
}

// GetByConn 按连接查会话。
func (m *SessionManager) GetByConn(connID uint64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byConn[connID]
}

// Add 登记会话；返回被顶掉的旧会话（同 UID 在线时），旧会话由调用方负责踢线。
func (m *SessionManager) Add(s *Session) (old *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old = m.byUID[s.UID]
	m.byUID[s.UID] = s
	m.byConn[s.Conn.ID()] = s
	return old
}

// Remove 解除会话登记：仅当传入指针与登记的会话一致才生效（防止顶号后误删新会话）。
func (m *SessionManager) Remove(s *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.byUID[s.UID] == s {
		delete(m.byUID, s.UID)
	}
	if m.byConn[s.Conn.ID()] == s {
		delete(m.byConn, s.Conn.ID())
	}
}

// Count 当前在线会话数。
func (m *SessionManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byUID)
}

// All 返回会话快照。
func (m *SessionManager) All() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Session, 0, len(m.byUID))
	for _, s := range m.byUID {
		out = append(out, s)
	}
	return out
}
