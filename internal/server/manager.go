package server

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"servergame/internal/protocol"
)

// Manager 连接管理器：统计在线连接、全服广播。
type Manager struct {
	mu    sync.RWMutex
	conns map[uint64]*Conn
}

// NewManager 创建连接管理器。
func NewManager() *Manager {
	return &Manager{conns: make(map[uint64]*Conn)}
}

// Add 登记连接。
func (m *Manager) Add(c *Conn) {
	m.mu.Lock()
	m.conns[c.id] = c
	m.mu.Unlock()
}

// Remove 移除连接（幂等）。
func (m *Manager) Remove(c *Conn) {
	m.mu.Lock()
	delete(m.conns, c.id)
	m.mu.Unlock()
}

// Count 当前连接数（含未登录）。
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// List 返回连接快照。
func (m *Manager) List() []*Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Conn, 0, len(m.conns))
	for _, c := range m.conns {
		out = append(out, c)
	}
	return out
}

// Broadcast 向所有连接广播消息（发送失败忽略：队列满的连接会被自动踢掉）。
func (m *Manager) Broadcast(msg *protocol.Message) {
	for _, c := range m.List() {
		_ = c.Send(msg)
	}
}

// BroadcastMsg 序列化 pb 消息后向所有连接广播。
func (m *Manager) BroadcastMsg(id uint16, msg proto.Message) error {
	pm, err := protocol.NewMessage(id, msg)
	if err != nil {
		return err
	}
	m.Broadcast(pm)
	return nil
}

// BroadcastExcept 广播消息但跳过指定连接。
func (m *Manager) BroadcastExcept(msg *protocol.Message, exceptID uint64) {
	for _, c := range m.List() {
		if c.id == exceptID {
			continue
		}
		_ = c.Send(msg)
	}
}

// CloseAll 踢掉全部连接（优雅停机时使用）。
func (m *Manager) CloseAll(reason string) {
	for _, c := range m.List() {
		c.CloseAfterDrain(reason, 200*time.Millisecond)
	}
}
