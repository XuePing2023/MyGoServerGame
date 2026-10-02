package protocol

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"servergame/internal/errs"
	gamepb "servergame/pb/game/v1"
)

// ConnHandler 由网络层实现的连接接口，路由器只依赖该抽象，
// 不感知底层是 TCP 还是 WebSocket。
type ConnHandler interface {
	// ID 连接 ID（进程内自增）。
	ID() uint64
	// Send 异步发送消息（内部有发送队列，不会阻塞调用方）。
	Send(m *Message) error
	// SendMsg 序列化 pb 消息后异步发送。
	SendMsg(id uint16, m proto.Message) error
	// Close 主动断开连接（幂等），reason 会记录在日志中。
	Close(reason string)
	// CloseAfterDrain 先尽量冲刷发送队列再断开（用于下发通知后再断线）。
	CloseAfterDrain(reason string, maxWait time.Duration)
	// RemoteAddr 对端地址（用于日志）。
	RemoteAddr() string
	// SetData / Data 绑定会话等上层数据。
	SetData(v any)
	Data() any
}

// RawHandler 最底层的消息处理签名：请求体尚未反序列化。
type RawHandler func(conn ConnHandler, m *Message)

// Router 消息路由器：消息 ID -> 处理函数。
// 处理函数在各自连接的读协程内串行执行，天然保证单连接消息有序，
// 因此 handler 内部无需再对会话状态加锁。
type Router struct {
	mu       sync.RWMutex
	handlers map[uint16]RawHandler
	types    map[uint16]string
}

// NewRouter 创建路由器。
func NewRouter() *Router {
	return &Router{
		handlers: make(map[uint16]RawHandler),
		types:    make(map[uint16]string),
	}
}

// Handle 注册消息处理函数，重复注册视为编码错误，直接 panic。
func (r *Router) Handle(id uint16, sample proto.Message, h RawHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.handlers[id]; dup {
		panic(fmt.Sprintf("消息 ID %d 重复注册", id))
	}
	r.handlers[id] = h
	r.types[id] = reflect.TypeOf(sample).String()
}

// Dispatch 分发一条已解码的消息。未知消息 ID 返回错误（由调用方决定是否断开）。
func (r *Router) Dispatch(conn ConnHandler, m *Message) error {
	r.mu.RLock()
	h, ok := r.handlers[m.ID]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("未注册的消息 ID: %d", m.ID)
	}
	h(conn, m)
	return nil
}

// Types 返回 消息ID -> 请求类型名 映射，供调试端点展示路由表。
func (r *Router) Types() map[uint16]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[uint16]string, len(r.types))
	for k, v := range r.types {
		out[k] = v
	}
	return out
}

// SendError 把 handler 返回的 error 统一转换成 ErrorNotify 下发。
func SendError(conn ConnHandler, reqID uint16, err error) {
	e := errs.From(err)
	notify := &gamepb.ErrorNotify{
		Code:     e.Code,
		Msg:      e.Msg,
		ReqMsgId: gamepb.MsgID(reqID),
	}
	m, mErr := NewMessage(uint16(gamepb.MsgID_ERROR_NOTIFY), notify)
	if mErr != nil {
		return
	}
	_ = conn.Send(m)
}
