// Package server 提供与协议无关的网络层：统一连接抽象 + TCP / WebSocket 两种入口。
// 连接对象内置发送队列与写协程，读协程收到完整帧后交给路由器分发。
package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"servergame/internal/errs"
	"servergame/internal/pkg/logger"
	"servergame/internal/protocol"
)

// Kind 连接类型。
type Kind int

const (
	KindTCP Kind = iota
	KindWS
)

func (k Kind) String() string {
	if k == KindWS {
		return "ws"
	}
	return "tcp"
}

var (
	// ErrConnClosed 连接已关闭。
	ErrConnClosed = errors.New("连接已关闭")
	// ErrSendQueueFull 发送队列已满（慢消费者会被踢下线）。
	ErrSendQueueFull = errors.New("发送队列已满")
)

var connSeq atomic.Uint64

// WriteTimeout 单次写出超时。
const WriteTimeout = 10 * time.Second

// Conn 一个客户端连接（TCP 或 WebSocket 的统一抽象）。
type Conn struct {
	id   uint64
	kind Kind
	addr string

	read     func() (*protocol.Message, error) // 阻塞读一条完整消息
	write    func(*protocol.Message) error     // 阻塞写一条完整消息
	rawClose func() error                      // 关闭底层通道
	router   *protocol.Router

	sendCh    chan *protocol.Message
	stop      chan struct{}
	stopOnce  sync.Once
	closed    atomic.Bool
	closeOnce sync.Once

	reasonMu sync.Mutex
	reason   string

	lastActive atomic.Int64 // unix 毫秒
	userData   atomic.Value // game 层存放会话等绑定数据

	onClose     func(c *Conn, reason string) // 由上层设置，仅触发一次
	onCloseOnce sync.Once
}

// ConnOpts 构造连接所需的依赖。
type ConnOpts struct {
	Kind         Kind
	Addr         string
	Read         func() (*protocol.Message, error)
	Write        func(*protocol.Message) error
	RawClose     func() error
	Router       *protocol.Router
	SendChanSize int
	OnClose      func(c *Conn, reason string)
}

// StartConn 创建连接并启动读/写协程。
func StartConn(opts ConnOpts) *Conn {
	c := &Conn{
		id:       connSeq.Add(1),
		kind:     opts.Kind,
		addr:     opts.Addr,
		read:     opts.Read,
		write:    opts.Write,
		rawClose: opts.RawClose,
		router:   opts.Router,
		sendCh:   make(chan *protocol.Message, opts.SendChanSize),
		stop:     make(chan struct{}),
	}
	if opts.OnClose != nil {
		c.onClose = opts.OnClose
	}
	c.touch()
	go c.readLoop()
	go c.writeLoop()
	return c
}

// ID 连接 ID（进程内自增）。
func (c *Conn) ID() uint64 { return c.id }

// Kind 连接类型。
func (c *Conn) Kind() Kind { return c.kind }

// IsWebSocket 是否 WebSocket 连接。
func (c *Conn) IsWebSocket() bool { return c.kind == KindWS }

// RemoteAddr 对端地址。
func (c *Conn) RemoteAddr() string { return c.addr }

// SetData 绑定会话等上层数据。
func (c *Conn) SetData(v any) { c.userData.Store(v) }

// Data 读取绑定的上层数据。
func (c *Conn) Data() any { return c.userData.Load() }

// LastActive 最近一次收到数据的时间。
func (c *Conn) LastActive() time.Time {
	return time.UnixMilli(c.lastActive.Load())
}

// Send 异步发送消息。队列满时踢掉慢消费者并返回错误；不会阻塞调用方。
func (c *Conn) Send(m *protocol.Message) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	select {
	case c.sendCh <- m:
		return nil
	case <-c.stop:
		return ErrConnClosed
	default:
		go c.Close("发送队列溢出，判定为慢消费者")
		return ErrSendQueueFull
	}
}

// SendMsg 序列化 pb 消息后发送。
func (c *Conn) SendMsg(id uint16, m proto.Message) error {
	pm, err := protocol.NewMessage(id, m)
	if err != nil {
		return err
	}
	return c.Send(pm)
}

// Close 主动关闭连接（幂等）。底层通道关闭后读协程退出并触发 onClose。
func (c *Conn) Close(reason string) {
	c.setReason(reason)
	c.closed.Store(true)
	c.closeOnce.Do(func() {
		if c.rawClose != nil {
			_ = c.rawClose()
		}
	})
}

// CloseAfterDrain 先尽量把发送队列冲刷完再关闭（用于下发 KickNotify 后再断开）。
func (c *Conn) CloseAfterDrain(reason string, maxWait time.Duration) {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) && len(c.sendCh) > 0 && !c.closed.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	c.Close(reason)
}

// Reason 返回关闭原因。
func (c *Conn) Reason() string {
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	return c.reason
}

func (c *Conn) setReason(reason string) {
	if reason == "" {
		return
	}
	c.reasonMu.Lock()
	if c.reason == "" {
		c.reason = reason
	}
	c.reasonMu.Unlock()
}

func (c *Conn) touch() {
	c.lastActive.Store(time.Now().UnixMilli())
}

// readLoop 读协程：阻塞读帧 -> 更新活跃时间 -> 交给路由器分发。
// 任何读错误都会终结循环并触发清理。
func (c *Conn) readLoop() {
	var reason string
	defer func() {
		if r := recover(); r != nil {
			logger.L.Errorf("连接 #%d 读协程 panic: %v", c.id, r)
			reason = "读协程 panic"
		}
		c.cleanup(reason)
	}()

	for {
		m, err := c.read()
		if err != nil {
			reason = readErrReason(err)
			return
		}
		c.touch()
		if err := c.router.Dispatch(c, m); err != nil {
			// 未知消息：提示客户端但不断开
			logger.L.Debugf("连接 #%d (%s): %v", c.id, c.addr, err)
			protocol.SendError(c, m.ID, errs.BadRequest("未知消息 ID: %d", m.ID))
		}
	}
}

// writeLoop 写协程：串行消费发送队列。
func (c *Conn) writeLoop() {
	for {
		select {
		case m := <-c.sendCh:
			if err := c.write(m); err != nil {
				logger.L.Debugf("连接 #%d (%s) 写失败: %v", c.id, c.addr, err)
				c.Close("写失败")
				return
			}
		case <-c.stop:
			return
		}
	}
}

// cleanup 连接退出时的统一清理：只执行一次。
func (c *Conn) cleanup(readReason string) {
	c.setReason(readReason)
	c.closed.Store(true)
	c.closeOnce.Do(func() {
		if c.rawClose != nil {
			_ = c.rawClose()
		}
	})
	c.stopOnce.Do(func() { close(c.stop) })
	c.onCloseOnce.Do(func() {
		if c.onClose != nil {
			c.onClose(c, c.Reason())
		}
	})
	logger.L.Debugf("连接 #%d (%s) 已断开: %s", c.id, c.addr, c.Reason())
}

// readErrReason 把底层读错误翻译成可读的关闭原因。
func readErrReason(err error) string {
	switch {
	case errors.Is(err, io.EOF):
		return "客户端主动断开"
	case errors.Is(err, net.ErrClosed):
		return "连接已被服务器关闭"
	case errors.Is(err, protocol.ErrPacketSize):
		return "非法包大小"
	default:
		return fmt.Sprintf("读错误: %v", err)
	}
}
