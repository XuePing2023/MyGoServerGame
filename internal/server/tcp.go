package server

import (
	"errors"
	"net"
	"time"

	"servergame/internal/pkg/logger"
	"servergame/internal/protocol"
)

// ConnConfig 接入一个新连接所需的公共参数（由 game 层装配）。
type ConnConfig struct {
	Router         *protocol.Router
	SendChanSize   int
	MaxPacketBytes int
	// OnOpen 连接建立后的回调（用于登记连接、推送服务器信息）。
	OnOpen func(c *Conn)
	// OnClose 连接关闭（读协程退出）后的回调，只触发一次。
	OnClose func(c *Conn, reason string)
}

func (cfg *ConnConfig) sendChanSize() int {
	if cfg.SendChanSize > 0 {
		return cfg.SendChanSize
	}
	return 256
}

// TCPServer TCP 长连接服务。
type TCPServer struct {
	addr string
	cfg  *ConnConfig
	l    net.Listener
}

// NewTCPServer 创建 TCP 服务。
func NewTCPServer(addr string, cfg *ConnConfig) *TCPServer {
	return &TCPServer{addr: addr, cfg: cfg}
}

// Start 开始监听并接受连接。
func (s *TCPServer) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.l = l
	go s.acceptLoop()
	logger.L.Infof("TCP 服务已启动: %s", l.Addr())
	return nil
}

func (s *TCPServer) acceptLoop() {
	for {
		raw, err := s.l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logger.L.Errorf("TCP Accept 异常: %v", err)
			time.Sleep(100 * time.Millisecond) // 避免 accept 持续报错时空转
			continue
		}
		go s.serveConn(raw)
	}
}

func (s *TCPServer) serveConn(raw net.Conn) {
	if tcp, ok := raw.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	c := StartConn(ConnOpts{
		Kind:         KindTCP,
		Addr:         raw.RemoteAddr().String(),
		SendChanSize: s.cfg.sendChanSize(),
		Router:       s.cfg.Router,
		OnClose:      s.cfg.OnClose,
		Read: func() (*protocol.Message, error) {
			return protocol.ReadFrame(raw, s.cfg.MaxPacketBytes)
		},
		Write: func(m *protocol.Message) error {
			_ = raw.SetWriteDeadline(time.Now().Add(WriteTimeout))
			_, err := raw.Write(m.PackTCP())
			return err
		},
		RawClose: func() error { return raw.Close() },
	})
	if s.cfg.OnOpen != nil {
		s.cfg.OnOpen(c)
	}
}

// Addr 实际监听地址（Start 后可用）。
func (s *TCPServer) Addr() net.Addr {
	if s.l == nil {
		return nil
	}
	return s.l.Addr()
}

// Stop 停止接受新连接（已有连接不受影响，由上层统一踢除）。
func (s *TCPServer) Stop() {
	if s.l != nil {
		_ = s.l.Close()
	}
}
