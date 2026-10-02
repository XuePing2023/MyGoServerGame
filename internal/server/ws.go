package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"servergame/internal/pkg/logger"
	"servergame/internal/protocol"
)

// WSServer WebSocket 长连接服务（任何路径均可升级为 WS）。
type WSServer struct {
	addr string
	cfg  *ConnConfig
	l    net.Listener
	srv  *http.Server
}

// NewWSServer 创建 WebSocket 服务。
func NewWSServer(addr string, cfg *ConnConfig) *WSServer {
	s := &WSServer{addr: addr, cfg: cfg}
	s.srv = &http.Server{Handler: s.routes()}
	return s
}

func (s *WSServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleWS)
	return mux
}

// Start 启动 HTTP 服务（后台 Accept）。
func (s *WSServer) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.l = l
	go func() {
		logger.L.Infof("WebSocket 服务已启动: ws://%s", l.Addr())
		if err := s.srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.L.Errorf("WebSocket 服务异常退出: %v", err)
		}
	}()
	return nil
}

// Addr 实际监听地址（Start 后可用）。
func (s *WSServer) Addr() net.Addr {
	if s.l == nil {
		return nil
	}
	return s.l.Addr()
}

func (s *WSServer) handleWS(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		// 游戏服务器通常对来源不做限制（鉴权由登录消息完成），
		// 如需限制请在此处校验 r.Header.Get("Origin")。
		CheckOrigin: func(*http.Request) bool { return true },
	}
	raw, err := up.Upgrade(w, r, nil)
	if err != nil {
		logger.L.Warnf("WebSocket 升级失败: %v", err)
		return
	}

	c := StartConn(ConnOpts{
		Kind:         KindWS,
		Addr:         raw.RemoteAddr().String(),
		SendChanSize: s.cfg.sendChanSize(),
		Router:       s.cfg.Router,
		OnClose:      s.cfg.OnClose,
		Read: func() (*protocol.Message, error) {
			raw.SetReadLimit(int64(s.cfg.MaxPacketBytes) + 2)
			mt, data, err := raw.ReadMessage()
			if err != nil {
				return nil, err
			}
			if mt != websocket.BinaryMessage {
				return nil, fmt.Errorf("不支持的 WS 消息类型: %d", mt)
			}
			return protocol.ParseWSFrame(data, s.cfg.MaxPacketBytes)
		},
		Write: func(m *protocol.Message) error {
			_ = raw.SetWriteDeadline(time.Now().Add(WriteTimeout))
			return raw.WriteMessage(websocket.BinaryMessage, m.PackWS())
		},
		RawClose: func() error {
			// 发送 Close 握手帧后关闭底层连接
			_ = raw.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(time.Second))
			return raw.Close()
		},
	})
	if s.cfg.OnOpen != nil {
		s.cfg.OnOpen(c)
	}
}

// Stop 优雅停机：停止接受新连接与新升级请求。
func (s *WSServer) Stop() {
	if s.l != nil {
		_ = s.l.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
}
