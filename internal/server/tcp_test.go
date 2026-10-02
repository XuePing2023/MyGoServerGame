package server

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"servergame/internal/pkg/logger"
	"servergame/internal/protocol"
	gamepb "servergame/pb/game/v1"
)

// TestMain 初始化测试日志（网络层代码会打日志）。
func TestMain(m *testing.M) {
	logger.InitForTest()
	os.Exit(m.Run())
}

// newEchoRouter 构造一个只处理心跳并回 ACK 的路由。
func newEchoRouter() *protocol.Router {
	r := protocol.NewRouter()
	r.Handle(uint16(gamepb.MsgID_HEARTBEAT_REQ), (*gamepb.HeartbeatReq)(nil), func(conn protocol.ConnHandler, m *protocol.Message) {
		req := &gamepb.HeartbeatReq{}
		if err := m.Unmarshal(req); err != nil {
			return
		}
		_ = conn.SendMsg(uint16(gamepb.MsgID_HEARTBEAT_ACK), &gamepb.HeartbeatAck{ClientTime: req.ClientTime, ServerTime: 1000})
	})
	return r
}

// TestTCPHeartbeatRoundTrip 真实 TCP 回环：发心跳 -> 收 ACK。
func TestTCPHeartbeatRoundTrip(t *testing.T) {
	srv := NewTCPServer("127.0.0.1:0", &ConnConfig{
		Router:         newEchoRouter(),
		SendChanSize:   16,
		MaxPacketBytes: 65536,
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("启动 TCP 服务: %v", err)
	}
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("拨号: %v", err)
	}
	defer func() { _ = conn.Close() }()

	req, _ := protocol.NewMessage(uint16(gamepb.MsgID_HEARTBEAT_REQ), &gamepb.HeartbeatReq{ClientTime: 42})
	if _, err := conn.Write(req.PackTCP()); err != nil {
		t.Fatalf("发送: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	ack, err := protocol.ReadFrame(conn, 65536)
	if err != nil {
		t.Fatalf("读取 ACK: %v", err)
	}
	if uint16(gamepb.MsgID_HEARTBEAT_ACK) != ack.ID {
		t.Fatalf("应为心跳 ACK, got id=%d", ack.ID)
	}
	var reply gamepb.HeartbeatAck
	if err := ack.Unmarshal(&reply); err != nil {
		t.Fatalf("解析 ACK: %v", err)
	}
	if reply.ClientTime != 42 || reply.ServerTime != 1000 {
		t.Fatalf("ACK 内容不符: client=%d server=%d", reply.ClientTime, reply.ServerTime)
	}
}

// TestWSHeartbeatRoundTrip 真实 WebSocket 回环：发心跳 -> 收 ACK。
func TestWSHeartbeatRoundTrip(t *testing.T) {
	srv := NewWSServer("127.0.0.1:0", &ConnConfig{
		Router:         newEchoRouter(),
		SendChanSize:   16,
		MaxPacketBytes: 65536,
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("启动 WS 服务: %v", err)
	}
	defer srv.Stop()

	d := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	ws, _, err := d.Dial("ws://"+srv.Addr().String(), nil)
	if err != nil {
		t.Fatalf("WS 拨号: %v", err)
	}
	defer func() { _ = ws.Close() }()

	req, _ := protocol.NewMessage(uint16(gamepb.MsgID_HEARTBEAT_REQ), &gamepb.HeartbeatReq{ClientTime: 99})
	if err := ws.WriteMessage(websocket.BinaryMessage, req.PackWS()); err != nil {
		t.Fatalf("发送: %v", err)
	}
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	mt, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("读取 ACK: %v", err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("应为二进制帧, got %d", mt)
	}
	ack, err := protocol.ParseWSFrame(data, 65536)
	if err != nil {
		t.Fatalf("解析帧: %v", err)
	}
	if uint16(gamepb.MsgID_HEARTBEAT_ACK) != ack.ID {
		t.Fatalf("应为心跳 ACK, got id=%d", ack.ID)
	}
}

// TestConnSendQueueFull 验证发送队列打满时 Send 返回错误并踢线。
func TestConnSendQueueFull(t *testing.T) {
	readBlock := make(chan struct{})
	defer close(readBlock) // 测试结束让读协程退出

	writeEntered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release) // 释放阻塞的写协程
	c := StartConn(ConnOpts{
		Kind: KindTCP,
		Addr: "test",
		Read: func() (*protocol.Message, error) {
			<-readBlock
			return nil, net.ErrClosed
		},
		Write: func(m *protocol.Message) error {
			select {
			case writeEntered <- struct{}{}:
			default:
			}
			<-release // 写协程在此阻塞，队列无法排空
			return nil
		},
		RawClose:     func() error { return nil },
		Router:       protocol.NewRouter(),
		SendChanSize: 1,
	})

	msg, _ := protocol.NewMessage(1, &gamepb.HeartbeatReq{})

	// 第 1 条：写协程立即取走并阻塞在 Write 里
	if err := c.Send(msg); err != nil {
		t.Fatalf("第一次 Send 不应失败: %v", err)
	}
	select {
	case <-writeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("写协程未消费第一条消息")
	}

	// 第 2 条：填满队列（容量 1）
	if err := c.Send(msg); err != nil {
		t.Fatalf("第二次 Send 不应失败: %v", err)
	}

	// 第 3 条：队列已满 -> ErrSendQueueFull 并触发踢线
	for i := 0; i < 100; i++ {
		err := c.Send(msg)
		if err == ErrSendQueueFull {
			return // 符合预期
		}
		if err != nil {
			t.Fatalf("第三次 Send 应返回 ErrSendQueueFull, got %v", err)
		}
		// 写协程仍阻塞，队列不会排空；若误判成功则让出 CPU 重试
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("队列满未被触发")
}
