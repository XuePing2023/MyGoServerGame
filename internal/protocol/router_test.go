package protocol

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	gamepb "servergame/pb/game/v1"
)

// routerFakeConn 路由测试用的连接实现。
type routerFakeConn struct {
	sent []*Message
}

func (f *routerFakeConn) ID() uint64                               { return 1 }
func (f *routerFakeConn) Send(m *Message) error                    { f.sent = append(f.sent, m); return nil }
func (f *routerFakeConn) SendMsg(id uint16, m proto.Message) error { return nil }
func (f *routerFakeConn) Close(string)                             {}
func (f *routerFakeConn) CloseAfterDrain(string, time.Duration)    {}
func (f *routerFakeConn) RemoteAddr() string                       { return "test" }
func (f *routerFakeConn) SetData(any)                              {}
func (f *routerFakeConn) Data() any                                { return nil }

// TestRouterDispatch 验证类型化注册与分发。
func TestRouterDispatch(t *testing.T) {
	r := NewRouter()
	var got *gamepb.HeartbeatReq
	r.Handle(uint16(gamepb.MsgID_HEARTBEAT_REQ), (*gamepb.HeartbeatReq)(nil), func(conn ConnHandler, m *Message) {
		req := &gamepb.HeartbeatReq{}
		if err := m.Unmarshal(req); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		got = req
	})

	conn := &routerFakeConn{}
	m, err := NewMessage(uint16(gamepb.MsgID_HEARTBEAT_REQ), &gamepb.HeartbeatReq{ClientTime: 777})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := r.Dispatch(conn, m); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got == nil || got.ClientTime != 777 {
		t.Fatalf("handler 未收到正确的消息: %+v", got)
	}
}

// TestRouterUnknown 验证未知消息 ID 报错。
func TestRouterUnknown(t *testing.T) {
	r := NewRouter()
	conn := &routerFakeConn{}
	err := r.Dispatch(conn, &Message{ID: 60000})
	if err == nil {
		t.Fatal("未知消息 ID 应返回错误")
	}
}

// TestRouterDuplicate 验证重复注册 panic。
func TestRouterDuplicate(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("重复注册应 panic")
		}
	}()
	r := NewRouter()
	sample := (*gamepb.Empty)(nil)
	r.Handle(uint16(gamepb.MsgID_GET_PROFILE_REQ), sample, func(ConnHandler, *Message) {})
	r.Handle(uint16(gamepb.MsgID_GET_PROFILE_REQ), sample, func(ConnHandler, *Message) {})
}

// TestSendError 验证错误统一转换为 ErrorNotify。
func TestSendError(t *testing.T) {
	conn := &routerFakeConn{}
	SendError(conn, uint16(gamepb.MsgID_WORLD_CHAT_REQ), errTest{})
	if len(conn.sent) != 1 {
		t.Fatalf("应发送 1 条 ErrorNotify, got %d", len(conn.sent))
	}
	m := conn.sent[0]
	if uint16(gamepb.MsgID_ERROR_NOTIFY) != m.ID {
		t.Fatalf("消息 ID 不符: %d", m.ID)
	}
	notify := &gamepb.ErrorNotify{}
	if err := m.Unmarshal(notify); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if notify.Code != gamepb.ErrCode_ERR_INTERNAL || notify.ReqMsgId != gamepb.MsgID_WORLD_CHAT_REQ {
		t.Fatalf("错误内容不符: code=%v req=%v", notify.Code, notify.ReqMsgId)
	}
}

type errTest struct{}

func (errTest) Error() string { return "boom" }
