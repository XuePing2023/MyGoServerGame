package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	gamepb "servergame/pb/game/v1"
)

// buildFrame 手工构造一个 TCP 帧（与 PackTCP 格式一致，用于互验）。
func buildFrame(t *testing.T, id uint16, body []byte) []byte {
	t.Helper()
	buf := make([]byte, 4+2+len(body))
	binary.BigEndian.PutUint32(buf, uint32(2+len(body)))
	binary.BigEndian.PutUint16(buf[4:], id)
	copy(buf[6:], body)
	return buf
}

// TestTCPFrameRoundTrip 验证 PackTCP 与 ReadFrame 的互逆性。
func TestTCPFrameRoundTrip(t *testing.T) {
	msg, err := NewMessage(uint16(gamepb.MsgID_HEARTBEAT_REQ), &gamepb.HeartbeatReq{ClientTime: 12345})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	frame := msg.PackTCP()

	// 与手工构造的帧逐字节比对
	want := buildFrame(t, uint16(gamepb.MsgID_HEARTBEAT_REQ), msg.Body)
	if !bytes.Equal(frame, want) {
		t.Fatalf("PackTCP 输出与期望帧不一致:\n got %v\nwant %v", frame, want)
	}

	got, err := ReadFrame(bytes.NewReader(frame), 65536)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.ID != msg.ID {
		t.Fatalf("消息 ID 不一致: got %d want %d", got.ID, msg.ID)
	}
	if !bytes.Equal(got.Body, msg.Body) {
		t.Fatalf("消息体不一致: got %v want %v", got.Body, msg.Body)
	}

	// 反序列化校验
	var req gamepb.HeartbeatReq
	if err := got.Unmarshal(&req); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if req.ClientTime != 12345 {
		t.Fatalf("字段不一致: got %d", req.ClientTime)
	}
}

// TestTCPFramesBackToBack 验证粘包场景：一串帧连读。
func TestTCPFramesBackToBack(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 5; i++ {
		m, err := NewMessage(uint16(i+1), &gamepb.HeartbeatReq{ClientTime: int64(i)})
		if err != nil {
			t.Fatalf("NewMessage: %v", err)
		}
		buf.Write(m.PackTCP())
	}
	r := bytes.NewReader(buf.Bytes())
	for i := 0; i < 5; i++ {
		m, err := ReadFrame(r, 65536)
		if err != nil {
			t.Fatalf("第 %d 帧 ReadFrame: %v", i+1, err)
		}
		if int(m.ID) != i+1 {
			t.Fatalf("第 %d 帧 ID 不一致: %d", i+1, m.ID)
		}
	}
	// 读完应为 EOF
	if _, err := ReadFrame(r, 65536); err == nil {
		t.Fatal("流结束后应返回 EOF")
	}
}

// TestReadFrameBadSize 验证长度域非法时的防护。
func TestReadFrameBadSize(t *testing.T) {
	// 过短
	var tiny [4]byte
	binary.BigEndian.PutUint32(tiny[:], 1)
	if _, err := ReadFrame(bytes.NewReader(tiny[:]), 65536); !errors.Is(err, ErrPacketSize) {
		t.Fatalf("过短帧应返回 ErrPacketSize, got %v", err)
	}
	// 过大
	var huge [4]byte
	binary.BigEndian.PutUint32(huge[:], 1<<20)
	if _, err := ReadFrame(bytes.NewReader(huge[:]), 65536); !errors.Is(err, ErrPacketSize) {
		t.Fatalf("超大帧应返回 ErrPacketSize, got %v", err)
	}
}

// TestWSFrame 验证 WebSocket 载荷的打包与解析。
func TestWSFrame(t *testing.T) {
	msg, err := NewMessage(uint16(gamepb.MsgID_WORLD_CHAT_REQ), &gamepb.WorldChatReq{Content: "你好"})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	payload := msg.PackWS()

	got, err := ParseWSFrame(payload, 65536)
	if err != nil {
		t.Fatalf("ParseWSFrame: %v", err)
	}
	if got.ID != msg.ID {
		t.Fatalf("消息 ID 不一致: got %d want %d", got.ID, msg.ID)
	}

	var chat gamepb.WorldChatReq
	if err := got.Unmarshal(&chat); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if chat.Content != "你好" {
		t.Fatalf("内容不一致: %q", chat.Content)
	}

	// 过短帧
	if _, err := ParseWSFrame([]byte{1}, 65536); !errors.Is(err, ErrPacketSize) {
		t.Fatalf("过短 WS 帧应返回 ErrPacketSize, got %v", err)
	}
}

// TestEmptyBody 空 protobuf 消息体（Marshal 结果为空）应可正常收发。
func TestEmptyBody(t *testing.T) {
	msg, err := NewMessage(uint16(gamepb.MsgID_GET_PROFILE_REQ), &gamepb.Empty{})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if len(msg.Body) != 0 {
		t.Fatalf("Empty 应编码为空消息体, got %d 字节", len(msg.Body))
	}
	got, err := ReadFrame(bytes.NewReader(msg.PackTCP()), 65536)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	var e gamepb.Empty
	if err := got.Unmarshal(&e); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
}
