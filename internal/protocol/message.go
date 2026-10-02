// Package protocol 定义游戏的消息封装、帧编解码与消息路由。
//
// 帧格式（TCP）  ：| 4B 长度(大端) | 2B 消息ID(大端) | 消息体(protobuf) |
//
//	长度 = 2 + len(消息体)，不含长度域自身。
//
// 帧格式（WebSocket）：二进制帧 = | 2B 消息ID(大端) | 消息体(protobuf) |，
//
//	WS 帧自带边界，不再重复长度前缀。
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

// Message 一次网络消息：2 字节消息 ID + 已序列化的 protobuf 消息体。
type Message struct {
	ID   uint16
	Body []byte
}

// NewMessage 将 pb 消息序列化后封装成网络消息。
func NewMessage(id uint16, m proto.Message) (*Message, error) {
	body, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("序列化消息 %d 失败: %w", id, err)
	}
	return &Message{ID: id, Body: body}, nil
}

// Unmarshal 将消息体反序列化到指定的 pb 结构。
func (m *Message) Unmarshal(dst proto.Message) error {
	if err := proto.Unmarshal(m.Body, dst); err != nil {
		return fmt.Errorf("反序列化消息 %d 失败: %w", m.ID, err)
	}
	return nil
}

// HeaderSize TCP 帧：4 字节长度 + 2 字节消息 ID。
const HeaderSize = 6

// PackTCP 打包成 TCP 帧（含长度前缀）。
func (m *Message) PackTCP() []byte {
	n := 2 + len(m.Body)
	buf := make([]byte, 4+n)
	binary.BigEndian.PutUint32(buf, uint32(n))
	binary.BigEndian.PutUint16(buf[4:], m.ID)
	copy(buf[6:], m.Body)
	return buf
}

// PackWS 打包成 WebSocket 二进制载荷（不含长度前缀）。
func (m *Message) PackWS() []byte {
	buf := make([]byte, 2+len(m.Body))
	binary.BigEndian.PutUint16(buf, m.ID)
	copy(buf[2:], m.Body)
	return buf
}

// ReadFrame 从字节流中读取一个完整的 TCP 帧。
// 返回 ErrPacketSize 表示长度域非法（调用方应当断开连接）。
func ReadFrame(r io.Reader, maxPacketBytes int) (*Message, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n < 2 || int(n) > maxPacketBytes {
		return nil, fmt.Errorf("帧长度 %d 超出范围 [2, %d]: %w", n, maxPacketBytes, ErrPacketSize)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return &Message{ID: binary.BigEndian.Uint16(buf[:2]), Body: buf[2:]}, nil
}

// ParseWSFrame 解析 WebSocket 二进制帧载荷。
func ParseWSFrame(payload []byte, maxPacketBytes int) (*Message, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("WS 帧过短: %d 字节: %w", len(payload), ErrPacketSize)
	}
	if len(payload) > maxPacketBytes {
		return nil, fmt.Errorf("WS 帧长度 %d 超出上限 %d: %w", len(payload), maxPacketBytes, ErrPacketSize)
	}
	return &Message{ID: binary.BigEndian.Uint16(payload[:2]), Body: payload[2:]}, nil
}
