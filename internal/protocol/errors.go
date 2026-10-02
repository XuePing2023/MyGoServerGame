package protocol

import "errors"

// ErrPacketSize 帧长度非法（过小或超过上限）。
var ErrPacketSize = errors.New("非法的包大小")
