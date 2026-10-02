// Package errs 定义游戏服务器的业务错误：handler 返回 *Error，
// 由消息分发层统一转换成 ErrorNotify（错误码为 pb 的 ErrCode）推给客户端。
package errs

import (
	"errors"
	"fmt"

	gamepb "servergame/pb/game/v1"
)

// Error 业务错误。
type Error struct {
	Code gamepb.ErrCode `json:"code"`
	Msg  string         `json:"msg"`
}

func (e *Error) Error() string { return fmt.Sprintf("[%d] %s", e.Code, e.Msg) }

// New 构造业务错误。
func New(code gamepb.ErrCode, msg string) *Error { return &Error{Code: code, Msg: msg} }

// Newf 构造带格式化的业务错误。
func Newf(code gamepb.ErrCode, format string, args ...any) *Error {
	return New(code, fmt.Sprintf(format, args...))
}

// 常用快捷构造。
func BadRequest(format string, args ...any) *Error {
	return Newf(gamepb.ErrCode_ERR_BAD_REQUEST, format, args...)
}
func Unauthorized(format string, args ...any) *Error {
	return Newf(gamepb.ErrCode_ERR_UNAUTHORIZED, format, args...)
}
func Internal(format string, args ...any) *Error {
	return Newf(gamepb.ErrCode_ERR_INTERNAL, format, args...)
}
func RateLimited(format string, args ...any) *Error {
	return Newf(gamepb.ErrCode_ERR_RATE_LIMITED, format, args...)
}
func GameState(format string, args ...any) *Error {
	return Newf(gamepb.ErrCode_ERR_GAME_STATE, format, args...)
}

// From 将任意 error 尝试转换为 *Error；非业务错误统一包装成内部错误。
func From(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return New(gamepb.ErrCode_ERR_INTERNAL, "服务器开小差了，请稍后再试")
}

// CodeOf 取错误的业务码；非业务错误返回 ERR_INTERNAL。
func CodeOf(err error) gamepb.ErrCode {
	if e := From(err); e != nil {
		return e.Code
	}
	return gamepb.ErrCode_ERR_INTERNAL
}
