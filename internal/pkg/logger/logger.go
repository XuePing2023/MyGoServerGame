// Package logger 提供基于 zap 的全局日志：控制台彩色输出 + 文件轮转（lumberjack）。
package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// L 全局 Sugared Logger，在 Init 之后使用。
var L *zap.SugaredLogger

// Init 初始化全局日志。level: debug/info/warn/error。
func Init(level, file string, maxSizeMB, maxBackups, maxAgeDays int) error {
	if L != nil {
		return nil
	}

	lvl := zapcore.InfoLevel
	if err := lvl.Set(level); err != nil {
		lvl = zapcore.InfoLevel
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encCfg.EncodeLevel = zapcore.CapitalLevelEncoder

	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encCfg),
		zapcore.Lock(os.Stdout),
		lvl,
	)

	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg),
		zapcore.AddSync(&lumberjack.Logger{
			Filename:   file,
			MaxSize:    maxSizeMB,
			MaxBackups: maxBackups,
			MaxAge:     maxAgeDays,
			Compress:   true,
		}),
		lvl,
	)

	L = zap.New(zapcore.NewTee(consoleCore, fileCore), zap.AddCallerSkip(1)).Sugar()
	return nil
}

// InitForTest 供单元测试使用：仅输出到标准错误的最简日志。
// 若全局 logger 已初始化则不做任何事。
func InitForTest() {
	if L != nil {
		return
	}
	cfg := zap.NewDevelopmentEncoderConfig()
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(cfg), zapcore.Lock(os.Stderr), zapcore.DebugLevel)
	L = zap.New(core).Sugar()
}

// Sync 刷新缓冲，程序退出前调用。
func Sync() {
	if L != nil {
		_ = L.Sync()
	}
}
