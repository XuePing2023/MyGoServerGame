// Package store 定义持久化接口与 MongoDB 实现。
// 游戏逻辑只依赖接口，便于单元测试时替换为内存实现。
package store

import (
	"context"
	"errors"
	"time"

	"servergame/internal/model"
)

// 通用存储错误，由实现层映射。
var (
	// ErrNotFound 记录不存在。
	ErrNotFound = errors.New("记录不存在")
	// ErrDuplicate 唯一键冲突（用户名/昵称重复等）。
	ErrDuplicate = errors.New("记录已存在")
)

// AccountStore 账号存储。
type AccountStore interface {
	FindByUsername(ctx context.Context, username string) (*model.Account, error)
	FindByDeviceID(ctx context.Context, deviceID string) (*model.Account, error)
	// Create 插入账号；UID 由实现分配并回填到 acc.UID。
	Create(ctx context.Context, acc *model.Account) error
	// TouchLogin 更新最近登录时间。
	TouchLogin(ctx context.Context, uid int64, at time.Time) error
}

// PlayerStore 玩家数据存储。
type PlayerStore interface {
	FindByUID(ctx context.Context, uid int64) (*model.Player, error)
	// Create 插入玩家（昵称唯一索引冲突时返回 ErrDuplicate）。
	Create(ctx context.Context, p *model.Player) error
	// Save 全量保存玩家（昵称唯一索引冲突时返回 ErrDuplicate）。
	Save(ctx context.Context, p *model.Player) error
}
