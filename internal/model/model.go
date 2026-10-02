// Package model 定义 MongoDB 中的持久化结构与集合名。
package model

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// 集合名。
const (
	ColAccount = "accounts"
	ColPlayer  = "players"
	ColCounter = "counters" // 自增 ID 序列
)

// Account 登录账号（游客或正式账号）。
type Account struct {
	ID           primitive.ObjectID `bson:"_id,omitempty"`
	UID          int64              `bson:"uid"`                    // 全局唯一玩家 ID
	Username     string             `bson:"username"`               // 登录名（游客为 guest_xxx）
	PasswordHash string             `bson:"passwordHash,omitempty"` // bcrypt 哈希；游客无密码
	IsGuest      bool               `bson:"isGuest"`
	DeviceID     string             `bson:"deviceId,omitempty"` // 游客绑定的设备号
	CreatedAt    time.Time          `bson:"createdAt"`
	LastLoginAt  time.Time          `bson:"lastLoginAt"`
}

// Player 玩家可变数据（与账号一对一）。
type Player struct {
	UID         int64     `bson:"uid"`
	Nickname    string    `bson:"nickname"`
	Level       int32     `bson:"level"`
	Exp         int64     `bson:"exp"`
	Gold        int64     `bson:"gold"`
	Diamond     int64     `bson:"diamond"`
	WinCount    int64     `bson:"winCount"`
	LoseCount   int64     `bson:"loseCount"`
	DrawCount   int64     `bson:"drawCount"`
	CreatedAt   time.Time `bson:"createdAt"`
	LastLoginAt time.Time `bson:"lastLoginAt"`
}

// Counter 自增序列文档。
type Counter struct {
	ID  string `bson:"_id"`
	Seq int64  `bson:"seq"`
}
