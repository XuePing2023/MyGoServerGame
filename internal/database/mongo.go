// Package database 负责 MongoDB 连接与索引初始化。
package database

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"servergame/internal/config"
	"servergame/internal/model"
)

// Connect 连接 MongoDB 并返回客户端与 Database。
func Connect(ctx context.Context, cfg *config.Config) (*mongo.Client, *mongo.Database, error) {
	timeout := cfg.MongoTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(cfg.Mongo.URI).
		SetConnectTimeout(timeout).
		SetServerSelectionTimeout(timeout)

	// v2 驱动：Connect 不再接收 ctx，连接由客户端在后台按需建立，
	// 用 Ping 校验连通性（ctx 同时约束连接建立与 Ping 的超时）。
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("创建 MongoDB 客户端失败: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, nil, fmt.Errorf("MongoDB Ping 失败（请确认服务已启动，URI: %s）: %w", cfg.Mongo.URI, err)
	}
	return client, client.Database(cfg.Mongo.Database), nil
}

// EnsureIndexes 创建全部业务索引（幂等）。
func EnsureIndexes(ctx context.Context, db *mongo.Database) error {
	models := map[string][]mongo.IndexModel{
		model.ColAccount: {
			{Keys: bson.D{{Key: "uid", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "username", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "deviceId", Value: 1}}}, // 游客按设备号查找
		},
		model.ColPlayer: {
			{Keys: bson.D{{Key: "uid", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "nickname", Value: 1}}, Options: options.Index().SetUnique(true)},
		},
	}

	for collName, idxModels := range models {
		if len(idxModels) == 0 {
			continue
		}
		if _, err := db.Collection(collName).Indexes().CreateMany(ctx, idxModels); err != nil {
			return fmt.Errorf("为集合 %s 创建索引失败: %w", collName, err)
		}
	}
	return nil
}

// Disconnect 断开 MongoDB 连接。
func Disconnect(ctx context.Context, client *mongo.Client) {
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		fmt.Println("断开 MongoDB 失败:", err)
	}
}
