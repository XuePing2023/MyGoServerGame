package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"servergame/internal/model"
)

// mongoAccount / mongoPlayer 分开实现两个接口：
// 两个接口都有 Create 方法且签名不同，无法由同一类型同时实现。
type mongoAccount struct{ db *mongo.Database }
type mongoPlayer struct{ db *mongo.Database }

// NewMongo 基于 MongoDB 构建 store。
func NewMongo(db *mongo.Database) (AccountStore, PlayerStore) {
	return &mongoAccount{db: db}, &mongoPlayer{db: db}
}

// mapErr 统一把 mongo 错误映射为存储层错误。
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ErrNotFound
	}
	if mongo.IsDuplicateKeyError(err) { // E11000 唯一键冲突（用户名/昵称重复等）
		return ErrDuplicate
	}
	return err
}

// nextSeq 原子自增计数器，用于分配全局唯一 UID。
func nextSeq(ctx context.Context, db *mongo.Database, name string) (int64, error) {
	res := db.Collection(model.ColCounter).FindOneAndUpdate(
		ctx,
		bson.M{"_id": name},
		bson.M{"$inc": bson.M{"seq": 1}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	)
	var c model.Counter
	if err := res.Decode(&c); err != nil {
		return 0, fmt.Errorf("分配序列 %s 失败: %w", name, err)
	}
	return c.Seq, nil
}

// ---------------------------------------------------------------------------
// AccountStore
// ---------------------------------------------------------------------------

func (s *mongoAccount) FindByUsername(ctx context.Context, username string) (*model.Account, error) {
	var acc model.Account
	err := s.db.Collection(model.ColAccount).FindOne(ctx, bson.M{"username": username}).Decode(&acc)
	if err := mapErr(err); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (s *mongoAccount) FindByDeviceID(ctx context.Context, deviceID string) (*model.Account, error) {
	var acc model.Account
	err := s.db.Collection(model.ColAccount).FindOne(ctx, bson.M{"deviceId": deviceID, "isGuest": true}).Decode(&acc)
	if err := mapErr(err); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (s *mongoAccount) Create(ctx context.Context, acc *model.Account) error {
	uid, err := nextSeq(ctx, s.db, "uid")
	if err != nil {
		return err
	}
	acc.UID = uid
	if acc.CreatedAt.IsZero() {
		acc.CreatedAt = time.Now()
	}
	_, err = s.db.Collection(model.ColAccount).InsertOne(ctx, acc)
	return mapErr(err)
}

func (s *mongoAccount) TouchLogin(ctx context.Context, uid int64, at time.Time) error {
	_, err := s.db.Collection(model.ColAccount).UpdateOne(
		ctx,
		bson.M{"uid": uid},
		bson.M{"$set": bson.M{"lastLoginAt": at}},
	)
	return mapErr(err)
}

// ---------------------------------------------------------------------------
// PlayerStore
// ---------------------------------------------------------------------------

func (s *mongoPlayer) FindByUID(ctx context.Context, uid int64) (*model.Player, error) {
	var p model.Player
	err := s.db.Collection(model.ColPlayer).FindOne(ctx, bson.M{"uid": uid}).Decode(&p)
	if err := mapErr(err); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *mongoPlayer) Create(ctx context.Context, p *model.Player) error {
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now()
	}
	_, err := s.db.Collection(model.ColPlayer).InsertOne(ctx, p)
	return mapErr(err)
}

func (s *mongoPlayer) Save(ctx context.Context, p *model.Player) error {
	filter := bson.M{"uid": p.UID}
	update := bson.M{"$set": bson.M{
		"nickname":    p.Nickname,
		"level":       p.Level,
		"exp":         p.Exp,
		"gold":        p.Gold,
		"diamond":     p.Diamond,
		"winCount":    p.WinCount,
		"loseCount":   p.LoseCount,
		"drawCount":   p.DrawCount,
		"lastLoginAt": p.LastLoginAt,
	}}
	_, err := s.db.Collection(model.ColPlayer).UpdateOne(ctx, filter, update)
	return mapErr(err)
}
