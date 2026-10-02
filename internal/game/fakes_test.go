package game

import (
	"context"
	"sync"
	"time"

	"servergame/internal/model"
	"servergame/internal/store"
)

// ---------------------------------------------------------------------------
// 内存版存储实现（单元测试用）
// ---------------------------------------------------------------------------

type fakeAccounts struct {
	mu        sync.Mutex
	byUser    map[string]*model.Account
	byDevice  map[string]*model.Account
	nextUID   int64
	failTouch bool
}

func newFakeAccounts() *fakeAccounts {
	return &fakeAccounts{
		byUser:   make(map[string]*model.Account),
		byDevice: make(map[string]*model.Account),
		nextUID:  1000,
	}
}

func (f *fakeAccounts) FindByUsername(_ context.Context, username string) (*model.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	acc, ok := f.byUser[username]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *acc
	return &cp, nil
}

func (f *fakeAccounts) FindByDeviceID(_ context.Context, deviceID string) (*model.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	acc, ok := f.byDevice[deviceID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *acc
	return &cp, nil
}

func (f *fakeAccounts) Create(_ context.Context, acc *model.Account) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.byUser[acc.Username]; exists {
		return store.ErrDuplicate
	}
	f.nextUID++
	acc.UID = f.nextUID
	if acc.CreatedAt.IsZero() {
		acc.CreatedAt = time.Now()
	}
	cp := *acc
	f.byUser[acc.Username] = &cp
	if acc.DeviceID != "" {
		f.byDevice[acc.DeviceID] = &cp
	}
	return nil
}

func (f *fakeAccounts) TouchLogin(_ context.Context, uid int64, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failTouch {
		return context.DeadlineExceeded
	}
	for _, acc := range f.byUser {
		if acc.UID == uid {
			acc.LastLoginAt = at
		}
	}
	return nil
}

type fakePlayers struct {
	mu    sync.Mutex
	byUID map[int64]*model.Player
}

func newFakePlayers() *fakePlayers {
	return &fakePlayers{byUID: make(map[int64]*model.Player)}
}

func (f *fakePlayers) FindByUID(_ context.Context, uid int64) (*model.Player, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.byUID[uid]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *p
	return &cp, nil
}

func (f *fakePlayers) Create(_ context.Context, p *model.Player) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.byUID {
		if existing.Nickname == p.Nickname {
			return store.ErrDuplicate
		}
	}
	cp := *p
	f.byUID[p.UID] = &cp
	return nil
}

func (f *fakePlayers) Save(_ context.Context, p *model.Player) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for uid, existing := range f.byUID {
		// 模拟昵称唯一索引
		if uid != p.UID && existing.Nickname == p.Nickname {
			return store.ErrDuplicate
		}
	}
	if _, ok := f.byUID[p.UID]; !ok {
		return store.ErrNotFound
	}
	cp := *p
	f.byUID[p.UID] = &cp
	return nil
}
