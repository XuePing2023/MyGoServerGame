// Package registry 基于 etcd 实现服务注册（租约保活）与服务发现（前缀监听）。
//
// 注册模型：
//
//	key   = {prefix}/{serviceType}/{nodeID}
//	value = NodeInfo 的 JSON
//
// 每个节点持有 TTL 租约并自动续期；节点宕机后租约到期，key 被自动删除，
// 其他节点通过 Watch 感知到下线。
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"servergame/internal/pkg/logger"
)

// NodeInfo 注册到 etcd 的节点描述。
type NodeInfo struct {
	ID        string `json:"id"`        // 节点唯一 ID
	Type      string `json:"type"`      // 服务类型，如 "game"
	Host      string `json:"host"`      // 对外可达 IP
	TCPPort   int    `json:"tcpPort"`   // TCP 端口（0 表示未开放）
	WSPort    int    `json:"wsPort"`    // WebSocket 端口（0 表示未开放）
	Weight    int    `json:"weight"`    // 负载权重（保留字段）
	Version   string `json:"version"`   // 服务版本
	StartedAt int64  `json:"startedAt"` // 启动毫秒时间戳
}

// Addr 返回节点的主机:端口 形式（优先 TCP，其次 WS）。
func (n *NodeInfo) Addr() string {
	if n.TCPPort > 0 {
		return fmt.Sprintf("%s:%d", n.Host, n.TCPPort)
	}
	if n.WSPort > 0 {
		return fmt.Sprintf("%s:%d", n.Host, n.WSPort)
	}
	return n.Host
}

// ParseNode 从 etcd 的 value 解析节点信息。
func ParseNode(data []byte) (*NodeInfo, error) {
	var n NodeInfo
	if err := json.Unmarshal(data, &n); err != nil {
		return nil, err
	}
	return &n, nil
}

// Registry etcd 注册客户端：负责本节点的注册与保活。
type Registry struct {
	cli        *clientv3.Client
	keyPrefix  string
	node       NodeInfo
	leaseID    clientv3.LeaseID
	cancelKeep context.CancelFunc
	stopOnce   sync.Once
	done       chan struct{}
}

// New 创建 etcd 客户端并校验连通性。
func New(ctx context.Context, endpoints []string, dialTimeout time.Duration, username, password string) (*Registry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		Username:    username,
		Password:    password,
		// 连接类错误由本模块自行记录，关闭 etcd 客户端的内部日志
		Logger: zap.NewNop(),
	})
	if err != nil {
		return nil, fmt.Errorf("创建 etcd 客户端失败: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	if _, err := cli.Status(pingCtx, endpoints[0]); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("etcd 连接失败（endpoint: %s）: %w", endpoints[0], err)
	}
	return &Registry{cli: cli, done: make(chan struct{})}, nil
}

// Register 注册节点：写入 key 并绑定 TTL 租约，后台协程自动续租。
// 期间 etcd 短暂不可用会自动重试；调用 Stop 后注销。
func (r *Registry) Register(ctx context.Context, keyPrefix string, node NodeInfo, ttlSeconds int) error {
	r.keyPrefix = keyPrefix
	r.node = node

	value, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("序列化节点信息失败: %w", err)
	}
	if err := r.putWithNewLease(ctx, value, ttlSeconds); err != nil {
		return err
	}

	keepCtx, cancel := context.WithCancel(context.Background())
	r.cancelKeep = cancel
	go r.keepAliveLoop(keepCtx, r.nodeKey(), value, ttlSeconds)

	logger.L.Infof("已注册到 etcd: %s (ttl=%ds)", r.nodeKey(), ttlSeconds)
	return nil
}

func (r *Registry) putWithNewLease(ctx context.Context, value []byte, ttlSeconds int) error {
	grant, err := r.cli.Grant(ctx, int64(ttlSeconds))
	if err != nil {
		return fmt.Errorf("申请 etcd 租约失败: %w", err)
	}
	r.leaseID = grant.ID
	if _, err := r.cli.Put(ctx, r.nodeKey(), string(value), clientv3.WithLease(grant.ID)); err != nil {
		return fmt.Errorf("写入 etcd 失败 (key=%s): %w", r.nodeKey(), err)
	}
	return nil
}

// keepAliveLoop 持续续租；续租通道意外关闭（如 etcd 重启）时延迟后重新注册。
func (r *Registry) keepAliveLoop(ctx context.Context, key string, value []byte, ttlSeconds int) {
	defer close(r.done)
	for {
		r.keepAliveOnce(ctx)

		// 走到这里说明续租结束（ctx 取消或通道关闭）
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		if err := r.putWithNewLease(ctx, value, ttlSeconds); err != nil {
			logger.L.Warnf("etcd 重新注册失败: %v", err)
			continue
		}
		logger.L.Info("etcd 节点信息已重新注册")
	}
}

// keepAliveOnce 阻塞续租直到通道关闭或 ctx 取消。
func (r *Registry) keepAliveOnce(ctx context.Context) {
	ch, err := r.cli.KeepAlive(ctx, r.leaseID)
	if err != nil {
		logger.L.Warnf("etcd 续租失败: %v", err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ch:
			if !ok {
				logger.L.Warn("etcd 续租通道关闭，准备重新注册")
				return
			}
			// 收到续租响应即代表租约有效
		}
	}
}

// Client 暴露底层 etcd 客户端（服务发现器与注册器共用一个连接）。
func (r *Registry) Client() *clientv3.Client { return r.cli }

// Stop 注销节点：撤销租约并关闭 etcd 客户端。
func (r *Registry) Stop() {
	r.stopOnce.Do(func() {
		if r.cancelKeep != nil {
			r.cancelKeep()
			select {
			case <-r.done:
			case <-time.After(2 * time.Second):
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if r.leaseID != 0 {
			if _, err := r.cli.Revoke(ctx, r.leaseID); err != nil {
				logger.L.Warnf("撤销 etcd 租约失败: %v", err)
			}
		}
		if err := r.cli.Close(); err != nil {
			logger.L.Warnf("关闭 etcd 客户端失败: %v", err)
		}
		logger.L.Info("已从 etcd 注销")
	})
}

func (r *Registry) nodeKey() string {
	return fmt.Sprintf("%s/%s/%s", r.keyPrefix, r.node.Type, r.node.ID)
}
