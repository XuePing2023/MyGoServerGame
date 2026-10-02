package registry

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"servergame/internal/pkg/logger"
)

// Discovery 服务发现：监听 {prefix}/{type} 前缀下所有节点的上下线，
// 在内存中维护最新节点列表，并提供轮询负载选择器。
type Discovery struct {
	cli *clientv3.Client

	mu    sync.RWMutex
	nodes map[string]map[string]*NodeInfo // type -> nodeID -> node
	rr    atomic.Uint64                   // 轮询计数器

	onChange func(eventType string, node *NodeInfo)
}

// NewDiscovery 创建发现器并开始监听。
func NewDiscovery(cli *clientv3.Client, keyPrefix string, onChange func(eventType string, node *NodeInfo)) *Discovery {
	d := &Discovery{
		cli:      cli,
		nodes:    make(map[string]map[string]*NodeInfo),
		onChange: onChange,
	}
	go d.watchLoop(keyPrefix)
	return d
}

// Nodes 返回指定类型的节点快照（按 ID 排序，保证输出稳定）。
func (d *Discovery) Nodes(serviceType string) []*NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	byID := d.nodes[serviceType]
	out := make([]*NodeInfo, 0, len(byID))
	for _, n := range byID {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Pick 轮询选择一个指定类型的节点；没有可用节点返回 nil。
func (d *Discovery) Pick(serviceType string) *NodeInfo {
	nodes := d.Nodes(serviceType)
	if len(nodes) == 0 {
		return nil
	}
	return nodes[d.rr.Add(1)%uint64(len(nodes))]
}

// Types 当前已发现的全部服务类型。
func (d *Discovery) Types() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, 0, len(d.nodes))
	for t, m := range d.nodes {
		if len(m) > 0 {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

func (d *Discovery) watchLoop(keyPrefix string) {
	for {
		err := d.watchOnce(keyPrefix)
		if err == nil {
			return
		}
		logger.L.Warnf("etcd Watch 中断: %v，2 秒后重连", err)
		time.Sleep(2 * time.Second)
	}
}

// watchOnce 首次全量拉取后从当前 revision 开始增量监听。
func (d *Discovery) watchOnce(keyPrefix string) error {
	ctx := context.Background()

	// 1. 全量同步
	resp, err := d.cli.Get(ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("拉取节点列表失败: %w", err)
	}
	for _, kv := range resp.Kvs {
		d.applyPut(kv.Key, kv.Value)
	}
	logger.L.Infof("服务发现完成全量同步: 共 %d 个节点", len(resp.Kvs))

	// 2. 从全量 revision 之后开始监听，避免漏事件
	wch := d.cli.Watch(ctx, keyPrefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(resp.Header.Revision+1),
		clientv3.WithPrevKV())
	for wresp := range wch {
		if wresp.Err() != nil {
			return fmt.Errorf("watch 通道错误: %w", wresp.Err())
		}
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				d.applyPut(ev.Kv.Key, ev.Kv.Value)
			case clientv3.EventTypeDelete:
				d.applyDelete(ev.Kv.Key)
			}
		}
	}
	return fmt.Errorf("watch 通道被关闭")
}

func (d *Discovery) applyPut(key, value []byte) {
	node, err := ParseNode(value)
	if err != nil {
		logger.L.Warnf("忽略非法的节点数据 (key=%s): %v", key, err)
		return
	}
	d.mu.Lock()
	if d.nodes[node.Type] == nil {
		d.nodes[node.Type] = make(map[string]*NodeInfo)
	}
	d.nodes[node.Type][node.ID] = node
	count := len(d.nodes[node.Type])
	d.mu.Unlock()

	logger.L.Infof("节点上线: %s (type=%s, addr=%s)，该类型当前 %d 个节点", node.ID, node.Type, node.Addr(), count)
	if d.onChange != nil {
		d.onChange("PUT", node)
	}
}

func (d *Discovery) applyDelete(key []byte) {
	// key 形如 {prefix}/{type}/{id}
	s, id, ok := splitNodeKey(string(key))
	if !ok {
		logger.L.Debugf("忽略无法解析的下线节点 key: %s", key)
		return
	}
	d.mu.Lock()
	if byID := d.nodes[s]; byID != nil {
		delete(byID, id)
	}
	d.mu.Unlock()

	logger.L.Infof("节点下线: %s (type=%s)", id, s)
	if d.onChange != nil {
		d.onChange("DELETE", &NodeInfo{ID: id, Type: s})
	}
}

// splitNodeKey 从 {prefix}/{type}/{id} 中提取 type 与 id。
// 前缀可能包含多级目录，因此取最后两段。
func splitNodeKey(key string) (serviceType, nodeID string, ok bool) {
	// 找到最后一段
	last := len(key)
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			last = i
			break
		}
	}
	if last == len(key) {
		return "", "", false
	}
	nodeID = key[last+1:]

	prev := -1
	for i := last - 1; i >= 0; i-- {
		if key[i] == '/' {
			prev = i
			break
		}
	}
	if prev == last-1 {
		return "", "", false
	}
	serviceType = key[prev+1 : last]
	return serviceType, nodeID, true
}
