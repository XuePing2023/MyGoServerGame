// ServerGame 游戏服务器启动入口。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof" // 注册 pprof 处理器到默认 mux
	"os"
	"os/signal"
	"syscall"
	"time"

	"servergame/internal/config"
	"servergame/internal/database"
	"servergame/internal/game"
	"servergame/internal/pkg/logger"
	"servergame/internal/registry"
	"servergame/internal/server"
	"servergame/internal/store"
)

func main() {
	cfg := config.Load()

	// 目录准备与日志初始化
	if dir := cfg.LogDir(); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	if err := logger.Init(cfg.Log.Level, cfg.Log.File, cfg.Log.MaxSizeMB, cfg.Log.MaxBackups, cfg.Log.MaxAgeDays); err != nil {
		fmt.Println("初始化日志失败:", err)
		os.Exit(1)
	}
	logger.L.Infof("正在启动 %s v%s (env=%s, nodeID=%s) ...", cfg.App.Name, cfg.App.Version, cfg.App.Env, cfg.NodeID())

	// MongoDB
	bg := context.Background()
	client, db, err := database.Connect(bg, cfg)
	if err != nil {
		logger.L.Fatalf("%v\n提示：请确认 MongoDB 已启动（本地可执行: docker run -d -p 27017:27017 --name servergame-mongo mongo:7）", err)
	}
	defer func() { database.Disconnect(context.Background(), client) }()
	logger.L.Infof("MongoDB 已连接: %s / 库 %s", cfg.Mongo.URI, cfg.Mongo.Database)

	ctxIdx, cancelIdx := context.WithTimeout(bg, cfg.MongoTimeout())
	err = database.EnsureIndexes(ctxIdx, db)
	cancelIdx()
	if err != nil {
		logger.L.Fatalf("初始化索引失败: %v", err)
	}

	// 存储层与游戏主体
	accounts, players := store.NewMongo(db)
	g := game.New(cfg, accounts, players)

	// etcd 服务注册与发现
	var reg *registry.Registry
	var disc *registry.Discovery
	if cfg.Etcd.Enabled {
		reg, err = registry.New(bg, cfg.Etcd.Endpoints,
			time.Duration(cfg.Etcd.DialTimeout)*time.Second,
			cfg.Etcd.Username, cfg.Etcd.Password)
		if err != nil {
			logger.L.Fatalf("%v\n提示：请确认 etcd 已启动（本地可执行: docker run -d -p 2379:2379 --name servergame-etcd bitnami/etcd:3.6 etcd --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379）", err)
		}
		node := registry.NodeInfo{
			ID:        cfg.NodeID(),
			Type:      "game",
			Host:      cfg.AdvertiseHost(),
			TCPPort:   cfg.App.TCPPort,
			WSPort:    cfg.App.WSPort,
			Weight:    1,
			Version:   cfg.App.Version,
			StartedAt: time.Now().UnixMilli(),
		}
		if err := reg.Register(bg, cfg.Etcd.RegisterKeyPfx, node, cfg.Etcd.RegisterTTL); err != nil {
			logger.L.Fatalf("注册到 etcd 失败: %v", err)
		}
		disc = registry.NewDiscovery(reg.Client(), cfg.Etcd.RegisterKeyPfx, nil)
		logger.L.Infof("服务发现已启动，监听前缀: %s", cfg.Etcd.RegisterKeyPfx)
	}

	// 网络服务
	if cfg.App.TCPPort > 0 {
		tcpSrv := server.NewTCPServer(fmt.Sprintf(":%d", cfg.App.TCPPort), g.ConnConfig())
		if err := tcpSrv.Start(); err != nil {
			logger.L.Fatalf("TCP 服务启动失败: %v", err)
		}
		defer tcpSrv.Stop()
	}
	if cfg.App.WSPort > 0 {
		wsSrv := server.NewWSServer(fmt.Sprintf(":%d", cfg.App.WSPort), g.ConnConfig())
		if err := wsSrv.Start(); err != nil {
			logger.L.Fatalf("WebSocket 服务启动失败: %v", err)
		}
		defer wsSrv.Stop()
	}

	// 调试 HTTP
	if cfg.Debug.Enabled {
		dbgSrv := startDebugHTTP(cfg, g, disc)
		defer func() {
			ctx, cancel := context.WithTimeout(bg, 3*time.Second)
			defer cancel()
			_ = dbgSrv.Shutdown(ctx)
		}()
	}

	// 游戏后台协程
	g.Start()

	// 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.L.Info("收到退出信号，正在优雅停机 ...")

	// 1. 踢掉全部连接（下发维护通知）
	g.BroadcastAnnounce("服务器即将维护，请稍后重新登录")
	g.KickAll("服务器维护")
	// 2. 停后台协程并落盘会话数据
	g.Stop()
	// 3. 注销 etcd（defer 顺序处理网络监听与 Mongo）
	if reg != nil {
		reg.Stop()
	}
	logger.L.Info("服务已退出")
	logger.Sync()
}

// startDebugHTTP 启动运维调试 HTTP（默认只绑回环地址）。
func startDebugHTTP(cfg *config.Config, g *game.Game, disc *registry.Discovery) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/debug/stats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, g.Stats())
	})

	mux.HandleFunc("/debug/nodes", func(w http.ResponseWriter, _ *http.Request) {
		if disc == nil {
			writeJSON(w, map[string]any{"etcd": "disabled"})
			return
		}
		out := make(map[string][]*registry.NodeInfo, 4)
		for _, t := range disc.Types() {
			out[t] = disc.Nodes(t)
		}
		writeJSON(w, out)
	})

	mux.HandleFunc("/debug/broadcast", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		text := r.FormValue("text")
		if text == "" {
			var body struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
				text = body.Text
			}
		}
		if text == "" {
			http.Error(w, "text required", http.StatusBadRequest)
			return
		}
		g.BroadcastAnnounce(text)
		writeJSON(w, map[string]any{"ok": true})
	})

	// pprof：/debug/pprof/
	mux.Handle("/debug/pprof/", http.DefaultServeMux)

	addr := fmt.Sprintf("%s:%d", cfg.Debug.Host, cfg.Debug.Port)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.L.Infof("调试 HTTP 已启动: http://%s (healthz / debug/stats / debug/nodes / debug/broadcast / debug/pprof)", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.L.Errorf("调试 HTTP 异常退出: %v", err)
		}
	}()
	return srv
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}
