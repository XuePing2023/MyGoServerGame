// Package config 负责加载与暴露服务器配置。
// 优先级：环境变量（SG_ 前缀） > config.yaml > 内置默认值，
// 任何一层缺失都能回退默认值，保证“零配置”也能启动。
package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"

	"servergame/internal/pkg/randx"
)

type Config struct {
	App   AppConfig   `mapstructure:"app"`
	Log   LogConfig   `mapstructure:"log"`
	Mongo MongoConfig `mapstructure:"mongo"`
	Etcd  EtcdConfig  `mapstructure:"etcd"`
	Game  GameConfig  `mapstructure:"game"`
	Debug DebugConfig `mapstructure:"debug"`
}

// AppConfig 进程与网络入口配置。
type AppConfig struct {
	Name           string `mapstructure:"name"`
	Version        string `mapstructure:"version"`
	Env            string `mapstructure:"env"`
	NodeID         string `mapstructure:"nodeID"`         // 注册到 etcd 的节点 ID，留空自动生成
	Host           string `mapstructure:"host"`           // 对外宣告的 IP（服务发现用），留空自动探测
	TCPPort        int    `mapstructure:"tcpPort"`        // TCP 监听端口，0 表示不启用
	WSPort         int    `mapstructure:"wsPort"`         // WebSocket 监听端口，0 表示不启用
	MaxPacketBytes int    `mapstructure:"maxPacketBytes"` // 单包大小上限（含消息头）
	SendChanSize   int    `mapstructure:"sendChanSize"`   // 每连接发送队列长度，满了踢慢消费者
}

type LogConfig struct {
	Level      string `mapstructure:"level"`
	File       string `mapstructure:"file"`
	MaxSizeMB  int    `mapstructure:"maxSizeMB"`
	MaxBackups int    `mapstructure:"maxBackups"`
	MaxAgeDays int    `mapstructure:"maxAgeDays"`
}

type MongoConfig struct {
	URI            string `mapstructure:"uri"`
	Database       string `mapstructure:"database"`
	TimeoutSeconds int    `mapstructure:"timeoutSeconds"`
}

// EtcdConfig 服务注册与发现。
type EtcdConfig struct {
	Enabled        bool     `mapstructure:"enabled"`
	Endpoints      []string `mapstructure:"endpoints"`
	DialTimeout    int      `mapstructure:"dialTimeoutSeconds"`
	Username       string   `mapstructure:"username"`
	Password       string   `mapstructure:"password"`
	RegisterTTL    int      `mapstructure:"registerTTLSeconds"` // 租约 TTL，超时未续约自动摘除
	RegisterKeyPfx string   `mapstructure:"registerKeyPrefix"`  // 注册 key 前缀
}

// GameConfig 业务行为参数。
type GameConfig struct {
	HeartbeatIntervalMs int   `mapstructure:"heartbeatIntervalMs"` // 建议客户端的心跳间隔
	IdleTimeoutSeconds  int   `mapstructure:"idleTimeoutSeconds"`  // 连续无数据的踢线阈值
	AutosaveSeconds     int   `mapstructure:"autosaveSeconds"`     // 脏数据自动落盘周期
	ChatIntervalMs      int   `mapstructure:"chatIntervalMs"`      // 世界/房间聊天最小发言间隔
	MaxContentLength    int   `mapstructure:"maxContentLength"`    // 聊天内容最大长度
	MaxRooms            int   `mapstructure:"maxRooms"`            // 房间总数上限
	RewardWinExp        int64 `mapstructure:"rewardWinExp"`        // 猜拳胜利奖励经验
	RewardWinGold       int64 `mapstructure:"rewardWinGold"`       // 猜拳胜利奖励金币
	RewardLoseExp       int64 `mapstructure:"rewardLoseExp"`
	RewardLoseGold      int64 `mapstructure:"rewardLoseGold"`
	RewardDrawExp       int64 `mapstructure:"rewardDrawExp"`
	RewardDrawGold      int64 `mapstructure:"rewardDrawGold"`
}

// DebugConfig 运维调试 HTTP。
type DebugConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Host    string `mapstructure:"host"` // 默认只绑回环地址
	Port    int    `mapstructure:"port"`
}

// Load 按 SG_CONFIG 指定路径或工作目录下 config.yaml 加载配置。
func Load() *Config {
	v := viper.New()
	setDefaults(v)

	v.SetEnvPrefix("SG")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	path := os.Getenv("SG_CONFIG")
	if path == "" {
		v.AddConfigPath(".")
		v.SetConfigName("config")
		v.SetConfigType("yaml")
	} else {
		v.SetConfigFile(path)
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok && path != "" {
			// 显式指定的配置文件读取失败时直接报错；缺省文件则使用默认值
			if !os.IsNotExist(err) {
				panic("读取配置文件失败: " + err.Error())
			}
		}
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		panic("解析配置失败: " + err.Error())
	}
	cfg.normalize()
	return cfg
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.name", "ServerGame")
	v.SetDefault("app.version", "1.0.0")
	v.SetDefault("app.env", "dev")
	v.SetDefault("app.nodeID", "")
	v.SetDefault("app.host", "")
	v.SetDefault("app.tcpPort", 7001)
	v.SetDefault("app.wsPort", 7002)
	v.SetDefault("app.maxPacketBytes", 65536)
	v.SetDefault("app.sendChanSize", 256)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.file", "logs/server.log")
	v.SetDefault("log.maxSizeMB", 100)
	v.SetDefault("log.maxBackups", 7)
	v.SetDefault("log.maxAgeDays", 30)

	v.SetDefault("mongo.uri", "mongodb://localhost:27017")
	v.SetDefault("mongo.database", "server_game")
	v.SetDefault("mongo.timeoutSeconds", 10)

	v.SetDefault("etcd.enabled", true)
	v.SetDefault("etcd.endpoints", []string{"localhost:2379"})
	v.SetDefault("etcd.dialTimeoutSeconds", 5)
	v.SetDefault("etcd.username", "")
	v.SetDefault("etcd.password", "")
	v.SetDefault("etcd.registerTTLSeconds", 15)
	v.SetDefault("etcd.registerKeyPrefix", "/servergame/nodes")

	v.SetDefault("game.heartbeatIntervalMs", 15000)
	v.SetDefault("game.idleTimeoutSeconds", 60)
	v.SetDefault("game.autosaveSeconds", 60)
	v.SetDefault("game.chatIntervalMs", 1000)
	v.SetDefault("game.maxContentLength", 200)
	v.SetDefault("game.maxRooms", 1000)
	v.SetDefault("game.rewardWinExp", 50)
	v.SetDefault("game.rewardWinGold", 100)
	v.SetDefault("game.rewardLoseExp", 10)
	v.SetDefault("game.rewardLoseGold", 10)
	v.SetDefault("game.rewardDrawExp", 25)
	v.SetDefault("game.rewardDrawGold", 30)

	v.SetDefault("debug.enabled", true)
	v.SetDefault("debug.host", "127.0.0.1")
	v.SetDefault("debug.port", 7003)
}

func (c *Config) normalize() {
	if c.App.Name == "" {
		c.App.Name = "ServerGame"
	}
	if c.App.TCPPort < 0 {
		c.App.TCPPort = 0
	}
	if c.App.WSPort < 0 {
		c.App.WSPort = 0
	}
	if c.App.TCPPort == 0 && c.App.WSPort == 0 {
		// 两个入口都关了就没有服务了，强制保留 TCP
		c.App.TCPPort = 7001
	}
	if c.App.MaxPacketBytes < 1024 {
		c.App.MaxPacketBytes = 65536
	}
	if c.App.SendChanSize <= 0 {
		c.App.SendChanSize = 256
	}
	if c.Mongo.TimeoutSeconds <= 0 {
		c.Mongo.TimeoutSeconds = 10
	}
	if c.Etcd.DialTimeout <= 0 {
		c.Etcd.DialTimeout = 5
	}
	if c.Etcd.RegisterTTL < 5 {
		c.Etcd.RegisterTTL = 15
	}
	if c.Etcd.RegisterKeyPfx == "" {
		c.Etcd.RegisterKeyPfx = "/servergame/nodes"
	}
	if c.Game.IdleTimeoutSeconds < 15 {
		c.Game.IdleTimeoutSeconds = 60
	}
	if c.Game.AutosaveSeconds <= 0 {
		c.Game.AutosaveSeconds = 60
	}
	if c.Game.ChatIntervalMs < 0 {
		c.Game.ChatIntervalMs = 1000
	}
	if c.Game.MaxContentLength <= 0 {
		c.Game.MaxContentLength = 200
	}
	if c.Game.MaxRooms <= 0 {
		c.Game.MaxRooms = 1000
	}
	if c.Debug.Port <= 0 {
		c.Debug.Enabled = false
	}
	if c.Log.File == "" {
		c.Log.File = "logs/server.log"
	}
}

// IsProd 是否生产环境。
func (c *Config) IsProd() bool { return strings.EqualFold(c.App.Env, "prod") }

// MongoTimeout 数据库操作超时时间。
func (c *Config) MongoTimeout() time.Duration {
	return time.Duration(c.Mongo.TimeoutSeconds) * time.Second
}

// IdleTimeout 连接空闲踢线阈值。
func (c *Config) IdleTimeout() time.Duration {
	return time.Duration(c.Game.IdleTimeoutSeconds) * time.Second
}

// AdvertiseHost 返回对外宣告的主机地址：显式配置 > 出口 IP 探测 > 127.0.0.1。
func (c *Config) AdvertiseHost() string {
	if c.App.Host != "" {
		return c.App.Host
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer func() { _ = conn.Close() }()
		if ip := conn.LocalAddr().(*net.UDPAddr).IP; !ip.IsLoopback() {
			return ip.String()
		}
	}
	return "127.0.0.1"
}

// NodeID 服务节点 ID：优先取配置，否则按 主机名-随机 生成。
func (c *Config) NodeID() string {
	if c.App.NodeID != "" {
		return c.App.NodeID
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%s", host, randx.RandomHex(3))
}

// LogDir 配置中日志文件的目录（可能是空串表示当前目录）。
func (c *Config) LogDir() string {
	return filepath.Dir(c.Log.File)
}
