# ServerGame

开箱即用的 Go 游戏服务器骨架：**TCP + WebSocket 双入口、protobuf 协议、MongoDB 持久化、etcd 服务注册与发现**。

内置一套可玩的示例业务（游客/账号登录、玩家资料、世界聊天、双人对战房间——猜拳小游戏），所有底层设施（拆包、路由、会话、心跳踢线、自动落盘、优雅停机）均为生产可用写法，替换业务消息即可快速开发自己的游戏。

## 功能特性

- **双协议入口**：TCP（4 字节长度前缀帧）与 WebSocket（二进制帧），同一套路由与业务层
- **protobuf 协议**：`proto/game/v1/game.proto` 单一事实来源，`protoc` 一键生成 Go 代码
- **MongoDB 持久化**：账号（bcrypt 密码哈希）、玩家资料、自增 UID 序列；内存态 + 脏标记 + 周期落盘 + 下线即存
- **etcd 服务注册与发现**：TTL 租约自动续期、宕机自动摘除、Watch 前缀实时感知节点上下线、轮询负载选择器
- **连接治理**：心跳保活、空闲踢线、发送队列慢消费者踢线、顶号下线、掉线自动退房
- **运维调试 HTTP**：`/healthz`、`/debug/stats`、`/debug/nodes`、`/debug/broadcast`、`/debug/pprof`
- **优雅停机**：公告 → 踢线 → 落盘 → 注销 etcd → 断开 Mongo

## 目录结构

```
ServerGame/
├── cmd/server/          # 服务器启动入口
├── cmd/client/          # 演示客户端（交互式，展示协议用法）
├── proto/game/v1/       # 协议定义（单一事实来源）
├── pb/game/v1/          # protoc 生成的 Go 代码
├── internal/
│   ├── config/          # 配置加载（viper，环境变量前缀 SG_）
│   ├── errs/            # 业务错误（统一转 ErrorNotify）
│   ├── pkg/logger/      # zap 日志（控制台 + 文件轮转）
│   ├── pkg/randx/       # 随机工具
│   ├── protocol/        # 消息封装、帧编解码、消息路由
│   ├── server/          # 网络层：连接抽象、TCP、WebSocket、连接管理
│   ├── registry/        # etcd 注册（租约保活）与发现（Watch）
│   ├── database/        # Mongo 连接与索引
│   ├── model/           # 持久化结构
│   ├── store/           # 存储接口 + Mongo 实现（可替换为内存实现做测试）
│   └── game/            # 业务：会话、登录、玩家、聊天、房间/猜拳
├── config.yaml          # 配置文件
├── genproto.bat/.sh     # pb 代码再生成脚本
├── docker-compose.yml   # 一键拉起 mongo + etcd + 本服务
└── Makefile
```

## 快速开始

### 1. 准备依赖（二选一）

```bash
# 方式一：docker-compose 一键拉起全部（含本服务）
docker compose up -d

# 方式二：只拉起依赖，本地跑服务器
docker run -d -p 27017:27017 --name servergame-mongo mongo:7
docker run -d -p 2379:2379 --name servergame-etcd bitnami/etcd:3.6 \
  ALLOW_NONE_AUTHENTICATION=yes etcd \
  --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379
```

本地没有 etcd 时也可以禁用服务注册：`SG_ETCD_ENABLED=false go run ./cmd/server`（MongoDB 仍必需）。

### 2. 启动服务器

```bash
go run ./cmd/server
# 或
make run
```

启动后监听：TCP `:7001`、WebSocket `:7002`、调试 HTTP `127.0.0.1:7003`。

### 3. 用演示客户端试玩

```bash
go run ./cmd/client                # 默认 TCP
go run ./cmd/client -mode ws -addr 127.0.0.1:7002
```

交互命令：

```
/guest [设备号]         游客登录（首次自动建号）
/register 用户 密码     注册并登录
/login 用户 密码        账号登录
/me                    查询资料
/name 新昵称           改名
/chat 内容             世界频道发言
/create                创建房间
/join 房间号           加入房间
/leave                 离开房间
/rchat 内容            房间内发言
/rock /paper /scissors 猜拳出招（双人对局，双方出招即结算）
/quit                  退出
```

开两个客户端即可体验：A `/create` → B `/join 房间号` → 双方 `/rock` `/scissors` 出招结算，胜利加经验金币并自动升级。

## 通信协议

### 帧格式

```
TCP      ：| 4B 长度(大端) | 2B 消息ID(大端) | 消息体(protobuf) |
           长度 = 2 + 消息体字节数（不含长度域自身）
WebSocket：二进制帧 = | 2B 消息ID(大端) | 消息体(protobuf) |
```

- 消息 ID 与全部消息结构定义见 `proto/game/v1/game.proto`
- 服务器在连接建立后立即推送 `SERVER_INFO_NOTIFY`（名称/版本/建议心跳间隔/空闲踢线阈值/服务器时间）
- 请求失败统一收到 `ERROR_NOTIFY{code, msg, reqMsgId}`，错误码见 proto 中 `ErrCode`
- 单包上限默认 64KB（`app.maxPacketBytes`），超限断开

### 消息一览

| 分组 | 消息 | 说明 |
| --- | --- | --- |
| 系统 | `HEARTBEAT_REQ/ACK` | 心跳（建议 15s 一次，任何数据都会刷新活跃时间） |
| | `KICK_NOTIFY` / `ERROR_NOTIFY` | 踢下线 / 通用错误 |
| | `SERVER_INFO_NOTIFY` / `SERVER_BROADCAST_NOTIFY` / `ONLINE_COUNT_NOTIFY` | 服务器信息 / 全服公告 / 在线人数 |
| 账号 | `GUEST_LOGIN_REQ/RES` | 设备号登录，首次自动建号 |
| | `ACCOUNT_REGISTER_REQ/RES` | 注册（成功即登录，bcrypt 存储） |
| | `ACCOUNT_LOGIN_REQ/RES` | 账号登录 |
| 玩家 | `GET_PROFILE_REQ/RES`、`RENAME_REQ/RES` | 资料 / 改名（昵称全局唯一） |
| 聊天 | `WORLD_CHAT_REQ/RES/NOTIFY` | 世界频道（默认 1s 限流） |
| 房间 | `CREATE/JOIN/LEAVE_ROOM_*`、`ROOM_EVENT_NOTIFY` | 房间生命周期与人齐开局事件 |
| | `ROOM_CHAT_REQ/NOTIFY` | 房间内聊天 |
| 对战 | `RPS_PLAY_REQ/RES`、`RPS_ROUND_NOTIFY` | 猜拳出招、确认、双方视角结算（含奖励后资料） |

### 服务器处理模型

- 每个连接一个读协程 + 一个写协程，发送队列默认 256，**单连接消息串行处理**（天然有序，handler 无需对会话状态加锁——跨会话的状态修改除外，已内置锁）
- 发送队列打满判定为慢消费者，直接踢线
- 空闲超时（默认 60s 无任何数据）自动踢线
- 玩家数据内存可写 + 脏标记，默认 60s 自动落盘，掉线/顶号/停机即时落盘

## 服务注册与发现（etcd）

注册模型：

```
key   = /servergame/nodes/{serviceType}/{nodeID}
value = {"id","type","host","tcpPort","wsPort","weight","version","startedAt"}
```

- 本节点以 TTL 租约注册并自动续期；进程退出主动 Revoke；宕机后租约到期自动摘除
- etcd 重启等故障自动重连并重新注册
- `Discovery` 监听前缀，全量同步 + 增量 Watch，维护各类型节点的最新列表
- `discovery.Pick("game")` 提供轮询选择器，供网关/管理后台等消费方回连

查看当前节点：`curl http://127.0.0.1:7003/debug/nodes`

## 配置

优先级：环境变量（前缀 `SG_`，`.` 换 `_`）> `config.yaml` > 内置默认值。指定配置文件：`SG_CONFIG=/path/config.yaml`。

常用环境变量：

```bash
SG_APP_TCPPORT=7001            # TCP 端口
SG_APP_WSPORT=7002             # WebSocket 端口
SG_APP_HOST=10.0.0.5           # 对外宣告 IP（服务发现）
SG_MONGO_URI=mongodb://localhost:27017
SG_MONGO_DATABASE=server_game
SG_ETCD_ENABLED=false          # 关闭 etcd
SG_ETCD_ENDPOINTS=10.0.0.1:2379
SG_LOG_LEVEL=debug
SG_GAME_IDLETIMEOUTSECONDS=60
```

完整项与默认值见 `config.yaml` 与 `internal/config/config.go`。

## 调试端点（默认仅绑定 127.0.0.1）

| 端点 | 说明 |
| --- | --- |
| `GET /healthz` | 存活探针 |
| `GET /debug/stats` | 连接数/会话数/房间数/路由表 |
| `GET /debug/nodes` | 已发现的全部服务节点 |
| `POST /debug/broadcast?text=公告` | 全服公告（也支持 JSON body `{"text":"..."}`） |
| `/debug/pprof/` | 性能剖析 |

## 开发

```bash
make test        # 单元测试（编解码/路由/业务全流程，内存存储，无需数据库）
make vet         # 静态检查
make build       # 构建 bin/servergame.exe 与演示客户端
make genproto    # 修改 proto 后重新生成（需 protoc 与 protoc-gen-go）
```

测试不依赖外部服务：`internal/game` 的用例通过内存版 store 与 fake 连接驱动真实路由，覆盖登录/改名/聊天/限流/建房/对战/掉线退房/顶号等全流程。

### 新增一条消息的步骤

1. `proto/game/v1/game.proto` 增加 `MsgID` 枚举值与 message，运行 `make genproto`
2. 在 `internal/game/` 写 handler：`func (g *Game) onXxx(s *Session, req *gamepb.XxxReq) error`
3. 在 `registerRoutes()` 注册：无需登录用 `regPublic(g, id, g.onXxx)`，需登录用 `g.regAuth(id, g.onXxx)`
4. 返回 `error` 会自动转换成 `ERROR_NOTIFY` 下发

## 部署

```bash
make build-linux        # 产出 bin/servergame-linux
./servergame-linux      # 读取同目录 config.yaml，或用 SG_ 环境变量注入
```

容器化：`docker compose up -d --build`（内含 mongo、etcd 与本服务）。

## 已知取舍（生产化提示）

- 顶号瞬间旧连接的未落盘修改存在极小的丢失窗口；生产可在登录路径引入串行化队列
- 演示房间/猜拳为单节点内存实现；多节点对战需将房间状态下沉到共享存储或引入房间服务与转发层
- etcd Discovery 的 Watch 断线重连间隔固定 2s，可按需加入退避
- 密码哈希使用 bcrypt 默认成本；对外网关建议在登录消息外再加 TLS/证书校验
