// ServerGame 演示客户端：展示协议帧用法（TCP / WebSocket 均支持）。
//
// 用法：
//
//	go run ./cmd/client -mode tcp  -addr 127.0.0.1:7001
//	go run ./cmd/client -mode ws   -addr 127.0.0.1:7002
//
// 交互命令：
//
//	/guest [设备号]        游客登录
//	/register 用户 密码    注册并登录
//	/login 用户 密码       账号登录
//	/me                    查询资料
//	/name 新昵称           改名
//	/chat 内容             世界频道发言
//	/create                创建房间
//	/join 房间号           加入房间
//	/leave                 离开房间
//	/rchat 内容            房间内发言
//	/rock /paper /scissors 猜拳出招
//	/quit                  退出
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"servergame/internal/protocol"
	gamepb "servergame/pb/game/v1"
)

var myNickname = "（未登录）"

func main() {
	mode := flag.String("mode", "tcp", "连接方式: tcp | ws")
	addr := flag.String("addr", "127.0.0.1:7001", "服务器地址")
	flag.Parse()

	c, err := dial(*mode, *addr)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	fmt.Printf("已连接到 %s (%s)\n", *addr, *mode)

	done := make(chan struct{})
	// 读协程：解析并打印服务端推送
	go func() {
		defer close(done)
		for {
			m, err := c.read()
			if err != nil {
				fmt.Println("连接断开:", err)
				return
			}
			handlePush(m)
		}
	}()

	// 心跳协程
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = c.send(uint16(gamepb.MsgID_HEARTBEAT_REQ), &gamepb.HeartbeatReq{ClientTime: time.Now().UnixMilli()})
			}
		}
	}()

	// 交互循环
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)
	fmt.Println("输入 /guest 开始游客登录，/help 查看命令")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if !handleCommand(c, line) {
			break
		}
	}
	_ = c.close()
}

// ---------------------------------------------------------------------------
// 连接抽象：对 TCP 与 WS 统一 read/send/close
// ---------------------------------------------------------------------------

type clientConn struct {
	mode string
	raw  net.Conn
	ws   *websocket.Conn
}

func dial(mode, addr string) (*clientConn, error) {
	switch mode {
	case "ws":
		d := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
		ws, _, err := d.Dial("ws://"+addr, nil)
		if err != nil {
			return nil, err
		}
		return &clientConn{mode: "ws", ws: ws}, nil
	default:
		raw, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		return &clientConn{mode: "tcp", raw: raw}, nil
	}
}

func (c *clientConn) read() (*protocol.Message, error) {
	if c.mode == "ws" {
		mt, data, err := c.ws.ReadMessage()
		if err != nil {
			return nil, err
		}
		if mt != websocket.BinaryMessage {
			return nil, fmt.Errorf("收到了非二进制帧")
		}
		return protocol.ParseWSFrame(data, 1<<20)
	}
	return protocol.ReadFrame(c.raw, 1<<20)
}

func (c *clientConn) send(id uint16, m proto.Message) error {
	pm, err := protocol.NewMessage(id, m)
	if err != nil {
		return err
	}
	if c.mode == "ws" {
		_ = c.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return c.ws.WriteMessage(websocket.BinaryMessage, pm.PackWS())
	}
	_, err = c.raw.Write(pm.PackTCP())
	return err
}

func (c *clientConn) close() error {
	if c.mode == "ws" {
		return c.ws.Close()
	}
	return c.raw.Close()
}

// ---------------------------------------------------------------------------
// 命令处理
// ---------------------------------------------------------------------------

// handleCommand 执行一条输入命令；返回 false 表示退出。
func handleCommand(c *clientConn, line string) bool {
	cmd, args := splitCmd(line)
	switch cmd {
	case "/help":
		printHelp()
	case "/quit", "/exit":
		return false
	case "/guest":
		device := "demo-device-0001"
		if len(args) > 0 {
			device = args[0]
		}
		must(c.send(uint16(gamepb.MsgID_GUEST_LOGIN_REQ), &gamepb.GuestLoginReq{DeviceId: device}))
	case "/register":
		if len(args) < 2 {
			fmt.Println("用法: /register 用户名 密码")
			return true
		}
		must(c.send(uint16(gamepb.MsgID_ACCOUNT_REGISTER_REQ), &gamepb.AccountRegisterReq{Username: args[0], Password: args[1]}))
	case "/login":
		if len(args) < 2 {
			fmt.Println("用法: /login 用户名 密码")
			return true
		}
		must(c.send(uint16(gamepb.MsgID_ACCOUNT_LOGIN_REQ), &gamepb.AccountLoginReq{Username: args[0], Password: args[1]}))
	case "/me":
		must(c.send(uint16(gamepb.MsgID_GET_PROFILE_REQ), &gamepb.Empty{}))
	case "/name":
		if len(args) < 1 {
			fmt.Println("用法: /name 新昵称")
			return true
		}
		must(c.send(uint16(gamepb.MsgID_RENAME_REQ), &gamepb.RenameReq{Nickname: strings.Join(args, " ")}))
	case "/chat":
		must(c.send(uint16(gamepb.MsgID_WORLD_CHAT_REQ), &gamepb.WorldChatReq{Content: strings.Join(args, " ")}))
	case "/create":
		must(c.send(uint16(gamepb.MsgID_CREATE_ROOM_REQ), &gamepb.Empty{}))
	case "/join":
		if len(args) < 1 {
			fmt.Println("用法: /join 房间号")
			return true
		}
		var roomID int64
		if _, err := fmt.Sscanf(args[0], "%d", &roomID); err != nil {
			fmt.Println("房间号需为数字")
			return true
		}
		must(c.send(uint16(gamepb.MsgID_JOIN_ROOM_REQ), &gamepb.JoinRoomReq{RoomId: roomID}))
	case "/leave":
		must(c.send(uint16(gamepb.MsgID_LEAVE_ROOM_REQ), &gamepb.Empty{}))
	case "/rchat":
		must(c.send(uint16(gamepb.MsgID_ROOM_CHAT_REQ), &gamepb.RoomChatReq{Content: strings.Join(args, " ")}))
	case "/rock":
		play(c, gamepb.RpsPlayReq_ROCK)
	case "/paper":
		play(c, gamepb.RpsPlayReq_PAPER)
	case "/scissors":
		play(c, gamepb.RpsPlayReq_SCISSORS)
	default:
		fmt.Println("未知命令，/help 查看帮助")
	}
	return true
}

func play(c *clientConn, move gamepb.RpsPlayReq_Move) {
	must(c.send(uint16(gamepb.MsgID_RPS_PLAY_REQ), &gamepb.RpsPlayReq{Move: move}))
}

func splitCmd(line string) (string, []string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", nil
	}
	return parts[0], parts[1:]
}

func must(err error) {
	if err != nil {
		fmt.Println("发送失败:", err)
	}
}

func printHelp() {
	fmt.Println(`/guest [设备号]         游客登录
/register 用户 密码     注册并登录
/login 用户 密码        账号登录
/me                     查询资料
/name 新昵称            改名
/chat 内容              世界频道发言
/create                 创建房间
/join 房间号            加入房间
/leave                  离开房间
/rchat 内容             房间内发言
/rock /paper /scissors  猜拳出招
/quit                   退出`)
}

// ---------------------------------------------------------------------------
// 服务端消息打印
// ---------------------------------------------------------------------------

func handlePush(m *protocol.Message) {
	switch gamepb.MsgID(m.ID) {
	case gamepb.MsgID_HEARTBEAT_ACK:
		// 心跳静默
	case gamepb.MsgID_SERVER_INFO_NOTIFY:
		var msg gamepb.ServerInfoNotify
		if ok(m, &msg) {
			fmt.Printf("[服务器] %s v%s（建议心跳 %dms，空闲踢线 %dms）\n",
				msg.Name, msg.Version, msg.HeartbeatIntervalMs, msg.IdleTimeoutMs)
		}
	case gamepb.MsgID_ONLINE_COUNT_NOTIFY:
		var msg gamepb.OnlineCountNotify
		if ok(m, &msg) {
			fmt.Printf("[在线] 当前 %d 人\n", msg.Online)
		}
	case gamepb.MsgID_GUEST_LOGIN_RES, gamepb.MsgID_ACCOUNT_LOGIN_RES:
		var msg gamepb.LoginRes
		if ok(m, &msg) {
			myNickname = msg.Profile.Nickname
			fmt.Printf("[登录成功] %s 昵称=%s 等级=%d 金币=%d (uid=%d)\n",
				isNewText(msg.IsNew), msg.Profile.Nickname, msg.Profile.Level, msg.Profile.Gold, msg.Profile.Uid)
		}
	case gamepb.MsgID_ACCOUNT_REGISTER_RES:
		var msg gamepb.AccountRegisterRes
		if ok(m, &msg) {
			myNickname = msg.Profile.Nickname
			fmt.Printf("[注册成功] %s (uid=%d)，已自动登录\n", msg.Username, msg.Uid)
		}
	case gamepb.MsgID_GET_PROFILE_RES:
		var msg gamepb.GetProfileRes
		if ok(m, &msg) {
			p := msg.Profile
			fmt.Printf("[资料] %s Lv.%d 经验=%d 金币=%d 钻石=%d 胜/负/平=%d/%d/%d\n",
				p.Nickname, p.Level, p.Exp, p.Gold, p.Diamond, p.WinCount, p.LoseCount, p.DrawCount)
		}
	case gamepb.MsgID_RENAME_RES:
		var msg gamepb.RenameRes
		if ok(m, &msg) {
			myNickname = msg.Nickname
			fmt.Printf("[改名成功] 新昵称: %s\n", msg.Nickname)
		}
	case gamepb.MsgID_WORLD_CHAT_RES:
		fmt.Println("[世界] 发言已送达")
	case gamepb.MsgID_WORLD_CHAT_NOTIFY:
		var msg gamepb.WorldChatNotify
		if ok(m, &msg) {
			fmt.Printf("[世界] %s: %s\n", msg.Nickname, msg.Content)
		}
	case gamepb.MsgID_CREATE_ROOM_RES:
		var msg gamepb.CreateRoomRes
		if ok(m, &msg) {
			fmt.Printf("[房间] 创建成功，房间号 %d，等待对手加入（/join %d）\n", msg.RoomId, msg.RoomId)
		}
	case gamepb.MsgID_JOIN_ROOM_RES:
		var msg gamepb.JoinRoomRes
		if ok(m, &msg) {
			fmt.Printf("[房间] 已加入 %d，成员: %s\n", msg.RoomId, memberNames(msg.Members))
		}
	case gamepb.MsgID_LEAVE_ROOM_RES:
		var msg gamepb.LeaveRoomRes
		if ok(m, &msg) {
			fmt.Printf("[房间] 已离开 %d\n", msg.RoomId)
		}
	case gamepb.MsgID_ROOM_EVENT_NOTIFY:
		var msg gamepb.RoomEventNotify
		if ok(m, &msg) {
			fmt.Printf("[房间 %d] %s（成员: %s）\n", msg.RoomId, msg.Text, memberNames(msg.Members))
		}
	case gamepb.MsgID_ROOM_CHAT_NOTIFY:
		var msg gamepb.RoomChatNotify
		if ok(m, &msg) {
			fmt.Printf("[房间] %s: %s\n", msg.Nickname, msg.Content)
		}
	case gamepb.MsgID_RPS_PLAY_RES:
		var msg gamepb.RpsPlayRes
		if ok(m, &msg) {
			if msg.Accepted {
				fmt.Println("[猜拳] 出招成功，等待对方…")
			} else {
				fmt.Println("[猜拳] 出招未被接受")
			}
		}
	case gamepb.MsgID_RPS_ROUND_NOTIFY:
		var msg gamepb.RpsRoundNotify
		if ok(m, &msg) {
			fmt.Printf("[猜拳] 我出 %s / 对方出 %s -> %s %s\n",
				moveName(msg.MyMove), moveName(msg.OppMove), outcomeName(msg.Outcome), msg.ExtraMsg)
			if msg.Profile != nil {
				fmt.Printf("       当前: Lv.%d 经验=%d 金币=%d 胜/负/平=%d/%d/%d\n",
					msg.Profile.Level, msg.Profile.Exp, msg.Profile.Gold,
					msg.Profile.WinCount, msg.Profile.LoseCount, msg.Profile.DrawCount)
			}
		}
	case gamepb.MsgID_SERVER_BROADCAST_NOTIFY:
		var msg gamepb.ServerBroadcastNotify
		if ok(m, &msg) {
			fmt.Printf("[公告] %s\n", msg.Text)
		}
	case gamepb.MsgID_KICK_NOTIFY:
		var msg gamepb.KickNotify
		if ok(m, &msg) {
			fmt.Printf("[被踢下线] %s\n", msg.Reason)
		}
	case gamepb.MsgID_ERROR_NOTIFY:
		var msg gamepb.ErrorNotify
		if ok(m, &msg) {
			fmt.Printf("[错误] code=%d %s (请求消息: %d)\n", msg.Code, msg.Msg, msg.ReqMsgId)
		}
	default:
		fmt.Printf("[未知消息] id=%d len=%d\n", m.ID, len(m.Body))
	}
}

func ok(m *protocol.Message, dst proto.Message) bool {
	if err := m.Unmarshal(dst); err != nil {
		fmt.Println("消息解析失败:", err)
		return false
	}
	return true
}

func memberNames(members []*gamepb.RoomMember) string {
	names := make([]string, 0, len(members))
	for _, mm := range members {
		names = append(names, mm.Nickname)
	}
	return strings.Join(names, ", ")
}

func isNewText(isNew bool) string {
	if isNew {
		return "新角色"
	}
	return "欢迎回来"
}

func moveName(m gamepb.RpsPlayReq_Move) string {
	switch m {
	case gamepb.RpsPlayReq_ROCK:
		return "石头"
	case gamepb.RpsPlayReq_PAPER:
		return "布"
	case gamepb.RpsPlayReq_SCISSORS:
		return "剪刀"
	default:
		return "未出招"
	}
}

func outcomeName(o gamepb.RpsRoundNotify_Outcome) string {
	switch o {
	case gamepb.RpsRoundNotify_WIN:
		return "你赢了！"
	case gamepb.RpsRoundNotify_LOSE:
		return "你输了…"
	default:
		return "平局"
	}
}
