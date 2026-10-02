package game

import (
	"servergame/internal/errs"
	gamepb "servergame/pb/game/v1"
)

// onCreateRoom 创建房间。
func (g *Game) onCreateRoom(s *Session, _ *gamepb.Empty) error {
	roomID, err := g.rooms.Create(s)
	if err != nil {
		return err
	}
	return s.Conn.SendMsg(uint16(gamepb.MsgID_CREATE_ROOM_RES), &gamepb.CreateRoomRes{RoomId: roomID})
}

// onJoinRoom 加入房间。
func (g *Game) onJoinRoom(s *Session, req *gamepb.JoinRoomReq) error {
	if req.RoomId < 100000 || req.RoomId > 999999 {
		return errs.BadRequest("房间号需为 6 位数字")
	}
	members, err := g.rooms.Join(s, req.RoomId)
	if err != nil {
		return err
	}
	return s.Conn.SendMsg(uint16(gamepb.MsgID_JOIN_ROOM_RES), &gamepb.JoinRoomRes{
		RoomId:  req.RoomId,
		Members: members,
	})
}

// onLeaveRoom 离开房间。
func (g *Game) onLeaveRoom(s *Session, _ *gamepb.Empty) error {
	roomID, ok := g.rooms.Leave(s)
	if !ok {
		return errs.New(gamepb.ErrCode_ERR_NOT_IN_ROOM, "你不在房间中")
	}
	return s.Conn.SendMsg(uint16(gamepb.MsgID_LEAVE_ROOM_RES), &gamepb.LeaveRoomRes{RoomId: roomID})
}

// onRoomChat 房间内发言。
func (g *Game) onRoomChat(s *Session, req *gamepb.RoomChatReq) error {
	content, err := g.validateContent(req.Content)
	if err != nil {
		return err
	}
	if !s.ChatAllowed(int64(g.cfg.Game.ChatIntervalMs)) {
		return errs.RateLimited("发言太快，休息一下")
	}
	return g.rooms.RoomChat(s, content)
}

// onRpsPlay 猜拳出招。
func (g *Game) onRpsPlay(s *Session, req *gamepb.RpsPlayReq) error {
	return g.rooms.Play(g, s, req.Move)
}
