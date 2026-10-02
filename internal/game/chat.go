package game

import (
	"strings"
	"unicode/utf8"

	"servergame/internal/errs"
	gamepb "servergame/pb/game/v1"
)

// validateContent 校验聊天内容：去首尾空白后需为 1~maxLen 个字符。
func (g *Game) validateContent(content string) (string, error) {
	content = strings.TrimSpace(content)
	if n := utf8.RuneCountInString(content); n < 1 || n > g.cfg.Game.MaxContentLength {
		return "", errs.BadRequest("内容需为 1~%d 个字符", g.cfg.Game.MaxContentLength)
	}
	return content, nil
}

// onWorldChat 世界频道发言：确认 + 全服广播。
func (g *Game) onWorldChat(s *Session, req *gamepb.WorldChatReq) error {
	content, err := g.validateContent(req.Content)
	if err != nil {
		return err
	}
	if !s.ChatAllowed(int64(g.cfg.Game.ChatIntervalMs)) {
		return errs.RateLimited("发言太快，休息一下")
	}

	ts := nowMs()
	if err := s.Conn.SendMsg(uint16(gamepb.MsgID_WORLD_CHAT_RES), &gamepb.WorldChatRes{Ts: ts}); err != nil {
		return nil // 连接已断开，无需广播
	}
	_ = g.conns.BroadcastMsg(uint16(gamepb.MsgID_WORLD_CHAT_NOTIFY), &gamepb.WorldChatNotify{
		Uid:      s.UID,
		Nickname: s.Nickname(),
		Content:  content,
		Ts:       ts,
	})
	return nil
}
