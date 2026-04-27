package mailbox

import (
	"context"
	"errors"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/store"
)

// SyncMessage is the mailbox/mobile JSON view of an Entmoot message.
type SyncMessage struct {
	MessageID   entmoot.MessageID `json:"message_id"`
	GroupID     entmoot.GroupID   `json:"group_id"`
	Author      uint32            `json:"author"`
	Topics      []string          `json:"topic"`
	Content     string            `json:"content"`
	TimestampMS int64             `json:"timestamp_ms"`
}

// PullResult is returned by mailbox pull APIs.
type PullResult struct {
	ClientID   string          `json:"client_id"`
	GroupID    entmoot.GroupID `json:"group_id"`
	Count      int             `json:"count"`
	HasMore    bool            `json:"has_more"`
	NextCursor Cursor          `json:"next_cursor"`
	Messages   []SyncMessage   `json:"messages"`
}

// AckResult is returned after advancing a mailbox cursor to a message.
type AckResult struct {
	ClientID    string            `json:"client_id"`
	GroupID     entmoot.GroupID   `json:"group_id"`
	MessageID   entmoot.MessageID `json:"message_id"`
	TimestampMS int64             `json:"timestamp_ms"`
	Cursor      Cursor            `json:"cursor"`
}

// CursorResult describes the durable cursor plus current unread count.
type CursorResult struct {
	ClientID string          `json:"client_id"`
	GroupID  entmoot.GroupID `json:"group_id"`
	Cursor   Cursor          `json:"cursor"`
	Unread   int             `json:"unread"`
}

// Pull returns unread messages for clientID without advancing its durable
// cursor. The returned NextCursor is the cursor the client should ack after it
// has durably processed the page.
func (s *Service) Pull(ctx context.Context, groupID entmoot.GroupID, clientID string, limit int) (PullResult, error) {
	if clientID == "" {
		return PullResult{}, ErrInvalidClient
	}
	if limit < 0 {
		limit = 0
	}
	fetchLimit := limit
	if fetchLimit > 0 {
		fetchLimit++
	}
	msgs, next, err := s.MessagesSince(ctx, groupID, clientID, Cursor{}, fetchLimit)
	if err != nil {
		return PullResult{}, err
	}
	hasMore := false
	if limit > 0 && len(msgs) > limit {
		hasMore = true
		msgs = msgs[:limit]
		next = cursorFromMessages(s.currentCursor(ctx, groupID, clientID), msgs)
	}
	return PullResult{
		ClientID:   clientID,
		GroupID:    groupID,
		Count:      len(msgs),
		HasMore:    hasMore,
		NextCursor: next,
		Messages:   MessagesView(msgs),
	}, nil
}

// AckMessage advances clientID's cursor to messageID, after verifying the
// message exists in groupID.
func (s *Service) AckMessage(ctx context.Context, groupID entmoot.GroupID, clientID string, messageID entmoot.MessageID) (AckResult, error) {
	if clientID == "" {
		return AckResult{}, ErrInvalidClient
	}
	msg, err := s.store.Get(ctx, groupID, messageID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return AckResult{}, store.ErrNotFound
		}
		return AckResult{}, err
	}
	cursor := Cursor{MessageID: msg.ID, TimestampMS: msg.Timestamp}
	if err := s.AckCursorContext(ctx, groupID, clientID, cursor); err != nil {
		return AckResult{}, err
	}
	return AckResult{
		ClientID:    clientID,
		GroupID:     groupID,
		MessageID:   msg.ID,
		TimestampMS: msg.Timestamp,
		Cursor:      cursor,
	}, nil
}

// CursorStatus returns the current durable cursor and unread count.
func (s *Service) CursorStatus(ctx context.Context, groupID entmoot.GroupID, clientID string) (CursorResult, error) {
	if clientID == "" {
		return CursorResult{}, ErrInvalidClient
	}
	cursor, err := s.CursorContext(ctx, groupID, clientID)
	if err != nil {
		return CursorResult{}, err
	}
	unread, err := s.UnreadCount(ctx, groupID, clientID)
	if err != nil {
		return CursorResult{}, err
	}
	return CursorResult{
		ClientID: clientID,
		GroupID:  groupID,
		Cursor:   cursor,
		Unread:   unread,
	}, nil
}

// MessagesView converts Entmoot messages into the mobile mailbox schema.
func MessagesView(msgs []entmoot.Message) []SyncMessage {
	if msgs == nil {
		return []SyncMessage{}
	}
	out := make([]SyncMessage, 0, len(msgs))
	for _, msg := range msgs {
		out = append(out, MessageView(msg))
	}
	return out
}

// MessageView converts one Entmoot message into the mobile mailbox schema.
func MessageView(m entmoot.Message) SyncMessage {
	return SyncMessage{
		MessageID:   m.ID,
		GroupID:     m.GroupID,
		Author:      uint32(m.Author.PilotNodeID),
		Topics:      normTopics(m.Topics),
		Content:     string(m.Content),
		TimestampMS: m.Timestamp,
	}
}

func (s *Service) currentCursor(ctx context.Context, groupID entmoot.GroupID, clientID string) Cursor {
	cursor, err := s.CursorContext(ctx, groupID, clientID)
	if err != nil {
		return Cursor{}
	}
	return cursor
}

func normTopics(t []string) []string {
	if t == nil {
		return []string{}
	}
	return t
}
