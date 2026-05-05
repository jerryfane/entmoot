package mailbox

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/order"
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

// HistoryResult is returned by read-only latest-message APIs. It does not
// include or update a per-client cursor.
type HistoryResult struct {
	GroupID            entmoot.GroupID     `json:"group_id"`
	Count              int                 `json:"count"`
	HasMore            bool                `json:"has_more"`
	NextCursor         string              `json:"next_cursor,omitempty"`
	Messages           []SyncMessage       `json:"messages"`
	NextCursorBoundary *store.PageBoundary `json:"-"`
}

// TopicSummary describes one topic's message volume and latest activity.
type TopicSummary struct {
	Topic             string `json:"topic"`
	Count             int    `json:"count"`
	LatestMessageAtMS int64  `json:"latest_message_at_ms"`
}

// TopicsResult is returned by read-only topic index APIs.
type TopicsResult struct {
	GroupID entmoot.GroupID `json:"group_id"`
	Count   int             `json:"count"`
	Topics  []TopicSummary  `json:"topics"`
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

// History returns the most recent messages in groupID without reading or
// advancing any mailbox cursor. Results are returned oldest-to-newest so
// clients can render them as a conversation.
func (s *Service) History(ctx context.Context, groupID entmoot.GroupID, limit int) (HistoryResult, error) {
	return s.HistoryBefore(ctx, groupID, limit, nil)
}

// HistoryBefore returns one read-only history page older than boundary. A nil
// boundary returns the latest page.
func (s *Service) HistoryBefore(ctx context.Context, groupID entmoot.GroupID, limit int, boundary *store.PageBoundary) (HistoryResult, error) {
	if limit <= 0 {
		return HistoryResult{
			GroupID:  groupID,
			Count:    0,
			Messages: MessagesView(nil),
		}, nil
	}
	msgs, err := s.store.LatestBefore(ctx, groupID, limit+1, boundary)
	if err != nil {
		return HistoryResult{}, err
	}
	return historyResultFromMessages(groupID, msgs, limit)
}

// TopicHistory returns the most recent messages in groupID that contain topic.
func (s *Service) TopicHistory(ctx context.Context, groupID entmoot.GroupID, topic string, limit int) (HistoryResult, error) {
	return s.TopicHistoryBefore(ctx, groupID, topic, limit, nil)
}

// TopicHistoryBefore returns one read-only topic history page older than
// boundary. A nil boundary returns the latest topic page.
func (s *Service) TopicHistoryBefore(ctx context.Context, groupID entmoot.GroupID, topic string, limit int, boundary *store.PageBoundary) (HistoryResult, error) {
	if limit <= 0 || topic == "" {
		return HistoryResult{
			GroupID:  groupID,
			Count:    0,
			Messages: MessagesView(nil),
		}, nil
	}
	msgs, err := s.store.LatestByTopicBefore(ctx, groupID, topic, limit+1, boundary)
	if err != nil {
		return HistoryResult{}, err
	}
	return historyResultFromMessages(groupID, msgs, limit)
}

func historyResultFromMessages(groupID entmoot.GroupID, msgs []entmoot.Message, limit int) (HistoryResult, error) {
	hasMore := limit > 0 && len(msgs) > limit
	selected := msgs
	if hasMore {
		byRecency := append([]entmoot.Message(nil), msgs...)
		sort.Slice(byRecency, func(i, j int) bool {
			return messageNewerThan(byRecency[i], byRecency[j])
		})
		selected = byRecency[:limit]
	}
	selected, err := orderMessagesTopologically(selected)
	if err != nil {
		return HistoryResult{}, err
	}
	result := HistoryResult{
		GroupID:  groupID,
		Count:    len(selected),
		HasMore:  hasMore,
		Messages: MessagesView(selected),
	}
	if hasMore && len(selected) > 0 {
		oldest := oldestByRecency(selected)
		result.NextCursorBoundary = &store.PageBoundary{
			TimestampMS:  oldest.Timestamp,
			AuthorNodeID: oldest.Author.PilotNodeID,
			MessageID:    oldest.ID,
		}
	}
	return result, nil
}

func messageNewerThan(a, b entmoot.Message) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp > b.Timestamp
	}
	if a.Author.PilotNodeID != b.Author.PilotNodeID {
		return a.Author.PilotNodeID > b.Author.PilotNodeID
	}
	return bytes.Compare(a.ID[:], b.ID[:]) > 0
}

func oldestByRecency(msgs []entmoot.Message) entmoot.Message {
	oldest := msgs[0]
	for _, msg := range msgs[1:] {
		if messageNewerThan(oldest, msg) {
			oldest = msg
		}
	}
	return oldest
}

func orderMessagesTopologically(msgs []entmoot.Message) ([]entmoot.Message, error) {
	if len(msgs) == 0 {
		return []entmoot.Message{}, nil
	}
	ids, err := order.Topological(msgs)
	if err != nil {
		return nil, err
	}
	byID := make(map[entmoot.MessageID]entmoot.Message, len(msgs))
	for _, msg := range msgs {
		byID[msg.ID] = msg
	}
	out := make([]entmoot.Message, 0, len(ids))
	for _, id := range ids {
		out = append(out, byID[id])
	}
	return out, nil
}

// Topics returns topic aggregates for groupID without touching mailbox cursors.
func (s *Service) Topics(ctx context.Context, groupID entmoot.GroupID, limit int) (TopicsResult, error) {
	if limit <= 0 {
		return TopicsResult{
			GroupID: groupID,
			Count:   0,
			Topics:  []TopicSummary{},
		}, nil
	}
	summaries, err := s.store.Topics(ctx, groupID, limit)
	if err != nil {
		return TopicsResult{}, err
	}
	topics := make([]TopicSummary, 0, len(summaries))
	for _, summary := range summaries {
		topics = append(topics, TopicSummary{
			Topic:             summary.Topic,
			Count:             summary.Count,
			LatestMessageAtMS: summary.LatestMessageAtMS,
		})
	}
	return TopicsResult{
		GroupID: groupID,
		Count:   len(topics),
		Topics:  topics,
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
