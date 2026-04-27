// Package mailbox provides ESP/service-peer sync cursors over an Entmoot store.
//
// ESP means Entmoot Service Provider: an always-on service peer that can sync,
// notify, and relay already-signed messages for mobile/intermittent clients.
// Mailbox state is local ESP state. It does not change Entmoot consensus,
// message ordering, gossip, or reconciliation.
package mailbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/events"
	"entmoot/pkg/entmoot/store"
)

// ErrInvalidClient is returned when a mailbox client id is empty.
var ErrInvalidClient = errors.New("mailbox: invalid client")

// Cursor identifies the last delivered/read message for one client and group.
type Cursor struct {
	MessageID   entmoot.MessageID `json:"message_id"`
	TimestampMS int64             `json:"timestamp_ms"`
}

// CursorStore persists per-client mailbox cursors.
type CursorStore interface {
	GetCursor(ctx context.Context, groupID entmoot.GroupID, clientID string) (Cursor, error)
	AckCursor(ctx context.Context, groupID entmoot.GroupID, clientID string, cursor Cursor) (bool, error)
	Close() error
}

// Service tracks per-client cursors over an existing MessageStore.
type Service struct {
	store   store.MessageStore
	cursors CursorStore
	sink    events.Sink
}

// New returns a mailbox service backed by st and an in-memory cursor store.
func New(st store.MessageStore, sink events.Sink) (*Service, error) {
	return NewWithCursorStore(st, NewMemoryCursorStore(), sink)
}

// NewWithCursorStore returns a mailbox service backed by st and cursors.
func NewWithCursorStore(st store.MessageStore, cursors CursorStore, sink events.Sink) (*Service, error) {
	if st == nil {
		return nil, errors.New("mailbox: nil store")
	}
	if cursors == nil {
		return nil, errors.New("mailbox: nil cursor store")
	}
	if sink == nil {
		sink = events.NopSink{}
	}
	return &Service{
		store:   st,
		cursors: cursors,
		sink:    sink,
	}, nil
}

// MessagesSince returns messages after cursor. When cursor is zero, the
// stored client cursor is used. limit <= 0 means no limit.
func (s *Service) MessagesSince(ctx context.Context, groupID entmoot.GroupID, clientID string, cursor Cursor, limit int) ([]entmoot.Message, Cursor, error) {
	if clientID == "" {
		return nil, Cursor{}, ErrInvalidClient
	}
	if cursor == (Cursor{}) {
		var err error
		cursor, err = s.CursorContext(ctx, groupID, clientID)
		if err != nil {
			return nil, Cursor{}, err
		}
	}
	all, err := s.store.Range(ctx, groupID, 0, 0)
	if err != nil {
		return nil, Cursor{}, fmt.Errorf("mailbox: range: %w", err)
	}

	start := 0
	if cursor.MessageID != (entmoot.MessageID{}) {
		found := false
		for i, m := range all {
			if m.ID == cursor.MessageID {
				start = i + 1
				found = true
				break
			}
		}
		if !found && cursor.TimestampMS > 0 {
			start = afterTimestamp(all, cursor.TimestampMS)
		}
	} else if cursor.TimestampMS > 0 {
		start = afterTimestamp(all, cursor.TimestampMS)
	}
	if start > len(all) {
		start = len(all)
	}
	out := append([]entmoot.Message(nil), all[start:]...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	next := cursorFromMessages(cursor, out)
	return out, next, nil
}

// AckCursor advances the stored cursor for a client and group. Replaying the
// same or an older cursor is idempotent.
func (s *Service) AckCursor(groupID entmoot.GroupID, clientID string, cursor Cursor) error {
	return s.AckCursorContext(context.Background(), groupID, clientID, cursor)
}

// AckCursorContext advances the stored cursor with caller-controlled
// cancellation. Replaying the same or an older cursor is idempotent.
func (s *Service) AckCursorContext(ctx context.Context, groupID entmoot.GroupID, clientID string, cursor Cursor) error {
	if clientID == "" {
		return ErrInvalidClient
	}
	if cursor == (Cursor{}) {
		return nil
	}
	advanced, err := s.cursors.AckCursor(ctx, groupID, clientID, cursor)
	if err != nil {
		return err
	}
	if advanced {
		s.sink.Emit(events.Event{
			Type:      events.TypeMailboxCursorAdvanced,
			GroupID:   groupID,
			MessageID: cursor.MessageID,
			ClientID:  clientID,
			At:        time.Now(),
		})
	}
	return nil
}

// Cursor returns the stored cursor for clientID in groupID.
func (s *Service) Cursor(groupID entmoot.GroupID, clientID string) Cursor {
	cursor, _ := s.CursorContext(context.Background(), groupID, clientID)
	return cursor
}

// CursorContext returns the stored cursor for clientID in groupID with
// caller-controlled cancellation.
func (s *Service) CursorContext(ctx context.Context, groupID entmoot.GroupID, clientID string) (Cursor, error) {
	if clientID == "" {
		return Cursor{}, ErrInvalidClient
	}
	return s.cursors.GetCursor(ctx, groupID, clientID)
}

// UnreadCount returns the number of messages after the stored cursor.
func (s *Service) UnreadCount(ctx context.Context, groupID entmoot.GroupID, clientID string) (int, error) {
	msgs, _, err := s.MessagesSince(ctx, groupID, clientID, Cursor{}, 0)
	if err != nil {
		return 0, err
	}
	return len(msgs), nil
}

func cursorFromMessages(fallback Cursor, msgs []entmoot.Message) Cursor {
	if len(msgs) == 0 {
		return fallback
	}
	last := msgs[len(msgs)-1]
	return Cursor{MessageID: last.ID, TimestampMS: last.Timestamp}
}

func afterTimestamp(msgs []entmoot.Message, timestampMS int64) int {
	start := 0
	for start < len(msgs) && msgs[start].Timestamp <= timestampMS {
		start++
	}
	return start
}

func cursorAfter(a, b Cursor) bool {
	if b == (Cursor{}) {
		return true
	}
	if a.TimestampMS != b.TimestampMS {
		return a.TimestampMS > b.TimestampMS
	}
	return string(a.MessageID[:]) > string(b.MessageID[:])
}
