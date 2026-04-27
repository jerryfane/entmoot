// Package mailbox provides service-peer sync cursors over an Entmoot store.
//
// It is a local index for mobile/service clients. It does not change Entmoot
// consensus, message ordering, gossip, or reconciliation.
package mailbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

// Service tracks per-client cursors over an existing MessageStore.
type Service struct {
	store store.MessageStore
	sink  events.Sink

	mu      sync.RWMutex
	cursors map[key]Cursor
}

type key struct {
	group  entmoot.GroupID
	client string
}

// New returns a mailbox service backed by st.
func New(st store.MessageStore, sink events.Sink) (*Service, error) {
	if st == nil {
		return nil, errors.New("mailbox: nil store")
	}
	if sink == nil {
		sink = events.NopSink{}
	}
	return &Service{
		store:   st,
		sink:    sink,
		cursors: make(map[key]Cursor),
	}, nil
}

// MessagesSince returns messages after cursor. When cursor is zero, the
// stored client cursor is used. limit <= 0 means no limit.
func (s *Service) MessagesSince(ctx context.Context, groupID entmoot.GroupID, clientID string, cursor Cursor, limit int) ([]entmoot.Message, Cursor, error) {
	if clientID == "" {
		return nil, Cursor{}, ErrInvalidClient
	}
	if cursor == (Cursor{}) {
		cursor = s.Cursor(groupID, clientID)
	}
	all, err := s.store.Range(ctx, groupID, 0, 0)
	if err != nil {
		return nil, Cursor{}, fmt.Errorf("mailbox: range: %w", err)
	}

	start := 0
	if cursor.MessageID != (entmoot.MessageID{}) {
		for i, m := range all {
			if m.ID == cursor.MessageID {
				start = i + 1
				break
			}
		}
	} else if cursor.TimestampMS > 0 {
		for start < len(all) && all[start].Timestamp <= cursor.TimestampMS {
			start++
		}
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
	if clientID == "" {
		return ErrInvalidClient
	}
	if cursor == (Cursor{}) {
		return nil
	}
	k := key{group: groupID, client: clientID}
	s.mu.Lock()
	cur := s.cursors[k]
	if cursorAfter(cursor, cur) {
		s.cursors[k] = cursor
	}
	s.mu.Unlock()
	s.sink.Emit(events.Event{
		Type:      events.TypeMailboxCursorAdvanced,
		GroupID:   groupID,
		MessageID: cursor.MessageID,
		ClientID:  clientID,
		At:        time.Now(),
	})
	return nil
}

// Cursor returns the stored cursor for clientID in groupID.
func (s *Service) Cursor(groupID entmoot.GroupID, clientID string) Cursor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursors[key{group: groupID, client: clientID}]
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

func cursorAfter(a, b Cursor) bool {
	if b == (Cursor{}) {
		return true
	}
	if a.TimestampMS != b.TimestampMS {
		return a.TimestampMS > b.TimestampMS
	}
	return string(a.MessageID[:]) > string(b.MessageID[:])
}
