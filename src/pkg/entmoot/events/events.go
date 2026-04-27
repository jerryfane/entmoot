// Package events provides a small in-process event surface for service-peer
// integrations such as mobile mailbox indexing and future notification
// bridges. It is intentionally not a gossip protocol; events are local
// observations emitted by the running daemon.
package events

import (
	"sync"
	"time"

	"entmoot/pkg/entmoot"
)

// Type names a local service event.
type Type string

const (
	TypeMessageIngested        Type = "message_ingested"
	TypeMailboxCursorAdvanced  Type = "mailbox_cursor_advanced"
	TypeReconcileStatusChanged Type = "reconcile_status_changed"
)

// Event is the common local event envelope.
type Event struct {
	Type      Type              `json:"type"`
	GroupID   entmoot.GroupID   `json:"group_id"`
	MessageID entmoot.MessageID `json:"message_id,omitempty"`
	ClientID  string            `json:"client_id,omitempty"`
	PeerID    entmoot.NodeID    `json:"peer_id,omitempty"`
	Status    string            `json:"status,omitempty"`
	At        time.Time         `json:"at"`
}

// Sink consumes local events. Implementations must not block indefinitely.
type Sink interface {
	Emit(Event)
}

// NopSink drops every event.
type NopSink struct{}

func (NopSink) Emit(Event) {}

// Bus is a non-blocking in-process fan-out sink.
type Bus struct {
	mu     sync.Mutex
	subs   map[int]chan<- Event
	nextID int
}

// NewBus returns an empty event bus.
func NewBus() *Bus {
	return &Bus{subs: make(map[int]chan<- Event)}
}

// Subscribe registers ch for future events. Delivery is non-blocking; a slow
// subscriber can miss events rather than stalling the writer path.
func (b *Bus) Subscribe(ch chan<- Event) func() {
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.subs[id] = ch
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		delete(b.subs, id)
		b.mu.Unlock()
	}
}

// Emit implements Sink.
func (b *Bus) Emit(ev Event) {
	if b == nil {
		return
	}
	if ev.At.IsZero() {
		ev.At = time.Now()
	}
	b.mu.Lock()
	subs := make([]chan<- Event, 0, len(b.subs))
	for _, ch := range b.subs {
		subs = append(subs, ch)
	}
	b.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
		}
	}
}
