package mailbox

import (
	"context"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/store"
)

func TestMessagesSinceAndAckCursor(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	for i := 1; i <= 3; i++ {
		if err := st.Put(ctx, message(gid, int64(i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	msgs, next, err := svc.MessagesSince(ctx, gid, "ios-1", Cursor{}, 2)
	if err != nil {
		t.Fatalf("MessagesSince: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("MessagesSince len = %d, want 2", len(msgs))
	}
	if err := svc.AckCursor(gid, "ios-1", next); err != nil {
		t.Fatalf("AckCursor: %v", err)
	}
	count, err := svc.UnreadCount(ctx, gid, "ios-1")
	if err != nil {
		t.Fatalf("UnreadCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("UnreadCount = %d, want 1", count)
	}

	msgs, _, err = svc.MessagesSince(ctx, gid, "ios-1", Cursor{}, 0)
	if err != nil {
		t.Fatalf("MessagesSince after ack: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Timestamp != 3 {
		t.Fatalf("MessagesSince after ack = %+v, want only timestamp 3", msgs)
	}
}

func TestMessagesSinceRejectsEmptyClient(t *testing.T) {
	svc, err := New(store.NewMemory(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, _, err := svc.MessagesSince(context.Background(), groupID(1), "", Cursor{}, 0); err == nil {
		t.Fatalf("MessagesSince accepted empty client")
	}
}

func TestHistoryLimitZeroReturnsEmptyPage(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	for i := 1; i <= 3; i++ {
		if err := st.Put(ctx, message(gid, int64(i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	history, err := svc.History(ctx, gid, 0)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if history.Count != 0 || len(history.Messages) != 0 {
		t.Fatalf("history count/messages = %d/%d, want empty page", history.Count, len(history.Messages))
	}
}

func TestHistoryReturnsLatestTopologicalPage(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	parent := message(gid, 100)
	parent.Content = []byte("parent")
	parent.ID = canonical.MessageID(parent)
	child := message(gid, 50)
	child.Content = []byte("child")
	child.Parents = []entmoot.MessageID{parent.ID}
	child.ID = canonical.MessageID(child)
	for _, msg := range []entmoot.Message{child, parent} {
		if err := st.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	history, err := svc.History(ctx, gid, 2)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if got := []string{history.Messages[0].Content, history.Messages[1].Content}; got[0] != "parent" || got[1] != "child" {
		t.Fatalf("history contents = %q, want parent, child", got)
	}

	history, err = svc.History(ctx, gid, 1)
	if err != nil {
		t.Fatalf("History limited: %v", err)
	}
	if history.Count != 1 || history.Messages[0].Content != "parent" {
		t.Fatalf("limited history = %+v, want parent", history.Messages)
	}
}

func TestMemoryCursorStoreIsMonotonic(t *testing.T) {
	ctx := context.Background()
	cursors := NewMemoryCursorStore()
	gid := groupID(1)
	newer := Cursor{MessageID: messageID(2), TimestampMS: 2}
	older := Cursor{MessageID: messageID(1), TimestampMS: 1}

	advanced, err := cursors.AckCursor(ctx, gid, "ios-1", newer)
	if err != nil {
		t.Fatalf("AckCursor newer: %v", err)
	}
	if !advanced {
		t.Fatalf("AckCursor newer did not advance")
	}
	advanced, err = cursors.AckCursor(ctx, gid, "ios-1", older)
	if err != nil {
		t.Fatalf("AckCursor older: %v", err)
	}
	if advanced {
		t.Fatalf("AckCursor older advanced")
	}
	got, err := cursors.GetCursor(ctx, gid, "ios-1")
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if got != newer {
		t.Fatalf("cursor = %+v, want %+v", got, newer)
	}
}

func TestMessagesSinceFallsBackToTimestampWhenCursorIDMissing(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	for i := 1; i <= 3; i++ {
		if err := st.Put(ctx, message(gid, int64(i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	msgs, next, err := svc.MessagesSince(ctx, gid, "ios-1", Cursor{MessageID: messageID(99), TimestampMS: 2}, 0)
	if err != nil {
		t.Fatalf("MessagesSince: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Timestamp != 3 {
		t.Fatalf("MessagesSince = %+v, want only timestamp 3", msgs)
	}
	if next.MessageID != msgs[0].ID || next.TimestampMS != msgs[0].Timestamp {
		t.Fatalf("next cursor = %+v, want last returned message", next)
	}
}

func message(gid entmoot.GroupID, ts int64) entmoot.Message {
	m := entmoot.Message{
		GroupID:   gid,
		Author:    entmoot.NodeInfo{PilotNodeID: 10, EntmootPubKey: []byte("pub")},
		Timestamp: ts,
		Topics:    []string{"t"},
		Content:   []byte{byte(ts)},
	}
	m.ID = canonical.MessageID(m)
	return m
}

func messageID(seed byte) entmoot.MessageID {
	var id entmoot.MessageID
	id[0] = seed
	return id
}

func groupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}
