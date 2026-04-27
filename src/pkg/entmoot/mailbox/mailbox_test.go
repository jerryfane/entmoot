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

func groupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}
