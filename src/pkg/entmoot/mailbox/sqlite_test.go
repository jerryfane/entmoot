package mailbox

import (
	"context"
	"testing"
)

func TestSQLiteCursorStorePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	gid := groupID(1)
	newer := Cursor{MessageID: messageID(2), TimestampMS: 2}
	older := Cursor{MessageID: messageID(1), TimestampMS: 1}

	cursors, err := OpenSQLiteCursorStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteCursorStore: %v", err)
	}
	advanced, err := cursors.AckCursor(ctx, gid, "ios-1", newer)
	if err != nil {
		t.Fatalf("AckCursor newer: %v", err)
	}
	if !advanced {
		t.Fatalf("AckCursor newer did not advance")
	}
	if err := cursors.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	cursors, err = OpenSQLiteCursorStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteCursorStore reopen: %v", err)
	}
	defer func() { _ = cursors.Close() }()
	got, err := cursors.GetCursor(ctx, gid, "ios-1")
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if got != newer {
		t.Fatalf("cursor after reopen = %+v, want %+v", got, newer)
	}
	advanced, err = cursors.AckCursor(ctx, gid, "ios-1", older)
	if err != nil {
		t.Fatalf("AckCursor older: %v", err)
	}
	if advanced {
		t.Fatalf("AckCursor older advanced")
	}
	got, err = cursors.GetCursor(ctx, gid, "ios-1")
	if err != nil {
		t.Fatalf("GetCursor after older ack: %v", err)
	}
	if got != newer {
		t.Fatalf("cursor after older ack = %+v, want %+v", got, newer)
	}
}
