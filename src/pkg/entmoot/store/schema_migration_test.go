package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// TestOpenDropsTheRetiredRangeIndex proves the schema retires an index left by
// an older binary, not just that new databases lack it.
func TestOpenDropsTheRetiredRangeIndex(t *testing.T) {
	root := t.TempDir()
	gid := randGroupID(t)
	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	author := testAuthor(1, 0xAA)
	if _, err := s.Put(context.Background(), gid, mkMsg(t, gid, author, 10, "seed")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	path := filepath.Join(root, "groups", encodeGroupDirName(gid), "messages.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_messages_group_id_range ON messages(group_id, message_id ASC)`); err != nil {
		t.Fatalf("recreate legacy index: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_group_id_range'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("legacy index not present before reopen: n=%d err=%v", n, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}
	s2, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if _, err := s2.Latest(context.Background(), gid, 1); err != nil {
		t.Fatalf("Latest after reopen: %v", err)
	}
	db2, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw 2: %v", err)
	}
	defer db2.Close()
	if err := db2.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_group_id_range'`).Scan(&n); err != nil {
		t.Fatalf("count after reopen: %v", err)
	}
	if n != 0 {
		t.Fatalf("retired index still present after reopen")
	}
}
