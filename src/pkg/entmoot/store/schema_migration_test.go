package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
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

// TestOpenSucceedsWhileAnotherWriterHoldsTheLock pins the lock profile of the
// open path. Retiring the legacy index needs a write transaction, which the
// schema block does not, so doing it inside the block made the first open of
// an existing database fail with SQLITE_BUSY whenever a second process on the
// same data root held the write lock. This fleet runs `serve` and `esp serve`
// against one root, so that open must still succeed and simply leave the index
// for a later open.
func TestOpenSucceedsWhileAnotherWriterHoldsTheLock(t *testing.T) {
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

	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer legacy.Close()
	if _, err := legacy.Exec(`CREATE INDEX IF NOT EXISTS idx_messages_group_id_range ON messages(group_id, message_id ASC)`); err != nil {
		t.Fatalf("recreate legacy index: %v", err)
	}

	// Hold the write lock for longer than the open path's busy_timeout.
	holder, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(250)")
	if err != nil {
		t.Fatalf("open holder: %v", err)
	}
	defer holder.Close()
	tx, err := holder.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(`UPDATE messages SET timestamp_ms = timestamp_ms WHERE 1`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("take write lock: %v", err)
	}

	started := time.Now()
	reopened, err := OpenSQLite(root)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("open under a held write lock must succeed, got: %v", err)
	}
	if _, err := reopened.Latest(context.Background(), gid, 1); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Latest under a held write lock: %v", err)
	}
	// The retirement must give up quickly rather than burn the open path's
	// full 5s busy_timeout waiting for an unrelated writer.
	if waited := time.Since(started); waited > 2*time.Second {
		_ = tx.Rollback()
		t.Fatalf("contended open took %s, want it to abandon the retirement quickly", waited)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened: %v", err)
	}

	// With the lock released, a later open retires the index.
	final, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("final open: %v", err)
	}
	defer final.Close()
	if _, err := final.Latest(context.Background(), gid, 1); err != nil {
		t.Fatalf("Latest on final open: %v", err)
	}
	var n int
	if err := legacy.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_group_id_range'`).Scan(&n); err != nil {
		t.Fatalf("count after later open: %v", err)
	}
	if n != 0 {
		t.Fatalf("retired index survived an uncontended open")
	}
}
