package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"entmoot/pkg/entmoot"
)

// TestJSONL_ReopenPersistence verifies that messages written before Close
// are rehydrated into the in-memory index when OpenJSONL reopens the same
// data root.
func TestJSONL_ReopenPersistence(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	s, err := OpenJSONL(root)
	if err != nil {
		t.Fatalf("OpenJSONL #1: %v", err)
	}
	gid := randGroupID(t)
	m := mkMsg(t, gid, testAuthor(1, 0x01), 1_000, "persist-me")
	if err := s.Put(ctx, m); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenJSONL(root)
	if err != nil {
		t.Fatalf("OpenJSONL #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	got, err := s2.Get(ctx, gid, m.ID)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if got.ID != m.ID {
		t.Fatalf("got ID %x, want %x", got.ID, m.ID)
	}
	if string(got.Content) != "persist-me" {
		t.Fatalf("content = %q, want persist-me", got.Content)
	}
}

// TestJSONL_MalformedLineSkipped verifies that a corrupt line in
// messages.jsonl is skipped at load time and valid lines after it are loaded.
func TestJSONL_MalformedLineSkipped(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	// First Put a valid message so the directory and file exist in the
	// canonical layout, then append a malformed line and a second valid line.
	s, err := OpenJSONL(root)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	gid := randGroupID(t)
	good1 := mkMsg(t, gid, testAuthor(1, 0x01), 1_000, "good1")
	if err := s.Put(ctx, good1); err != nil {
		t.Fatalf("Put good1: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(root, "groups", encodeGroupDirName(gid), "messages.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	if _, err := f.Write([]byte("this is not json\n")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close after garbage: %v", err)
	}

	// Reopen, Put a second valid message so we also test the path where new
	// valid lines are appended after a malformed one.
	s2, err := OpenJSONL(root)
	if err != nil {
		t.Fatalf("OpenJSONL #2: %v", err)
	}
	good2 := mkMsg(t, gid, testAuthor(1, 0x01), 2_000, "good2")
	if err := s2.Put(ctx, good2); err != nil {
		t.Fatalf("Put good2: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("Close #2: %v", err)
	}

	// Reopen once more and verify both good messages are present.
	s3, err := OpenJSONL(root)
	if err != nil {
		t.Fatalf("OpenJSONL #3: %v", err)
	}
	t.Cleanup(func() { _ = s3.Close() })

	for _, want := range []entmoot.Message{good1, good2} {
		got, err := s3.Get(ctx, gid, want.ID)
		if err != nil {
			t.Fatalf("Get %q: %v", want.Content, err)
		}
		if got.ID != want.ID {
			t.Fatalf("Get %q: id mismatch", want.Content)
		}
	}
	msgs, err := s3.Range(ctx, gid, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("Range len = %d, want 2", len(msgs))
	}
}

// TestJSONL_FilePermissions verifies the documented 0700 group-dir and 0600
// messages.jsonl perms. Skipped on filesystems that don't report modes
// (e.g., some CI sandboxes); the usual darwin/linux temp dirs do.
func TestJSONL_FilePermissions(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := OpenJSONL(root)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	m := mkMsg(t, gid, testAuthor(1, 0x01), 1, "perm")
	if err := s.Put(ctx, m); err != nil {
		t.Fatalf("Put: %v", err)
	}

	groupDir := filepath.Join(root, "groups", encodeGroupDirName(gid))
	dirInfo, err := os.Stat(groupDir)
	if err != nil {
		t.Fatalf("stat group dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("group dir perms = %o, want 0700", got)
	}

	path := filepath.Join(groupDir, "messages.jsonl")
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat jsonl: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("jsonl perms = %o, want 0600", got)
	}
}
