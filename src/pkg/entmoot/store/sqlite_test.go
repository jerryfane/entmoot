package store

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"

	_ "modernc.org/sqlite"
)

// TestSQLiteWALMode verifies that WAL is actually engaged after open by
// querying PRAGMA journal_mode on the raw database file created for the
// group.
func TestSQLiteWALMode(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Put forces the lazy open so the file exists on disk.
	gid := randGroupID(t)
	m := mkMsg(t, gid, testAuthor(1, 0xAA), 1_000, "wal-check")
	if err := s.Put(ctx, m); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Locate the database file under <root>/groups/<b64>/messages.sqlite.
	dbPath := filepath.Join(root, "groups", encodeGroupDirName(gid), "messages.sqlite")
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db file missing: %v", err)
	}

	// Open a side handle without pragma params to observe the persisted
	// journal_mode (WAL is file-persistent in SQLite).
	q := url.Values{}
	q.Add("_pragma", "query_only(1)")
	side, err := sql.Open("sqlite", "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		t.Fatalf("sql.Open side: %v", err)
	}
	defer side.Close()

	var mode string
	if err := side.QueryRowContext(ctx, "PRAGMA journal_mode;").Scan(&mode); err != nil {
		t.Fatalf("scan journal_mode: %v", err)
	}
	if !strings.EqualFold(mode, "wal") {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
}

// TestSQLiteReopen verifies that state survives Close + OpenSQLite on the
// same root.
func TestSQLiteReopen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite #1: %v", err)
	}
	gid := randGroupID(t)
	m := mkMsg(t, gid, testAuthor(1, 0xAA), 1_000, "persisted")
	if err := s.Put(ctx, m); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	has, err := s2.Has(ctx, gid, m.ID)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !has {
		t.Fatal("Has=false after reopen; expected persisted message")
	}

	got, err := s2.Get(ctx, gid, m.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != m.ID || got.Timestamp != m.Timestamp {
		t.Fatalf("roundtrip mismatch: got id=%x ts=%d, want id=%x ts=%d",
			got.ID, got.Timestamp, m.ID, m.Timestamp)
	}
}

// TestSQLiteConcurrentReaderWriter runs one writer goroutine putting 100
// messages while a reader goroutine repeatedly calls Has and Range. No
// errors, and the reader must make forward progress (i.e. not deadlock on
// the writer under WAL).
func TestSQLiteConcurrentReaderWriter(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	const nMessages = 100
	msgs := make([]entmoot.Message, nMessages)
	for i := 0; i < nMessages; i++ {
		msgs[i] = mkMsg(t, gid, testAuthor(uint32(i%4)+1, byte(i)), int64(i)+1, "m")
	}

	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		readOps  atomic.Int64
		firstErr atomic.Value
	)
	captureErr := func(err error) {
		if err == nil {
			return
		}
		firstErr.CompareAndSwap(nil, err)
	}

	// Writer: sequential Puts.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stop.Store(true)
		for _, m := range msgs {
			if err := s.Put(ctx, m); err != nil {
				captureErr(err)
				return
			}
			// Nudge the scheduler so the reader gets a turn.
			runtime.Gosched()
		}
	}()

	// Reader: Has + Range in a tight loop until the writer stops.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if _, err := s.Has(ctx, gid, msgs[0].ID); err != nil {
				captureErr(err)
				return
			}
			if _, err := s.Range(ctx, gid, 0, 0); err != nil {
				captureErr(err)
				return
			}
			readOps.Add(1)
		}
	}()

	// Overall timeout guard against a hypothetical deadlock.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent reader/writer timed out after 30s")
	}

	if v := firstErr.Load(); v != nil {
		t.Fatalf("concurrent error: %v", v)
	}
	if readOps.Load() == 0 {
		t.Fatal("reader made zero progress; WAL concurrency broken or race unlucky")
	}

	got, err := s.Range(ctx, gid, 0, 0)
	if err != nil {
		t.Fatalf("final Range: %v", err)
	}
	if len(got) != nMessages {
		t.Fatalf("final message count = %d, want %d", len(got), nMessages)
	}
}

// TestSQLiteMerkleRootStable verifies that MerkleRoot returns the same value
// on repeated calls against identical input, and that a freshly-opened store
// reads back the same root after a checkpoint.
func TestSQLiteMerkleRootStable(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	gid := randGroupID(t)
	for i := 0; i < 5; i++ {
		m := mkMsg(t, gid, testAuthor(uint32(i+1), byte(i+1)), int64(100+i*10), "m")
		if err := s.Put(ctx, m); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	roots := make([][32]byte, 4)
	for i := range roots {
		r, err := s.MerkleRoot(ctx, gid)
		if err != nil {
			t.Fatalf("MerkleRoot #%d: %v", i, err)
		}
		roots[i] = r
	}
	for i := 1; i < len(roots); i++ {
		if roots[i] != roots[0] {
			t.Fatalf("MerkleRoot unstable: call %d = %x, call 0 = %x", i, roots[i], roots[0])
		}
	}
	if roots[0] == ([32]byte{}) {
		t.Fatal("expected non-zero root for non-empty group")
	}

	// Close, reopen, verify the root survives.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	r2, err := s2.MerkleRoot(ctx, gid)
	if err != nil {
		t.Fatalf("MerkleRoot post-reopen: %v", err)
	}
	if r2 != roots[0] {
		t.Fatalf("MerkleRoot post-reopen = %x, want %x", r2, roots[0])
	}
}

// TestSQLiteTopologicalOrder verifies that Range returns messages in the
// same order that pkg/entmoot/order.Topological would yield, including when
// parent/child edges force an ordering that contradicts insertion order.
func TestSQLiteTopologicalOrder(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	// Genesis and two descendants; child at t=50, genesis at t=100 would be
	// out of order by timestamp alone but the DAG must force genesis first.
	genesis := mkMsg(t, gid, testAuthor(1, 0x01), 100, "genesis")
	child := entmoot.Message{
		GroupID:   gid,
		Author:    testAuthor(1, 0x01),
		Timestamp: 50,
		Content:   []byte("child"),
		Parents:   []entmoot.MessageID{genesis.ID},
	}
	// Recompute id with parents set.
	child.ID = canonical.MessageID(child)
	if err := s.Put(ctx, child); err != nil {
		t.Fatalf("Put child: %v", err)
	}
	if err := s.Put(ctx, genesis); err != nil {
		t.Fatalf("Put genesis: %v", err)
	}

	got, err := s.Range(ctx, gid, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Range len = %d, want 2", len(got))
	}
	if got[0].ID != genesis.ID {
		t.Fatalf("got[0] = %x, want genesis %x (DAG edge ignored)", got[0].ID, genesis.ID)
	}
	if got[1].ID != child.ID {
		t.Fatalf("got[1] = %x, want child %x", got[1].ID, child.ID)
	}
}

// TestSQLiteFilePermissions verifies that the created SQLite database file
// is 0600 (owner-only read/write), matching the layout expected in
// CLI_DESIGN.md.
func TestSQLiteFilePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix mode bits not meaningful on windows")
	}
	ctx := context.Background()
	root := t.TempDir()
	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	m := mkMsg(t, gid, testAuthor(1, 0x01), 1_000, "perm")
	if err := s.Put(ctx, m); err != nil {
		t.Fatalf("Put: %v", err)
	}

	dbPath := filepath.Join(root, "groups", encodeGroupDirName(gid), "messages.sqlite")
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("sqlite file mode = %04o, want 0600", mode)
	}
}
