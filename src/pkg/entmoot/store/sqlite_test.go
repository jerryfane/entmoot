package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
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

func TestSQLiteReadMissAndPutMismatchDoNotCreateGroups(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	trustedGroup := randGroupID(t)
	foreignGroup := randGroupID(t)
	m := mkMsg(t, foreignGroup, testAuthor(1, 0xEF), 1_000, "foreign")

	has, err := s.Has(ctx, foreignGroup, m.ID)
	if err != nil {
		t.Fatalf("Has missing group: %v", err)
	}
	if has {
		t.Fatal("Has missing group=true")
	}
	if _, err := s.Put(ctx, trustedGroup, m); err == nil {
		t.Fatal("Put mismatch returned nil error")
	}

	for _, gid := range []entmoot.GroupID{trustedGroup, foreignGroup} {
		groupDir := filepath.Join(root, "groups", gid.DirName())
		if _, err := os.Stat(groupDir); !os.IsNotExist(err) {
			t.Fatalf("group directory %q exists after read miss or rejected Put: %v", groupDir, err)
		}
	}
}

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
	if _, err := s.Put(ctx, m.GroupID, m); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Locate the database file under <root>/groups/<b64>/messages.sqlite.
	dbPath := filepath.Join(root, "groups", gid.DirName(), "messages.sqlite")
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
	if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
		if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
	if _, err := s.Put(ctx, child.GroupID, child); err != nil {
		t.Fatalf("Put child: %v", err)
	}
	if _, err := s.Put(ctx, genesis.GroupID, genesis); err != nil {
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

func TestSQLiteLatestUsesBoundedIndexOrder(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	for i := 0; i < 3; i++ {
		m := mkMsg(t, gid, testAuthor(uint32(i+1), byte(i+1)), int64(1_000+i), "seed")
		if _, err := s.Put(ctx, m.GroupID, m); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	dbPath := filepath.Join(root, "groups", gid.DirName(), "messages.sqlite")
	side, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("sql.Open side: %v", err)
	}
	defer side.Close()

	rows, err := side.QueryContext(ctx, `
		EXPLAIN QUERY PLAN
		SELECT canonical_bytes FROM messages
		WHERE group_id = ?
		ORDER BY timestamp_ms DESC, author_member_id DESC, message_id DESC
		LIMIT ?;`,
		gid[:], 1,
	)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer rows.Close()

	var sawLatestIndex bool
	var details []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		details = append(details, detail)
		if strings.Contains(detail, "idx_messages_group_latest") {
			sawLatestIndex = true
		}
		if strings.HasPrefix(detail, "SCAN messages") || strings.Contains(detail, "USE TEMP B-TREE") {
			t.Fatalf("latest query planner used unbounded work:\n  detail: %q\n  rows=%q", detail, details)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("latest plan rows: %v", err)
	}
	if !sawLatestIndex {
		t.Fatalf("EXPLAIN QUERY PLAN did not reference idx_messages_group_latest; rows=%q", details)
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
	if _, err := s.Put(ctx, m.GroupID, m); err != nil {
		t.Fatalf("Put: %v", err)
	}

	dbPath := filepath.Join(root, "groups", gid.DirName(), "messages.sqlite")
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("sqlite file mode = %04o, want 0600", mode)
	}
}

func TestSQLiteGroupDBUsesSingleConnection(t *testing.T) {
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	db, err := s.dbFor(randGroupID(t))
	if err != nil {
		t.Fatalf("dbFor: %v", err)
	}
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections = %d, want 1", got)
	}
}

func TestSQLiteMessageIDsPageEnumeratesMoreThanWirePage(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	const messageCount = 1029
	want := make(map[entmoot.MessageID]struct{}, messageCount)
	for i := 0; i < messageCount; i++ {
		msg := mkMsg(t, gid, testAuthor(uint32(i%7+1), byte(i)), int64(i/3+1), fmt.Sprintf("page-%d", i))
		if _, err := s.Put(ctx, gid, msg); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		want[msg.ID] = struct{}{}
	}

	var (
		cursor     *RangeCursor
		generation uint64
		got        = make(map[entmoot.MessageID]struct{}, messageCount)
	)
	for pageNumber := 0; ; pageNumber++ {
		page, err := s.MessageIDsPage(ctx, gid, 0, cursor, generation, 113)
		if err != nil {
			t.Fatalf("MessageIDsPage %d: %v", pageNumber, err)
		}
		if page.SnapshotChanged {
			t.Fatalf("MessageIDsPage %d unexpectedly changed snapshot", pageNumber)
		}
		if generation == 0 {
			generation = page.Generation
		} else if page.Generation != generation {
			t.Fatalf("page generation = %d, want %d", page.Generation, generation)
		}
		for _, id := range page.IDs {
			if _, duplicate := got[id]; duplicate {
				t.Fatalf("duplicate id %s", id)
			}
			if _, known := want[id]; !known {
				t.Fatalf("unknown id %s", id)
			}
			got[id] = struct{}{}
		}
		if !page.HasMore {
			break
		}
		if page.Next == nil {
			t.Fatalf("page %d has_more without cursor", pageNumber)
		}
		cursor = page.Next
	}
	if len(got) != messageCount {
		t.Fatalf("enumerated %d ids, want %d", len(got), messageCount)
	}

	first, err := s.MessageIDsPage(ctx, gid, 0, nil, 0, 10)
	if err != nil {
		t.Fatalf("first mutation page: %v", err)
	}
	newMessage := mkMsg(t, gid, testAuthor(99, 0x99), messageCount+1, "generation-change")
	if _, err := s.Put(ctx, gid, newMessage); err != nil {
		t.Fatalf("Put generation change: %v", err)
	}
	changed, err := s.MessageIDsPage(ctx, gid, 0, first.Next, first.Generation, 10)
	if err != nil {
		t.Fatalf("changed mutation page: %v", err)
	}
	if !changed.SnapshotChanged {
		t.Fatalf("continuation across mutation did not report snapshot change")
	}

	want[newMessage.ID] = struct{}{}
	cursor = nil
	generation = 0
	resumed := make(map[entmoot.MessageID]struct{}, messageCount+1)
	for pageNumber := 0; ; pageNumber++ {
		page, err := s.MessageIDsPage(ctx, gid, 0, cursor, generation, 127)
		if err != nil {
			t.Fatalf("resumed MessageIDsPage %d: %v", pageNumber, err)
		}
		if page.SnapshotChanged {
			t.Fatalf("resumed MessageIDsPage %d unexpectedly changed", pageNumber)
		}
		if generation == 0 {
			generation = page.Generation
		}
		for _, id := range page.IDs {
			if _, duplicate := resumed[id]; duplicate {
				t.Fatalf("resumed duplicate id %s", id)
			}
			if _, known := want[id]; !known {
				t.Fatalf("resumed unknown id %s", id)
			}
			resumed[id] = struct{}{}
		}
		if !page.HasMore {
			break
		}
		if page.Next == nil {
			t.Fatalf("resumed page %d has_more without cursor", pageNumber)
		}
		cursor = page.Next
	}
	if len(resumed) != messageCount+1 {
		t.Fatalf("resumed enumeration = %d ids, want %d", len(resumed), messageCount+1)
	}
}

func TestSQLitePruneTombstonePreventsResurrection(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	msg := mkMsg(t, gid, testAuthor(1, 0x01), 10, "prune-me")
	if _, err := s.Put(ctx, gid, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	recent := mkMsg(t, gid, testAuthor(2, 0x02), 30, "keep-me")
	if _, err := s.Put(ctx, gid, recent); err != nil {
		t.Fatalf("Put recent: %v", err)
	}
	beforePrune, err := s.MessageIDsPage(ctx, gid, 0, nil, 0, 1)
	if err != nil {
		t.Fatalf("MessageIDsPage before prune: %v", err)
	}
	pruned, err := s.PruneBefore(ctx, gid, 20)
	if err != nil {
		t.Fatalf("PruneBefore: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("pruned = %d, want 1", pruned)
	}
	hasTombstone, err := s.HasTombstone(ctx, gid, msg.ID)
	if err != nil {
		t.Fatalf("HasTombstone: %v", err)
	}
	if !hasTombstone {
		t.Fatal("pruned message has no tombstone")
	}
	changed, err := s.MessageIDsPage(ctx, gid, 0, beforePrune.Next, beforePrune.Generation, 1)
	if err != nil {
		t.Fatalf("MessageIDsPage after prune: %v", err)
	}
	if !changed.SnapshotChanged {
		t.Fatal("continuation across prune did not report snapshot change")
	}
	if inserted, err := s.Put(ctx, gid, msg); inserted || !errors.Is(err, ErrPruned) {
		t.Fatalf("Put pruned message = (%v, %v), want (false, ErrPruned)", inserted, err)
	}
	page, err := s.MessageIDsPage(ctx, gid, 0, nil, 0, 10)
	if err != nil {
		t.Fatalf("MessageIDsPage: %v", err)
	}
	if page.CoverageFloorMS != 20 {
		t.Fatalf("coverage floor = %d, want 20", page.CoverageFloorMS)
	}
	if len(page.IDs) != 1 || page.IDs[0] != recent.ID {
		t.Fatalf("page IDs = %v, want retained id %s", page.IDs, recent.ID)
	}
}

func TestSQLiteMerkleCacheTracksCommittedGeneration(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	first := mkMsg(t, gid, testAuthor(1, 0x01), 10, "first")
	if _, err := s.Put(ctx, gid, first); err != nil {
		t.Fatalf("Put first: %v", err)
	}
	firstRoot, err := s.MerkleRoot(ctx, gid)
	if err != nil {
		t.Fatalf("MerkleRoot first: %v", err)
	}
	db, exists, err := s.dbForExisting(gid)
	if err != nil || !exists {
		t.Fatalf("dbForExisting = (%v, %v)", exists, err)
	}
	var generation, rootGeneration int64
	var cached []byte
	if err := db.QueryRowContext(ctx, `
		SELECT generation, root_generation, merkle_root
		FROM group_sync_state WHERE group_id = ?;`,
		gid[:],
	).Scan(&generation, &rootGeneration, &cached); err != nil {
		t.Fatalf("read first cache: %v", err)
	}
	if rootGeneration != generation || !bytes.Equal(cached, firstRoot[:]) {
		t.Fatalf("first cache generation/root = (%d, %d, %x), want current %x", generation, rootGeneration, cached, firstRoot)
	}

	second := mkMsg(t, gid, testAuthor(2, 0x02), 20, "second")
	if _, err := s.Put(ctx, gid, second); err != nil {
		t.Fatalf("Put second: %v", err)
	}
	var changedGeneration, staleRootGeneration int64
	if err := db.QueryRowContext(ctx, `
		SELECT generation, root_generation
		FROM group_sync_state WHERE group_id = ?;`,
		gid[:],
	).Scan(&changedGeneration, &staleRootGeneration); err != nil {
		t.Fatalf("read invalidated cache: %v", err)
	}
	if changedGeneration <= generation || staleRootGeneration == changedGeneration {
		t.Fatalf("cache was not invalidated: generation=%d root_generation=%d prior=%d", changedGeneration, staleRootGeneration, generation)
	}
	secondRoot, err := s.MerkleRoot(ctx, gid)
	if err != nil {
		t.Fatalf("MerkleRoot second: %v", err)
	}
	if secondRoot == firstRoot {
		t.Fatal("Merkle root did not change after insert")
	}
	if err := db.QueryRowContext(ctx, `
		SELECT generation, root_generation, merkle_root
		FROM group_sync_state WHERE group_id = ?;`,
		gid[:],
	).Scan(&generation, &rootGeneration, &cached); err != nil {
		t.Fatalf("read refreshed cache: %v", err)
	}
	if rootGeneration != generation || !bytes.Equal(cached, secondRoot[:]) {
		t.Fatalf("refreshed cache generation/root = (%d, %d, %x), want current %x", generation, rootGeneration, cached, secondRoot)
	}

	third := mkMsg(t, gid, testAuthor(3, 0x03), 30, "direct-import")
	encoded, err := canonical.Encode(third)
	if err != nil {
		t.Fatalf("canonical encode third: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO messages
		  (message_id, group_id, author_member_id, timestamp_ms,
		   content, parents, signature, canonical_bytes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		third.ID[:], gid[:], third.Author.MemberID[:], third.Timestamp,
		notNilBytes(third.Content), parentsBlob(third.Parents), notNilBytes(third.Signature), encoded,
	); err != nil {
		t.Fatalf("direct import insert: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT generation, root_generation
		FROM group_sync_state WHERE group_id = ?;`,
		gid[:],
	).Scan(&changedGeneration, &staleRootGeneration); err != nil {
		t.Fatalf("read direct-import invalidation: %v", err)
	}
	if changedGeneration <= generation || staleRootGeneration == changedGeneration {
		t.Fatalf("direct import bypassed cache invalidation: generation=%d root_generation=%d prior=%d", changedGeneration, staleRootGeneration, generation)
	}
}
