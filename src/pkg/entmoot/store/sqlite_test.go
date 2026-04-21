package store

import (
	"bytes"
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
	"entmoot/pkg/entmoot/wire"

	_ "modernc.org/sqlite"
)

// mkTransportAd constructs a wire.TransportAd for tests. The signature is
// a user-controlled blob so tiebreak tests can pin exact bytes; the shape
// is otherwise production-realistic.
func mkTransportAd(gid entmoot.GroupID, authorNodeID entmoot.NodeID, seq uint64, issuedMs, notAfterMs int64, sig []byte) wire.TransportAd {
	return wire.TransportAd{
		GroupID: gid,
		Author: entmoot.NodeInfo{
			PilotNodeID:   authorNodeID,
			EntmootPubKey: bytes.Repeat([]byte{byte(authorNodeID)}, 32),
		},
		Seq: seq,
		Endpoints: []entmoot.NodeEndpoint{
			{Network: "tcp", Addr: "198.51.100.1:4001"},
		},
		IssuedAt:  issuedMs,
		NotAfter:  notAfterMs,
		Signature: sig,
	}
}

func TestTransportAdPutAndGet(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	ad := mkTransportAd(gid, 42, 1, 1_000, 10_000, []byte("sig-v1"))

	replaced, err := s.PutTransportAd(ctx, ad)
	if err != nil {
		t.Fatalf("PutTransportAd: %v", err)
	}
	if !replaced {
		t.Fatal("PutTransportAd replaced=false on first insert; want true")
	}

	got, ok, err := s.GetTransportAd(ctx, gid, 42)
	if err != nil {
		t.Fatalf("GetTransportAd: %v", err)
	}
	if !ok {
		t.Fatal("GetTransportAd ok=false after Put")
	}
	if got.Seq != ad.Seq {
		t.Fatalf("Seq = %d, want %d", got.Seq, ad.Seq)
	}
	if got.Author.PilotNodeID != ad.Author.PilotNodeID {
		t.Fatalf("Author.PilotNodeID = %d, want %d",
			got.Author.PilotNodeID, ad.Author.PilotNodeID)
	}
	if got.IssuedAt != ad.IssuedAt || got.NotAfter != ad.NotAfter {
		t.Fatalf("timestamps (%d,%d), want (%d,%d)",
			got.IssuedAt, got.NotAfter, ad.IssuedAt, ad.NotAfter)
	}
	if !bytes.Equal(got.Signature, ad.Signature) {
		t.Fatalf("Signature differs: got %x, want %x", got.Signature, ad.Signature)
	}
	if len(got.Endpoints) != 1 || got.Endpoints[0] != ad.Endpoints[0] {
		t.Fatalf("Endpoints = %+v, want %+v", got.Endpoints, ad.Endpoints)
	}
}

func TestTransportAdReplaceOnHigherSeq(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	ad1 := mkTransportAd(gid, 7, 1, 1_000, 10_000, []byte("sig-v1"))
	ad2 := mkTransportAd(gid, 7, 2, 2_000, 20_000, []byte("sig-v2"))

	if _, err := s.PutTransportAd(ctx, ad1); err != nil {
		t.Fatalf("Put ad1: %v", err)
	}
	replaced, err := s.PutTransportAd(ctx, ad2)
	if err != nil {
		t.Fatalf("Put ad2: %v", err)
	}
	if !replaced {
		t.Fatal("replaced=false on higher-seq put; want true")
	}
	got, ok, err := s.GetTransportAd(ctx, gid, 7)
	if err != nil || !ok {
		t.Fatalf("Get: err=%v ok=%v", err, ok)
	}
	if got.Seq != 2 {
		t.Fatalf("Seq = %d, want 2", got.Seq)
	}
	if !bytes.Equal(got.Signature, ad2.Signature) {
		t.Fatalf("Signature = %x, want %x", got.Signature, ad2.Signature)
	}
}

func TestTransportAdRejectLowerSeq(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	ad2 := mkTransportAd(gid, 7, 2, 2_000, 20_000, []byte("sig-v2"))
	ad1 := mkTransportAd(gid, 7, 1, 1_000, 10_000, []byte("sig-v1"))

	if _, err := s.PutTransportAd(ctx, ad2); err != nil {
		t.Fatalf("Put ad2: %v", err)
	}
	replaced, err := s.PutTransportAd(ctx, ad1)
	if err != nil {
		t.Fatalf("Put ad1: %v", err)
	}
	if replaced {
		t.Fatal("replaced=true on lower-seq put; want false")
	}

	got, ok, err := s.GetTransportAd(ctx, gid, 7)
	if err != nil || !ok {
		t.Fatalf("Get: err=%v ok=%v", err, ok)
	}
	if got.Seq != 2 {
		t.Fatalf("Seq = %d, want 2 (ad2 should have won)", got.Seq)
	}
	if !bytes.Equal(got.Signature, ad2.Signature) {
		t.Fatalf("Signature = %x, want ad2's %x", got.Signature, ad2.Signature)
	}
}

func TestTransportAdTiebreakOnSignature(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	// Same seq, different signature bytes. Lex-greater must win.
	sigLow := []byte{0x10, 0x00}
	sigHigh := []byte{0x20, 0x00}

	// Insert low first, then high — high should replace.
	adLow := mkTransportAd(gid, 7, 5, 5_000, 50_000, sigLow)
	adHigh := mkTransportAd(gid, 7, 5, 5_000, 50_000, sigHigh)

	if _, err := s.PutTransportAd(ctx, adLow); err != nil {
		t.Fatalf("Put adLow: %v", err)
	}
	replaced, err := s.PutTransportAd(ctx, adHigh)
	if err != nil {
		t.Fatalf("Put adHigh: %v", err)
	}
	if !replaced {
		t.Fatal("replaced=false on lex-greater-sig equal-seq put; want true")
	}
	got, ok, err := s.GetTransportAd(ctx, gid, 7)
	if err != nil || !ok {
		t.Fatalf("Get: err=%v ok=%v", err, ok)
	}
	if !bytes.Equal(got.Signature, sigHigh) {
		t.Fatalf("Signature = %x, want lex-greater %x", got.Signature, sigHigh)
	}

	// Now put adLow again — must be rejected (lex-smaller-or-equal).
	replaced, err = s.PutTransportAd(ctx, adLow)
	if err != nil {
		t.Fatalf("Put adLow repeat: %v", err)
	}
	if replaced {
		t.Fatal("replaced=true on lex-smaller-sig equal-seq put; want false")
	}
	got, _, _ = s.GetTransportAd(ctx, gid, 7)
	if !bytes.Equal(got.Signature, sigHigh) {
		t.Fatalf("Signature = %x after reject, want lex-greater still %x",
			got.Signature, sigHigh)
	}
}

func TestTransportAdGCExpired(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	now := time.UnixMilli(100_000)
	// Two expired (NotAfter < now), one still valid.
	ads := []wire.TransportAd{
		mkTransportAd(gid, 1, 1, 1_000, 50_000, []byte("a")),
		mkTransportAd(gid, 2, 1, 1_000, 99_999, []byte("b")),
		mkTransportAd(gid, 3, 1, 1_000, 200_000, []byte("c")),
	}
	for _, ad := range ads {
		if _, err := s.PutTransportAd(ctx, ad); err != nil {
			t.Fatalf("Put %d: %v", ad.Author.PilotNodeID, err)
		}
	}

	n, err := s.GCExpiredTransportAds(ctx, now)
	if err != nil {
		t.Fatalf("GCExpiredTransportAds: %v", err)
	}
	if n != 2 {
		t.Fatalf("GC returned %d, want 2", n)
	}

	// Only author 3 should remain.
	if _, ok, _ := s.GetTransportAd(ctx, gid, 1); ok {
		t.Fatal("author 1 should have been GC'd")
	}
	if _, ok, _ := s.GetTransportAd(ctx, gid, 2); ok {
		t.Fatal("author 2 should have been GC'd")
	}
	if _, ok, _ := s.GetTransportAd(ctx, gid, 3); !ok {
		t.Fatal("author 3 should have survived GC")
	}
}

func TestTransportAdGetAllFilterExpired(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	now := time.UnixMilli(100_000)
	ads := []wire.TransportAd{
		mkTransportAd(gid, 1, 1, 1_000, 50_000, []byte("a")),
		mkTransportAd(gid, 2, 1, 1_000, 99_999, []byte("b")),
		mkTransportAd(gid, 3, 1, 1_000, 200_000, []byte("c")),
	}
	for _, ad := range ads {
		if _, err := s.PutTransportAd(ctx, ad); err != nil {
			t.Fatalf("Put %d: %v", ad.Author.PilotNodeID, err)
		}
	}

	got, err := s.GetAllTransportAds(ctx, gid, now, false)
	if err != nil {
		t.Fatalf("GetAllTransportAds: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("unexpired count = %d, want 1", len(got))
	}
	if got[0].Author.PilotNodeID != 3 {
		t.Fatalf("surviving author = %d, want 3", got[0].Author.PilotNodeID)
	}

	// includeExpired=true returns all three, sorted by author_node_id.
	all, err := s.GetAllTransportAds(ctx, gid, now, true)
	if err != nil {
		t.Fatalf("GetAllTransportAds inclExpired: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all count = %d, want 3", len(all))
	}
	for i, ad := range all {
		wantID := entmoot.NodeID(i + 1)
		if ad.Author.PilotNodeID != wantID {
			t.Fatalf("all[%d].Author = %d, want %d (sort by author)",
				i, ad.Author.PilotNodeID, wantID)
		}
	}
}

func TestBumpTransportAdSeqMonotonic(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	for i, want := range []uint64{1, 2, 3} {
		got, err := s.BumpTransportAdSeq(ctx, gid, 99)
		if err != nil {
			t.Fatalf("BumpTransportAdSeq #%d: %v", i, err)
		}
		if got != want {
			t.Fatalf("BumpTransportAdSeq #%d = %d, want %d", i, got, want)
		}
	}
}

func TestBumpTransportAdSeqPersistsAcrossOpen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite #1: %v", err)
	}
	gid := randGroupID(t)
	for i := 0; i < 3; i++ {
		if _, err := s.BumpTransportAdSeq(ctx, gid, 99); err != nil {
			t.Fatalf("BumpTransportAdSeq %d: %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	got, err := s2.BumpTransportAdSeq(ctx, gid, 99)
	if err != nil {
		t.Fatalf("BumpTransportAdSeq post-reopen: %v", err)
	}
	if got != 4 {
		t.Fatalf("seq = %d, want 4 (counter should persist across Close)", got)
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
