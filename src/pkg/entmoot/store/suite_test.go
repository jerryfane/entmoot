package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sort"
	"sync"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
)

// mkMsg constructs a Message with a canonical-encoding-derived ID. Parents
// and Signature are left zero; the store does not verify signatures, so this
// keeps the test cases focused on storage behavior.
func mkMsg(t *testing.T, gid entmoot.GroupID, author entmoot.NodeInfo, ts int64, content string) entmoot.Message {
	t.Helper()
	m := entmoot.Message{
		GroupID:   gid,
		Author:    author,
		Timestamp: ts,
		Content:   []byte(content),
	}
	m.ID = canonical.MessageID(m)
	return m
}

// randGroupID returns a fresh, non-zero GroupID. Fatal on rand failure.
func randGroupID(t *testing.T) entmoot.GroupID {
	t.Helper()
	var g entmoot.GroupID
	if _, err := rand.Read(g[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return g
}

// testAuthor returns a distinct deterministic NodeInfo for tests.
func testAuthor(nodeID uint32, pubTag byte) entmoot.NodeInfo {
	pk := make([]byte, 32)
	for i := range pk {
		pk[i] = pubTag
	}
	return entmoot.NodeInfo{
		PilotNodeID:   entmoot.NodeID(nodeID),
		EntmootPubKey: pk,
	}
}

// runStoreSuite runs the full per-implementation test suite using newStore as
// the factory. Each sub-test gets a fresh store so state does not leak.
func runStoreSuite(t *testing.T, newStore func(t *testing.T) MessageStore) {
	ctx := context.Background()

	t.Run("PutGetRoundTrip", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m := mkMsg(t, gid, testAuthor(1, 0xAA), 1_000, "hello")
		if err := s.Put(ctx, m); err != nil {
			t.Fatalf("Put: %v", err)
		}
		got, err := s.Get(ctx, gid, m.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}

		// Byte-equal after canonical re-encode. Comparing canonical encodings
		// catches silent field mutation even when the Go values are not
		// deeply equal (e.g., nil vs empty slice).
		wantBytes, err := canonical.Encode(m)
		if err != nil {
			t.Fatalf("Encode want: %v", err)
		}
		gotBytes, err := canonical.Encode(got)
		if err != nil {
			t.Fatalf("Encode got: %v", err)
		}
		if !bytes.Equal(wantBytes, gotBytes) {
			t.Fatalf("canonical encoding mismatch:\nwant: %s\n got: %s", wantBytes, gotBytes)
		}
	})

	t.Run("HasBeforeAndAfterPut", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m := mkMsg(t, gid, testAuthor(1, 0xBB), 2_000, "x")

		has, err := s.Has(ctx, gid, m.ID)
		if err != nil {
			t.Fatalf("Has before: %v", err)
		}
		if has {
			t.Fatal("Has=true before Put")
		}
		if err := s.Put(ctx, m); err != nil {
			t.Fatalf("Put: %v", err)
		}
		has, err = s.Has(ctx, gid, m.ID)
		if err != nil {
			t.Fatalf("Has after: %v", err)
		}
		if !has {
			t.Fatal("Has=false after Put")
		}
	})

	t.Run("GetUnknownIsErrNotFound", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		// Fabricate a non-zero id that was never Put.
		var id entmoot.MessageID
		id[0] = 0x01
		_, err := s.Get(ctx, gid, id)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("PutIsIdempotent", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m := mkMsg(t, gid, testAuthor(1, 0xCC), 3_000, "dup")
		if err := s.Put(ctx, m); err != nil {
			t.Fatalf("Put #1: %v", err)
		}
		if err := s.Put(ctx, m); err != nil {
			t.Fatalf("Put #2: %v", err)
		}
		msgs, err := s.Range(ctx, gid, 0, 0)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Range len = %d, want 1", len(msgs))
		}
	})

	t.Run("MultipleMessagesSameGroup", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m1 := mkMsg(t, gid, testAuthor(1, 0x11), 1_000, "a")
		m2 := mkMsg(t, gid, testAuthor(2, 0x22), 2_000, "b")
		for _, m := range []entmoot.Message{m1, m2} {
			if err := s.Put(ctx, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if _, err := s.Get(ctx, gid, m1.ID); err != nil {
			t.Fatalf("Get m1: %v", err)
		}
		if _, err := s.Get(ctx, gid, m2.ID); err != nil {
			t.Fatalf("Get m2: %v", err)
		}
	})

	t.Run("GroupIsolation", func(t *testing.T) {
		s := newStore(t)
		gidA := randGroupID(t)
		gidB := randGroupID(t)
		m := mkMsg(t, gidA, testAuthor(1, 0xDD), 1_000, "only-in-a")
		if err := s.Put(ctx, m); err != nil {
			t.Fatalf("Put: %v", err)
		}
		_, err := s.Get(ctx, gidB, m.ID)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get wrong-group err = %v, want ErrNotFound", err)
		}
		has, err := s.Has(ctx, gidB, m.ID)
		if err != nil {
			t.Fatalf("Has wrong-group: %v", err)
		}
		if has {
			t.Fatal("Has wrong-group = true")
		}
	})

	t.Run("RangeAllTopological", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		// Timestamps chosen distinct so topological order here is identical
		// to timestamp order; the store must preserve the ordering contract.
		m1 := mkMsg(t, gid, testAuthor(1, 0x01), 10, "one")
		m2 := mkMsg(t, gid, testAuthor(1, 0x01), 20, "two")
		m3 := mkMsg(t, gid, testAuthor(1, 0x01), 30, "three")
		for _, m := range []entmoot.Message{m3, m1, m2} { // insert out of order
			if err := s.Put(ctx, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		got, err := s.Range(ctx, gid, 0, 0)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("len = %d, want 3", len(got))
		}
		want := []entmoot.MessageID{m1.ID, m2.ID, m3.ID}
		for i, m := range got {
			if m.ID != want[i] {
				t.Fatalf("got[%d].ID = %x, want %x", i, m.ID, want[i])
			}
		}
	})

	t.Run("RangeBoundedWindow", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m1 := mkMsg(t, gid, testAuthor(1, 0x01), 10, "a")
		m2 := mkMsg(t, gid, testAuthor(1, 0x01), 20, "b")
		m3 := mkMsg(t, gid, testAuthor(1, 0x01), 30, "c")
		for _, m := range []entmoot.Message{m1, m2, m3} {
			if err := s.Put(ctx, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		// [10, 30) -> m1 and m2.
		got, err := s.Range(ctx, gid, 10, 30)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}
		if len(got) != 2 || got[0].ID != m1.ID || got[1].ID != m2.ID {
			t.Fatalf("window [10,30) got %d messages (%v)", len(got), idsOf(got))
		}
		// since=25 means only m3 qualifies.
		got, err = s.Range(ctx, gid, 25, 0)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}
		if len(got) != 1 || got[0].ID != m3.ID {
			t.Fatalf("since=25 got %v", idsOf(got))
		}
	})

	t.Run("EmptyGroupMerkleRoot", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		root, err := s.MerkleRoot(ctx, gid)
		if err != nil {
			t.Fatalf("MerkleRoot: %v", err)
		}
		if root != ([32]byte{}) {
			t.Fatalf("empty root = %x, want zero", root)
		}
	})

	t.Run("MerkleRootStable", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		for i := 0; i < 3; i++ {
			m := mkMsg(t, gid, testAuthor(uint32(i+1), byte(i+1)), int64(100+i*10), "m")
			if err := s.Put(ctx, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		a, err := s.MerkleRoot(ctx, gid)
		if err != nil {
			t.Fatalf("MerkleRoot #1: %v", err)
		}
		b, err := s.MerkleRoot(ctx, gid)
		if err != nil {
			t.Fatalf("MerkleRoot #2: %v", err)
		}
		if a != b {
			t.Fatalf("root unstable: %x vs %x", a, b)
		}
		if a == ([32]byte{}) {
			t.Fatal("expected non-zero root for non-empty group")
		}
	})

	t.Run("ZeroGroupIDRejected", func(t *testing.T) {
		s := newStore(t)
		var zero entmoot.GroupID
		m := mkMsg(t, randGroupID(t), testAuthor(1, 0x01), 1_000, "x")
		m.GroupID = zero
		// Recompute ID under the zeroed GroupID so the id is valid shape.
		m.ID = canonical.MessageID(m)
		if err := s.Put(ctx, m); err == nil {
			t.Fatal("Put with zero GroupID returned nil, want error")
		}
	})

	t.Run("ZeroMessageIDRejected", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m := entmoot.Message{
			GroupID:   gid,
			Author:    testAuthor(1, 0x01),
			Timestamp: 1_000,
			Content:   []byte("x"),
			// ID deliberately left zero.
		}
		if err := s.Put(ctx, m); err == nil {
			t.Fatal("Put with zero MessageID returned nil, want error")
		}
	})

	t.Run("IterMessageIDsInIDRange", func(t *testing.T) {
		testIterMessageIDsInIDRange(t, newStore)
	})

	t.Run("ConcurrentPuts", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		const goroutines = 8
		const perG = 8

		var wg sync.WaitGroup
		errCh := make(chan error, goroutines*perG)
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(author uint32) {
				defer wg.Done()
				for i := 0; i < perG; i++ {
					m := mkMsg(t, gid, testAuthor(author, byte(author)), int64(author)*1000+int64(i), "c")
					if err := s.Put(ctx, m); err != nil {
						errCh <- err
						return
					}
				}
			}(uint32(g + 1))
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatalf("concurrent Put: %v", err)
		}

		got, err := s.Range(ctx, gid, 0, 0)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}
		if len(got) != goroutines*perG {
			t.Fatalf("Range len = %d, want %d", len(got), goroutines*perG)
		}
	})
}

// idsOf returns the MessageIDs of a slice of messages, for error reporting.
func idsOf(msgs []entmoot.Message) []entmoot.MessageID {
	out := make([]entmoot.MessageID, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}

// mkMsgWithID constructs a Message with a caller-specified MessageID rather
// than deriving it from canonical.MessageID. The store does not check that
// m.ID matches its canonical encoding, so this gives tests precise control
// over the byte-range keyspace used by IterMessageIDsInIDRange.
func mkMsgWithID(gid entmoot.GroupID, id entmoot.MessageID, author entmoot.NodeInfo, ts int64, content string) entmoot.Message {
	return entmoot.Message{
		ID:        id,
		GroupID:   gid,
		Author:    author,
		Timestamp: ts,
		Content:   []byte(content),
	}
}

// mkID builds a 32-byte MessageID whose first byte is prefix and whose
// second byte is tag. Remaining bytes are zero. That is enough to place
// ids at known points in the byte-sort space while keeping every id
// distinct and non-zero (so the store does not reject them).
func mkID(prefix, tag byte) entmoot.MessageID {
	var id entmoot.MessageID
	id[0] = prefix
	id[1] = tag
	return id
}

// testIterMessageIDsInIDRange exercises the IterMessageIDsInIDRange contract
// on whichever MessageStore newStore returns. It is invoked from the main
// suite so all three backends (Memory / JSONL / SQLite) share one test body.
func testIterMessageIDsInIDRange(t *testing.T, newStore func(t *testing.T) MessageStore) {
	t.Helper()
	ctx := context.Background()
	var zeroID entmoot.MessageID
	var maxID entmoot.MessageID
	for i := range maxID {
		maxID[i] = 0xFF
	}

	// Ten ids spanning the keyspace. Prefixes intentionally include
	// 0x00, 0x40, 0x80, 0xC0, 0xFF; the second byte disambiguates pairs
	// that share a prefix so byte-order is total and well-defined.
	baseIDs := []entmoot.MessageID{
		mkID(0x00, 0x01),
		mkID(0x00, 0x02),
		mkID(0x10, 0x01),
		mkID(0x40, 0x01),
		mkID(0x40, 0x02),
		mkID(0x80, 0x01),
		mkID(0xC0, 0x01),
		mkID(0xC0, 0x02),
		mkID(0xFE, 0x01),
		mkID(0xFF, 0x01),
	}

	// sortedCopy returns baseIDs byte-ascending. baseIDs above is already
	// constructed in ascending order, but asserting it explicitly would
	// couple this test to that construction; we sort a fresh copy instead.
	sortedCopy := func() []entmoot.MessageID {
		out := append([]entmoot.MessageID(nil), baseIDs...)
		sort.Slice(out, func(i, j int) bool {
			return bytes.Compare(out[i][:], out[j][:]) < 0
		})
		return out
	}

	populate := func(t *testing.T, s MessageStore, gid entmoot.GroupID, ids []entmoot.MessageID) {
		t.Helper()
		for i, id := range ids {
			m := mkMsgWithID(gid, id, testAuthor(uint32(i+1), byte(i+1)), int64(1000+i), "c")
			if err := s.Put(ctx, m); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}
	}

	assertEqualIDs := func(t *testing.T, got, want []entmoot.MessageID) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("len got=%d want=%d\ngot=%x\nwant=%x", len(got), len(want), got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("idx %d: got=%x want=%x\nfull got=%x\nfull want=%x",
					i, got[i], want[i], got, want)
			}
		}
	}

	t.Run("EmptyGroup", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		got, err := s.IterMessageIDsInIDRange(ctx, gid, zeroID, zeroID)
		if err != nil {
			t.Fatalf("Iter: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("empty group returned %d ids (%x), want 0", len(got), got)
		}
	})

	t.Run("FullRangeZeroHiUnbounded", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		populate(t, s, gid, baseIDs)

		got, err := s.IterMessageIDsInIDRange(ctx, gid, zeroID, zeroID)
		if err != nil {
			t.Fatalf("Iter: %v", err)
		}
		assertEqualIDs(t, got, sortedCopy())
	})

	t.Run("FullRangeExplicitMaxHi", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		populate(t, s, gid, baseIDs)

		// hi == all-0xFF. Since baseIDs includes mkID(0xFF,0x01), and the
		// upper bound is exclusive, that id must still appear (it is
		// strictly less than 0xFF..0xFF).
		got, err := s.IterMessageIDsInIDRange(ctx, gid, zeroID, maxID)
		if err != nil {
			t.Fatalf("Iter: %v", err)
		}
		assertEqualIDs(t, got, sortedCopy())
	})

	t.Run("TightRangeExactlyTwo", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		populate(t, s, gid, baseIDs)

		// [0x40..0x02, 0xC0..0x02): contains 0x40-02, 0x80-01, 0xC0-01.
		// Pick a smaller slice: [0x80..0x01, 0xC0..0x02) contains
		// 0x80-01 and 0xC0-01. Exactly 2.
		lo := mkID(0x80, 0x01)
		hi := mkID(0xC0, 0x02)
		got, err := s.IterMessageIDsInIDRange(ctx, gid, lo, hi)
		if err != nil {
			t.Fatalf("Iter: %v", err)
		}
		want := []entmoot.MessageID{mkID(0x80, 0x01), mkID(0xC0, 0x01)}
		assertEqualIDs(t, got, want)
	})

	t.Run("LowerBoundInclusive", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		populate(t, s, gid, baseIDs)

		lo := mkID(0x40, 0x01) // exactly equal to one of our ids
		hi := mkID(0x40, 0x02) // exclusive upper on next id in the sequence
		got, err := s.IterMessageIDsInIDRange(ctx, gid, lo, hi)
		if err != nil {
			t.Fatalf("Iter: %v", err)
		}
		want := []entmoot.MessageID{mkID(0x40, 0x01)}
		assertEqualIDs(t, got, want)
	})

	t.Run("UpperBoundExclusive", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		populate(t, s, gid, baseIDs)

		// Range ending exactly at 0x80..0x01 must exclude that id.
		lo := zeroID
		hi := mkID(0x80, 0x01)
		got, err := s.IterMessageIDsInIDRange(ctx, gid, lo, hi)
		if err != nil {
			t.Fatalf("Iter: %v", err)
		}
		// Want all ids strictly less than 0x80..0x01.
		want := []entmoot.MessageID{
			mkID(0x00, 0x01),
			mkID(0x00, 0x02),
			mkID(0x10, 0x01),
			mkID(0x40, 0x01),
			mkID(0x40, 0x02),
		}
		assertEqualIDs(t, got, want)
	})

	t.Run("InsertOrderIndependence", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)

		// Take five of the base ids and insert them in strictly-reversed
		// byte order; IterMessageIDsInIDRange must still return them
		// byte-ascending.
		subset := []entmoot.MessageID{
			mkID(0x00, 0x01),
			mkID(0x40, 0x01),
			mkID(0x80, 0x01),
			mkID(0xC0, 0x01),
			mkID(0xFF, 0x01),
		}
		reversed := make([]entmoot.MessageID, len(subset))
		for i, id := range subset {
			reversed[len(subset)-1-i] = id
		}
		populate(t, s, gid, reversed)

		got, err := s.IterMessageIDsInIDRange(ctx, gid, zeroID, zeroID)
		if err != nil {
			t.Fatalf("Iter: %v", err)
		}
		assertEqualIDs(t, got, subset)
	})

	t.Run("WrongGroup", func(t *testing.T) {
		s := newStore(t)
		gidA := randGroupID(t)
		gidB := randGroupID(t)
		populate(t, s, gidA, baseIDs)

		// Different group is completely empty wrt the query.
		got, err := s.IterMessageIDsInIDRange(ctx, gidB, zeroID, zeroID)
		if err != nil {
			t.Fatalf("Iter gidB: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("wrong-group query returned %d ids, want 0\nids=%x", len(got), got)
		}

		// Populating gidB with a single distinct id must not leak into
		// gidA's result and vice-versa.
		onlyInB := []entmoot.MessageID{mkID(0x20, 0xAA)}
		populate(t, s, gidB, onlyInB)

		gotA, err := s.IterMessageIDsInIDRange(ctx, gidA, zeroID, zeroID)
		if err != nil {
			t.Fatalf("Iter gidA: %v", err)
		}
		assertEqualIDs(t, gotA, sortedCopy())

		gotB, err := s.IterMessageIDsInIDRange(ctx, gidB, zeroID, zeroID)
		if err != nil {
			t.Fatalf("Iter gidB post-put: %v", err)
		}
		assertEqualIDs(t, gotB, onlyInB)
	})
}

// TestMemory runs the shared suite against Memory.
func TestMemory(t *testing.T) {
	runStoreSuite(t, func(_ *testing.T) MessageStore { return NewMemory() })
}

// TestJSONL runs the shared suite against JSONL using a per-subtest TempDir.
func TestJSONL(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) MessageStore {
		s, err := OpenJSONL(t.TempDir())
		if err != nil {
			t.Fatalf("OpenJSONL: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

// TestSQLite runs the shared suite against SQLite using a per-subtest TempDir.
func TestSQLite(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) MessageStore {
		s, err := OpenSQLite(t.TempDir())
		if err != nil {
			t.Fatalf("OpenSQLite: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}
