package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	entpolicy "entmoot/pkg/entmoot/policy"
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
	memberID, err := entmoot.MemberIDFromPublicKey(pk)
	if err != nil {
		panic(err)
	}
	return entmoot.NodeInfo{
		PilotNodeID:   entmoot.NodeID(nodeID),
		EntmootPubKey: pk,
		MemberID:      &memberID,
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
		if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
		if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
		inserted, err := s.Put(ctx, gid, m)
		if err != nil {
			t.Fatalf("Put #1: %v", err)
		}
		if !inserted {
			t.Fatal("Put #1 inserted=false, want true")
		}
		inserted, err = s.Put(ctx, gid, m)
		if err != nil {
			t.Fatalf("Put #2: %v", err)
		}
		if inserted {
			t.Fatal("Put #2 inserted=true, want false")
		}
		msgs, err := s.Range(ctx, gid, 0, 0)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Range len = %d, want 1", len(msgs))
		}
	})

	t.Run("PutRejectsExpectedGroupMismatch", func(t *testing.T) {
		s := newStore(t)
		trustedGroup := randGroupID(t)
		foreignGroup := randGroupID(t)
		m := mkMsg(t, foreignGroup, testAuthor(1, 0xCD), 3_100, "foreign")
		inserted, err := s.Put(ctx, trustedGroup, m)
		if err == nil {
			t.Fatal("Put mismatch returned nil error")
		}
		if inserted {
			t.Fatal("Put mismatch inserted=true")
		}
		has, hasErr := s.Has(ctx, foreignGroup, m.ID)
		if hasErr != nil {
			t.Fatalf("Has foreign group: %v", hasErr)
		}
		if has {
			t.Fatal("mismatched message was stored")
		}
	})

	t.Run("MultipleMessagesSameGroup", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m1 := mkMsg(t, gid, testAuthor(1, 0x11), 1_000, "a")
		m2 := mkMsg(t, gid, testAuthor(2, 0x22), 2_000, "b")
		for _, m := range []entmoot.Message{m1, m2} {
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
		if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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

	t.Run("LatestBoundedByTimestamp", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m1 := mkMsg(t, gid, testAuthor(1, 0x01), 10, "old")
		m2 := mkMsg(t, gid, testAuthor(1, 0x01), 20, "middle")
		m3 := mkMsg(t, gid, testAuthor(1, 0x01), 30, "new")
		for _, m := range []entmoot.Message{m3, m1, m2} {
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		got, err := s.Latest(ctx, gid, 2)
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if len(got) != 2 || got[0].ID != m2.ID || got[1].ID != m3.ID {
			t.Fatalf("Latest got %v, want middle,new", idsOf(got))
		}

		got, err = s.Latest(ctx, gid, 0)
		if err != nil {
			t.Fatalf("Latest zero: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("Latest zero len = %d, want 0", len(got))
		}
	})

	t.Run("LatestTopologicalWithinSelectedPage", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		parent := mkMsg(t, gid, testAuthor(1, 0x01), 100, "parent")
		child := mkMsg(t, gid, testAuthor(1, 0x01), 50, "child")
		child.Parents = []entmoot.MessageID{parent.ID}
		child.ID = canonical.MessageID(child)
		old := mkMsg(t, gid, testAuthor(1, 0x01), 10, "old")
		for _, m := range []entmoot.Message{child, old, parent} {
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		got, err := s.Latest(ctx, gid, 2)
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if len(got) != 2 || got[0].ID != parent.ID || got[1].ID != child.ID {
			t.Fatalf("Latest topological page got %v, want parent,child", idsOf(got))
		}
	})

	t.Run("LatestBeforeUsesStableRecencyBoundary", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		m1 := mkMsg(t, gid, testAuthor(1, 0x01), 10, "old")
		m2 := mkMsg(t, gid, testAuthor(1, 0x01), 20, "middle")
		m3 := mkMsg(t, gid, testAuthor(2, 0x02), 20, "middle newer author")
		m4 := mkMsg(t, gid, testAuthor(1, 0x01), 30, "new")
		for _, m := range []entmoot.Message{m4, m1, m3, m2} {
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		got, err := s.LatestBefore(ctx, gid, 2, &PageBoundary{
			TimestampMS:    m3.Timestamp,
			AuthorMemberID: *m3.Author.MemberID,
			MessageID:      m3.ID,
		})
		if err != nil {
			t.Fatalf("LatestBefore: %v", err)
		}
		if len(got) != 2 || got[0].ID != m1.ID || got[1].ID != m2.ID {
			t.Fatalf("LatestBefore got %v, want old,middle", idsOf(got))
		}
	})

	t.Run("TopicsAndLatestByTopic", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		withTopics := func(ts int64, content string, topics ...string) entmoot.Message {
			m := mkMsg(t, gid, testAuthor(1, 0x01), ts, content)
			m.Topics = topics
			m.ID = canonical.MessageID(m)
			return m
		}
		oldOps := withTopics(10, "old ops", "ops")
		newResearchOps := withTopics(20, "new research ops", "research", "ops")
		chat := withTopics(30, "chat", "chat")
		for _, m := range []entmoot.Message{chat, oldOps, newResearchOps} {
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		topics, err := s.Topics(ctx, gid, 10)
		if err != nil {
			t.Fatalf("Topics: %v", err)
		}
		if len(topics) != 3 {
			t.Fatalf("Topics len = %d, want 3", len(topics))
		}
		if topics[0].Topic != "ops" || topics[0].Count != 2 || topics[0].LatestMessageAtMS != 20 {
			t.Fatalf("top topic = %+v, want ops count 2 latest 20", topics[0])
		}
		if topics[1].Topic != "chat" || topics[2].Topic != "research" {
			t.Fatalf("topic tie order = %q, %q; want chat, research", topics[1].Topic, topics[2].Topic)
		}

		msgs, err := s.LatestByTopic(ctx, gid, "ops", 2)
		if err != nil {
			t.Fatalf("LatestByTopic: %v", err)
		}
		if len(msgs) != 2 || msgs[0].ID != oldOps.ID || msgs[1].ID != newResearchOps.ID {
			t.Fatalf("LatestByTopic got %v, want oldOps,newResearchOps", idsOf(msgs))
		}

		msgs, err = s.LatestByTopicBefore(ctx, gid, "ops", 1, &PageBoundary{
			TimestampMS:    newResearchOps.Timestamp,
			AuthorMemberID: *newResearchOps.Author.MemberID,
			MessageID:      newResearchOps.ID,
		})
		if err != nil {
			t.Fatalf("LatestByTopicBefore: %v", err)
		}
		if len(msgs) != 1 || msgs[0].ID != oldOps.ID {
			t.Fatalf("LatestByTopicBefore got %v, want oldOps", idsOf(msgs))
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
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
		if _, err := s.Put(ctx, m.GroupID, m); err == nil {
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
		if _, err := s.Put(ctx, m.GroupID, m); err == nil {
			t.Fatal("Put with zero MessageID returned nil, want error")
		}
	})

	t.Run("PruneBeforeBoundsGroupContent", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		other := randGroupID(t)
		old := mkMsg(t, gid, testAuthor(1, 0x01), 10, "old")
		edge := mkMsg(t, gid, testAuthor(1, 0x01), 20, "edge")
		newer := mkMsg(t, gid, testAuthor(1, 0x01), 30, "new")
		otherOld := mkMsg(t, other, testAuthor(1, 0x01), 10, "other")
		for _, m := range []entmoot.Message{old, edge, newer, otherOld} {
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		pruned, err := PruneBefore(ctx, s, gid, 20)
		if err != nil {
			t.Fatalf("PruneBefore: %v", err)
		}
		if pruned != 1 {
			t.Fatalf("PruneBefore pruned = %d, want 1", pruned)
		}
		if _, err := s.Get(ctx, gid, old.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get old err = %v, want ErrNotFound", err)
		}
		for _, m := range []entmoot.Message{edge, newer, otherOld} {
			if _, err := s.Get(ctx, m.GroupID, m.ID); err != nil {
				t.Fatalf("Get retained %s: %v", m.ID, err)
			}
		}
		got, err := s.Range(ctx, gid, 0, 0)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}
		if len(got) != 2 || got[0].ID != edge.ID || got[1].ID != newer.ID {
			t.Fatalf("Range after prune got %v, want edge,newer", idsOf(got))
		}
	})

	t.Run("PruneBeforeExceptTopicsPreservesPolicyUpdates", func(t *testing.T) {
		s := newStore(t)
		gid := randGroupID(t)
		oldContent := mkMsg(t, gid, testAuthor(1, 0x01), 10, "old")
		oldPolicy := mkMsg(t, gid, testAuthor(1, 0x01), 10, "policy")
		oldPolicy.Topics = []string{entpolicy.UpdateTopic}
		oldPolicy.ID = canonical.MessageID(oldPolicy)
		newContent := mkMsg(t, gid, testAuthor(1, 0x01), 30, "new")
		for _, m := range []entmoot.Message{oldContent, oldPolicy, newContent} {
			if _, err := s.Put(ctx, m.GroupID, m); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		pruned, err := PruneBeforeExceptTopics(ctx, s, gid, 20, []string{entpolicy.UpdateTopic})
		if err != nil {
			t.Fatalf("PruneBeforeExceptTopics: %v", err)
		}
		if pruned != 1 {
			t.Fatalf("PruneBeforeExceptTopics pruned = %d, want 1", pruned)
		}
		if _, err := s.Get(ctx, gid, oldContent.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get old content err = %v, want ErrNotFound", err)
		}
		for _, m := range []entmoot.Message{oldPolicy, newContent} {
			if _, err := s.Get(ctx, gid, m.ID); err != nil {
				t.Fatalf("Get retained %s: %v", m.ID, err)
			}
		}
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
					if _, err := s.Put(ctx, m.GroupID, m); err != nil {
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
