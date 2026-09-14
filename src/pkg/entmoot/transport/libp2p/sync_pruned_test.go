package libp2ptransport

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// wrappedStore is shaped like the daemon's own destination: a wrapper that
// implements MessageStore and forwards the retention interfaces explicitly.
// The fix has to work through that shape, not only against a bare *SQLite.
type wrappedStore struct {
	inner *store.SQLite
}

func (w wrappedStore) Put(ctx context.Context, group entmoot.GroupID, message entmoot.Message) (bool, error) {
	return w.inner.Put(ctx, group, message)
}
func (w wrappedStore) Get(ctx context.Context, group entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error) {
	return w.inner.Get(ctx, group, id)
}
func (w wrappedStore) Has(ctx context.Context, group entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	return w.inner.Has(ctx, group, id)
}
func (w wrappedStore) Range(ctx context.Context, group entmoot.GroupID, since, until int64) ([]entmoot.Message, error) {
	return w.inner.Range(ctx, group, since, until)
}
func (w wrappedStore) Latest(ctx context.Context, group entmoot.GroupID, limit int) ([]entmoot.Message, error) {
	return w.inner.Latest(ctx, group, limit)
}
func (w wrappedStore) LatestBefore(ctx context.Context, group entmoot.GroupID, limit int, boundary *store.PageBoundary) ([]entmoot.Message, error) {
	return w.inner.LatestBefore(ctx, group, limit, boundary)
}
func (w wrappedStore) Topics(ctx context.Context, group entmoot.GroupID, limit int) ([]store.TopicSummary, error) {
	return w.inner.Topics(ctx, group, limit)
}
func (w wrappedStore) LatestByTopic(ctx context.Context, group entmoot.GroupID, topic string, limit int) ([]entmoot.Message, error) {
	return w.inner.LatestByTopic(ctx, group, topic, limit)
}
func (w wrappedStore) LatestByTopicBefore(ctx context.Context, group entmoot.GroupID, topic string, limit int, boundary *store.PageBoundary) ([]entmoot.Message, error) {
	return w.inner.LatestByTopicBefore(ctx, group, topic, limit, boundary)
}
func (w wrappedStore) MerkleRoot(ctx context.Context, group entmoot.GroupID) ([32]byte, error) {
	return w.inner.MerkleRoot(ctx, group)
}
func (w wrappedStore) IterMessageIDsInIDRange(ctx context.Context, group entmoot.GroupID, loID, hiID entmoot.MessageID) ([]entmoot.MessageID, error) {
	return w.inner.IterMessageIDsInIDRange(ctx, group, loID, hiID)
}
func (w wrappedStore) HasTombstone(ctx context.Context, group entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	return store.HasTombstone(ctx, w.inner, group, id)
}
func (w wrappedStore) Close() error { return nil }

// A node with a shorter retention window keeps being offered messages it has
// already dropped. Before this fix it asked for them every round and its own
// store refused them with ErrPruned, which aborted the whole keeper pass: one
// expired message stalled history sync permanently.
func TestPrunedHistoryDoesNotStallSyncOrCountAsMissing(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	for sequence := 3; sequence <= 12; sequence++ {
		f.addMessage(t, group, sequence)
	}
	server := &SyncServer{
		Host: f.serverHost, Admission: NewBootstrapAdmission(), Store: f.store,
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) { log, ok := f.logs[id]; return log, ok },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}

	local, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	// The destination is the wrapper shape the daemon builds, so the tombstone
	// path has to reach through it rather than only working against *SQLite.
	destination := wrappedStore{inner: local}
	validate := func(message entmoot.Message, _ *merkle.Proof) error {
		return signing.VerifyMessage(message, message.Author)
	}
	keepers := []peer.AddrInfo{f.remote}
	state := new(HistorySyncState)

	// First pass: take everything the keeper serves.
	first := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, state)[0]
	if first.Err != nil || first.Inserted == 0 {
		t.Fatalf("initial sync inserted=%d err=%v", first.Inserted, first.Err)
	}

	// Retention drops the oldest half locally, leaving tombstones. The keeper
	// keeps serving them, as a peer with a longer window does.
	// Fixture messages use timestamp 10_000+sequence.
	cutoff := int64(10_000 + 7)
	pruned, err := store.PruneBeforeExceptTopics(context.Background(), local, group, cutoff, nil)
	if err != nil || pruned == 0 {
		t.Fatalf("prune removed %d messages: %v", pruned, err)
	}

	// Later passes must still complete, report the dropped ids as pruned
	// rather than missing, and never re-insert them.
	for pass := 0; pass < 3; pass++ {
		state = new(HistorySyncState)
		item := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, state)[0]
		if item.Err != nil {
			t.Fatalf("pass %d failed after pruning: %v", pass, item.Err)
		}
		if item.Inserted != 0 {
			t.Fatalf("pass %d re-inserted %d pruned messages", pass, item.Inserted)
		}
		if item.MissingBodies != 0 {
			t.Fatalf("pass %d counted %d pruned ids as missing bodies", pass, item.MissingBodies)
		}
		if item.PrunedLocally != int(pruned) {
			t.Fatalf("pass %d reported %d pruned identifiers, want %d", pass, item.PrunedLocally, pruned)
		}
		if !item.Available {
			t.Fatalf("pass %d reported the keeper unavailable", pass)
		}
		// Recognising the tombstone means the body is never requested again.
		// A pass that re-downloads the dropped history costs bodies; a pass
		// that recognises it costs only the listing, well under a quarter of
		// the initial transfer.
		if item.TransferredBytes*4 >= first.TransferredBytes {
			t.Fatalf("pass %d transferred %d bytes against an initial %d: pruned bodies were re-requested",
				pass, item.TransferredBytes, first.TransferredBytes)
		}
	}

	// The pruned messages stay gone: a node's own retention decision wins.
	for _, id := range f.ids[group][:2] {
		if present, err := local.Has(context.Background(), group, id); err != nil || present {
			t.Fatalf("pruned message %s came back: present=%t err=%v", id, present, err)
		}
	}
}

// hidesTombstones answers the tombstone check as if the identifier were still
// fetchable, while the store underneath already holds the tombstone. That is
// the shape of a retention pass landing between the check and the insert: the
// client asks for the body and Put refuses it.
type hidesTombstones struct {
	wrappedStore
}

func (hidesTombstones) HasTombstone(context.Context, entmoot.GroupID, entmoot.MessageID) (bool, error) {
	return false, nil
}

// Retention can tombstone an identifier between listing and insertion. The
// refusal is correct; failing the keeper over it is not.
func TestPruneDuringInsertionIsAnIntentionalGap(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	for sequence := 3; sequence <= 12; sequence++ {
		f.addMessage(t, group, sequence)
	}
	server := &SyncServer{
		Host: f.serverHost, Admission: NewBootstrapAdmission(), Store: f.store,
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) { log, ok := f.logs[id]; return log, ok },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	local, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	base := wrappedStore{inner: local}
	validate := func(message entmoot.Message, _ *merkle.Proof) error {
		return signing.VerifyMessage(message, message.Author)
	}
	keepers := []peer.AddrInfo{f.remote}

	// Take the history, then prune it: the store now holds tombstones.
	if first := SyncFromKeepers(f.ctx, f.client, group, keepers, base, validate, new(HistorySyncState))[0]; first.Err != nil {
		t.Fatalf("initial sync failed: %v", first.Err)
	}
	pruned, err := store.PruneBeforeExceptTopics(context.Background(), local, group, int64(10_000+12), nil)
	if err != nil || pruned == 0 {
		t.Fatalf("prune removed %d messages: %v", pruned, err)
	}

	// Hiding the tombstone check reproduces retention landing after the check
	// and before the insert: the bodies are requested and Put refuses them.
	destination := hidesTombstones{wrappedStore: base}
	item := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, new(HistorySyncState))[0]
	if item.Err != nil {
		t.Fatalf("a prune between listing and insertion failed the keeper: %v", item.Err)
	}
	if !item.Available {
		t.Fatal("keeper reported unavailable after an insert-time refusal")
	}
	if item.PrunedLocally != int(pruned) {
		t.Fatalf("insert-time refusals reported %d pruned, want %d", item.PrunedLocally, pruned)
	}
	if item.Inserted != 0 {
		t.Fatalf("a pruned message was re-inserted %d times", item.Inserted)
	}
}

// The coverage floor advances whenever retention runs, even when it deletes
// nothing and for messages retention deliberately exempts. Using it to narrow
// what a node asks for would hide history it still wants, so a message kept
// below the floor must still arrive.
func TestRetainedHistoryBelowTheCoverageFloorStillSyncs(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	for sequence := 3; sequence <= 12; sequence++ {
		f.addMessage(t, group, sequence)
	}
	server := &SyncServer{
		Host: f.serverHost, Admission: NewBootstrapAdmission(), Store: f.store,
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) { log, ok := f.logs[id]; return log, ok },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	destination, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	validate := func(message entmoot.Message, _ *merkle.Proof) error {
		return signing.VerifyMessage(message, message.Author)
	}
	keepers := []peer.AddrInfo{f.remote}

	// Retention runs on an empty store, exactly as it does at session start on
	// a joining node: nothing is deleted, but the floor moves.
	cutoff := int64(10_000 + 9)
	if pruned, err := store.PruneBeforeExceptTopics(context.Background(), destination, group, cutoff, nil); err != nil || pruned != 0 {
		t.Fatalf("prune on an empty store removed %d: %v", pruned, err)
	}
	floor, err := destination.CoverageFloor(context.Background(), group)
	if err != nil || floor == 0 {
		t.Fatalf("coverage floor after prune = %d/%v, want a floor", floor, err)
	}

	item := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, new(HistorySyncState))[0]
	if item.Err != nil {
		t.Fatalf("sync failed: %v", item.Err)
	}
	if item.PrunedLocally != 0 {
		t.Fatalf("a floor with no tombstones reported %d pruned identifiers", item.PrunedLocally)
	}
	// Every message the keeper holds must land, including those older than the
	// floor this node advanced without dropping anything.
	for _, id := range f.ids[group] {
		present, err := destination.Has(context.Background(), group, id)
		if err != nil {
			t.Fatal(err)
		}
		if !present {
			t.Fatalf("history below the coverage floor was never fetched: %s", id)
		}
	}
}

// A historical message whose roster checkpoint is not on this node's chain yet
// cannot be authorized, but it is not junk and it is not the keeper's fault:
// the pass skips it, keeps the keeper, and must not claim convergence, because
// claiming it would stop the retry that eventually picks the message up.
func TestUnknownRosterHeadIsAGapNotConvergence(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	for sequence := 3; sequence <= 6; sequence++ {
		f.addMessage(t, group, sequence)
	}
	local, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()

	// The last identifier names a checkpoint this node has not synchronized,
	// which is what the daemon's historical validation reports mid-catch-up.
	unsynchronized := f.ids[group][len(f.ids[group])-1]
	var lagging atomic.Bool
	lagging.Store(true)
	validate := func(message entmoot.Message, _ *merkle.Proof) error {
		if lagging.Load() && message.ID == unsynchronized {
			return fmt.Errorf("%w: historical head", entmoot.ErrRosterHeadUnknown)
		}
		return signing.VerifyMessage(message, message.Author)
	}
	keepers := []peer.AddrInfo{f.remote}

	item := SyncFromKeepers(f.ctx, f.client, group, keepers, local, validate, new(HistorySyncState))[0]
	if item.Err != nil {
		t.Fatalf("an unknown checkpoint failed the keeper: %v", item.Err)
	}
	if !item.Available {
		t.Fatal("keeper reported unavailable for an unknown checkpoint")
	}
	if item.UnknownHeads != 1 {
		t.Fatalf("unknown heads = %d, want 1", item.UnknownHeads)
	}
	if item.Inserted != len(f.ids[group])-1 {
		t.Fatalf("inserted %d of %d authorizable messages", item.Inserted, len(f.ids[group])-1)
	}
	if item.ConvergedHint {
		t.Fatal("a pass that skipped a message claimed convergence")
	}
	if present, err := local.Has(context.Background(), group, unsynchronized); err != nil || present {
		t.Fatalf("an unauthorized message was stored: present=%t err=%v", present, err)
	}

	// After roster synchronization the retry completes and converges.
	lagging.Store(false)
	retry := SyncFromKeepers(f.ctx, f.client, group, keepers, local, validate, new(HistorySyncState))[0]
	if retry.Err != nil || retry.Inserted != 1 || retry.UnknownHeads != 0 {
		t.Fatalf("retry after roster sync: inserted=%d unknown=%d err=%v", retry.Inserted, retry.UnknownHeads, retry.Err)
	}
	if !retry.ConvergedHint {
		t.Fatal("a complete pass did not report convergence")
	}
}
