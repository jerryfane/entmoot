package libp2ptransport

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// floorlessStore hides its retention floor, as a peer running a server that
// predates coverage windows does. The client then receives identifiers it has
// already pruned and must recognise them from its own tombstones.
type floorlessStore struct {
	*store.SQLite
}

func (floorlessStore) CoverageFloor(context.Context, entmoot.GroupID) (int64, error) {
	return 0, nil
}

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
	// Syncing through a destination that reports no coverage floor keeps the
	// keeper listing the pruned identifiers, which is what makes the tombstone
	// check load-bearing rather than incidental.
	destination := floorlessStore{SQLite: local}
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
		if !item.Available {
			t.Fatalf("pass %d reported the keeper unavailable", pass)
		}
	}

	// The pruned messages stay gone: a node's own retention decision wins.
	for _, id := range f.ids[group][:2] {
		if present, err := local.Has(context.Background(), group, id); err != nil || present {
			t.Fatalf("pruned message %s came back: present=%t err=%v", id, present, err)
		}
	}
}

// A node that knows its own retention floor should stop being offered history
// below it at all, instead of filtering it on every pass.
func TestSyncRequestsOnlyTheWindowThisNodeRetains(t *testing.T) {
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

	full := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, new(HistorySyncState))[0]
	if full.Err != nil || full.Listed == 0 {
		t.Fatalf("initial sync listed=%d err=%v", full.Listed, full.Err)
	}

	cutoff := int64(10_000 + 9)
	if pruned, err := store.PruneBeforeExceptTopics(context.Background(), destination, group, cutoff, nil); err != nil || pruned == 0 {
		t.Fatalf("prune removed %d messages: %v", pruned, err)
	}
	floor, err := destination.CoverageFloor(context.Background(), group)
	if err != nil || floor == 0 {
		t.Fatalf("coverage floor after prune = %d/%v, want a retention floor", floor, err)
	}

	narrowed := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, new(HistorySyncState))[0]
	if narrowed.Err != nil {
		t.Fatalf("sync after prune failed: %v", narrowed.Err)
	}
	if narrowed.Listed >= full.Listed {
		t.Fatalf("keeper still listed %d of %d identifiers below the retention floor", narrowed.Listed, full.Listed)
	}
	if narrowed.Inserted != 0 {
		t.Fatalf("sync after prune re-inserted %d messages", narrowed.Inserted)
	}
}
