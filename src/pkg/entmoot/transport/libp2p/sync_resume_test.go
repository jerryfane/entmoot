package libp2ptransport

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// The observer delegates storage and paging to SQLite. Its only intervention
// closes a real transport connection in the middle of a body response.
type interruptedHistoryStore struct {
	*store.SQLite
	reads        atomic.Uint64
	initialPages atomic.Uint64
	interrupt    func() error
}

func (s *interruptedHistoryStore) Get(ctx context.Context, group entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error) {
	if s.reads.Add(1) == 97 {
		if err := s.interrupt(); err != nil {
			return entmoot.Message{}, err
		}
	}
	return s.SQLite.Get(ctx, group, id)
}

func (s *interruptedHistoryStore) MessageIDsPage(ctx context.Context, group entmoot.GroupID, since int64, after *store.RangeCursor, generation uint64, limit int) (store.MessageIDPage, error) {
	if after == nil {
		s.initialPages.Add(1)
	}
	return s.SQLite.MessageIDsPage(ctx, group, since, after, generation, limit)
}

func TestHistoryResumesInterruptedPageWithoutRestartingSnapshot(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	// The fixture seeds sequences 1-4; carry the group to 600 messages.
	for sequence := 5; sequence <= 600; sequence++ {
		f.addMessage(t, group, sequence)
	}
	observed := &interruptedHistoryStore{SQLite: f.store, interrupt: func() error { return f.client.Network().ClosePeer(f.remote.ID) }}
	// Reuse the real fixture host and authorization, replacing only its store
	// with an observing decorator. No protocol response is fabricated.
	server := &SyncServer{
		Host: f.serverHost, Store: observed,
		Group: func(id entmoot.GroupID) (*membership.Group, bool) {
			group, ok := f.membership[id]
			return group, ok
		},
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	destination := store.NewMemory()
	defer destination.Close()
	state := new(HistorySyncState)
	validate := func(message entmoot.Message, _ *merkle.Proof) error {
		return signing.VerifyMessage(message, message.Author)
	}
	keepers := []peer.AddrInfo{f.remote}
	// A connection cut mid-body must cost neither the snapshot nor the history
	// already transferred. How many passes the client needs is its own business.
	inserted := 0
	converged := false
	for pass := 0; pass < 4 && !converged; pass++ {
		item := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, state)[0]
		inserted += item.Inserted
		converged = item.ConvergedHint
	}
	if !converged || inserted != 600 {
		t.Fatalf("interrupted history converged=%t inserted=%d, want 600", converged, inserted)
	}
	for _, id := range f.ids[group] {
		if present, err := destination.Has(f.ctx, group, id); err != nil || !present {
			t.Fatalf("missing signed history %s: %v", id, err)
		}
	}
	if starts := observed.initialPages.Load(); starts != 1 {
		t.Fatalf("restarted history from the beginning %d times", starts)
	}
}
