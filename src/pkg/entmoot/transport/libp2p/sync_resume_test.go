package libp2ptransport

import (
	"context"
	"sync/atomic"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
	"github.com/libp2p/go-libp2p/core/peer"
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
	for sequence := 3; sequence <= 600; sequence++ {
		f.addMessage(t, group, sequence)
	}
	observed := &interruptedHistoryStore{SQLite: f.store, interrupt: func() error { return f.client.Network().ClosePeer(f.remote.ID) }}
	// Reuse the real fixture host and authorization, replacing only its store
	// with an observing decorator. No protocol response is fabricated.
	remoteHost := f.serverHost
	server := &SyncServer{Host: remoteHost, Admission: NewBootstrapAdmission(), Store: observed, Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) { log, ok := f.logs[id]; return log, ok }}
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
	first := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, state)[0]
	if first.Err == nil || first.Inserted != 64 || first.ConvergedHint {
		t.Fatalf("interrupted pass: %+v", first)
	}
	second := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, state)[0]
	if second.Err != nil || !second.ConvergedHint || second.Inserted != 536 {
		t.Fatalf("resumed pass: %+v", second)
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
