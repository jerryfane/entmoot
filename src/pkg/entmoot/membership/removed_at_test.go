package membership

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

// RemovedAt runs on every reconciliation pass with the targets of every live
// invite the node minted, and an unlimited open invite lets anybody mint
// those for identities that never join. Asking about a thousand of them over
// a 2000-record window used to take seconds, all of it under the group's read
// lock, so a writer - SignRecord, Apply - waited that long too.
func TestRemovedAtIsCheapForNeverMembersAndDoesNotHoldTheLock(t *testing.T) {
	group, records := benchmarkGroup(t, 2000)
	ids := neverMembers(t, 1000)
	var removed []entmoot.MemberID
	for _, rec := range records {
		if rec.Kind == KindRemove && len(removed) < 10 {
			id, err := rec.SubjectMemberID()
			if err != nil {
				t.Fatal(err)
			}
			removed = append(removed, id)
		}
	}
	group.RemovedAt(ids, entmoot.MemberID{})
	start := time.Now()
	none := group.RemovedAt(ids, entmoot.MemberID{})
	took := time.Since(start)
	t.Logf("RemovedAt over %d never-member ids and %d records: %v", len(ids), len(records), took)
	if took > 100*time.Millisecond && !raceEnabled {
		t.Fatalf("RemovedAt over %d never-member ids and %d records took %v, want well under 100ms", len(ids), len(records), took)
	}
	if len(none) != 0 {
		t.Fatalf("RemovedAt reported %d never-members as removed", len(none))
	}

	ids = append(ids, removed...)
	got := group.RemovedAt(ids, entmoot.MemberID{})
	if len(got) != len(removed) {
		t.Fatalf("RemovedAt reported %d removals, want the %d removed members", len(got), len(removed))
	}
	for _, id := range removed {
		if _, ok := got[id]; !ok {
			t.Fatalf("removed member %s not reported", id)
		}
	}

	// A writer is kept waiting only while RemovedAt copies what it needs.
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			group.RemovedAt(nil, entmoot.MemberID{})
			group.RemovedAt(ids, entmoot.MemberID{})
		}
	}()
	var worst time.Duration
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		asked := time.Now()
		group.mu.Lock()
		if waited := time.Since(asked); waited > worst {
			worst = waited
		}
		group.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	t.Logf("longest a writer waited for the group's lock: %v", worst)
	if worst > 25*time.Millisecond && !raceEnabled {
		t.Fatalf("a writer waited %v for the group's lock while RemovedAt ran", worst)
	}
}
