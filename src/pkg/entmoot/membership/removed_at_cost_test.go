package membership

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// churnGroup builds a group through Apply and SignCheckpoint the way a
// founder's node sees one: identities join with the founder's invite and go
// again - every other one removed by the founder, the rest leaving - until
// about n records are held, with the founder signing a checkpoint after
// every `every` records, or after every departure when every is zero, as
// revocation seals do. Checkpoints retire what they cover into history, which
// is never pruned, so the held records grow with n.
func churnGroup(tb testing.TB, n, every int) (*Group, entmoot.MemberID) {
	tb.Helper()
	founder, err := keystore.Generate()
	if err != nil {
		tb.Fatal(err)
	}
	founderInfo, err := identityInfo(founder)
	if err != nil {
		tb.Fatal(err)
	}
	gid := entmoot.GroupID{0x5c}
	clock := int64(1_000)
	group, err := Create(tb.TempDir(), founder, founderInfo, gid, DefaultPolicy(), clock)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = group.Close() })
	// Durability is not under test, and a sync per record is most of what
	// building thousands of them costs.
	if _, err := group.db.Exec(`PRAGMA synchronous = OFF`); err != nil {
		tb.Fatal(err)
	}
	group.SetNow(func() time.Time { return time.UnixMilli(clock) })
	apply := func(identity *keystore.Identity, rec Record) {
		tb.Helper()
		info, err := identityInfo(identity)
		if err != nil {
			tb.Fatal(err)
		}
		clock += 10
		rec.GroupID, rec.Actor, rec.Timestamp = gid, info, clock
		if rec.Kind == KindJoin || rec.Kind == KindLeave {
			rec.Subject = info
		}
		signed, err := SignRecord(identity, rec)
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := group.Apply(signed); err != nil {
			tb.Fatalf("apply %s: %v", rec.Kind, err)
		}
	}
	checkpoint := func() {
		tb.Helper()
		if _, _, err := group.SignCheckpoint(founder, true); err != nil {
			tb.Fatalf("checkpoint: %v", err)
		}
	}
	held := 0
	for i := 0; held < n; i++ {
		identity, err := keystore.Generate()
		if err != nil {
			tb.Fatal(err)
		}
		info, err := identityInfo(identity)
		if err != nil {
			tb.Fatal(err)
		}
		capability := entmoot.BootstrapCapability{
			GroupID: gid, Founder: founderInfo, MaxUses: 1, IssuedAtMS: clock, ExpiresAtMS: clock + 3_600_000,
			TargetPublicKey: info.EntmootPubKey, TargetMemberID: *info.MemberID, TargetPeerID: info.PeerID,
		}
		if _, err := rand.Read(capability.Nonce[:]); err != nil {
			tb.Fatal(err)
		}
		if err := SignInvite(founder, &capability); err != nil {
			tb.Fatal(err)
		}
		apply(identity, Record{Kind: KindJoin, Invite: &capability})
		if i%2 == 0 {
			apply(founder, Record{Kind: KindRemove, Subject: info})
		} else {
			apply(identity, Record{Kind: KindLeave})
		}
		for range 2 {
			held++
			if every > 0 && held%every == 0 {
				checkpoint()
			}
		}
		if every == 0 {
			checkpoint()
		}
	}
	return group, *founderInfo.MemberID
}

// coldRemovedAt is one RemovedAt over every member, as the reconciler's
// first pass after opening the group or after the canonical checkpoint moved
// asks it, with nothing kept from an earlier call.
func coldRemovedAt(group *Group, self entmoot.MemberID) map[entmoot.MemberID]Removal {
	group.endingsMu.Lock()
	group.endingsCache = endingsCache{}
	group.endingsMu.Unlock()
	return group.RemovedAt(nil, self)
}

// fastest runs f a few times and returns its quickest run, the least noisy
// estimate of what it costs.
func fastest(f func()) time.Duration {
	best := time.Duration(1<<63 - 1)
	for range 3 {
		start := time.Now()
		f()
		best = min(best, time.Since(start))
	}
	return best
}

// TestRemovedAtCostsAboutOneProjection: how memberships ended is read from
// one projection of the held records, however many checkpoints the members
// asked about went out in. Re-projecting everything held after each such
// checkpoint made one call quadratic in the group's history: with a
// checkpoint every 64 records, a pass over 6000 records cost about 90 full
// projections, and with one after every departure, as revocation seals cause,
// several hundred. The groups here are smaller than that, to keep the test
// quick; the benchmarks below use the full sizes. The bound is relative to a plain projection of the same
// records measured in the same run, so it holds on a slow or loaded machine.
func TestRemovedAtCostsAboutOneProjection(t *testing.T) {
	if testing.Short() {
		t.Skip("builds groups of several thousand records")
	}
	for _, tc := range []struct {
		name     string
		records  int
		every    int
		minGoing int
		// bound is how many plain projections of the held records one call
		// may cost. One projection, plus reading the members of every
		// checkpoint on the chain, is what it costs here; re-projecting per
		// checkpoint cost about 15 in the first case and 150 in the second.
		bound int
	}{
		{"checkpoint every 64 records", 2000, 64, 990, 6},
		{"checkpoint after every departure", 600, 0, 290, 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group, self := churnGroup(t, tc.records, tc.every)
			group.mu.RLock()
			base := group.checkpoints[group.canonicalID]
			for {
				previous, ok := group.checkpoints[base.Previous]
				if !ok {
					break
				}
				base = previous
			}
			records := make([]Record, 0, len(group.records)+len(group.history))
			for _, rec := range group.records {
				records = append(records, rec)
			}
			for _, rec := range group.history {
				records = append(records, rec)
			}
			checkpoints := len(group.checkpoints)
			group.mu.RUnlock()
			if len(records) < tc.records || checkpoints < tc.records/max(tc.every, 2)/2 {
				t.Fatalf("precondition: %d records over %d checkpoints", len(records), checkpoints)
			}
			if removed := coldRemovedAt(group, self); len(removed) < tc.minGoing/2 {
				t.Fatalf("precondition: %d removals reported", len(removed))
			}
			projection := fastest(func() { project(base, records) })
			call := fastest(func() { coldRemovedAt(group, self) })
			t.Logf("%d records, %d checkpoints: RemovedAt %v, one projection %v (%.1fx)",
				len(records), checkpoints, call, projection, float64(call)/float64(projection))
			if call > time.Duration(tc.bound)*projection {
				t.Fatalf("RemovedAt took %v, more than %d projections of the %d held records (%v each)", call, tc.bound, len(records), projection)
			}
		})
	}
}

// TestRemovedAtKeepsItsProjectionUntilSomethingChanges: a reconcile pass
// with nothing new applied reuses the projection the last one made, and any
// change to what the group holds - every one reprojects - makes the next one
// start afresh.
func TestRemovedAtKeepsItsProjectionUntilSomethingChanges(t *testing.T) {
	group, self := churnGroup(t, 300, 64)
	kept := func() *membershipEndings {
		group.endingsMu.Lock()
		defer group.endingsMu.Unlock()
		return group.endingsCache.endings
	}
	first := group.RemovedAt(nil, self)
	made := kept()
	if made == nil {
		t.Fatal("RemovedAt kept no projection")
	}
	if again := group.RemovedAt(nil, self); len(again) != len(first) || kept() != made {
		t.Fatal("a second call with nothing applied projected again")
	}
	group.mu.Lock()
	group.reproject()
	group.mu.Unlock()
	group.RemovedAt(nil, self)
	if kept() == made {
		t.Fatal("a change to what the group holds did not start a new projection")
	}
}

// BenchmarkRemovedAtChurn is one cold RemovedAt over every member after
// ordinary churn, a checkpoint every 64 records.
func BenchmarkRemovedAtChurn(b *testing.B) {
	for _, n := range []int{2000, 6000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			group, self := churnGroup(b, n, 64)
			b.ResetTimer()
			for range b.N {
				coldRemovedAt(group, self)
			}
		})
	}
}

// BenchmarkRemovedAtSealEveryDeparture is one cold RemovedAt over every
// member when a checkpoint follows every departure.
func BenchmarkRemovedAtSealEveryDeparture(b *testing.B) {
	group, self := churnGroup(b, 1200, 0)
	b.ResetTimer()
	for range b.N {
		coldRemovedAt(group, self)
	}
}
