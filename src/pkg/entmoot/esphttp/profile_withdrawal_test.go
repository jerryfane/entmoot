package esphttp

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestWithdrawalBeatsAnOlderProfileInEveryStore pins that both StateStore
// implementations agree. They order differently by construction — SQLite uses
// an ON CONFLICT comparison on observed_at_ms, the in-memory store uses
// shouldReplaceNodeProfile — so a rule proven against one says nothing about
// the other, and a withdrawal that holds in production but not in tests (or
// the reverse) is worse than either.
func TestWithdrawalBeatsAnOlderProfileInEveryStore(t *testing.T) {
	ctx := context.Background()
	gid := testGroupID(3)
	memberID := testMemberID(9)
	const pubkey = "dGVzdC1wdWJrZXk="

	for _, tc := range []struct {
		name  string
		state StateStore
	}{
		{name: "memory", state: NewMemoryStateStore()},
		{name: "sqlite", state: mustOpenTestStateStore(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Relative to now: a record whose expiry is in the past is
			// filtered on read, so fixed historical timestamps would test
			// nothing.
			now := time.Now().UnixMilli()
			early, late := now, now+1000
			const hour = int64(3_600_000)

			if err := ObserveMemberProfileNodeProfile(ctx, tc.state, gid, memberID, pubkey, "pi-burj", early, early+hour); err != nil {
				t.Fatalf("observe: %v", err)
			}
			if rec, ok, err := tc.state.GetNodeProfile(ctx, memberID); err != nil || !ok || rec.Hostname != "pi-burj" {
				t.Fatalf("after observe: rec=%+v ok=%v err=%v", rec, ok, err)
			}

			if err := WithdrawMemberProfileNodeProfile(ctx, tc.state, gid, memberID, pubkey, late); err != nil {
				t.Fatalf("withdraw: %v", err)
			}
			if _, ok, err := tc.state.GetNodeProfile(ctx, memberID); err != nil || ok {
				t.Fatalf("after withdrawal: ok=%v err=%v, want no visible profile", ok, err)
			}

			// The older profile arrives late. It must not come back.
			if err := ObserveMemberProfileNodeProfile(ctx, tc.state, gid, memberID, pubkey, "pi-burj", early, early+hour); err != nil {
				t.Fatalf("re-observe: %v", err)
			}
			if rec, ok, err := tc.state.GetNodeProfile(ctx, memberID); err != nil || ok {
				t.Fatalf("an older profile resurrected a withdrawn name: rec=%+v ok=%v err=%v", rec, ok, err)
			}

			// A newer profile is still accepted, or withdrawal would be permanent.
			if err := ObserveMemberProfileNodeProfile(ctx, tc.state, gid, memberID, pubkey, "second", late+1000, late+hour); err != nil {
				t.Fatalf("observe newer: %v", err)
			}
			if rec, ok, err := tc.state.GetNodeProfile(ctx, memberID); err != nil || !ok || rec.Hostname != "second" {
				t.Fatalf("a newer profile was refused after withdrawal: rec=%+v ok=%v err=%v", rec, ok, err)
			}
		})
	}
}

func mustOpenTestStateStore(t *testing.T) StateStore {
	t.Helper()
	state, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })
	return state
}

// TestEqualIssueTimesResolveTheSameWayInEveryStore pins the tie-break. Two
// nodes that see the same profile and withdrawal in opposite orders must end
// up with the same name, or they disagree indefinitely — which is the whole
// point of ordering by the author's clock rather than by arrival.
func TestEqualIssueTimesResolveTheSameWayInEveryStore(t *testing.T) {
	ctx := context.Background()
	gid := testGroupID(4)
	memberID := testMemberID(11)
	const pubkey = "dGVzdC1wdWJrZXk="

	for _, tc := range []struct{ name string }{{name: "memory"}, {name: "sqlite"}} {
		t.Run(tc.name, func(t *testing.T) {
			newStore := func() StateStore {
				if tc.name == "memory" {
					return NewMemoryStateStore()
				}
				return mustOpenTestStateStore(t)
			}
			at := time.Now().UnixMilli()

			// Profile first, then the withdrawal at the same millisecond.
			a := newStore()
			if err := ObserveMemberProfileNodeProfile(ctx, a, gid, memberID, pubkey, "pi-burj", at, at+3_600_000); err != nil {
				t.Fatalf("observe: %v", err)
			}
			if err := WithdrawMemberProfileNodeProfile(ctx, a, gid, memberID, pubkey, at); err != nil {
				t.Fatalf("withdraw: %v", err)
			}
			_, okA, err := a.GetNodeProfile(ctx, memberID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			// The opposite arrival order.
			b := newStore()
			if err := WithdrawMemberProfileNodeProfile(ctx, b, gid, memberID, pubkey, at); err != nil {
				t.Fatalf("withdraw: %v", err)
			}
			if err := ObserveMemberProfileNodeProfile(ctx, b, gid, memberID, pubkey, "pi-burj", at, at+3_600_000); err != nil {
				t.Fatalf("observe: %v", err)
			}
			_, okB, err := b.GetNodeProfile(ctx, memberID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			if okA != okB {
				t.Fatalf("arrival order changed the outcome: profile-first visible=%v, withdrawal-first visible=%v", okA, okB)
			}
			if okA {
				t.Fatal("a withdrawal issued in the same millisecond lost to the profile; the tie must favour withdrawal")
			}
		})
	}
}

// TestEqualIssueTimesBetweenTwoProfilesAreDeterministic covers the other tie:
// two names at the same millisecond must resolve by a rule every node computes
// identically, not by which arrived first.
func TestEqualIssueTimesBetweenTwoProfilesAreDeterministic(t *testing.T) {
	ctx := context.Background()
	gid := testGroupID(5)
	memberID := testMemberID(12)
	const pubkey = "dGVzdC1wdWJrZXk="
	at := time.Now().UnixMilli()

	outcome := func(first, second string, state StateStore) string {
		if err := ObserveMemberProfileNodeProfile(ctx, state, gid, memberID, pubkey, first, at, at+3_600_000); err != nil {
			t.Fatalf("observe %q: %v", first, err)
		}
		if err := ObserveMemberProfileNodeProfile(ctx, state, gid, memberID, pubkey, second, at, at+3_600_000); err != nil {
			t.Fatalf("observe %q: %v", second, err)
		}
		rec, ok, err := state.GetNodeProfile(ctx, memberID)
		if err != nil || !ok {
			t.Fatalf("get: ok=%v err=%v", ok, err)
		}
		return rec.Hostname
	}

	for _, store := range []struct {
		name string
		make func() StateStore
	}{
		{name: "memory", make: func() StateStore { return NewMemoryStateStore() }},
		{name: "sqlite", make: func() StateStore { return mustOpenTestStateStore(t) }},
	} {
		t.Run(store.name, func(t *testing.T) {
			forward := outcome("alpha", "beta", store.make())
			reverse := outcome("beta", "alpha", store.make())
			if forward != reverse {
				t.Fatalf("arrival order decided the name: %q vs %q", forward, reverse)
			}
			if forward != "alpha" {
				t.Fatalf("tie resolved to %q, want the lower hostname", forward)
			}
		})
	}
}

// TestExpiredRecordsAreReplacedIdenticallyInEveryStore covers the branch that
// runs before the tie-break: an expired observation must be replaceable by a
// fresher one in both stores, and the withdrawal tombstone — which is stored
// permanently expired on purpose — must not be.
func TestExpiredRecordsAreReplacedIdenticallyInEveryStore(t *testing.T) {
	ctx := context.Background()
	gid := testGroupID(6)
	const pubkey = "dGVzdC1wdWJrZXk="

	stores := []struct {
		name string
		make func() StateStore
	}{
		{name: "memory", make: func() StateStore { return NewMemoryStateStore() }},
		{name: "sqlite", make: func() StateStore { return mustOpenTestStateStore(t) }},
	}

	t.Run("expired observation is replaced", func(t *testing.T) {
		at := time.Now().UnixMilli()
		var results []string
		for _, s := range stores {
			state := s.make()
			memberID := testMemberID(21)
			// An observation whose expiry is already in the past.
			if err := ObserveMemberProfileNodeProfile(ctx, state, gid, memberID, pubkey, "stale", at, at-1000); err != nil {
				t.Fatalf("%s observe stale: %v", s.name, err)
			}
			// A fresher one at the same instant, which the tie-break alone
			// would reject on hostname ordering.
			if err := ObserveMemberProfileNodeProfile(ctx, state, gid, memberID, pubkey, "zzz-fresh", at, at+3_600_000); err != nil {
				t.Fatalf("%s observe fresh: %v", s.name, err)
			}
			rec, ok, err := state.GetNodeProfile(ctx, memberID)
			if err != nil {
				t.Fatalf("%s get: %v", s.name, err)
			}
			results = append(results, fmt.Sprintf("visible=%v hostname=%q", ok, rec.Hostname))
		}
		if results[0] != results[1] {
			t.Fatalf("stores disagree about an expired record: memory %s, sqlite %s", results[0], results[1])
		}
		if results[0] != `visible=true hostname="zzz-fresh"` {
			t.Fatalf("expired observation was not replaced: %s", results[0])
		}
	})

	t.Run("withdrawal tombstone is not bypassed", func(t *testing.T) {
		at := time.Now().UnixMilli()
		var results []bool
		for _, s := range stores {
			state := s.make()
			memberID := testMemberID(22)
			if err := WithdrawMemberProfileNodeProfile(ctx, state, gid, memberID, pubkey, at); err != nil {
				t.Fatalf("%s withdraw: %v", s.name, err)
			}
			// An older profile arriving afterwards must not resurrect the name,
			// even though the tombstone is permanently expired.
			if err := ObserveMemberProfileNodeProfile(ctx, state, gid, memberID, pubkey, "resurrected", at-1000, at+3_600_000); err != nil {
				t.Fatalf("%s observe: %v", s.name, err)
			}
			_, ok, err := state.GetNodeProfile(ctx, memberID)
			if err != nil {
				t.Fatalf("%s get: %v", s.name, err)
			}
			results = append(results, ok)
		}
		if results[0] != results[1] {
			t.Fatalf("stores disagree about the tombstone: memory visible=%v, sqlite visible=%v", results[0], results[1])
		}
		if results[0] {
			t.Fatal("an older profile resurrected a withdrawn name through the expired bypass")
		}
	})
}
