package esphttp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	entmoot "entmoot/pkg/entmoot"
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

// TestExpiredRecordsAreReplacedIdenticallyInEveryStore covers the expiry
// clause of the tie-break in both stores: at an exact tie the longer-lived
// claim wins, so a short-lived observation gives way to a fresher one, while
// the withdrawal tombstone — stored permanently expired on purpose — is
// ordered by the tombstone clause ahead of it and holds.
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

	t.Run("withdrawal tombstone still wins", func(t *testing.T) {
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
			t.Fatal("an older profile resurrected a withdrawn name")
		}
	})
}

// TestProfileReplacementIsATotalOrderInEveryStore enumerates every ordered
// pair of record kinds against both stores. Two properties must hold for
// convergence: the outcome must not depend on which record arrived first, and
// the two stores must agree. Every earlier defect in this area — the
// receipt-time ordering, the memory-only expiry bypass, the expiry applied
// before the comparison instead of inside the tie-break, the clock consulted
// at write time — shows up as a
// failure of one of the two.
func TestProfileReplacementIsATotalOrderInEveryStore(t *testing.T) {
	ctx := context.Background()
	gid := testGroupID(7)
	const pubkey = "dGVzdC1wdWJrZXk="
	at := time.Now().UnixMilli()

	type candidate struct {
		name  string
		apply func(state StateStore, memberID entmoot.MemberID) error
	}
	profileAt := func(label string, issuedAtMS, expiresAtMS int64) candidate {
		return candidate{
			name: label,
			apply: func(state StateStore, memberID entmoot.MemberID) error {
				return ObserveMemberProfileNodeProfile(ctx, state, gid, memberID, pubkey, label, issuedAtMS, expiresAtMS)
			},
		}
	}
	withdrawalAt := func(label string, issuedAtMS int64) candidate {
		return candidate{
			name: label,
			apply: func(state StateStore, memberID entmoot.MemberID) error {
				return WithdrawMemberProfileNodeProfile(ctx, state, gid, memberID, pubkey, issuedAtMS)
			},
		}
	}
	candidates := []candidate{
		profileAt("alpha", at, at+3_600_000),
		profileAt("beta", at, at+3_600_000),
		profileAt("later", at+1000, at+3_600_000),
		profileAt("earlier", at-1000, at+3_600_000),
		profileAt("expired-same-time", at, at-1),
		// Hostnames chosen to sort AGAINST the rule they test: if expiry stops
		// deciding the tie, the hostname order alone picks the expired record
		// here, so a store that drops the stale tie-break is caught.
		profileAt("aaa-expired", at, at-1),
		profileAt("zzz-fresh", at, at+3_600_000),
		// Two never-expires sentinels: 0 and -1 are the same claim about
		// lifetime, so comparing raw expiries leaves them mutually
		// non-replacing and the first one stored wins.
		profileAt("zzz-never", at, 0),
		profileAt("aaa-never", at, -1),
		profileAt("expired-later", at+1000, at-1),
		withdrawalAt("withdrawal-same-time", at),
		withdrawalAt("withdrawal-later", at+1000),
	}

	outcome := func(state StateStore, memberID entmoot.MemberID, first, second candidate) string {
		if err := first.apply(state, memberID); err != nil {
			t.Fatalf("apply %s: %v", first.name, err)
		}
		if err := second.apply(state, memberID); err != nil {
			t.Fatalf("apply %s: %v", second.name, err)
		}
		rec, ok, err := state.GetNodeProfile(ctx, memberID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !ok {
			return "<none>"
		}
		return rec.Hostname
	}

	stores := []struct {
		name string
		make func() StateStore
	}{
		{name: "memory", make: func() StateStore { return NewMemoryStateStore() }},
		{name: "sqlite", make: func() StateStore { return mustOpenTestStateStore(t) }},
	}

	member := uint32(100)
	next := func() entmoot.MemberID {
		member++
		return testMemberID(member)
	}

	pairs, orderDependent, divergent := 0, 0, 0
	for i, a := range candidates {
		for j, b := range candidates {
			if i == j {
				continue
			}
			pairs++
			results := make(map[string][2]string, len(stores))
			for _, s := range stores {
				forward := outcome(s.make(), next(), a, b)
				reverse := outcome(s.make(), next(), b, a)
				results[s.name] = [2]string{forward, reverse}
				if forward != reverse {
					orderDependent++
					t.Errorf("%s: %s then %s = %q, reversed = %q: arrival order decided the name",
						s.name, a.name, b.name, forward, reverse)
				}
			}
			if results["memory"] != results["sqlite"] {
				divergent++
				t.Errorf("stores disagree on %s/%s: memory %v, sqlite %v", a.name, b.name, results["memory"], results["sqlite"])
			}
		}
	}
	if pairs != len(candidates)*(len(candidates)-1) {
		t.Fatalf("enumerated %d pairs, want %d", pairs, len(candidates)*(len(candidates)-1))
	}
	t.Logf("%d ordered pairs, %d order-dependent, %d divergent", pairs, orderDependent, divergent)
}

// TestStoredWinnerDoesNotDependOnWhenItWasIngested pins that the comparison is
// clock-free. It used to ask "is the stored record expired right now", and the
// loser of a comparison is discarded rather than kept — so two nodes holding
// the same two claims stored different winners depending on whether the second
// arrived before or after the first expired, and disagreed until the nearer
// expiry passed. The claims below have equal confidence and equal issue times
// and differ only in expiry, which is the pair that exposed it.
func TestStoredWinnerDoesNotDependOnWhenItWasIngested(t *testing.T) {
	ctx := context.Background()
	gid := testGroupID(8)
	const pubkey = "dGVzdC1wdWJrZXk="
	at := int64(1_700_000_000_000)

	shortLived := MemberProfileRecord(gid, testMemberID(31), pubkey, "aaa", at, at+2_000)
	longLived := MemberProfileRecord(gid, testMemberID(31), pubkey, "zzz", at, at+3_600_000)

	ingest := func(clockAtSecond int64, first, second NodeProfileRecord) string {
		state := NewMemoryStateStore()
		state.clock = func() time.Time { return time.UnixMilli(at + clockAtSecond*1000) }
		if _, _, err := state.UpsertNodeProfile(ctx, first); err != nil {
			t.Fatalf("upsert first: %v", err)
		}
		if _, _, err := state.UpsertNodeProfile(ctx, second); err != nil {
			t.Fatalf("upsert second: %v", err)
		}
		rec, ok, err := state.GetNodeProfile(ctx, first.MemberID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !ok {
			return "<none>"
		}
		return rec.Hostname
	}

	// Node A sees both claims while both are live; node B sees the second only
	// after the short-lived one has expired. Same claims, same reading instant.
	nodeA := ingest(0, shortLived, longLived)
	nodeB := ingest(10, shortLived, longLived)
	reversed := ingest(10, longLived, shortLived)
	if nodeA != nodeB || nodeB != reversed {
		t.Fatalf("stored winner varies with ingest time or order: both-live %q, after-expiry %q, reversed %q", nodeA, nodeB, reversed)
	}
	if nodeA != "zzz" {
		t.Fatalf("stored winner = %q, want the longer-lived claim", nodeA)
	}
}

// TestTwoSourcesWithTheSameNameResolveDeterministically covers the pair the
// clauses above cannot separate. A member may hold more than one row — the
// upsert key is (member_id, source_key) — and two rows can agree on
// confidence, issue time, expiry and hostname while being different rows.
// Before the source-key clause the comparator was false in both directions for
// such a pair, and the selection loop ranges a Go map, so the served name
// depended on iteration order.
func TestTwoSourcesWithTheSameNameResolveDeterministically(t *testing.T) {
	ctx := context.Background()
	gid := testGroupID(9)
	memberID := testMemberID(41)
	at := time.Now().UnixMilli()

	records := map[string]NodeProfileRecord{}
	for _, pubkey := range []string{"cHVia2V5LWE=", "cHVia2V5LWI="} {
		rec := MemberProfileRecord(gid, memberID, pubkey, "same-name", at, at+3_600_000)
		rec.Confidence = NodeProfileConfidenceMemberProfile
		records[nodeProfileSourceKey(rec)] = rec
	}
	if len(records) != 2 {
		t.Fatalf("fixture built %d rows, want 2 distinct source keys", len(records))
	}

	// Ranging a map 200 times exposes order dependence: Go randomises it.
	var first string
	for i := 0; i < 200; i++ {
		best, ok := bestNodeProfile(records, at, nil, "")
		if !ok {
			t.Fatal("no record selected")
		}
		key := nodeProfileSourceKey(best)
		if i == 0 {
			first = key
			continue
		}
		if key != first {
			t.Fatalf("selection changed between iterations: %q then %q — map order is deciding", first, key)
		}
	}
	_ = ctx
}

// TestOpenInviteSchemaMigratesFromTheOldShape pins the migration, not the
// CREATE TABLE. A deployed ESP already has esp_open_invites without
// no_fallback_peers, and CREATE TABLE IF NOT EXISTS would leave it that way —
// every open-invite read then fails on the missing column.
func TestOpenInviteSchemaMigratesFromTheOldShape(t *testing.T) {
	dir := t.TempDir()
	old, err := sql.Open("sqlite", filepath.Join(dir, "esp.sqlite"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// The table exactly as an older build wrote it.
	if _, err := old.Exec(`CREATE TABLE esp_open_invites (
  token_hash TEXT PRIMARY KEY,
  group_id BLOB NOT NULL,
  device_id TEXT NOT NULL DEFAULT '',
  max_uses INTEGER NOT NULL,
  use_count INTEGER NOT NULL DEFAULT 0,
  revoked INTEGER NOT NULL DEFAULT 0,
  bootstrap_multiaddrs BLOB,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL
);`); err != nil {
		t.Fatalf("seed old schema: %v", err)
	}
	gid := testGroupID(3)
	if _, err := old.Exec(`INSERT INTO esp_open_invites
 (token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_multiaddrs, created_at_ms, updated_at_ms, expires_at_ms)
 VALUES ('hash', ?, 'dev', 1, 0, 0, NULL, 1, 1, 9999999999999)`, gid[:]); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	if err := old.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	state, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore on an old database: %v", err)
	}
	defer state.Close()

	invites, err := state.ListOpenInvitesByGroup(context.Background(), gid)
	if err != nil {
		t.Fatalf("reading open invites after migration: %v", err)
	}
	if len(invites) != 1 || invites[0].NoFallbackPeers {
		t.Fatalf("migrated rows = %+v, want the seeded row with no_fallback_peers false", invites)
	}
}
