package esphttp

import (
	"context"
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
