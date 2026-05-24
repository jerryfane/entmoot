package esphttp

import (
	"context"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

func TestNodeProfileHostnameNormalizeAndDisplayName(t *testing.T) {
	if got, ok := NormalizeNodeProfileHostname("  hermes  "); !ok || got != "hermes" {
		t.Fatalf("NormalizeNodeProfileHostname = %q/%v, want hermes/true", got, ok)
	}
	for _, hostname := range []string{"", "   ", "bad\nname", strings.Repeat("a", MaxNodeProfileHostnameBytes+1)} {
		if got, ok := NormalizeNodeProfileHostname(hostname); ok {
			t.Fatalf("NormalizeNodeProfileHostname(%q) = %q/true, want invalid", hostname, got)
		}
	}
	if got := NodeDisplayName(133053, " deimos "); got != "deimos#133053" {
		t.Fatalf("NodeDisplayName with hostname = %q", got)
	}
	if got := NodeDisplayName(133053, " "); got != "node-133053" {
		t.Fatalf("NodeDisplayName without hostname = %q", got)
	}
}

func TestStateStoresNodeProfilesPrecedenceTieBreakAndExpiry(t *testing.T) {
	ctx := context.Background()
	const futureMS = 4_102_444_800_000
	for _, tc := range []struct {
		name string
		open func(*testing.T) StateStore
	}{
		{name: "memory", open: func(t *testing.T) StateStore {
			store := NewMemoryStateStore()
			store.clock = func() time.Time { return time.UnixMilli(1_000) }
			return store
		}},
		{name: "sqlite", open: func(t *testing.T) StateStore {
			store, err := OpenSQLiteStateStore(t.TempDir())
			if err != nil {
				t.Fatalf("OpenSQLiteStateStore: %v", err)
			}
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			defer store.Close()
			groupID := entmoot.GroupID{1, 2, 3}

			rec, changed, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:        133053,
				Hostname:      " pilot-host ",
				Source:        NodeProfileSourcePilotInfo,
				ObservedAtMS:  10,
				ExpiresAtMS:   futureMS,
				SourceGroupID: &groupID,
			})
			if err != nil || !changed {
				t.Fatalf("UpsertNodeProfile pilot changed/err = %v/%v", changed, err)
			}
			if rec.Hostname != "pilot-host" || rec.Confidence != NodeProfileConfidencePilotInfo || rec.SourceGroupID == nil || *rec.SourceGroupID != groupID {
				t.Fatalf("pilot record = %+v", rec)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "older-member",
				Source:       NodeProfileSourceMemberProfile,
				ObservedAtMS: 9,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || !changed || rec.Hostname != "older-member" || rec.Confidence != NodeProfileConfidenceMemberProfile {
				t.Fatalf("higher confidence record = %+v changed=%v err=%v", rec, changed, err)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "spoofed-invite",
				Source:       NodeProfileSourceFleetInvite,
				Confidence:   10_000,
				ObservedAtMS: 15,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || changed || rec.Hostname != "older-member" {
				t.Fatalf("spoofed confidence record = %+v changed=%v err=%v", rec, changed, err)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "newer-pilot",
				Source:       NodeProfileSourcePilotInfo,
				ObservedAtMS: 20,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || changed || rec.Hostname != "older-member" {
				t.Fatalf("lower confidence record = %+v changed=%v err=%v", rec, changed, err)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "newer-member",
				Source:       NodeProfileSourceMemberProfile,
				ObservedAtMS: 30,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || !changed || rec.Hostname != "newer-member" {
				t.Fatalf("newer equal confidence record = %+v changed=%v err=%v", rec, changed, err)
			}

			if _, changed, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133054,
				Hostname:     "expired",
				Source:       NodeProfileSourceMemberProfile,
				ObservedAtMS: 5,
				ExpiresAtMS:  500,
			}); err != nil || changed {
				t.Fatalf("expired upsert changed/err = %v/%v, want false/nil", changed, err)
			}
			if _, ok, err := store.GetNodeProfile(ctx, 133054); err != nil || ok {
				t.Fatalf("GetNodeProfile expired ok/err = %v/%v, want false/nil", ok, err)
			}
			if _, changed, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:      133055,
				Hostname:    "\t",
				Source:      NodeProfileSourceMemberProfile,
				ExpiresAtMS: futureMS,
			}); err != nil || changed {
				t.Fatalf("invalid hostname changed/err = %v/%v, want false/nil", changed, err)
			}

			got, ok, err := store.GetNodeProfile(ctx, 133053)
			if err != nil || !ok || got.Hostname != "newer-member" {
				t.Fatalf("GetNodeProfile = %+v ok=%v err=%v", got, ok, err)
			}
			listed, err := store.ListNodeProfiles(ctx, []entmoot.NodeID{133053, 133053, 133054, 0})
			if err != nil {
				t.Fatalf("ListNodeProfiles: %v", err)
			}
			if len(listed) != 1 || listed[133053].Hostname != "newer-member" {
				t.Fatalf("ListNodeProfiles = %+v", listed)
			}
		})
	}
}

func TestStateStoresNodeProfilesRejectUnknownSource(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		open func(*testing.T) StateStore
	}{
		{name: "memory", open: func(t *testing.T) StateStore { return NewMemoryStateStore() }},
		{name: "sqlite", open: func(t *testing.T) StateStore {
			store, err := OpenSQLiteStateStore(t.TempDir())
			if err != nil {
				t.Fatalf("OpenSQLiteStateStore: %v", err)
			}
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			defer store.Close()
			if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:   133053,
				Hostname: "deimos",
				Source:   "unknown",
			}); err == nil {
				t.Fatalf("UpsertNodeProfile unknown source err = nil")
			}
		})
	}
}
