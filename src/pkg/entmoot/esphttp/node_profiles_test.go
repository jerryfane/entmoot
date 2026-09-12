package esphttp

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestNodeProfileHostnameNormalizeAndDisplayName(t *testing.T) {
	member := testMemberID(133053)
	if name, ok := NormalizeNodeProfileHostname("  hermes  "); !ok || name != "hermes" {
		t.Fatalf("normalized hostname: %q/%v", name, ok)
	}
	for _, name := range []string{"", "   ", "bad\nname", strings.Repeat("a", MaxNodeProfileHostnameBytes+1)} {
		if _, ok := NormalizeNodeProfileHostname(name); ok {
			t.Fatalf("accepted invalid hostname %q", name)
		}
	}
	if name := NodeDisplayName(member, " deimos "); name != "deimos#"+member.String() {
		t.Fatalf("display name: %q", name)
	}
	if name := NodeDisplayName(member, " "); name != "member-"+member.String() {
		t.Fatalf("fallback display name: %q", name)
	}
}

func TestStateStoresNodeProfilesPrecedenceScopeAndExpiry(t *testing.T) {
	ctx := context.Background()
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.close()
			member := testMobileNode(t, 133053)
			id, key := *member.MemberID, base64.StdEncoding.EncodeToString(member.EntmootPubKey)
			gid, other := testGroupID(1), testGroupID(2)
			put := func(rec NodeProfileRecord) {
				t.Helper()
				if _, _, err := tc.store.UpsertNodeProfile(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			hostname := func(group entmoot.GroupID, publicKey string) string {
				t.Helper()
				rows, err := EnrichMemberDisplayNames(ctx, tc.store, group, []MemberSummary{{MemberID: id, EntmootPubKey: publicKey}})
				if err != nil {
					t.Fatal(err)
				}
				return rows[0].GlobalHostname
			}
			put(NodeProfileRecord{MemberID: id, Hostname: "invite", Source: NodeProfileSourceFleetInvite, Confidence: 10000, ObservedAtMS: 20})
			put(NodeProfileRecord{MemberID: id, Hostname: "member", Source: NodeProfileSourceFleetMember, ObservedAtMS: 10})
			if got := hostname(gid, key); got != "member" {
				t.Fatalf("source priority: %q", got)
			}
			put(NodeProfileRecord{MemberID: id, Hostname: "older", Source: NodeProfileSourceFleetMember, ObservedAtMS: 9})
			put(NodeProfileRecord{MemberID: id, Hostname: "same-time", Source: NodeProfileSourceFleetMember, ObservedAtMS: 10})
			if got := hostname(gid, key); got != "member" {
				t.Fatalf("older/equal observation replaced member: %q", got)
			}
			put(NodeProfileRecord{MemberID: id, EntmootPubKey: key, Hostname: "scoped", Source: NodeProfileSourceMemberProfile, SourceGroupID: &gid, ObservedAtMS: 30, ExpiresAtMS: 4_102_444_800_000})
			if got := hostname(gid, key); got != "scoped" {
				t.Fatalf("scoped priority: %q", got)
			}
			if got := hostname(other, key); got != "member" {
				t.Fatalf("profile crossed group: %q", got)
			}
			if got := hostname(gid, "another-key"); got != "member" {
				t.Fatalf("profile crossed key binding: %q", got)
			}
			put(NodeProfileRecord{MemberID: id, EntmootPubKey: key, Hostname: "expired", Source: NodeProfileSourceMemberProfile, SourceGroupID: &gid, ObservedAtMS: 31, ExpiresAtMS: 1})
			if got := hostname(gid, key); got != "member" {
				t.Fatalf("expired profile did not fall back: %q", got)
			}
		})
	}
}

func TestStateStoresNodeProfilesRejectUnknownSource(t *testing.T) {
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.close()
			id := testMemberID(1)
			if _, _, err := tc.store.UpsertNodeProfile(context.Background(), NodeProfileRecord{MemberID: id, Hostname: "unknown", Source: "unknown"}); err == nil {
				t.Fatal("accepted unknown profile source")
			}
			if _, found, err := tc.store.GetNodeProfile(context.Background(), id); err != nil || found {
				t.Fatalf("invalid profile persisted: found=%v err=%v", found, err)
			}
		})
	}
}

func TestStateStoresFleetProfilesFollowMembershipAndArchive(t *testing.T) {
	ctx := context.Background()
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.close()
			target := testMobileNode(t, 155760)
			id, key := *target.MemberID, base64.StdEncoding.EncodeToString(target.EntmootPubKey)
			for _, fleet := range []string{"fleet-a", "fleet-b"} {
				if _, err := tc.store.CreateFleet(ctx, FleetRecord{FleetID: fleet, Name: fleet, Coordinator: testMobileNode(t, 1), Status: FleetStatusActive}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tc.store.CreateFleetInvite(ctx, FleetInviteRecord{InviteID: "invite", FleetID: "fleet-b", MemberID: id, PeerID: target.PeerID, EntmootPubKey: key, Hostname: "invited", Status: FleetMemberInvited, ExpiresAtMS: 4_102_444_800_000}); err != nil {
				t.Fatal(err)
			}
			member := FleetMemberRecord{FleetID: "fleet-a", MemberID: id, PeerID: target.PeerID, EntmootPubKey: key, Hostname: "active", Role: FleetRoleAgent, Status: FleetMemberActive}
			want := func(hostname string) {
				t.Helper()
				profile, found, err := tc.store.GetNodeProfile(ctx, id)
				if err != nil || found != (hostname != "") || found && profile.Hostname != hostname {
					t.Fatalf("profile=%+v found=%v err=%v; want %q", profile, found, err, hostname)
				}
			}
			want("invited")
			if _, err := tc.store.UpsertFleetMember(ctx, member); err != nil {
				t.Fatal(err)
			}
			want("active")
			member.Status = FleetMemberRemoved
			if _, err := tc.store.UpsertFleetMember(ctx, member); err != nil {
				t.Fatal(err)
			}
			want("invited")
			member.Status = FleetMemberActive
			if _, err := tc.store.UpsertFleetMember(ctx, member); err != nil {
				t.Fatal(err)
			}
			if _, _, err := tc.store.ArchiveFleet(ctx, "fleet-a", 1000); err != nil {
				t.Fatal(err)
			}
			want("invited")
			if err := tc.store.DeleteFleetInvite(ctx, "invite"); err != nil {
				t.Fatal(err)
			}
			want("")
			if _, _, err := tc.store.RestoreFleet(ctx, "fleet-a", 2000); err != nil {
				t.Fatal(err)
			}
			want("active")
			if err := tc.store.DeleteFleetMember(ctx, "fleet-a", id); err != nil {
				t.Fatal(err)
			}
			want("")
			if _, err := tc.store.UpsertFleetMember(ctx, member); err != nil {
				t.Fatal(err)
			}
			if _, err := tc.store.CreateFleetInvite(ctx, FleetInviteRecord{InviteID: "pending", FleetID: "fleet-a", MemberID: id, PeerID: target.PeerID, EntmootPubKey: key, Hostname: "pending", Status: FleetMemberInvited}); err != nil {
				t.Fatal(err)
			}
			want("active")
			if err := tc.store.DeleteFleet(ctx, "fleet-a"); err != nil {
				t.Fatal(err)
			}
			want("")
		})
	}
}

func TestSQLiteStateStoreBackfillsFleetNodeProfilesOnOpen(t *testing.T) {
	ctx, dir := context.Background(), t.TempDir()
	state, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if _, err := state.CreateFleet(ctx, FleetRecord{FleetID: "fleet", Coordinator: testMobileNode(t, 1), Status: FleetStatusActive}); err != nil {
		t.Fatal(err)
	}
	memberInfo, inviteInfo := testMobileNode(t, 155760), testMobileNode(t, 133053)
	member, invite, stale := *memberInfo.MemberID, *inviteInfo.MemberID, testMemberID(999)
	if _, err := state.UpsertFleetMember(ctx, FleetMemberRecord{FleetID: "fleet", MemberID: member, PeerID: memberInfo.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(memberInfo.EntmootPubKey), Hostname: "hermes", Role: FleetRoleAgent, Status: FleetMemberActive}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateFleetInvite(ctx, FleetInviteRecord{InviteID: "invite", FleetID: "fleet", MemberID: invite, PeerID: inviteInfo.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(inviteInfo.EntmootPubKey), Hostname: "deimos", Status: FleetMemberInvited, ExpiresAtMS: 4_102_444_800_000}); err != nil {
		t.Fatal(err)
	}
	// Simulate missing/stale derived profiles across an upgrade, not lost Fleet rows.
	if _, err := state.db.ExecContext(ctx, `DELETE FROM esp_node_profile_sources`); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{NodeProfileSourceFleetMember, NodeProfileSourceFleetInvite} {
		if _, _, err := state.UpsertNodeProfile(ctx, NodeProfileRecord{MemberID: stale, Hostname: "stale", Source: source, ObservedAtMS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	for id, want := range map[entmoot.MemberID]string{member: "hermes", invite: "deimos", stale: ""} {
		profile, found, err := state.GetNodeProfile(ctx, id)
		if err != nil || found != (want != "") || found && profile.Hostname != want {
			t.Fatalf("reopened profile %s: %+v found=%v err=%v; want %q", id, profile, found, err, want)
		}
	}
}
