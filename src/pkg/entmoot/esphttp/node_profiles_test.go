package esphttp

import (
	"context"
	"encoding/base64"
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
			if err != nil || changed || rec.Hostname != "pilot-host" {
				t.Fatalf("group-scoped profile masked global record = %+v changed=%v err=%v", rec, changed, err)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "spoofed-invite",
				Source:       NodeProfileSourceFleetInvite,
				Confidence:   10_000,
				ObservedAtMS: 15,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || !changed || rec.Hostname != "spoofed-invite" || rec.Confidence != NodeProfileConfidenceFleetInvite {
				t.Fatalf("source confidence record = %+v changed=%v err=%v", rec, changed, err)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "newer-pilot",
				Source:       NodeProfileSourcePilotInfo,
				ObservedAtMS: 20,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || changed || rec.Hostname != "spoofed-invite" {
				t.Fatalf("lower confidence record = %+v changed=%v err=%v", rec, changed, err)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "fleet-member",
				Source:       NodeProfileSourceFleetMember,
				ObservedAtMS: 30,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || !changed || rec.Hostname != "fleet-member" || rec.Confidence != NodeProfileConfidenceFleetMember {
				t.Fatalf("higher global confidence record = %+v changed=%v err=%v", rec, changed, err)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "newer-member",
				Source:       NodeProfileSourceMemberProfile,
				ObservedAtMS: 40,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || changed || rec.Hostname != "fleet-member" {
				t.Fatalf("newer group-scoped record masked global record = %+v changed=%v err=%v", rec, changed, err)
			}

			rec, changed, err = store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133053,
				Hostname:     "newer-fleet-member",
				Source:       NodeProfileSourceFleetMember,
				ObservedAtMS: 50,
				ExpiresAtMS:  futureMS,
			})
			if err != nil || !changed || rec.Hostname != "newer-fleet-member" {
				t.Fatalf("newer equal global confidence record = %+v changed=%v err=%v", rec, changed, err)
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
			if err != nil || !ok || got.Hostname != "newer-fleet-member" {
				t.Fatalf("GetNodeProfile = %+v ok=%v err=%v", got, ok, err)
			}
			listed, err := store.ListNodeProfiles(ctx, []entmoot.NodeID{133053, 133053, 133054, 0})
			if err != nil {
				t.Fatalf("ListNodeProfiles: %v", err)
			}
			if len(listed) != 1 || listed[133053].Hostname != "newer-fleet-member" {
				t.Fatalf("ListNodeProfiles = %+v", listed)
			}
			memberPub := base64.StdEncoding.EncodeToString([]byte("member-pub"))
			if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       133056,
				Hostname:     "member-fallback",
				Source:       NodeProfileSourceFleetMember,
				ObservedAtMS: 10,
				ExpiresAtMS:  futureMS,
			}); err != nil {
				t.Fatalf("UpsertNodeProfile member fallback: %v", err)
			}
			if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:        133056,
				EntmootPubKey: memberPub,
				Hostname:      "member-profile",
				Source:        NodeProfileSourceMemberProfile,
				ObservedAtMS:  20,
				ExpiresAtMS:   futureMS,
				SourceGroupID: &groupID,
			}); err != nil {
				t.Fatalf("UpsertNodeProfile member profile with pubkey: %v", err)
			}
			memberLister, ok := store.(interface {
				ListNodeProfilesForMembers(context.Context, entmoot.GroupID, map[entmoot.NodeID]string) (map[entmoot.NodeID]NodeProfileRecord, error)
			})
			if !ok {
				t.Fatalf("store does not implement ListNodeProfilesForMembers")
			}
			memberProfiles, err := memberLister.ListNodeProfilesForMembers(ctx, groupID, map[entmoot.NodeID]string{133056: memberPub})
			if err != nil {
				t.Fatalf("ListNodeProfilesForMembers matching: %v", err)
			}
			if got := memberProfiles[133056]; got.Hostname != "member-profile" || got.EntmootPubKey != memberPub {
				t.Fatalf("matching member profile = %+v", got)
			}
			otherGroupID := testGroupID(2)
			if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:        133058,
				EntmootPubKey: memberPub,
				Hostname:      "other-group-same-identity",
				Source:        NodeProfileSourceMemberProfile,
				ObservedAtMS:  20,
				ExpiresAtMS:   futureMS,
				SourceGroupID: &otherGroupID,
			}); err != nil {
				t.Fatalf("UpsertNodeProfile other group same identity: %v", err)
			}
			memberProfiles, err = memberLister.ListNodeProfilesForMembers(ctx, groupID, map[entmoot.NodeID]string{133058: memberPub})
			if err != nil {
				t.Fatalf("ListNodeProfilesForMembers other group same identity: %v", err)
			}
			if got := memberProfiles[133058]; got.Hostname != "other-group-same-identity" || got.EntmootPubKey != memberPub {
				t.Fatalf("other group same identity member profile = %+v", got)
			}
			memberProfiles, err = memberLister.ListNodeProfilesForMembers(ctx, groupID, map[entmoot.NodeID]string{133056: base64.StdEncoding.EncodeToString([]byte("new-member-pub"))})
			if err != nil {
				t.Fatalf("ListNodeProfilesForMembers mismatch: %v", err)
			}
			if got := memberProfiles[133056]; got.Hostname != "member-fallback" {
				t.Fatalf("mismatched member profile = %+v, want fleet fallback", got)
			}

			oldMemberPub := base64.StdEncoding.EncodeToString([]byte("old-member-pub"))
			newMemberPub := base64.StdEncoding.EncodeToString([]byte("newer-member-pub"))
			if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:        133057,
				EntmootPubKey: oldMemberPub,
				Hostname:      "old-member-profile",
				Source:        NodeProfileSourceMemberProfile,
				ObservedAtMS:  100,
				ExpiresAtMS:   futureMS,
				SourceGroupID: &groupID,
			}); err != nil {
				t.Fatalf("UpsertNodeProfile old identity profile: %v", err)
			}
			if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:        133057,
				EntmootPubKey: newMemberPub,
				Hostname:      "new-member-profile",
				Source:        NodeProfileSourceMemberProfile,
				ObservedAtMS:  50,
				ExpiresAtMS:   futureMS,
				SourceGroupID: &groupID,
			}); err != nil {
				t.Fatalf("UpsertNodeProfile new identity profile: %v", err)
			}
			memberProfiles, err = memberLister.ListNodeProfilesForMembers(ctx, groupID, map[entmoot.NodeID]string{133057: newMemberPub})
			if err != nil {
				t.Fatalf("ListNodeProfilesForMembers changed identity: %v", err)
			}
			if got := memberProfiles[133057]; got.Hostname != "new-member-profile" || got.EntmootPubKey != newMemberPub {
				t.Fatalf("changed identity member profile = %+v", got)
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

func TestStateStoresFleetRowsPopulateNodeProfiles(t *testing.T) {
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
			for _, fleetID := range []string{"fleet-a", "fleet-b", "fleet-invite-a", "fleet-invite-b"} {
				if _, err := store.CreateFleet(ctx, FleetRecord{
					FleetID:     fleetID,
					Name:        fleetID,
					Status:      FleetStatusActive,
					Coordinator: entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: []byte("coordinator")},
				}); err != nil {
					t.Fatalf("CreateFleet %s: %v", fleetID, err)
				}
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-a",
				NodeID:        155759,
				EntmootPubKey: "pub-pending",
				Hostname:      "pending",
				Role:          FleetRoleAgent,
				Status:        FleetMemberInvited,
			}); err != nil {
				t.Fatalf("UpsertFleetMember invited: %v", err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 155759); err != nil || ok {
				t.Fatalf("GetNodeProfile invited member = %+v ok=%v err=%v, want none", profile, ok, err)
			}
			if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
				NodeID:       155761,
				Hostname:     "pilot-fallback",
				Source:       NodeProfileSourcePilotInfo,
				ObservedAtMS: 1,
			}); err != nil {
				t.Fatalf("UpsertNodeProfile pilot fallback: %v", err)
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-a",
				NodeID:        155761,
				EntmootPubKey: "pub-fallback",
				Hostname:      "fleet-fallback",
				Role:          FleetRoleAgent,
				Status:        FleetMemberActive,
			}); err != nil {
				t.Fatalf("UpsertFleetMember over pilot fallback: %v", err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 155761); err != nil || !ok || profile.Hostname != "fleet-fallback" {
				t.Fatalf("GetNodeProfile fleet over pilot = %+v ok=%v err=%v", profile, ok, err)
			}
			if err := store.DeleteFleetMember(ctx, "fleet-a", 155761); err != nil {
				t.Fatalf("DeleteFleetMember over pilot: %v", err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 155761); err != nil || !ok || profile.Hostname != "pilot-fallback" {
				t.Fatalf("GetNodeProfile restored pilot = %+v ok=%v err=%v", profile, ok, err)
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-a",
				NodeID:        155760,
				EntmootPubKey: "pub",
				Hostname:      "hermes",
				Role:          FleetRoleAgent,
				Status:        FleetMemberActive,
			}); err != nil {
				t.Fatalf("UpsertFleetMember: %v", err)
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-a",
				FleetID:       "fleet-invite-a",
				NodeID:        133053,
				EntmootPubKey: "pub-2",
				Hostname:      "deimos",
				Status:        FleetMemberInvited,
				ExpiresAtMS:   4_102_444_800_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite: %v", err)
			}
			memberProfile, ok, err := store.GetNodeProfile(ctx, 155760)
			if err != nil || !ok {
				t.Fatalf("GetNodeProfile member ok/err = %v/%v", ok, err)
			}
			if memberProfile.Hostname != "hermes" || memberProfile.Source != NodeProfileSourceFleetMember || memberProfile.Confidence != NodeProfileConfidenceFleetMember {
				t.Fatalf("member profile = %+v", memberProfile)
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-b",
				NodeID:        155760,
				EntmootPubKey: "pub",
				Hostname:      "hermes-backup",
				Role:          FleetRoleAgent,
				Status:        FleetMemberActive,
			}); err != nil {
				t.Fatalf("UpsertFleetMember backup: %v", err)
			}
			if err := store.DeleteFleetMember(ctx, "fleet-a", 155760); err != nil {
				t.Fatalf("DeleteFleetMember primary: %v", err)
			}
			memberProfile, ok, err = store.GetNodeProfile(ctx, 155760)
			if err != nil || !ok {
				t.Fatalf("GetNodeProfile backup member ok/err = %v/%v", ok, err)
			}
			if memberProfile.Hostname != "hermes-backup" || memberProfile.Source != NodeProfileSourceFleetMember {
				t.Fatalf("backup member profile = %+v", memberProfile)
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-b",
				NodeID:        155760,
				EntmootPubKey: "pub",
				Role:          FleetRoleAgent,
				Status:        FleetMemberRemoved,
			}); err != nil {
				t.Fatalf("UpsertFleetMember removed: %v", err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 155760); err != nil || ok {
				t.Fatalf("GetNodeProfile removed member = %+v ok=%v err=%v, want none", profile, ok, err)
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-a",
				NodeID:        155760,
				EntmootPubKey: "pub",
				Hostname:      "hermes",
				Role:          FleetRoleAgent,
				Status:        FleetMemberActive,
			}); err != nil {
				t.Fatalf("UpsertFleetMember active restore: %v", err)
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-b",
				NodeID:        155760,
				EntmootPubKey: "pub",
				Hostname:      "hermes-backup",
				Role:          FleetRoleAgent,
				Status:        FleetMemberActive,
			}); err != nil {
				t.Fatalf("UpsertFleetMember active backup restore: %v", err)
			}
			if err := store.DeleteFleet(ctx, "fleet-a"); err != nil {
				t.Fatalf("DeleteFleet primary member: %v", err)
			}
			memberProfile, ok, err = store.GetNodeProfile(ctx, 155760)
			if err != nil || !ok {
				t.Fatalf("GetNodeProfile backup member after fleet delete ok/err = %v/%v", ok, err)
			}
			if memberProfile.Hostname != "hermes-backup" || memberProfile.Source != NodeProfileSourceFleetMember {
				t.Fatalf("backup member profile after fleet delete = %+v", memberProfile)
			}
			if err := store.DeleteFleet(ctx, "fleet-b"); err != nil {
				t.Fatalf("DeleteFleet backup member: %v", err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 155760); err != nil || ok {
				t.Fatalf("GetNodeProfile deleted member fleets = %+v ok=%v err=%v, want none", profile, ok, err)
			}
			inviteProfile, ok, err := store.GetNodeProfile(ctx, 133053)
			if err != nil || !ok {
				t.Fatalf("GetNodeProfile invite ok/err = %v/%v", ok, err)
			}
			if inviteProfile.Hostname != "deimos" || inviteProfile.Source != NodeProfileSourceFleetInvite || inviteProfile.Confidence != NodeProfileConfidenceFleetInvite {
				t.Fatalf("invite profile = %+v", inviteProfile)
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-b",
				FleetID:       "fleet-invite-b",
				NodeID:        133053,
				EntmootPubKey: "pub-2",
				Hostname:      "deimos-backup",
				Status:        FleetMemberInvited,
				ExpiresAtMS:   4_102_444_800_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite backup: %v", err)
			}
			if err := store.DeleteFleetInvite(ctx, "invite-a"); err != nil {
				t.Fatalf("DeleteFleetInvite: %v", err)
			}
			inviteProfile, ok, err = store.GetNodeProfile(ctx, 133053)
			if err != nil || !ok {
				t.Fatalf("GetNodeProfile backup invite ok/err = %v/%v", ok, err)
			}
			if inviteProfile.Hostname != "deimos-backup" || inviteProfile.Source != NodeProfileSourceFleetInvite {
				t.Fatalf("backup invite profile = %+v", inviteProfile)
			}
			if err := store.DeleteFleetInvite(ctx, "invite-b"); err != nil {
				t.Fatalf("DeleteFleetInvite backup: %v", err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 133053); err != nil || ok {
				t.Fatalf("GetNodeProfile deleted invite = %+v ok=%v err=%v, want none", profile, ok, err)
			}
			for _, fleetID := range []string{"fleet-a", "fleet-b"} {
				if _, err := store.CreateFleet(ctx, FleetRecord{
					FleetID:     fleetID,
					Name:        fleetID,
					Status:      FleetStatusActive,
					Coordinator: entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: []byte("coordinator")},
				}); err != nil {
					t.Fatalf("RecreateFleet %s: %v", fleetID, err)
				}
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-c",
				FleetID:       "fleet-a",
				NodeID:        133053,
				EntmootPubKey: "pub-2",
				Hostname:      "deimos",
				Status:        FleetMemberInvited,
				ExpiresAtMS:   4_102_444_800_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite fleet delete primary: %v", err)
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-d",
				FleetID:       "fleet-b",
				NodeID:        133053,
				EntmootPubKey: "pub-2",
				Hostname:      "deimos-backup",
				Status:        FleetMemberInvited,
				ExpiresAtMS:   4_102_444_800_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite fleet delete backup: %v", err)
			}
			if err := store.DeleteFleet(ctx, "fleet-a"); err != nil {
				t.Fatalf("DeleteFleet primary invite: %v", err)
			}
			inviteProfile, ok, err = store.GetNodeProfile(ctx, 133053)
			if err != nil || !ok {
				t.Fatalf("GetNodeProfile backup invite after fleet delete ok/err = %v/%v", ok, err)
			}
			if inviteProfile.Hostname != "deimos-backup" || inviteProfile.Source != NodeProfileSourceFleetInvite {
				t.Fatalf("backup invite profile after fleet delete = %+v", inviteProfile)
			}
			if err := store.DeleteFleet(ctx, "fleet-b"); err != nil {
				t.Fatalf("DeleteFleet backup invite: %v", err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 133053); err != nil || ok {
				t.Fatalf("GetNodeProfile deleted invite fleets = %+v ok=%v err=%v, want none", profile, ok, err)
			}
			for _, fleetID := range []string{"fleet-archive-a", "fleet-archive-b"} {
				if _, err := store.CreateFleet(ctx, FleetRecord{
					FleetID:     fleetID,
					Name:        fleetID,
					Status:      FleetStatusActive,
					Coordinator: entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: []byte("coordinator")},
				}); err != nil {
					t.Fatalf("CreateFleet archive %s: %v", fleetID, err)
				}
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-archive-a",
				FleetID:       "fleet-archive-a",
				NodeID:        133053,
				EntmootPubKey: "pub-2",
				Hostname:      "deimos-archive",
				Status:        FleetMemberInvited,
				ExpiresAtMS:   4_102_444_800_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite archive primary: %v", err)
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-archive-b",
				FleetID:       "fleet-archive-b",
				NodeID:        133053,
				EntmootPubKey: "pub-2",
				Hostname:      "deimos-archive-backup",
				Status:        FleetMemberInvited,
				ExpiresAtMS:   4_102_444_800_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite archive backup: %v", err)
			}
			if _, ok, err := store.ArchiveFleet(ctx, "fleet-archive-a", 9_000); err != nil || !ok {
				t.Fatalf("ArchiveFleet primary invite ok/err = %v/%v", ok, err)
			}
			inviteProfile, ok, err = store.GetNodeProfile(ctx, 133053)
			if err != nil || !ok {
				t.Fatalf("GetNodeProfile backup invite after fleet archive ok/err = %v/%v", ok, err)
			}
			if inviteProfile.Hostname != "deimos-archive-backup" || inviteProfile.Source != NodeProfileSourceFleetInvite {
				t.Fatalf("backup invite profile after fleet archive = %+v", inviteProfile)
			}
			if _, ok, err := store.ArchiveFleet(ctx, "fleet-archive-b", 9_001); err != nil || !ok {
				t.Fatalf("ArchiveFleet backup invite ok/err = %v/%v", ok, err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 133053); err != nil || ok {
				t.Fatalf("GetNodeProfile archived invite fleets = %+v ok=%v err=%v, want none", profile, ok, err)
			}
			for _, fleetID := range []string{"fleet-archive-member-a", "fleet-archive-member-b"} {
				if _, err := store.CreateFleet(ctx, FleetRecord{
					FleetID:     fleetID,
					Name:        fleetID,
					Status:      FleetStatusActive,
					Coordinator: entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: []byte("coordinator")},
				}); err != nil {
					t.Fatalf("CreateFleet archive member %s: %v", fleetID, err)
				}
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-archive-member-a",
				NodeID:        155762,
				EntmootPubKey: "pub-archive-member",
				Hostname:      "hermes-archive",
				Role:          FleetRoleAgent,
				Status:        FleetMemberActive,
			}); err != nil {
				t.Fatalf("UpsertFleetMember archive primary: %v", err)
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-archive-member-b",
				NodeID:        155762,
				EntmootPubKey: "pub-archive-member",
				Hostname:      "hermes-archive-backup",
				Role:          FleetRoleAgent,
				Status:        FleetMemberActive,
			}); err != nil {
				t.Fatalf("UpsertFleetMember archive backup: %v", err)
			}
			if _, ok, err := store.ArchiveFleet(ctx, "fleet-archive-member-a", 9_002); err != nil || !ok {
				t.Fatalf("ArchiveFleet primary member ok/err = %v/%v", ok, err)
			}
			memberProfile, ok, err = store.GetNodeProfile(ctx, 155762)
			if err != nil || !ok {
				t.Fatalf("GetNodeProfile backup member after fleet archive ok/err = %v/%v", ok, err)
			}
			if memberProfile.Hostname != "hermes-archive-backup" || memberProfile.Source != NodeProfileSourceFleetMember {
				t.Fatalf("backup member profile after fleet archive = %+v", memberProfile)
			}
			if _, ok, err := store.ArchiveFleet(ctx, "fleet-archive-member-b", 9_003); err != nil || !ok {
				t.Fatalf("ArchiveFleet backup member ok/err = %v/%v", ok, err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 155762); err != nil || ok {
				t.Fatalf("GetNodeProfile archived member fleets = %+v ok=%v err=%v, want none", profile, ok, err)
			}
			if _, err := store.CreateFleet(ctx, FleetRecord{
				FleetID:     "fleet-accept",
				Name:        "Accept Fleet",
				Status:      FleetStatusActive,
				Coordinator: entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: []byte("coordinator")},
			}); err != nil {
				t.Fatalf("CreateFleet accept: %v", err)
			}
			acceptPub := base64.StdEncoding.EncodeToString([]byte("accept-pub"))
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-accept",
				NodeID:        133054,
				EntmootPubKey: acceptPub,
				Hostname:      "accept-invite",
				Role:          FleetRoleAgent,
				Status:        FleetMemberInvited,
			}); err != nil {
				t.Fatalf("UpsertFleetMember accept invited: %v", err)
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-accepted",
				FleetID:       "fleet-accept",
				NodeID:        133054,
				EntmootPubKey: acceptPub,
				Hostname:      "accept-invite",
				Status:        FleetMemberInvited,
				ExpiresAtMS:   4_102_444_800_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite accepted: %v", err)
			}
			if _, _, applied, err := store.ReconcileFleetInviteAcceptance(ctx, "fleet-accept", 133054, acceptPub, 5_000, "accept-member"); err != nil || !applied {
				t.Fatalf("ReconcileFleetInviteAcceptance applied/err = %v/%v", applied, err)
			}
			if err := store.DeleteFleetMember(ctx, "fleet-accept", 133054); err != nil {
				t.Fatalf("DeleteFleetMember accepted: %v", err)
			}
			if profile, ok, err := store.GetNodeProfile(ctx, 133054); err != nil || ok {
				t.Fatalf("GetNodeProfile accepted invite after member delete = %+v ok=%v err=%v, want none", profile, ok, err)
			}
		})
	}
}

func TestSQLiteStateStoreBackfillsFleetNodeProfilesOnOpen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	if _, err := store.CreateFleet(ctx, FleetRecord{
		FleetID:     "fleet-backfill",
		Name:        "Backfill Fleet",
		Status:      FleetStatusActive,
		Coordinator: entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: []byte("coordinator")},
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
		FleetID:       "fleet-backfill",
		NodeID:        155760,
		EntmootPubKey: "pub-member",
		Hostname:      "hermes",
		Role:          FleetRoleAgent,
		Status:        FleetMemberActive,
		UpdatedAtMS:   10,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
		InviteID:      "invite-backfill",
		FleetID:       "fleet-backfill",
		NodeID:        133053,
		EntmootPubKey: "pub-invite",
		Hostname:      "deimos",
		Status:        FleetMemberInvited,
		UpdatedAtMS:   11,
		ExpiresAtMS:   4_102_444_800_000,
	}); err != nil {
		t.Fatalf("CreateFleetInvite: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM esp_node_profile_sources`); err != nil {
		t.Fatalf("delete node profile sources: %v", err)
	}
	if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
		NodeID:       155761,
		Hostname:     "stale-member",
		Source:       NodeProfileSourceFleetMember,
		ObservedAtMS: 12,
	}); err != nil {
		t.Fatalf("UpsertNodeProfile stale member: %v", err)
	}
	if _, _, err := store.UpsertNodeProfile(ctx, NodeProfileRecord{
		NodeID:       133054,
		Hostname:     "stale-invite",
		Source:       NodeProfileSourceFleetInvite,
		ObservedAtMS: 13,
		ExpiresAtMS:  4_102_444_800_000,
	}); err != nil {
		t.Fatalf("UpsertNodeProfile stale invite: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store, err = OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("reopen SQLite state: %v", err)
	}
	defer store.Close()
	memberProfile, ok, err := store.GetNodeProfile(ctx, 155760)
	if err != nil || !ok || memberProfile.Hostname != "hermes" || memberProfile.Source != NodeProfileSourceFleetMember {
		t.Fatalf("GetNodeProfile backfilled member = %+v ok=%v err=%v", memberProfile, ok, err)
	}
	inviteProfile, ok, err := store.GetNodeProfile(ctx, 133053)
	if err != nil || !ok || inviteProfile.Hostname != "deimos" || inviteProfile.Source != NodeProfileSourceFleetInvite {
		t.Fatalf("GetNodeProfile backfilled invite = %+v ok=%v err=%v", inviteProfile, ok, err)
	}
	if profile, ok, err := store.GetNodeProfile(ctx, 155761); err != nil || ok {
		t.Fatalf("GetNodeProfile stale member after backfill = %+v ok=%v err=%v, want none", profile, ok, err)
	}
	if profile, ok, err := store.GetNodeProfile(ctx, 133054); err != nil || ok {
		t.Fatalf("GetNodeProfile stale invite after backfill = %+v ok=%v err=%v, want none", profile, ok, err)
	}
}
