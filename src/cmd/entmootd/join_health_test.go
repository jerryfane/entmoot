package main

import (
	"context"
	"encoding/json"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestJoinHealthFromDoctorGroups(t *testing.T) {
	groups := []doctorGroupReport{
		{
			LocalMember:       true,
			LocalMemberStatus: doctorLocalMemberOK,
			Members:           3,
			Peers: []doctorPeerReport{
				{NodeID: 1, Trust: "self", Profile: "ok", Transport: "ok"},
				{NodeID: 2, Trust: "trusted", Profile: "ok", Transport: "stale"},
				{NodeID: 3, Trust: "missing", Profile: "missing", Transport: "missing"},
			},
		},
		{
			LocalMember:       false,
			LocalMemberStatus: doctorLocalMemberIdentityMismatch,
			Members:           1,
			Peers: []doctorPeerReport{
				{NodeID: 4, Trust: "pending", Profile: "ok", Transport: "ok"},
			},
		},
	}

	health := joinHealthFromDoctorGroups(groups, true, true, 5)
	if health.Groups != 2 || health.Members != 4 || health.Peers != 3 {
		t.Fatalf("size counts = groups:%d members:%d peers:%d", health.Groups, health.Members, health.Peers)
	}
	if health.LocalMember || health.LocalMemberStatus != doctorLocalMemberIdentityMismatch {
		t.Fatalf("local membership = (%t, %q), want mismatch", health.LocalMember, health.LocalMemberStatus)
	}
	if !health.TrustQueryOK || !health.PendingQueryOK {
		t.Fatalf("query flags = trust:%t pending:%t, want true", health.TrustQueryOK, health.PendingQueryOK)
	}
	if health.TrustedPeers != 1 || health.PendingPeers != 1 || health.MissingTrust != 1 {
		t.Fatalf("trust counts = trusted:%d pending:%d missing:%d", health.TrustedPeers, health.PendingPeers, health.MissingTrust)
	}
	if health.ProfilesKnown != 3 || health.TransportAdsKnown != 2 || health.TransportAdsStale != 1 {
		t.Fatalf("ad counts = profiles:%d transport:%d stale:%d", health.ProfilesKnown, health.TransportAdsKnown, health.TransportAdsStale)
	}
	if health.OnboardingHandshakeCandidates != 5 || health.RouteProbe != "not_run" {
		t.Fatalf("onboarding/route = %d/%q", health.OnboardingHandshakeCandidates, health.RouteProbe)
	}
}

func TestJoinHealthOnboardingCandidatesWithoutTrustQuery(t *testing.T) {
	invite := entmoot.Invite{
		Founder: entmoot.NodeInfo{PilotNodeID: 2},
		BootstrapPeers: []entmoot.BootstrapPeer{
			{NodeID: 3},
		},
	}
	rosterMembers := []entmoot.NodeID{1, 2, 3, 4}
	trusted := map[entmoot.NodeID]struct{}{3: {}}

	if got := joinHealthOnboardingCandidates(1, invite, rosterMembers, trusted, false); got != 3 {
		t.Fatalf("candidates without trust query = %d, want 3", got)
	}
	if got := joinHealthOnboardingCandidates(1, invite, rosterMembers, trusted, true); got != 2 {
		t.Fatalf("candidates with trust query = %d, want 2", got)
	}
}

func TestBuildJoinHealthDoctorGroupDoesNotPopulateMessages(t *testing.T) {
	id := mustGenerateIdentity(t)
	r := newDoctorTestRoster(t, 7, id)
	sess := &groupSession{
		groupID: entmoot.GroupID{1},
		roster:  r,
	}

	group := buildJoinHealthDoctorGroup(context.Background(), nil, sess, 7, []byte(id.PublicKey), doctorPilotReport{
		Reachable:      true,
		NodeID:         7,
		TrustedQueryOK: true,
		PendingQueryOK: true,
	}, nil, nil)

	if group.Messages != 0 {
		t.Fatalf("messages = %d, want 0", group.Messages)
	}
	if !group.LocalMember || group.LocalMemberStatus != doctorLocalMemberOK {
		t.Fatalf("local membership = %t/%q, want ok", group.LocalMember, group.LocalMemberStatus)
	}
	if group.Members != 1 || len(group.Peers) != 1 || group.Peers[0].Trust != "self" {
		t.Fatalf("group = %+v, want one self peer", group)
	}
}

func TestGroupDaemonEventIncludesHealth(t *testing.T) {
	gid := entmoot.GroupID{0x42}
	health := joinHealthSummary{
		Groups:            1,
		Members:           2,
		Peers:             1,
		LocalMember:       true,
		LocalMemberStatus: doctorLocalMemberOK,
		RouteProbe:        "not_run",
	}
	event := groupDaemonEvent("joined", &globalFlags{
		socket:     "/tmp/pilot.sock",
		identity:   "/id.json",
		data:       "/data",
		listenPort: 1004,
	}, []entmoot.GroupID{gid}, 2, health, "/data/control.sock")

	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	var decoded struct {
		Event  string            `json:"event"`
		Health joinHealthSummary `json:"health"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if decoded.Event != "joined" {
		t.Fatalf("event = %q, want joined", decoded.Event)
	}
	if decoded.Health.Groups != 1 || decoded.Health.Peers != 1 || decoded.Health.RouteProbe != "not_run" {
		t.Fatalf("health = %+v", decoded.Health)
	}
}
