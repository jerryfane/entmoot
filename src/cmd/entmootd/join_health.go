package main

import (
	"context"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/store"
)

const joinHealthTimeout = 2 * time.Second

type joinHealthSummary struct {
	Groups                        int    `json:"groups"`
	Members                       int    `json:"members"`
	Peers                         int    `json:"peers"`
	LocalMember                   bool   `json:"local_member"`
	LocalMemberStatus             string `json:"local_member_status"`
	TrustQueryOK                  bool   `json:"trust_query_ok"`
	PendingQueryOK                bool   `json:"pending_query_ok"`
	TrustedPeers                  int    `json:"trusted_peers"`
	PendingPeers                  int    `json:"pending_peers"`
	MissingTrust                  int    `json:"missing_trust"`
	ProfilesKnown                 int    `json:"profiles_known"`
	TransportAdsKnown             int    `json:"transport_ads_known"`
	TransportAdsStale             int    `json:"transport_ads_stale"`
	OnboardingHandshakeCandidates int    `json:"onboarding_handshake_candidates"`
	RouteProbe                    string `json:"route_probe"`
}

func buildJoinHealthSummary(parent context.Context, runtime *groupRuntime, st *store.SQLite, localPub []byte) joinHealthSummary {
	ctx, cancel := context.WithTimeout(parent, joinHealthTimeout)
	defer cancel()

	trusted, trustOK := joinHealthTrustedPeers(ctx, runtime)
	pending, pendingOK := joinHealthPendingPeers(ctx, runtime)
	pilotReport := doctorPilotReport{
		Reachable:      true,
		NodeID:         runtime.nodeID,
		TrustedQueryOK: trustOK,
		PendingQueryOK: pendingOK,
	}

	groups := make([]doctorGroupReport, 0)
	onboardingCandidates := 0
	invites := runtime.JoinHealthInvites()
	for _, gid := range runtime.ActiveGroupIDs() {
		sess, ok := runtime.Get(gid)
		if !ok {
			continue
		}
		group := buildJoinHealthDoctorGroup(ctx, st, sess, runtime.nodeID, localPub, pilotReport, trusted, pending)
		if invite, ok := invites[gid]; ok {
			onboardingCandidates += joinHealthOnboardingCandidates(runtime.nodeID, invite, sess.roster.Members(), trusted, trustOK)
		}
		groups = append(groups, group)
	}
	return joinHealthFromDoctorGroups(groups, trustOK, pendingOK, onboardingCandidates)
}

func buildJoinHealthDoctorGroup(ctx context.Context, st *store.SQLite, sess *groupSession, localNode entmoot.NodeID, localPub []byte, pilotReport doctorPilotReport, trusted, pending map[entmoot.NodeID]struct{}) doctorGroupReport {
	members := sess.roster.Members()
	group := doctorGroupReport{
		GroupID: sess.groupID,
		Members: len(members),
	}
	group.LocalMember, group.LocalMemberStatus = doctorLocalMembership(sess.roster, localNode, localPub)
	now := time.Now()
	for _, id := range members {
		info, _ := sess.roster.MemberInfo(id)
		peer := doctorPeerReport{
			NodeID:    id,
			Roster:    true,
			Profile:   "missing",
			Transport: "missing",
			Trust:     doctorPeerTrust(localNode, id, pilotReport, trusted, pending),
			Route:     "not_checked",
		}
		if localNode != 0 && id == localNode {
			peer.Route = "self"
		}
		if st != nil {
			if ad, ok, err := st.GetMemberProfileAd(ctx, sess.groupID, id, now); err == nil && ok && memberProfileMatchesRosterInfo(ad, info) {
				peer.Profile = "ok"
				peer.Hostname = ad.Hostname
			}
			if ad, ok, err := st.GetTransportAd(ctx, sess.groupID, id); err == nil && ok {
				if ad.NotAfter > now.UnixMilli() {
					peer.Transport = "ok"
				} else {
					peer.Transport = "stale"
				}
			}
		}
		peer.Diagnosis = diagnosePeer(peer, false)
		group.Peers = append(group.Peers, peer)
	}
	return group
}

func joinHealthOnboardingCandidates(local entmoot.NodeID, invite entmoot.Invite, rosterMembers []entmoot.NodeID, trusted map[entmoot.NodeID]struct{}, trustOK bool) int {
	var trustedList []entmoot.NodeID
	if trustOK {
		trustedList = make([]entmoot.NodeID, 0, len(trusted))
		for nodeID := range trusted {
			trustedList = append(trustedList, nodeID)
		}
	}
	return len(selectOnboardingHandshakePeers(local, invite, rosterMembers, trustedList))
}

func joinHealthTrustedPeers(ctx context.Context, runtime *groupRuntime) (map[entmoot.NodeID]struct{}, bool) {
	out := map[entmoot.NodeID]struct{}{}
	if runtime == nil || runtime.mux == nil {
		return out, false
	}
	trusted, err := runtime.mux.TrustedPeers(ctx)
	if err != nil {
		return out, false
	}
	for _, nodeID := range trusted {
		out[nodeID] = struct{}{}
	}
	return out, true
}

func joinHealthPendingPeers(ctx context.Context, runtime *groupRuntime) (map[entmoot.NodeID]struct{}, bool) {
	out := map[entmoot.NodeID]struct{}{}
	if runtime == nil || runtime.pilotDriver == nil {
		return out, false
	}
	pending, err := runtime.pilotDriver.PendingHandshakes(ctx)
	if err != nil {
		return out, false
	}
	for _, item := range pending {
		out[entmoot.NodeID(item.NodeID)] = struct{}{}
	}
	return out, true
}

func joinHealthFromDoctorGroups(groups []doctorGroupReport, trustOK, pendingOK bool, onboardingCandidates int) joinHealthSummary {
	health := joinHealthSummary{
		Groups:                        len(groups),
		LocalMember:                   true,
		LocalMemberStatus:             doctorLocalMemberOK,
		TrustQueryOK:                  trustOK,
		PendingQueryOK:                pendingOK,
		OnboardingHandshakeCandidates: onboardingCandidates,
		RouteProbe:                    "not_run",
	}
	for _, group := range groups {
		if !group.LocalMember && health.LocalMember {
			health.LocalMember = false
			health.LocalMemberStatus = group.LocalMemberStatus
		}
		health.Members += group.Members
		for _, peer := range group.Peers {
			if peer.Trust != "self" {
				health.Peers++
			}
			switch peer.Trust {
			case "trusted":
				health.TrustedPeers++
			case "pending":
				health.PendingPeers++
			case "missing":
				health.MissingTrust++
			}
			if peer.Profile == "ok" {
				health.ProfilesKnown++
			}
			if peer.Transport == "ok" {
				health.TransportAdsKnown++
			}
			if peer.Transport == "stale" {
				health.TransportAdsStale++
			}
		}
	}
	return health
}
