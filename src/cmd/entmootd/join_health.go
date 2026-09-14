package main

import (
	"context"

	"entmoot/pkg/entmoot/store"
)

type joinHealthSummary struct {
	Groups            int    `json:"groups"`
	Members           int    `json:"members"`
	Peers             int    `json:"peers"`
	LocalMember       bool   `json:"local_member"`
	LocalMemberStatus string `json:"local_member_status"`
	RouteProbe        string `json:"route_probe"`
	// QuarantinedMessages counts live messages held because they name a roster
	// head this node has not synchronized yet. A non-zero value is a
	// synchronization gap that resolves itself; a value that stays non-zero is
	// a roster that is not converging.
	QuarantinedMessages int `json:"quarantined_messages"`
	// UnknownHeadMessages counts historical messages the last catch-up skipped
	// for the same reason. Unlike the live buffer these are not held, so the
	// count is what the most recent pass saw.
	UnknownHeadMessages int `json:"unknown_head_messages"`
	// RosterDivergence lists peers advertising a roster head this node holds
	// no chain for and could not take. It is the visible symptom of a forked
	// log, which retrying never repairs, so it belongs in status output and
	// not only in the daemon log.
	RosterDivergence []rosterDivergenceReport `json:"roster_divergence,omitempty"`
}

type rosterDivergenceReport struct {
	GroupID    string `json:"group_id"`
	PeerID     string `json:"peer_id"`
	LocalHead  string `json:"local_head"`
	RemoteHead string `json:"remote_head"`
	Reason     string `json:"reason"`
	SinceMS    int64  `json:"since_ms"`
}

func buildJoinHealthSummary(_ context.Context, runtime *groupRuntime, _ *store.SQLite, _ []byte) joinHealthSummary {
	health := joinHealthSummary{LocalMember: true, LocalMemberStatus: doctorLocalMemberOK, RouteProbe: "not_requested"}
	if runtime == nil {
		health.LocalMember = false
		health.LocalMemberStatus = "runtime_unavailable"
		return health
	}
	health.Groups = runtime.Count()
	health.Peers = len(runtime.host.Network().Peers())
	for _, groupID := range runtime.ActiveGroupIDs() {
		if session, ok := runtime.Get(groupID); ok {
			health.Members += len(session.roster.MemberIDs())
			if !session.roster.IsMemberID(runtime.binding.MemberID) {
				health.LocalMember = false
				health.LocalMemberStatus = "missing"
			}
			health.QuarantinedMessages += session.live.QuarantinedMessages()
			health.UnknownHeadMessages += int(session.unknownHeads.Load())
			health.RosterDivergence = append(health.RosterDivergence, session.rosterDivergenceReports(groupID)...)
		}
	}
	return health
}
