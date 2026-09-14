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
		}
	}
	return health
}
