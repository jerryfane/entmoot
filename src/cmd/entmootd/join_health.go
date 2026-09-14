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
	// QuarantinedMessages counts live messages held because they name a
	// checkpoint this node has not synchronized yet. A non-zero value is a
	// synchronization gap that resolves itself; a value that stays non-zero is
	// membership that is not converging.
	QuarantinedMessages int `json:"quarantined_messages"`
	// UnknownHeadMessages counts historical messages the last catch-up skipped
	// for the same reason. Unlike the live buffer these are not held, so the
	// count is what the most recent pass saw.
	UnknownHeadMessages int `json:"unknown_head_messages"`
	// PendingMembershipRecords counts membership records not yet folded into a
	// checkpoint. It is the growth an operator watches: a group whose admins
	// are all offline keeps accumulating records, and each new member replays
	// them.
	//
	// There is no divergence field, because there is no fork to report: peers
	// exchange records as a set, so two nodes that hold the same records
	// project the same membership regardless of the order they arrived in.
	PendingMembershipRecords int `json:"pending_membership_records"`
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
			health.Members += len(session.group.MemberIDs())
			if !session.group.IsMemberID(runtime.binding.MemberID) {
				health.LocalMember = false
				health.LocalMemberStatus = "missing"
			}
			health.QuarantinedMessages += session.live.QuarantinedMessages()
			health.UnknownHeadMessages += int(session.unknownHeads.Load())
			health.PendingMembershipRecords += session.group.EffectivePendingCount()
		}
	}
	return health
}
