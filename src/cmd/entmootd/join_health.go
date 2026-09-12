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
		}
	}
	return health
}
