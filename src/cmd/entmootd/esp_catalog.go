package main

import (
	"context"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/roster"
)

type localGroupCatalog struct {
	dataDir string
}

func (c localGroupCatalog) ListGroups(_ context.Context) ([]esphttp.GroupSummary, error) {
	gids, err := listGroupIDs(c.dataDir, nil)
	if err != nil {
		return nil, err
	}
	out := make([]esphttp.GroupSummary, 0, len(gids))
	for _, gid := range gids {
		group, ok, err := c.GetGroup(context.Background(), gid)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, group)
		}
	}
	return out, nil
}

func (c localGroupCatalog) GetGroup(_ context.Context, gid entmoot.GroupID) (esphttp.GroupSummary, bool, error) {
	r, err := roster.OpenJSONL(c.dataDir, gid)
	if err != nil {
		return esphttp.GroupSummary{}, false, err
	}
	defer r.Close()
	members := r.Members()
	if len(members) == 0 {
		return esphttp.GroupSummary{}, false, nil
	}
	return esphttp.GroupSummary{
		GroupID:    gid,
		Members:    len(members),
		RosterHead: r.Head(),
	}, true, nil
}

func (c localGroupCatalog) ListMembers(_ context.Context, gid entmoot.GroupID) ([]esphttp.MemberSummary, error) {
	r, err := roster.OpenJSONL(c.dataDir, gid)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	founder, _ := r.Founder()
	members := r.Members()
	out := make([]esphttp.MemberSummary, 0, len(members))
	for _, nodeID := range members {
		info, ok := r.MemberInfo(nodeID)
		if !ok {
			continue
		}
		out = append(out, esphttp.MemberSummary{
			NodeID:        nodeID,
			EntmootPubKey: encodeBase64(info.EntmootPubKey),
			Founder:       founder.PilotNodeID == nodeID,
		})
	}
	return out, nil
}
