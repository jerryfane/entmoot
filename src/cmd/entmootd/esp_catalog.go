package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/membership"
)

type localGroupCatalog struct {
	dataDir  string
	metadata esphttp.GroupMetadataStore
	state    esphttp.StateStore
}

type espDiagnosticsProvider struct {
	flags globalFlags
}

func (p espDiagnosticsProvider) GroupDiagnostics(ctx context.Context, gid entmoot.GroupID, probe bool, timeout time.Duration) (any, error) {
	report, err := buildDoctorReport(ctx, &p.flags, &gid, probe, timeout)
	if err != nil {
		return nil, err
	}
	if len(report.Groups) == 0 {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "group_not_found", Message: "group not joined"}
	}
	return report.Groups[0], nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (c localGroupCatalog) ListGroups(ctx context.Context) ([]esphttp.GroupSummary, error) {
	return c.ListGroupsWithOptions(ctx, esphttp.GroupListOptions{})
}

func (c localGroupCatalog) ListGroupsWithOptions(ctx context.Context, opts esphttp.GroupListOptions) ([]esphttp.GroupSummary, error) {
	gids, err := listGroupIDs(c.dataDir, nil)
	if err != nil {
		return nil, err
	}
	out := make([]esphttp.GroupSummary, 0, len(gids))
	for _, gid := range gids {
		group, ok, err := c.GetGroup(ctx, gid)
		if err != nil {
			return nil, err
		}
		if ok && groupVisibleForList(group.Metadata, opts) {
			out = append(out, group)
		}
	}
	return out, nil
}

func groupVisibleForList(meta map[string]interface{}, opts esphttp.GroupListOptions) bool {
	if !opts.IncludeHidden && groupHidden(meta) {
		return false
	}
	return true
}

func groupHidden(meta map[string]interface{}) bool {
	if meta == nil {
		return false
	}
	hidden, ok := meta["hidden"].(bool)
	return ok && hidden
}

func (c localGroupCatalog) GetGroup(ctx context.Context, gid entmoot.GroupID) (esphttp.GroupSummary, bool, error) {
	r, err := membership.Open(c.dataDir, gid)
	if err != nil {
		return esphttp.GroupSummary{}, false, err
	}
	defer r.Close()
	members := r.MemberIDs()
	if len(members) == 0 {
		return esphttp.GroupSummary{}, false, nil
	}
	group := esphttp.GroupSummary{
		GroupID:    gid,
		Members:    len(members),
		RosterHead: r.Canonical().ID,
	}
	if c.metadata != nil {
		if raw, ok, err := c.metadata.GetGroupMetadata(ctx, gid); err != nil {
			return esphttp.GroupSummary{}, false, err
		} else if ok && len(raw) > 0 {
			var meta map[string]interface{}
			if err := json.Unmarshal(raw, &meta); err != nil {
				slog.Warn("esp group metadata ignored: invalid JSON object",
					slog.String("group_id", gid.String()),
					slog.String("err", err.Error()))
				return group, true, nil
			}
			if meta == nil {
				slog.Warn("esp group metadata ignored: non-object JSON",
					slog.String("group_id", gid.String()))
				return group, true, nil
			}
			group.Metadata = meta
			if name, ok := meta["name"].(string); ok {
				group.Name = name
			}
			if description, ok := meta["description"].(string); ok {
				group.Description = description
			}
			group.Tags = metadataTags(meta["tags"])
		}
	}
	return group, true, nil
}

func (c localGroupCatalog) ListMembers(ctx context.Context, gid entmoot.GroupID) ([]esphttp.MemberSummary, error) {
	r, err := membership.Open(c.dataDir, gid)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	founder := r.Founder()
	founderID, _ := entmoot.MemberIDFromPublicKey(founder.EntmootPubKey)
	members := r.MemberIDs()
	out := make([]esphttp.MemberSummary, 0, len(members))
	for _, memberID := range members {
		info, ok := r.MemberInfoByID(memberID)
		if !ok {
			continue
		}
		out = append(out, esphttp.MemberSummary{
			MemberID:      memberID,
			PeerID:        info.PeerID,
			EntmootPubKey: encodeBase64(info.EntmootPubKey),
			Founder:       founderID == memberID,
		})
	}
	if c.state != nil {
		configs, err := c.state.ListLiveAgentConfigs(ctx, gid)
		if err != nil {
			return nil, err
		}
		presences, err := c.state.ListLiveAgentPresence(ctx, gid)
		if err != nil {
			return nil, err
		}
		liveByMember := esphttp.LiveAgentStatesByMember(configs, presences, time.Now().UnixMilli())
		for i := range out {
			if live, ok := liveByMember[out[i].MemberID]; ok {
				state := live
				out[i].Live = &state
			}
		}
	}
	return out, nil
}

func metadataTags(v any) []string {
	raw, ok := v.([]interface{})
	if !ok {
		return nil
	}
	tags := make([]string, 0, len(raw))
	for _, item := range raw {
		tag, ok := item.(string)
		if !ok {
			return nil
		}
		tags = append(tags, tag)
	}
	return tags
}
