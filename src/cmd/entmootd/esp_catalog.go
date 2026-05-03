package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/wire"
)

type memberProfileReader interface {
	GetMemberProfileAd(context.Context, entmoot.GroupID, entmoot.NodeID, time.Time) (wire.MemberProfileAd, bool, error)
}

type localGroupCatalog struct {
	dataDir  string
	metadata esphttp.GroupMetadataStore
	profiles memberProfileReader
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

func (c localGroupCatalog) ListGroups(ctx context.Context) ([]esphttp.GroupSummary, error) {
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
		if ok {
			out = append(out, group)
		}
	}
	return out, nil
}

func (c localGroupCatalog) GetGroup(ctx context.Context, gid entmoot.GroupID) (esphttp.GroupSummary, bool, error) {
	r, err := roster.OpenJSONL(c.dataDir, gid)
	if err != nil {
		return esphttp.GroupSummary{}, false, err
	}
	defer r.Close()
	members := r.Members()
	if len(members) == 0 {
		return esphttp.GroupSummary{}, false, nil
	}
	group := esphttp.GroupSummary{
		GroupID:    gid,
		Members:    len(members),
		RosterHead: r.Head(),
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
		member := esphttp.MemberSummary{
			NodeID:        nodeID,
			EntmootPubKey: encodeBase64(info.EntmootPubKey),
			Founder:       founder.PilotNodeID == nodeID,
		}
		if c.profiles != nil {
			ad, ok, err := c.profiles.GetMemberProfileAd(ctx, gid, nodeID, time.Now())
			if err != nil {
				slog.Warn("esp group member profile ignored",
					slog.String("group_id", gid.String()),
					slog.Uint64("node_id", uint64(nodeID)),
					slog.String("err", err.Error()))
			} else if ok {
				if memberProfileMatchesRosterInfo(ad, info) {
					member.Hostname = ad.Hostname
				} else {
					slog.Debug("esp group member profile ignored: identity mismatch",
						slog.String("group_id", gid.String()),
						slog.Uint64("node_id", uint64(nodeID)))
				}
			}
		}
		out = append(out, member)
	}
	return out, nil
}

func memberProfileMatchesRosterInfo(ad wire.MemberProfileAd, info entmoot.NodeInfo) bool {
	return ad.Author.PilotNodeID == info.PilotNodeID &&
		bytes.Equal(ad.Author.EntmootPubKey, info.EntmootPubKey)
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
