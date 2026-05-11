package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
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

func (p espDiagnosticsProvider) FleetDiagnostics(ctx context.Context, fleet esphttp.FleetRecord, members []esphttp.FleetMemberRecord, probe bool, timeout time.Duration) (any, error) {
	var control *doctorGroupReport
	var rosterIdentities []fleetRosterIdentity
	if fleet.ControlGroupID != (entmoot.GroupID{}) {
		report, err := buildDoctorReport(ctx, &p.flags, &fleet.ControlGroupID, probe, timeout)
		if err != nil {
			return nil, err
		}
		if len(report.Groups) > 0 {
			control = &report.Groups[0]
		}
		if control != nil {
			s, err := setup(&p.flags)
			if err != nil {
				return nil, err
			}
			if r, err := roster.OpenJSONL(s.dataDir, fleet.ControlGroupID); err == nil {
				for _, nodeID := range r.Members() {
					info, ok := r.MemberInfo(nodeID)
					if ok {
						rosterIdentities = append(rosterIdentities, fleetRosterIdentity{
							NodeID:        nodeID,
							EntmootPubKey: encodeBase64(info.EntmootPubKey),
						})
					}
				}
				_ = r.Close()
			}
		}
	}
	return buildFleetDiagnosticsReport(fleet, members, control, rosterIdentities), nil
}

type espFleetDiagnosticsReport struct {
	FleetID        string                    `json:"fleet_id"`
	Name           string                    `json:"name"`
	ControlGroupID entmoot.GroupID           `json:"control_group_id,omitempty"`
	Members        int                       `json:"members"`
	ActiveMembers  int                       `json:"active_members"`
	ControlMembers int                       `json:"control_members"`
	Consistent     bool                      `json:"consistent"`
	Suggestion     string                    `json:"suggestion,omitempty"`
	Peers          []espFleetDiagnosticsPeer `json:"peers"`
}

type espFleetDiagnosticsPeer struct {
	NodeID        entmoot.NodeID `json:"node_id"`
	EntmootPubKey string         `json:"entmoot_pubkey,omitempty"`
	Hostname      string         `json:"hostname,omitempty"`
	Role          string         `json:"role"`
	Status        string         `json:"status"`
	ControlRoster bool           `json:"control_roster"`
	Profile       string         `json:"profile"`
	Transport     string         `json:"transport"`
	Trust         string         `json:"trust"`
	Route         string         `json:"route"`
	RTTMS         int64          `json:"rtt_ms,omitempty"`
	Error         string         `json:"error,omitempty"`
	Diagnosis     string         `json:"diagnosis"`
	Suggestion    string         `json:"suggestion,omitempty"`
	NextCommand   string         `json:"next_command,omitempty"`
}

type fleetRosterIdentity struct {
	NodeID        entmoot.NodeID
	EntmootPubKey string
}

func buildFleetDiagnosticsReport(fleet esphttp.FleetRecord, members []esphttp.FleetMemberRecord, control *doctorGroupReport, rosterIdentities []fleetRosterIdentity) espFleetDiagnosticsReport {
	sort.Slice(members, func(i, j int) bool {
		if members[i].Role != members[j].Role {
			return members[i].Role == esphttp.FleetRoleCoordinator
		}
		return members[i].NodeID < members[j].NodeID
	})
	controlByNode := map[entmoot.NodeID]doctorPeerReport{}
	if control != nil {
		for _, peer := range control.Peers {
			controlByNode[peer.NodeID] = peer
		}
	}
	rosterByIdentity := map[string]fleetRosterIdentity{}
	controlByIdentity := map[string]doctorPeerReport{}
	for _, identity := range rosterIdentities {
		key := fleetDiagnosticsIdentityKey(identity.NodeID, identity.EntmootPubKey)
		rosterByIdentity[key] = identity
		if control, ok := controlByNode[identity.NodeID]; ok {
			controlByIdentity[key] = control
		}
	}
	memberIdentities := map[string]struct{}{}
	out := espFleetDiagnosticsReport{
		FleetID:        fleet.FleetID,
		Name:           fleet.Name,
		ControlGroupID: fleet.ControlGroupID,
		Members:        len(members),
		Consistent:     true,
	}
	for _, member := range members {
		if member.Status == esphttp.FleetMemberActive || member.Role == esphttp.FleetRoleCoordinator {
			out.ActiveMembers++
		}
		identityKey := fleetDiagnosticsIdentityKey(member.NodeID, member.EntmootPubKey)
		peer := fleetDiagnosticsPeerFromMember(member, controlByIdentity[identityKey])
		memberIdentities[identityKey] = struct{}{}
		_, peer.ControlRoster = rosterByIdentity[identityKey]
		membershipDiagnosis := fleetMemberDiagnosis(member, peer.ControlRoster)
		if fleetMembershipMismatch(membershipDiagnosis) {
			peer.Diagnosis = membershipDiagnosis
			peer.Suggestion = fleetMemberSuggestion(membershipDiagnosis)
			out.Consistent = false
		} else if peer.Diagnosis == "" {
			peer.Diagnosis = membershipDiagnosis
		}
		if peer.Suggestion == "" {
			peer.Suggestion = fleetMemberSuggestion(peer.Diagnosis)
		}
		out.Peers = append(out.Peers, peer)
	}
	for _, identity := range rosterIdentities {
		if _, ok := memberIdentities[fleetDiagnosticsIdentityKey(identity.NodeID, identity.EntmootPubKey)]; ok {
			continue
		}
		peer := fleetDiagnosticsPeerFromControlOnly(identity, controlByIdentity[fleetDiagnosticsIdentityKey(identity.NodeID, identity.EntmootPubKey)])
		out.Peers = append(out.Peers, peer)
		out.Consistent = false
	}
	if control != nil {
		out.ControlMembers = control.Members
	} else {
		out.ControlMembers = len(rosterIdentities)
	}
	if !out.Consistent {
		out.Suggestion = "Fleet state and control-group roster do not match; refresh Fleet state and inspect active members missing from or extra in the control group"
	}
	sort.SliceStable(out.Peers, func(i, j int) bool {
		if out.Peers[i].Role != out.Peers[j].Role {
			return out.Peers[i].Role == esphttp.FleetRoleCoordinator || out.Peers[j].Role == "control_only"
		}
		if out.Peers[i].NodeID != out.Peers[j].NodeID {
			return out.Peers[i].NodeID < out.Peers[j].NodeID
		}
		return out.Peers[i].EntmootPubKey < out.Peers[j].EntmootPubKey
	})
	return out
}

func fleetDiagnosticsPeerFromMember(member esphttp.FleetMemberRecord, control doctorPeerReport) espFleetDiagnosticsPeer {
	peer := espFleetDiagnosticsPeer{
		NodeID:        member.NodeID,
		EntmootPubKey: member.EntmootPubKey,
		Hostname:      member.Hostname,
		Role:          member.Role,
		Status:        member.Status,
		Profile:       "not_applicable",
		Transport:     "not_applicable",
		Trust:         "not_applicable",
		Route:         "not_checked",
	}
	if control.NodeID != 0 {
		peer.Hostname = firstNonEmpty(peer.Hostname, control.Hostname)
		peer.Profile = control.Profile
		peer.Transport = control.Transport
		peer.Trust = control.Trust
		peer.Route = control.Route
		peer.RTTMS = control.RTTMS
		peer.Error = control.Error
		peer.Diagnosis = control.Diagnosis
		peer.Suggestion = control.Suggestion
		peer.NextCommand = control.NextCommand
	}
	return peer
}

func fleetDiagnosticsPeerFromControlOnly(identity fleetRosterIdentity, control doctorPeerReport) espFleetDiagnosticsPeer {
	peer := espFleetDiagnosticsPeer{
		NodeID:        identity.NodeID,
		EntmootPubKey: identity.EntmootPubKey,
		Role:          "control_only",
		Status:        "missing_from_fleet_state",
		ControlRoster: true,
		Profile:       "not_applicable",
		Transport:     "not_applicable",
		Trust:         "not_applicable",
		Route:         "not_checked",
		Diagnosis:     "control_member_missing_from_fleet_state",
		Suggestion:    fleetMemberSuggestion("control_member_missing_from_fleet_state"),
	}
	if control.NodeID != 0 {
		peer.Hostname = control.Hostname
		peer.Profile = control.Profile
		peer.Transport = control.Transport
		peer.Trust = control.Trust
		peer.Route = control.Route
		peer.RTTMS = control.RTTMS
		peer.Error = control.Error
		peer.NextCommand = control.NextCommand
	}
	return peer
}

func fleetMemberDiagnosis(member esphttp.FleetMemberRecord, inControlRoster bool) string {
	switch member.Status {
	case esphttp.FleetMemberInvited:
		if inControlRoster {
			return "pending_but_in_control_group"
		}
		return "pending_invite"
	case esphttp.FleetMemberRemoved:
		if inControlRoster {
			return "removed_still_in_control_group"
		}
		return "removed"
	default:
		if !inControlRoster {
			return "active_missing_from_control_group"
		}
		return "ok"
	}
}

func fleetMembershipMismatch(diagnosis string) bool {
	switch diagnosis {
	case "active_missing_from_control_group", "pending_but_in_control_group", "removed_still_in_control_group", "control_member_missing_from_fleet_state":
		return true
	default:
		return false
	}
}

func fleetMemberSuggestion(diagnosis string) string {
	switch diagnosis {
	case "active_missing_from_control_group":
		return "active Fleet member is missing from the hidden control-group roster"
	case "pending_but_in_control_group":
		return "pending Fleet invite already appears in the control-group roster; refresh Fleet state"
	case "removed_still_in_control_group":
		return "removed Fleet member still appears in the control-group roster; remove it from the control group"
	case "control_member_missing_from_fleet_state":
		return "control-group roster contains a member missing from Fleet state; reconcile the Fleet member table"
	default:
		return ""
	}
}

func fleetDiagnosticsIdentityKey(nodeID entmoot.NodeID, entmootPubKey string) string {
	return strconv.FormatUint(uint64(nodeID), 10) + ":" + strings.TrimSpace(entmootPubKey)
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
	if groupFleetControl(meta) {
		return false
	}
	if !opts.IncludeHidden && groupHidden(meta) {
		return false
	}
	return true
}

func groupHidden(meta map[string]interface{}) bool {
	if meta == nil {
		return false
	}
	if hidden, ok := meta["hidden"].(bool); ok && hidden {
		return true
	}
	return groupFleetControl(meta)
}

func groupFleetControl(meta map[string]interface{}) bool {
	if meta == nil {
		return false
	}
	if hidden, ok := meta["fleet_control"].(bool); ok && hidden {
		return true
	}
	return false
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
	if c.state != nil {
		configs, err := c.state.ListLiveAgentConfigs(ctx, gid)
		if err != nil {
			return nil, err
		}
		presences, err := c.state.ListLiveAgentPresence(ctx, gid)
		if err != nil {
			return nil, err
		}
		liveByNode := esphttp.LiveAgentStatesByNode(configs, presences, time.Now().UnixMilli())
		for i := range out {
			if live, ok := liveByNode[out[i].NodeID]; ok {
				state := live
				out[i].Live = &state
			}
		}
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
