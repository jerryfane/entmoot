package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
)

type fleetCommandRunner struct {
	server *ipcServer
	state  *esphttp.SQLiteStateStore
	notify *notifyingStore
	logger *slog.Logger

	mu        sync.Mutex
	processed map[string]struct{}
}

type fleetCommandContext struct {
	fleet       esphttp.FleetRecord
	local       esphttp.FleetMemberRecord
	memberCount int
	source      string
}

func newFleetCommandRunner(server *ipcServer, state *esphttp.SQLiteStateStore, notify *notifyingStore, logger *slog.Logger) *fleetCommandRunner {
	if logger == nil {
		logger = slog.Default()
	}
	return &fleetCommandRunner{
		server:    server,
		state:     state,
		notify:    notify,
		logger:    logger,
		processed: make(map[string]struct{}),
	}
}

func (r *fleetCommandRunner) run(ctx context.Context) {
	if r == nil || r.server == nil || r.state == nil || r.notify == nil {
		return
	}
	ch := make(chan entmoot.Message, 64)
	unsub := r.notify.subscribe(ch)
	defer unsub()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	r.replayExisting(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			r.handleMessage(ctx, msg)
		case <-ticker.C:
			r.replayExisting(ctx)
		}
	}
}

func (r *fleetCommandRunner) replayExisting(ctx context.Context) {
	if r.server == nil || r.server.store == nil || r.server.runtime == nil {
		return
	}
	for _, gid := range r.server.runtime.ActiveGroupIDs() {
		msgs, err := r.server.store.LatestByTopic(ctx, gid, "fleet/commands", 200)
		if err != nil {
			r.logger.Warn("fleet command: replay failed", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
			continue
		}
		for _, msg := range msgs {
			if ctx.Err() != nil {
				return
			}
			r.handleMessage(ctx, msg)
		}
	}
}

func (r *fleetCommandRunner) handleMessage(ctx context.Context, msg entmoot.Message) {
	if !messageHasTopic(msg, "fleet/commands") {
		return
	}
	var cmd esphttp.FleetCommandEnvelope
	if err := json.Unmarshal(msg.Content, &cmd); err != nil || cmd.Type != esphttp.FleetCommandMessageType {
		return
	}
	if cmd.CommandID == "" {
		return
	}
	r.processCommand(ctx, msg, cmd)
}

func (r *fleetCommandRunner) markCommand(commandID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.processed[commandID]; ok {
		return false
	}
	r.processed[commandID] = struct{}{}
	return true
}

func (r *fleetCommandRunner) processCommand(ctx context.Context, msg entmoot.Message, cmd esphttp.FleetCommandEnvelope) {
	commandCtx, ok, err := r.commandContextForCommand(ctx, msg.GroupID, cmd)
	if err != nil {
		r.logger.Warn("fleet command: fleet lookup failed", slog.String("command_id", cmd.CommandID), slog.String("err", err.Error()))
		return
	}
	if !ok {
		return
	}
	if !esphttp.FleetTaskCanMutate(commandCtx.local) {
		return
	}
	if !fleetCommandTargetsLocal(cmd, commandCtx.local) {
		return
	}
	if commandCtx.local.Role == esphttp.FleetRoleCoordinator && cmd.Target.Kind == esphttp.FleetCommandTargetAll {
		return
	}
	if !r.markCommand(cmd.CommandID) {
		return
	}
	if cmd.ExpiresAtMS > 0 && cmd.ExpiresAtMS <= time.Now().UnixMilli() {
		r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusExpired, "Command expired", "", 0)
		return
	}
	if !fleetCommandIssuedByCoordinator(msg, cmd, commandCtx.fleet) {
		r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusRejected, "Command rejected: issuer is not the Fleet coordinator", "", 0)
		return
	}
	entry, ok := esphttp.FleetCommandCatalogLookup(cmd.Action)
	if !ok || !entry.AutoAcceptSafe || !cmd.AutoAccept {
		r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusRejected, "Command rejected: action is not auto-accepted", "", 0)
		return
	}
	started := time.Now().UnixMilli()
	r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusAccepted, "Command accepted", "", started)
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(entry.TimeoutMS)*time.Millisecond)
	defer cancel()
	output, err := r.execute(execCtx, commandCtx, cmd)
	status := esphttp.FleetCommandStatusCompleted
	summary := "Command completed"
	if err != nil {
		status = esphttp.FleetCommandStatusFailed
		summary = "Command failed: " + err.Error()
	}
	r.publishResult(ctx, msg.GroupID, cmd, status, summary, truncateCommandOutput(output, entry.MaxOutputBytes), started)
}

func (r *fleetCommandRunner) commandContextForCommand(ctx context.Context, groupID entmoot.GroupID, cmd esphttp.FleetCommandEnvelope) (fleetCommandContext, bool, error) {
	if cmd.FleetID == "" || cmd.ControlGroupID != groupID {
		return fleetCommandContext{}, false, nil
	}
	fleets, err := r.state.ListFleets(ctx)
	if err != nil {
		return fleetCommandContext{}, false, err
	}
	for _, fleet := range fleets {
		if fleet.ControlGroupID != groupID || fleet.FleetID != cmd.FleetID {
			continue
		}
		if fleet.Status != "" && fleet.Status != esphttp.FleetStatusActive {
			return fleetCommandContext{}, false, nil
		}
		members, err := r.state.ListFleetMembers(ctx, fleet.FleetID)
		if err != nil {
			return fleetCommandContext{}, false, err
		}
		local, ok := fleetCommandMemberForNode(members, r.server.nodeID)
		if !ok {
			return fleetCommandContext{}, false, nil
		}
		return fleetCommandContext{
			fleet:       fleet,
			local:       local,
			memberCount: len(members),
			source:      "fleet_state",
		}, true, nil
	}
	return r.commandContextFromControlRoster(ctx, groupID, cmd)
}

func (r *fleetCommandRunner) commandContextFromControlRoster(ctx context.Context, groupID entmoot.GroupID, cmd esphttp.FleetCommandEnvelope) (fleetCommandContext, bool, error) {
	if r.server == nil || r.server.identity == nil || r.server.dataDir == "" {
		return fleetCommandContext{}, false, nil
	}
	ok, err := r.groupMetadataMatchesFleet(ctx, groupID, cmd.FleetID)
	if err != nil || !ok {
		return fleetCommandContext{}, false, err
	}
	rlog, ok, err := openExistingRosterLog(r.server.dataDir, groupID)
	if err != nil || !ok {
		return fleetCommandContext{}, false, err
	}
	defer rlog.Close()
	localInfo, ok := rlog.MemberInfo(r.server.nodeID)
	if !ok || !bytes.Equal(localInfo.EntmootPubKey, r.server.identity.PublicKey) {
		return fleetCommandContext{}, false, nil
	}
	founder, ok := rlog.Founder()
	if !ok {
		return fleetCommandContext{}, false, nil
	}
	role := esphttp.FleetRoleAgent
	if founder.PilotNodeID == localInfo.PilotNodeID && bytes.Equal(founder.EntmootPubKey, localInfo.EntmootPubKey) {
		role = esphttp.FleetRoleCoordinator
	}
	return fleetCommandContext{
		fleet: esphttp.FleetRecord{
			FleetID:        cmd.FleetID,
			ControlGroupID: groupID,
			Coordinator:    founder,
			Status:         esphttp.FleetStatusActive,
		},
		local: esphttp.FleetMemberRecord{
			FleetID:       cmd.FleetID,
			NodeID:        localInfo.PilotNodeID,
			EntmootPubKey: encodeBase64(localInfo.EntmootPubKey),
			Role:          role,
			Status:        esphttp.FleetMemberActive,
		},
		memberCount: len(rlog.Members()),
		source:      "control_roster",
	}, true, nil
}

func (r *fleetCommandRunner) groupMetadataMatchesFleet(ctx context.Context, groupID entmoot.GroupID, fleetID string) (bool, error) {
	raw, ok, err := r.state.GetGroupMetadata(ctx, groupID)
	if err != nil || !ok {
		return false, err
	}
	return fleetControlMetadataMatches(raw, fleetID), nil
}

func (r *fleetCommandRunner) execute(ctx context.Context, commandCtx fleetCommandContext, cmd esphttp.FleetCommandEnvelope) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	switch cmd.Action {
	case esphttp.FleetCommandActionEcho:
		if v, ok := cmd.Args["message"].(string); ok {
			return v, nil
		}
		return "ok", nil
	case esphttp.FleetCommandActionEntmootVersion:
		return marshalCommandOutput(map[string]any{"version": version, "commit": commit, "date": date})
	case esphttp.FleetCommandActionEntmootInfo:
		return marshalCommandOutput(r.localInfo())
	case esphttp.FleetCommandActionEntmootDoctor:
		return marshalCommandOutput(r.localFleetState(ctx, commandCtx))
	case esphttp.FleetCommandActionPilotInfo:
		info, err := r.server.pilot.Info(ctx)
		if err != nil {
			return "", err
		}
		return marshalCommandOutput(redactCommandMap(info))
	case esphttp.FleetCommandActionFleetLocalState:
		return marshalCommandOutput(r.localFleetState(ctx, commandCtx))
	default:
		return "", fmt.Errorf("unsupported action")
	}
}

func (r *fleetCommandRunner) localInfo() map[string]any {
	groups := r.server.runtime.ActiveGroupIDs()
	out := make([]string, 0, len(groups))
	for _, gid := range groups {
		out = append(out, gid.String())
	}
	return map[string]any{
		"pilot_node_id": r.server.nodeID,
		"data_dir":      r.server.dataDir,
		"groups":        out,
		"running":       true,
	}
}

func (r *fleetCommandRunner) localFleetState(ctx context.Context, commandCtx fleetCommandContext) map[string]any {
	memberCount := commandCtx.memberCount
	source := commandCtx.source
	if commandCtx.source == "fleet_state" {
		members, err := r.state.ListFleetMembers(ctx, commandCtx.fleet.FleetID)
		if err == nil {
			memberCount = len(members)
		}
	}
	return map[string]any{
		"fleet_id":         commandCtx.fleet.FleetID,
		"control_group_id": commandCtx.fleet.ControlGroupID,
		"local_node_id":    r.server.nodeID,
		"members":          memberCount,
		"source":           source,
	}
}

func (r *fleetCommandRunner) publishResult(ctx context.Context, groupID entmoot.GroupID, cmd esphttp.FleetCommandEnvelope, status, summary, output string, startedAtMS int64) {
	result := esphttp.FleetCommandResultEnvelope{
		Type:          esphttp.FleetCommandResultType,
		Version:       1,
		CommandID:     cmd.CommandID,
		FleetID:       cmd.FleetID,
		AgentNodeID:   r.server.nodeID,
		Action:        cmd.Action,
		Status:        status,
		Summary:       summary,
		Output:        output,
		StartedAtMS:   startedAtMS,
		CompletedAtMS: time.Now().UnixMilli(),
	}
	body, err := json.Marshal(result)
	if err != nil {
		return
	}
	if _, frame := r.server.publishLocalMessage(ctx, groupID, []string{"fleet/commands/results"}, body); frame != nil {
		r.logger.Warn("fleet command: publish result failed", slog.String("command_id", cmd.CommandID), slog.String("err", frame.Message))
	}
}

func fleetCommandMemberForNode(members []esphttp.FleetMemberRecord, nodeID entmoot.NodeID) (esphttp.FleetMemberRecord, bool) {
	for _, member := range members {
		if member.NodeID == nodeID {
			return member, true
		}
	}
	return esphttp.FleetMemberRecord{}, false
}

func fleetCommandTargetsLocal(cmd esphttp.FleetCommandEnvelope, local esphttp.FleetMemberRecord) bool {
	switch esphttp.NormalizeFleetCommandTarget(cmd.Target.Kind) {
	case esphttp.FleetCommandTargetAll:
		return true
	case esphttp.FleetCommandTargetNode:
		return cmd.Target.PilotNodeID == local.NodeID
	default:
		return false
	}
}

func fleetCommandIssuedByCoordinator(msg entmoot.Message, cmd esphttp.FleetCommandEnvelope, fleet esphttp.FleetRecord) bool {
	if cmd.IssuerNodeID != fleet.Coordinator.PilotNodeID {
		return false
	}
	if msg.Author.PilotNodeID == fleet.Coordinator.PilotNodeID &&
		base64.StdEncoding.EncodeToString(msg.Author.EntmootPubKey) == base64.StdEncoding.EncodeToString(fleet.Coordinator.EntmootPubKey) {
		return true
	}
	return esphttp.VerifyFleetCommandIssuerProof(cmd, fleet.Coordinator.EntmootPubKey)
}

func messageHasTopic(msg entmoot.Message, want string) bool {
	for _, topic := range msg.Topics {
		if topic == want {
			return true
		}
	}
	return false
}

func marshalCommandOutput(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func truncateCommandOutput(output string, limit int) string {
	if limit <= 0 || len(output) <= limit {
		return output
	}
	return output[:limit] + "\n... truncated"
}

func redactCommandMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		lower := strings.ToLower(k)
		if strings.Contains(lower, "token") || strings.Contains(lower, "secret") || strings.Contains(lower, "key") {
			out[k] = "[redacted]"
			continue
		}
		out[k] = v
	}
	return out
}
