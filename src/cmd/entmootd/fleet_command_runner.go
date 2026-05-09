package main

import (
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
	if !r.markCommand(cmd.CommandID) {
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
	fleet, ok, err := r.fleetForCommand(ctx, msg.GroupID, cmd)
	if err != nil {
		r.logger.Warn("fleet command: fleet lookup failed", slog.String("command_id", cmd.CommandID), slog.String("err", err.Error()))
		return
	}
	if !ok {
		return
	}
	members, err := r.state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		r.logger.Warn("fleet command: member lookup failed", slog.String("fleet_id", fleet.FleetID), slog.String("err", err.Error()))
		return
	}
	local, ok := fleetCommandMemberForNode(members, r.server.nodeID)
	if !ok || !esphttp.FleetTaskCanMutate(local) {
		return
	}
	if !fleetCommandTargetsLocal(cmd, local) {
		return
	}
	if local.Role == esphttp.FleetRoleCoordinator && cmd.Target.Kind == esphttp.FleetCommandTargetAll {
		return
	}
	if cmd.ExpiresAtMS > 0 && cmd.ExpiresAtMS <= time.Now().UnixMilli() {
		r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusExpired, "Command expired", "", 0)
		return
	}
	if !fleetCommandIssuedByCoordinator(msg, cmd, fleet) {
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
	output, err := r.execute(execCtx, fleet, cmd)
	status := esphttp.FleetCommandStatusCompleted
	summary := "Command completed"
	if err != nil {
		status = esphttp.FleetCommandStatusFailed
		summary = "Command failed: " + err.Error()
	}
	r.publishResult(ctx, msg.GroupID, cmd, status, summary, truncateCommandOutput(output, entry.MaxOutputBytes), started)
}

func (r *fleetCommandRunner) fleetForCommand(ctx context.Context, groupID entmoot.GroupID, cmd esphttp.FleetCommandEnvelope) (esphttp.FleetRecord, bool, error) {
	fleets, err := r.state.ListFleets(ctx)
	if err != nil {
		return esphttp.FleetRecord{}, false, err
	}
	for _, fleet := range fleets {
		if fleet.Status == esphttp.FleetStatusActive &&
			fleet.ControlGroupID == groupID &&
			fleet.FleetID == cmd.FleetID &&
			cmd.ControlGroupID == groupID {
			return fleet, true, nil
		}
	}
	return esphttp.FleetRecord{}, false, nil
}

func (r *fleetCommandRunner) execute(ctx context.Context, fleet esphttp.FleetRecord, cmd esphttp.FleetCommandEnvelope) (string, error) {
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
		return marshalCommandOutput(r.localFleetState(ctx, fleet))
	case esphttp.FleetCommandActionPilotInfo:
		info, err := r.server.pilot.Info(ctx)
		if err != nil {
			return "", err
		}
		return marshalCommandOutput(redactCommandMap(info))
	case esphttp.FleetCommandActionFleetLocalState:
		return marshalCommandOutput(r.localFleetState(ctx, fleet))
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

func (r *fleetCommandRunner) localFleetState(ctx context.Context, fleet esphttp.FleetRecord) map[string]any {
	members, _ := r.state.ListFleetMembers(ctx, fleet.FleetID)
	return map[string]any{
		"fleet_id":         fleet.FleetID,
		"control_group_id": fleet.ControlGroupID,
		"local_node_id":    r.server.nodeID,
		"members":          len(members),
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
