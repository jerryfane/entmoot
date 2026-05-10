package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
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

type fleetCommandExecution struct {
	status  string
	summary string
	output  string
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
	if !ok {
		r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusRejected, "Command rejected: unsupported action", "", 0)
		return
	}
	if cmd.Action == esphttp.FleetCommandActionAgentInstruction {
		if cmd.AutoAccept {
			r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusRejected, "Command rejected: agent instructions require local opt-in", "", 0)
			return
		}
	} else if !entry.AutoAcceptSafe || !cmd.AutoAccept {
		r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusRejected, "Command rejected: action is not auto-accepted", "", 0)
		return
	}
	started := time.Now().UnixMilli()
	r.publishResult(ctx, msg.GroupID, cmd, esphttp.FleetCommandStatusAccepted, "Command accepted", "", started)
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(entry.TimeoutMS)*time.Millisecond)
	defer cancel()
	execResult, err := r.execute(execCtx, commandCtx, cmd)
	if err != nil {
		execResult = fleetCommandExecution{
			status:  esphttp.FleetCommandStatusFailed,
			summary: "Command failed: " + err.Error(),
		}
	}
	if execResult.status == "" {
		execResult.status = esphttp.FleetCommandStatusCompleted
	}
	if execResult.summary == "" {
		execResult.summary = "Command completed"
	}
	r.publishResult(ctx, msg.GroupID, cmd, execResult.status, execResult.summary, truncateCommandOutput(execResult.output, entry.MaxOutputBytes), started)
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

func (r *fleetCommandRunner) execute(ctx context.Context, commandCtx fleetCommandContext, cmd esphttp.FleetCommandEnvelope) (fleetCommandExecution, error) {
	select {
	case <-ctx.Done():
		return fleetCommandExecution{}, ctx.Err()
	default:
	}
	switch cmd.Action {
	case esphttp.FleetCommandActionEcho:
		if v, ok := cmd.Args["message"].(string); ok {
			return fleetCommandExecution{output: v}, nil
		}
		return fleetCommandExecution{output: "ok"}, nil
	case esphttp.FleetCommandActionEntmootVersion:
		return marshalCommandExecution(map[string]any{"version": version, "commit": commit, "date": date})
	case esphttp.FleetCommandActionEntmootInfo:
		return marshalCommandExecution(r.localInfo())
	case esphttp.FleetCommandActionEntmootDoctor:
		return marshalCommandExecution(r.localFleetState(ctx, commandCtx))
	case esphttp.FleetCommandActionPilotInfo:
		info, err := r.server.pilot.Info(ctx)
		if err != nil {
			return fleetCommandExecution{}, err
		}
		return marshalCommandExecution(redactCommandMap(info))
	case esphttp.FleetCommandActionFleetLocalState:
		return marshalCommandExecution(r.localFleetState(ctx, commandCtx))
	case esphttp.FleetCommandActionAgentInstruction:
		return r.dispatchAgentInstruction(ctx, commandCtx, cmd)
	default:
		return fleetCommandExecution{}, fmt.Errorf("unsupported action")
	}
}

func marshalCommandExecution(v any) (fleetCommandExecution, error) {
	output, err := marshalCommandOutput(v)
	if err != nil {
		return fleetCommandExecution{}, err
	}
	return fleetCommandExecution{output: output}, nil
}

func (r *fleetCommandRunner) dispatchAgentInstruction(ctx context.Context, commandCtx fleetCommandContext, cmd esphttp.FleetCommandEnvelope) (fleetCommandExecution, error) {
	if !envBool(os.Getenv("ENTMOOT_AGENT_INSTRUCTIONS")) {
		return fleetCommandExecution{
			status:  esphttp.FleetCommandStatusRejected,
			summary: "Agent instruction rejected: local agent instructions are not enabled",
		}, nil
	}
	instruction, timeoutMS, instructionContext, err := esphttp.FleetCommandInstructionArgs(cmd.Args)
	if err != nil {
		return fleetCommandExecution{}, err
	}
	receivedAt := time.Now().UnixMilli()
	payload := esphttp.NewAgentInstructionPayload(cmd, commandCtx.local.NodeID, instruction, instructionContext, timeoutMS, receivedAt)
	_, created, err := r.state.EnqueueAgentCommand(ctx, payload)
	if err != nil {
		return fleetCommandExecution{}, err
	}
	if !created {
		return fleetCommandExecution{
			status:  esphttp.FleetCommandStatusDuplicate,
			summary: "Agent instruction already queued for local agent runtime",
			output:  `{"queued":true}`,
		}, nil
	}
	return fleetCommandExecution{
		status:  esphttp.FleetCommandStatusRunning,
		summary: "Queued for local agent runtime",
		output:  `{"queued":true}`,
	}, nil
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
		Type:        esphttp.FleetCommandResultType,
		Version:     1,
		CommandID:   cmd.CommandID,
		FleetID:     cmd.FleetID,
		AgentNodeID: r.server.nodeID,
		Action:      cmd.Action,
		Status:      status,
		Summary:     summary,
		Output:      output,
		StartedAtMS: startedAtMS,
	}
	if fleetCommandStatusIsTerminal(status) {
		result.CompletedAtMS = time.Now().UnixMilli()
	}
	body, err := json.Marshal(result)
	if err != nil {
		return
	}
	if _, frame := r.server.publishLocalMessage(ctx, groupID, []string{"fleet/commands/results"}, body); frame != nil {
		r.logger.Warn("fleet command: publish result failed", slog.String("command_id", cmd.CommandID), slog.String("err", frame.Message))
	}
}

func fleetCommandStatusIsTerminal(status string) bool {
	switch esphttp.NormalizeFleetCommandResultStatus(status) {
	case esphttp.FleetCommandStatusCompleted,
		esphttp.FleetCommandStatusFailed,
		esphttp.FleetCommandStatusRejected,
		esphttp.FleetCommandStatusExpired,
		esphttp.FleetCommandStatusDuplicate:
		return true
	default:
		return false
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

func envBool(v string) bool {
	switch strings.TrimSpace(strings.ToLower(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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
