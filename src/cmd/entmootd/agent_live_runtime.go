package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/store"
)

const (
	liveActionReply            = "reply"
	liveActionMessageSummarize = "message.summarize"
	liveActionTaskCreate       = "task.create"
	liveActionCommandSend      = "command.send"
	liveActionAlertOwner       = "alert.owner"
	liveCursorMaxSeenIDs       = 512
	liveCursorOverlapWindow    = 10 * time.Minute
)

var errLiveActionTransport = errors.New("live action transport")

type agentLiveRuntimeConfig struct {
	groupID entmoot.GroupID
	nodeID  entmoot.NodeID
	runner  string
	timeout time.Duration
	limit   int
}

type agentLiveScanResult struct {
	Seen     int `json:"seen"`
	Matched  int `json:"matched"`
	Proposed int `json:"proposed"`
	Applied  int `json:"applied"`
	Rejected int `json:"rejected"`
}

type liveAgentRunnerContext struct {
	GroupID        entmoot.GroupID          `json:"group_id"`
	NodeID         entmoot.NodeID           `json:"node_id"`
	Mode           string                   `json:"mode"`
	TopicFilters   []string                 `json:"topic_filters"`
	AllowedActions []string                 `json:"allowed_actions,omitempty"`
	Trigger        string                   `json:"trigger"`
	Events         []liveAgentRunnerMessage `json:"events"`
	Instructions   string                   `json:"instructions"`
}

type liveAgentRunnerMessage struct {
	MessageID   entmoot.MessageID `json:"message_id"`
	AuthorNode  entmoot.NodeID    `json:"author_node"`
	Topics      []string          `json:"topics"`
	Content     string            `json:"content"`
	TimestampMS int64             `json:"timestamp_ms"`
}

type liveAgentRunnerOutput struct {
	Actions []liveAgentAction `json:"actions"`
}

type liveAgentAction struct {
	Kind           string                 `json:"kind"`
	Action         string                 `json:"action,omitempty"`
	Message        string                 `json:"message,omitempty"`
	Title          string                 `json:"title,omitempty"`
	Content        string                 `json:"content,omitempty"`
	Description    string                 `json:"description,omitempty"`
	Topic          string                 `json:"topic,omitempty"`
	FleetID        string                 `json:"fleet_id,omitempty"`
	Mode           string                 `json:"mode,omitempty"`
	AssigneeNodeID uint64                 `json:"assignee_node_id,omitempty"`
	Target         string                 `json:"target,omitempty"`
	TargetNodeID   uint64                 `json:"target_node_id,omitempty"`
	Args           map[string]interface{} `json:"args,omitempty"`
	Instruction    string                 `json:"instruction,omitempty"`
	TimeoutMS      int64                  `json:"timeout_ms,omitempty"`
	ExpiresAtMS    int64                  `json:"expires_at_ms,omitempty"`
	AutoAccept     *bool                  `json:"auto_accept,omitempty"`
}

func runAgentLiveScan(ctx context.Context, gf *globalFlags, state esphttp.StateStore, msgStore store.MessageStore, cfg esphttp.LiveAgentConfig, runCfg agentLiveRuntimeConfig) (agentLiveScanResult, error) {
	var result agentLiveScanResult
	cursor, ok, err := state.GetLiveAgentCursor(ctx, cfg.GroupID, cfg.NodeID)
	if err != nil {
		return result, err
	}
	if !ok {
		cursor = esphttp.LiveAgentCursor{
			GroupID:       cfg.GroupID,
			NodeID:        cfg.NodeID,
			ScanFloorAtMS: cfg.UpdatedAtMS,
			LastSeenAtMS:  cfg.UpdatedAtMS,
		}
	}
	scanFloor := liveScanFloor(cfg, cursor, ok)
	if cursor.ScanFloorAtMS <= 0 {
		cursor.ScanFloorAtMS = scanFloor
	}
	msgs, err := msgStore.Range(ctx, cfg.GroupID, scanFloor, 0)
	if err != nil {
		return result, err
	}
	if len(msgs) == 0 {
		return result, nil
	}
	sort.SliceStable(msgs, func(i, j int) bool {
		if msgs[i].Timestamp == msgs[j].Timestamp {
			if msgs[i].Author.PilotNodeID == msgs[j].Author.PilotNodeID {
				return bytes.Compare(msgs[i].ID[:], msgs[j].ID[:]) < 0
			}
			return msgs[i].Author.PilotNodeID < msgs[j].Author.PilotNodeID
		}
		return msgs[i].Timestamp < msgs[j].Timestamp
	})
	seen := liveSeenMessageSet(cursor.SeenMessageIDs)
	nextCursor := cursor
	events := make([]liveAgentRunnerMessage, 0)
	for _, msg := range msgs {
		if _, ok := seen[msg.ID]; ok {
			continue
		}
		if !liveMessageKeyAfterCursor(msg, cursor) && liveReplayWindowDisabled(cursor) {
			continue
		}
		result.Seen++
		msgCursor := advanceLiveCursorWithMessage(nextCursor, msg, seen)
		if msg.Author.PilotNodeID == cfg.NodeID {
			nextCursor = msgCursor
			continue
		}
		if !liveMessageMatchesTopics(cfg.TopicFilters, msg.Topics) {
			nextCursor = msgCursor
			continue
		}
		if cfg.Mode == esphttp.LiveModeReplyOnMention && !liveMessageMentionsAgent(msg, cfg.NodeID) {
			nextCursor = msgCursor
			continue
		}
		result.Matched++
		nextCursor = msgCursor
		if cfg.Mode == esphttp.LiveModeListen {
			continue
		}
		events = append(events, liveRunnerMessage(msg))
		if cfg.Mode != esphttp.LiveModeListen && runCfg.limit > 0 && len(events) >= runCfg.limit {
			break
		}
	}
	if cfg.Mode == esphttp.LiveModeListen || len(events) == 0 {
		if !liveCursorsEqual(nextCursor, cursor) {
			if err := persistLiveCursor(ctx, state, nextCursor); err != nil {
				return result, err
			}
		}
		return result, nil
	}
	if strings.TrimSpace(runCfg.runner) == "" {
		return result, fmt.Errorf("live mode matched %d event(s), but -runner or ENTMOOT_AGENT_RUNNER is not configured", len(events))
	}
	runnerCtx := liveAgentRunnerContext{
		GroupID:        cfg.GroupID,
		NodeID:         cfg.NodeID,
		Mode:           cfg.Mode,
		TopicFilters:   append([]string(nil), cfg.TopicFilters...),
		AllowedActions: append([]string(nil), cfg.AllowedActions...),
		Trigger:        liveTriggerForMode(cfg.Mode),
		Events:         events,
		Instructions:   "Return JSON only: {\"actions\":[{\"kind\":\"reply\",\"message\":\"...\"}]}. Supported local actions include reply, message.summarize, alert.owner, task.create with title, description, mode, fleet_id, and assignee_node_id, and command.send with action, args, target, target_node_id, instruction, timeout_ms, expires_at_ms, and auto_accept. Entmoot will validate and apply allowed actions. Do not claim that you posted anything yourself.",
	}
	output, err := runLiveAgentRunner(ctx, runCfg, runnerCtx)
	if err != nil {
		return result, err
	}
	result.Proposed = len(output.Actions)
	actions := output.Actions
	if cfg.MaxActionsPerScan > 0 && len(actions) > cfg.MaxActionsPerScan {
		result.Rejected += len(actions) - cfg.MaxActionsPerScan
		actions = actions[:cfg.MaxActionsPerScan]
	}
	for _, action := range actions {
		applied, err := applyLiveAgentAction(ctx, gf, state, cfg, events, action)
		if err != nil {
			if errors.Is(err, errLiveActionTransport) {
				if result.Applied > 0 && !liveCursorsEqual(nextCursor, cursor) {
					if persistErr := persistLiveCursor(ctx, state, nextCursor); persistErr != nil {
						return result, persistErr
					}
				}
				return result, err
			}
			result.Rejected++
			continue
		}
		if applied {
			result.Applied++
		} else {
			result.Rejected++
		}
	}
	if !liveCursorsEqual(nextCursor, cursor) {
		if err := persistLiveCursor(ctx, state, nextCursor); err != nil {
			return result, err
		}
	}
	return result, nil
}

func liveScanFloor(cfg esphttp.LiveAgentConfig, cursor esphttp.LiveAgentCursor, found bool) int64 {
	if !found || cursor.LastSeenAtMS <= 0 {
		return cfg.UpdatedAtMS
	}
	if len(cursor.SeenMessageIDs) >= liveCursorMaxSeenIDs {
		return cursor.LastSeenAtMS
	}
	floor := cursor.LastSeenAtMS - liveCursorOverlapWindow.Milliseconds()
	if floor < cfg.UpdatedAtMS {
		return cfg.UpdatedAtMS
	}
	if cursor.ScanFloorAtMS > floor {
		return cursor.ScanFloorAtMS
	}
	return floor
}

func persistLiveCursor(ctx context.Context, state esphttp.StateStore, cursor esphttp.LiveAgentCursor) error {
	cursor.UpdatedAtMS = time.Now().UnixMilli()
	if len(cursor.SeenMessageIDs) > liveCursorMaxSeenIDs {
		cursor.SeenMessageIDs = append([]entmoot.MessageID(nil), cursor.SeenMessageIDs[len(cursor.SeenMessageIDs)-liveCursorMaxSeenIDs:]...)
		if cursor.ScanFloorAtMS < cursor.LastSeenAtMS {
			cursor.ScanFloorAtMS = cursor.LastSeenAtMS
		}
	}
	_, err := state.UpsertLiveAgentCursor(ctx, cursor)
	return err
}

func runLiveAgentRunner(ctx context.Context, cfg agentLiveRuntimeConfig, liveCtx liveAgentRunnerContext) (liveAgentRunnerOutput, error) {
	runnerCtx := ctx
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		runnerCtx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}
	runner := strings.TrimSpace(cfg.runner)
	data, err := json.Marshal(liveCtx)
	if err != nil {
		return liveAgentRunnerOutput{}, err
	}
	if strings.EqualFold(runner, agentCommandRunnerOpenClaw) {
		selectorFlag, selectorValue := openClawAgentSelector()
		args := []string{
			"agent",
			selectorFlag, selectorValue,
			"--message", "Entmoot live interaction context JSON:\n" + string(data),
			"--json",
			"--timeout", strconv.Itoa(agentLiveTimeoutSeconds(cfg.timeout)),
		}
		cmd := exec.CommandContext(runnerCtx, openClawBinary(), args...)
		run := runLiveRuntimeProcess(cmd, nil, liveCtx)
		if runnerCtx.Err() != nil {
			return liveAgentRunnerOutput{}, fmt.Errorf("OpenClaw live interaction timed out: %s", strings.TrimSpace(run.stderr))
		}
		if run.err != nil {
			output := strings.TrimSpace(run.stderr)
			if output == "" {
				output = run.err.Error()
			}
			return liveAgentRunnerOutput{}, fmt.Errorf("OpenClaw live interaction failed: %s", addAgentRuntimeFailureAdvice(output))
		}
		return parseLiveRunnerOutput(openClawLiveFinalText(run.stdout))
	}
	cmd := exec.CommandContext(runnerCtx, runner)
	run := runLiveRuntimeProcess(cmd, data, liveCtx)
	if runnerCtx.Err() != nil {
		return liveAgentRunnerOutput{}, fmt.Errorf("live agent runtime timed out: %s", strings.TrimSpace(run.stderr))
	}
	if run.err != nil {
		output := strings.TrimSpace(run.stderr)
		if output == "" {
			output = run.err.Error()
		}
		return liveAgentRunnerOutput{}, fmt.Errorf("live agent runtime failed: %s", addAgentRuntimeFailureAdvice(output))
	}
	return parseLiveRunnerOutput(run.stdout)
}

func runLiveRuntimeProcess(cmd *exec.Cmd, stdin []byte, liveCtx liveAgentRunnerContext) agentRuntimeProcessResult {
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	cmd.Env = append(os.Environ(),
		"ENTMOOT_LIVE_GROUP_ID="+liveCtx.GroupID.String(),
		"ENTMOOT_LIVE_NODE_ID="+fmt.Sprintf("%d", liveCtx.NodeID),
		"ENTMOOT_LIVE_MODE="+liveCtx.Mode,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return agentRuntimeProcessResult{stdout: stdout.String(), stderr: stderr.String(), err: err}
}

func parseLiveRunnerOutput(stdout string) (liveAgentRunnerOutput, error) {
	raw := bytes.TrimSpace([]byte(stdout))
	if len(raw) == 0 {
		return liveAgentRunnerOutput{}, nil
	}
	var output liveAgentRunnerOutput
	if err := json.Unmarshal(raw, &output); err == nil {
		return output, nil
	}
	start := bytes.IndexByte(raw, '{')
	end := bytes.LastIndexByte(raw, '}')
	if start >= 0 && end > start {
		if err := json.Unmarshal(raw[start:end+1], &output); err == nil {
			return output, nil
		}
	}
	return liveAgentRunnerOutput{}, fmt.Errorf("live agent runtime returned invalid JSON")
}

func openClawLiveFinalText(stdout string) string {
	var report openClawAgentRunReport
	if err := json.Unmarshal([]byte(stdout), &report); err == nil {
		if strings.TrimSpace(report.Meta.FinalAssistantVisibleText) != "" {
			return report.Meta.FinalAssistantVisibleText
		}
		if strings.TrimSpace(report.Meta.FinalAssistantRawText) != "" {
			return report.Meta.FinalAssistantRawText
		}
	}
	return stdout
}

func applyLiveAgentAction(ctx context.Context, gf *globalFlags, state esphttp.StateStore, cfg esphttp.LiveAgentConfig, events []liveAgentRunnerMessage, action liveAgentAction) (bool, error) {
	kind := strings.TrimSpace(strings.ToLower(action.Kind))
	if !liveActionAllowed(cfg, kind) {
		return false, fmt.Errorf("live action %q is not allowed", kind)
	}
	switch kind {
	case liveActionReply, liveActionMessageSummarize, liveActionAlertOwner:
		message := strings.TrimSpace(firstNonEmpty(action.Message, action.Content, action.Title))
		if message == "" {
			return false, fmt.Errorf("live action %q has empty message", kind)
		}
		if cfg.MaxActionBytes > 0 && len([]byte(message)) > cfg.MaxActionBytes {
			return false, fmt.Errorf("live action %q message exceeds max_action_bytes", kind)
		}
		topic := strings.TrimSpace(action.Topic)
		if kind == liveActionAlertOwner && topic == "" {
			topic = "alerts/owner"
		}
		if topic == "" {
			topic = firstMatchedEventTopic(cfg.TopicFilters, events)
		}
		if topic == "" {
			topic = "chat"
		}
		if kind == liveActionAlertOwner && !liveConcreteTopic(topic) {
			return false, fmt.Errorf("live action %q topic %q is invalid", kind, topic)
		}
		if kind != liveActionAlertOwner && !liveActionTopicAllowed(cfg, topic) {
			return false, fmt.Errorf("live action %q topic %q is outside configured live filters", kind, topic)
		}
		if err := publishIPCMessage(ctx, gf, cfg.GroupID, []string{topic}, []byte(message)); err != nil {
			return false, fmt.Errorf("%w: %v", errLiveActionTransport, err)
		}
		return true, nil
	case liveActionTaskCreate:
		if state == nil {
			return false, errors.New("live action task.create requires state store")
		}
		return applyLiveAgentTaskCreate(ctx, gf, state, cfg, action)
	case liveActionCommandSend:
		if state == nil {
			return false, errors.New("live action command.send requires state store")
		}
		return applyLiveAgentCommandSend(ctx, gf, state, cfg, action)
	default:
		return false, fmt.Errorf("live action %q has no local executor yet", kind)
	}
}

func applyLiveAgentTaskCreate(ctx context.Context, gf *globalFlags, state esphttp.StateStore, cfg esphttp.LiveAgentConfig, action liveAgentAction) (bool, error) {
	fleet, err := liveActionFleet(ctx, state, cfg.GroupID, action.FleetID)
	if err != nil {
		return false, err
	}
	actor, err := liveActionFleetMember(ctx, state, fleet.FleetID, cfg.NodeID)
	if err != nil {
		return false, err
	}
	title, err := esphttp.NormalizeFleetTaskTitle(firstNonEmpty(action.Title, action.Message))
	if err != nil {
		return false, err
	}
	description, err := esphttp.NormalizeFleetTaskDescription(firstNonEmpty(action.Description, action.Content))
	if err != nil {
		return false, err
	}
	if cfg.MaxActionBytes > 0 && len([]byte(title))+len([]byte(description)) > cfg.MaxActionBytes {
		return false, errors.New("live action task.create payload exceeds max_action_bytes")
	}
	if strings.TrimSpace(action.Mode) != "" && !esphttp.IsValidFleetTaskMode(action.Mode) {
		return false, fmt.Errorf("live action task.create mode %q is invalid", action.Mode)
	}
	mode := esphttp.NormalizeFleetTaskMode(action.Mode)
	var assignee *esphttp.FleetMemberRecord
	if action.AssigneeNodeID != 0 {
		if action.AssigneeNodeID > uint64(^uint32(0)) {
			return false, fmt.Errorf("live action task.create assignee_node_id is too large: %d", action.AssigneeNodeID)
		}
		member, err := liveActionFleetMember(ctx, state, fleet.FleetID, entmoot.NodeID(action.AssigneeNodeID))
		if err != nil {
			return false, err
		}
		assignee = &member
	}
	if mode == esphttp.FleetTaskModeDirectAssignment && assignee == nil {
		return false, errors.New("live action task.create direct_assignee mode requires assignee_node_id")
	}
	if mode != esphttp.FleetTaskModeDirectAssignment && assignee != nil {
		return false, errors.New("live action task.create assignee_node_id is only valid for direct_assignee mode")
	}
	if assignee != nil && !esphttp.FleetTaskIsCoordinator(actor) {
		return false, esphttp.ErrFleetTaskUnauthorized
	}
	now := time.Now().UnixMilli()
	task := esphttp.FleetTaskRecord{
		FleetID:     fleet.FleetID,
		Title:       title,
		Description: description,
		Mode:        mode,
		Status:      esphttp.FleetTaskStatusOpen,
		Creator:     esphttp.FleetTaskActorFromMember(actor),
		CreatedAtMS: now,
		UpdatedAtMS: now,
	}
	mutation, err := esphttp.ApplyFleetTaskMutation(task, esphttp.FleetTaskActionCreate, actor, now, nil, nil)
	if err != nil {
		return false, err
	}
	task, err = state.UpsertFleetTask(ctx, mutation.Task)
	if err != nil {
		return false, err
	}
	if err := appendLiveFleetActivity(ctx, state, fleet.FleetID, mutation, task, actor); err != nil {
		return false, err
	}
	publishLiveFleetTaskEvent(ctx, gf, fleet, mutation, task, actor)
	if assignee != nil {
		now = time.Now().UnixMilli()
		mutation, err = esphttp.ApplyFleetTaskMutation(task, esphttp.FleetTaskActionAssign, actor, now, assignee, nil)
		if err != nil {
			return false, err
		}
		updated, ok, err := state.UpdateFleetTaskIfCurrent(ctx, mutation.Task, mutation.ExpectedUpdatedAtMS)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, fmt.Errorf("%w: task changed concurrently", esphttp.ErrFleetTaskInvalidTransition)
		}
		task = updated
		if err := appendLiveFleetActivity(ctx, state, fleet.FleetID, mutation, task, actor); err != nil {
			return false, err
		}
		publishLiveFleetTaskEvent(ctx, gf, fleet, mutation, task, actor)
	}
	return true, nil
}

func liveActionFleet(ctx context.Context, state esphttp.StateStore, groupID entmoot.GroupID, rawFleetID string) (esphttp.FleetRecord, error) {
	if fleetID := strings.TrimSpace(rawFleetID); fleetID != "" {
		fleet, found, err := state.GetFleet(ctx, fleetID)
		if err != nil {
			return esphttp.FleetRecord{}, err
		}
		if !found {
			return esphttp.FleetRecord{}, fmt.Errorf("fleet %q not found", fleetID)
		}
		return liveActionValidateFleet(groupID, fleet)
	}
	fleet, found, err := state.GetFleetByControlGroup(ctx, groupID)
	if err != nil {
		return esphttp.FleetRecord{}, err
	}
	if !found {
		return esphttp.FleetRecord{}, fmt.Errorf("no fleet is linked to control group %s", groupID.String())
	}
	return liveActionValidateFleet(groupID, fleet)
}

func liveActionValidateFleet(groupID entmoot.GroupID, fleet esphttp.FleetRecord) (esphttp.FleetRecord, error) {
	if fleet.ControlGroupID != groupID {
		return esphttp.FleetRecord{}, fmt.Errorf("fleet %q is not linked to control group %s", fleet.FleetID, groupID.String())
	}
	if fleet.Status != "" && fleet.Status != esphttp.FleetStatusActive {
		return esphttp.FleetRecord{}, esphttp.ErrFleetNotActive
	}
	return fleet, nil
}

func applyLiveAgentCommandSend(ctx context.Context, gf *globalFlags, state esphttp.StateStore, cfg esphttp.LiveAgentConfig, action liveAgentAction) (bool, error) {
	fleet, err := liveActionFleet(ctx, state, cfg.GroupID, action.FleetID)
	if err != nil {
		return false, err
	}
	actor, err := liveActionFleetMember(ctx, state, fleet.FleetID, cfg.NodeID)
	if err != nil {
		return false, err
	}
	if !esphttp.FleetTaskIsCoordinator(actor) {
		return false, esphttp.ErrFleetTaskUnauthorized
	}
	if err := liveActionRequireCoordinatorPublisher(ctx, gf, fleet); err != nil {
		return false, err
	}
	commandAction := esphttp.NormalizeFleetCommandAction(action.Action)
	if commandAction == "" && strings.TrimSpace(action.Instruction) != "" {
		commandAction = esphttp.FleetCommandActionAgentInstruction
	}
	entry, found := esphttp.FleetCommandCatalogLookup(commandAction)
	if !found {
		return false, errors.New("live action command.send action is unsupported")
	}
	autoAccept := true
	if action.AutoAccept != nil {
		autoAccept = *action.AutoAccept
	} else if commandAction == esphttp.FleetCommandActionAgentInstruction {
		autoAccept = false
	}
	if autoAccept && !entry.AutoAcceptSafe {
		return false, errors.New("live action command.send action is not safe for auto-accept")
	}
	args, err := liveCommandArgs(action, commandAction)
	if err != nil {
		return false, err
	}
	if err := esphttp.ValidateFleetCommandArgs(commandAction, args); err != nil {
		return false, err
	}
	target, subject, err := liveCommandTarget(ctx, state, fleet.FleetID, action)
	if err != nil {
		return false, err
	}
	now := time.Now().UnixMilli()
	expiresAtMS := action.ExpiresAtMS
	if expiresAtMS == 0 {
		expiresAtMS = now + esphttp.DefaultFleetCommandTTL.Milliseconds()
	}
	if expiresAtMS <= now {
		return false, errors.New("live action command.send expiration must be in the future")
	}
	commandID, err := esphttp.NewFleetCommandID()
	if err != nil {
		return false, err
	}
	command := esphttp.FleetCommandEnvelope{
		Type:           esphttp.FleetCommandMessageType,
		Version:        1,
		CommandID:      commandID,
		FleetID:        fleet.FleetID,
		ControlGroupID: fleet.ControlGroupID,
		IssuerNodeID:   actor.NodeID,
		Target:         target,
		Action:         commandAction,
		Args:           args,
		AutoAccept:     autoAccept,
		CreatedAtMS:    now,
		ExpiresAtMS:    expiresAtMS,
	}
	body, err := json.Marshal(command)
	if err != nil {
		return false, err
	}
	if cfg.MaxActionBytes > 0 && len(body) > cfg.MaxActionBytes {
		return false, errors.New("live action command.send payload exceeds max_action_bytes")
	}
	if err := publishIPCMessage(ctx, gf, fleet.ControlGroupID, []string{"fleet/commands"}, body); err != nil {
		return false, fmt.Errorf("%w: %v", errLiveActionTransport, err)
	}
	if _, err := state.UpsertFleetCommand(ctx, command); err != nil {
		return true, nil
	}
	metadata, _ := json.Marshal(map[string]any{
		"command_id": command.CommandID,
		"action":     command.Action,
		"target":     command.Target,
	})
	_, _ = state.AppendFleetActivity(ctx, esphttp.FleetActivityRecord{
		FleetID:     fleet.FleetID,
		Type:        "command.sent",
		Actor:       esphttp.FleetTaskActorFromMember(actor),
		Subject:     subject,
		Summary:     "Command sent",
		Metadata:    metadata,
		CreatedAtMS: now,
	})
	return true, nil
}

func liveActionRequireCoordinatorPublisher(ctx context.Context, gf *globalFlags, fleet esphttp.FleetRecord) error {
	info, err := infoOverIPCContext(ctx, controlSocketPath(gf.data))
	if err != nil {
		return fmt.Errorf("%w: %v", errLiveActionTransport, err)
	}
	if info.PilotNodeID != fleet.Coordinator.PilotNodeID || !bytes.Equal(info.EntmootPubKey, fleet.Coordinator.EntmootPubKey) {
		return errors.New("live action command.send requires the local publisher to match the Fleet coordinator")
	}
	return nil
}

func liveCommandArgs(action liveAgentAction, commandAction string) (map[string]interface{}, error) {
	args := make(map[string]interface{}, len(action.Args)+2)
	for key, value := range action.Args {
		args[key] = value
	}
	if strings.TrimSpace(action.Instruction) != "" {
		if commandAction != esphttp.FleetCommandActionAgentInstruction {
			return nil, errors.New("live action command.send instruction is only valid for agent.instruction")
		}
		args["instruction"] = strings.TrimSpace(action.Instruction)
	}
	if action.TimeoutMS != 0 {
		if commandAction != esphttp.FleetCommandActionAgentInstruction {
			return nil, errors.New("live action command.send timeout_ms is only valid for agent.instruction")
		}
		args["timeout_ms"] = action.TimeoutMS
	}
	if len(args) == 0 {
		return nil, nil
	}
	return args, nil
}

func liveCommandTarget(ctx context.Context, state esphttp.StateStore, fleetID string, action liveAgentAction) (esphttp.FleetCommandTarget, *entmoot.NodeInfo, error) {
	targetKind := esphttp.NormalizeFleetCommandTarget(action.Target)
	target := esphttp.FleetCommandTarget{Kind: targetKind}
	switch targetKind {
	case esphttp.FleetCommandTargetAll:
		if action.TargetNodeID != 0 {
			return esphttp.FleetCommandTarget{}, nil, errors.New("live action command.send target_node_id requires target=node")
		}
		return target, nil, nil
	case esphttp.FleetCommandTargetNode:
		if action.TargetNodeID == 0 {
			return esphttp.FleetCommandTarget{}, nil, errors.New("live action command.send target_node_id is required for node target")
		}
		if action.TargetNodeID > uint64(^uint32(0)) {
			return esphttp.FleetCommandTarget{}, nil, fmt.Errorf("live action command.send target_node_id is too large: %d", action.TargetNodeID)
		}
		member, err := liveActionFleetMember(ctx, state, fleetID, entmoot.NodeID(action.TargetNodeID))
		if err != nil {
			return esphttp.FleetCommandTarget{}, nil, err
		}
		info := esphttp.FleetTaskActorFromMember(member)
		target.PilotNodeID = info.PilotNodeID
		return target, &info, nil
	default:
		return esphttp.FleetCommandTarget{}, nil, errors.New("live action command.send target is invalid")
	}
}

func liveActionFleetMember(ctx context.Context, state esphttp.StateStore, fleetID string, nodeID entmoot.NodeID) (esphttp.FleetMemberRecord, error) {
	members, err := state.ListFleetMembers(ctx, fleetID)
	if err != nil {
		return esphttp.FleetMemberRecord{}, err
	}
	for _, member := range members {
		if member.NodeID == nodeID && esphttp.FleetTaskCanMutate(member) {
			return member, nil
		}
	}
	return esphttp.FleetMemberRecord{}, fmt.Errorf("node %d is not an active member of fleet %s", nodeID, fleetID)
}

func appendLiveFleetActivity(ctx context.Context, state esphttp.StateStore, fleetID string, mutation esphttp.FleetTaskMutation, task esphttp.FleetTaskRecord, actor esphttp.FleetMemberRecord) error {
	if mutation.ActivityType == "" {
		return nil
	}
	eventID, err := esphttp.NewFleetActivityID()
	if err != nil {
		return err
	}
	_, err = state.AppendFleetActivity(ctx, esphttp.FleetActivityRecord{
		EventID:     eventID,
		FleetID:     fleetID,
		Type:        mutation.ActivityType,
		Actor:       esphttp.FleetTaskActorFromMember(actor),
		Subject:     mutation.Subject,
		Summary:     mutation.Summary,
		CreatedAtMS: task.UpdatedAtMS,
	})
	return err
}

func publishLiveFleetTaskEvent(ctx context.Context, gf *globalFlags, fleet esphttp.FleetRecord, mutation esphttp.FleetTaskMutation, task esphttp.FleetTaskRecord, actor esphttp.FleetMemberRecord) {
	if fleet.ControlGroupID == (entmoot.GroupID{}) {
		return
	}
	body, err := json.Marshal(map[string]any{
		"type":             "fleet.task",
		"fleet_id":         fleet.FleetID,
		"control_group_id": fleet.ControlGroupID,
		"task_id":          task.TaskID,
		"action":           mutation.Action,
		"status":           task.Status,
		"title":            task.Title,
		"actor_node_id":    actor.NodeID,
		"summary":          mutation.Summary,
		"created_at_ms":    task.UpdatedAtMS,
	})
	if err != nil {
		return
	}
	_ = publishIPCMessage(ctx, gf, fleet.ControlGroupID, []string{"fleet/tasks"}, body)
}

func liveMessageKeyAfterCursor(msg entmoot.Message, cursor esphttp.LiveAgentCursor) bool {
	if msg.Timestamp != cursor.LastSeenAtMS {
		return msg.Timestamp > cursor.LastSeenAtMS
	}
	if msg.Author.PilotNodeID != cursor.LastSeenAuthorNodeID {
		return msg.Author.PilotNodeID > cursor.LastSeenAuthorNodeID
	}
	return bytes.Compare(msg.ID[:], cursor.LastSeenMessageID[:]) > 0
}

func liveReplayWindowDisabled(cursor esphttp.LiveAgentCursor) bool {
	return cursor.ScanFloorAtMS >= cursor.LastSeenAtMS || len(cursor.SeenMessageIDs) >= liveCursorMaxSeenIDs
}

func liveSeenMessageSet(ids []entmoot.MessageID) map[entmoot.MessageID]struct{} {
	seen := make(map[entmoot.MessageID]struct{}, len(ids))
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	return seen
}

func advanceLiveCursorWithMessage(cursor esphttp.LiveAgentCursor, msg entmoot.Message, seen map[entmoot.MessageID]struct{}) esphttp.LiveAgentCursor {
	if _, ok := seen[msg.ID]; !ok {
		cursor.SeenMessageIDs = append(cursor.SeenMessageIDs, msg.ID)
		seen[msg.ID] = struct{}{}
	}
	if liveMessageKeyAfterCursor(msg, cursor) {
		cursor.LastSeenAtMS = msg.Timestamp
		cursor.LastSeenAuthorNodeID = msg.Author.PilotNodeID
		cursor.LastSeenMessageID = msg.ID
	}
	return cursor
}

func cursorFromMessage(gid entmoot.GroupID, nodeID entmoot.NodeID, msg entmoot.Message) esphttp.LiveAgentCursor {
	return esphttp.LiveAgentCursor{
		GroupID:              gid,
		NodeID:               nodeID,
		LastSeenAtMS:         msg.Timestamp,
		LastSeenAuthorNodeID: msg.Author.PilotNodeID,
		LastSeenMessageID:    msg.ID,
		SeenMessageIDs:       []entmoot.MessageID{msg.ID},
	}
}

func liveCursorsEqual(a, b esphttp.LiveAgentCursor) bool {
	return a.LastSeenAtMS == b.LastSeenAtMS &&
		a.LastSeenAuthorNodeID == b.LastSeenAuthorNodeID &&
		a.LastSeenMessageID == b.LastSeenMessageID &&
		liveMessageIDSlicesEqual(a.SeenMessageIDs, b.SeenMessageIDs)
}

func liveMessageIDSlicesEqual(a, b []entmoot.MessageID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func liveActionAllowed(cfg esphttp.LiveAgentConfig, kind string) bool {
	if cfg.Mode != esphttp.LiveModeOperator {
		return kind == liveActionReply || kind == liveActionMessageSummarize
	}
	actions := cfg.AllowedActions
	if len(actions) == 0 {
		actions = esphttp.DefaultLiveActions()
	}
	for _, action := range actions {
		if action == kind {
			return true
		}
	}
	return false
}

func liveMessageMatchesTopics(filters, topics []string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		for _, t := range topics {
			if esphttp.LiveTopicMatches(filter, t) {
				return true
			}
		}
	}
	return false
}

func liveActionTopicAllowed(cfg esphttp.LiveAgentConfig, topic string) bool {
	if !liveConcreteTopic(topic) {
		return false
	}
	if len(cfg.TopicFilters) == 0 {
		return true
	}
	for _, filter := range cfg.TopicFilters {
		if esphttp.LiveTopicMatches(filter, topic) {
			return true
		}
	}
	return false
}

func liveConcreteTopic(topic string) bool {
	return strings.TrimSpace(topic) != "" && esphttp.LiveTopicMatches("#", topic)
}

func liveMessageMentionsAgent(msg entmoot.Message, nodeID entmoot.NodeID) bool {
	content := strings.ToLower(string(msg.Content))
	id := fmt.Sprintf("%d", nodeID)
	return containsLiveMentionToken(content, "@"+id) ||
		containsLiveMentionToken(content, "@agent-"+id) ||
		containsLiveMentionToken(content, "node:"+id)
}

func containsLiveMentionToken(content, token string) bool {
	for start := strings.Index(content, token); start >= 0; {
		end := start + len(token)
		if liveMentionBoundaryBefore(content, start) && liveMentionBoundaryAfter(content, end) {
			return true
		}
		next := strings.Index(content[start+1:], token)
		if next < 0 {
			return false
		}
		start += next + 1
	}
	return false
}

func liveMentionBoundaryBefore(content string, start int) bool {
	return start == 0 || !liveMentionTokenChar(content[start-1])
}

func liveMentionBoundaryAfter(content string, end int) bool {
	return end == len(content) || !liveMentionTokenChar(content[end])
}

func liveMentionTokenChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_' || b == '-'
}

func liveRunnerMessage(msg entmoot.Message) liveAgentRunnerMessage {
	return liveAgentRunnerMessage{
		MessageID:   msg.ID,
		AuthorNode:  msg.Author.PilotNodeID,
		Topics:      append([]string(nil), msg.Topics...),
		Content:     string(msg.Content),
		TimestampMS: msg.Timestamp,
	}
}

func liveTriggerForMode(mode string) string {
	switch mode {
	case esphttp.LiveModeReplyOnMention:
		return "mention"
	case esphttp.LiveModeOperator:
		return "operator_topic_activity"
	default:
		return "topic_activity"
	}
}

func firstMatchedEventTopic(filters []string, events []liveAgentRunnerMessage) string {
	for _, event := range events {
		for _, topic := range event.Topics {
			topic = strings.TrimSpace(topic)
			if topic != "" && liveActionTopicAllowed(esphttp.LiveAgentConfig{TopicFilters: filters}, topic) {
				return topic
			}
		}
	}
	return ""
}

func agentLiveTimeoutSeconds(timeout time.Duration) int {
	if timeout <= 0 {
		return 60
	}
	seconds := int(timeout.Round(time.Second) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}
