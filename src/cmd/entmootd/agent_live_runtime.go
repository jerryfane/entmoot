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
	Kind    string `json:"kind"`
	Message string `json:"message,omitempty"`
	Title   string `json:"title,omitempty"`
	Content string `json:"content,omitempty"`
	Topic   string `json:"topic,omitempty"`
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
		Instructions:   "Return JSON only: {\"actions\":[{\"kind\":\"reply\",\"message\":\"...\"}]}. Entmoot will validate and apply allowed actions. Do not claim that you posted anything yourself.",
	}
	output, err := runLiveAgentRunner(ctx, runCfg, runnerCtx)
	if err != nil {
		return result, err
	}
	result.Proposed = len(output.Actions)
	for _, action := range output.Actions {
		applied, err := applyLiveAgentAction(ctx, gf, cfg, events, action)
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

func applyLiveAgentAction(ctx context.Context, gf *globalFlags, cfg esphttp.LiveAgentConfig, events []liveAgentRunnerMessage, action liveAgentAction) (bool, error) {
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
	default:
		return false, fmt.Errorf("live action %q has no local executor yet", kind)
	}
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
