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
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/membership"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/store"

	"golang.org/x/time/rate"
)

const (
	liveActionReply            = "reply"
	liveActionMessageSummarize = "message.summarize"
	liveActionMetadataUpdate   = "metadata.update"
	liveActionAlertOwner       = "alert.owner"
	liveCursorMaxSeenIDs       = 512
	liveCursorOverlapWindow    = 10 * time.Minute
)

const (
	agentLiveScanStatusOK      = "ok"
	agentLiveScanStatusError   = "error"
	agentLiveScanStatusBackoff = "backoff"

	liveRecoverableRunnerTimeout     = "runner_timeout"
	liveRecoverableRunnerFailed      = "runner_failed"
	liveRecoverableRunnerInvalidJSON = "runner_invalid_json"
	liveRecoverableActionTransport   = "action_transport"
	liveRecoverableTriggerRateLimit  = "trigger_rate_limited"
)

var (
	errLiveActionTransport   = errors.New("live action transport")
	errLiveRunnerTimeout     = errors.New("live runner timeout")
	errLiveRunnerFailed      = errors.New("live runner failed")
	errLiveRunnerInvalidJSON = errors.New("live runner invalid json")
)

type agentLiveRuntimeConfig struct {
	groupID        entmoot.GroupID
	nodeID         entmoot.MemberID
	runner         string
	timeout        time.Duration
	limit          int
	policies       livePolicyStore
	triggerLimiter *agentLiveTriggerLimiter
}

type livePolicyStore interface {
	Get(ctx context.Context, groupID entmoot.GroupID) (entpolicy.Policy, bool, error)
}

type agentLiveTriggerLimiter struct {
	clk clockSource
	mu  sync.Mutex
	m   map[agentLiveTriggerKey]*agentLiveTriggerBucket
}

type clockSource interface {
	Now() time.Time
}

type agentLiveTriggerKey struct {
	groupID entmoot.GroupID
	nodeID  entmoot.MemberID
}

type agentLiveTriggerBucket struct {
	rate    rate.Limit
	burst   int
	limiter *rate.Limiter
}

type systemClockSource struct{}

func (systemClockSource) Now() time.Time { return time.Now() }

func newAgentLiveTriggerLimiter(clk clockSource) *agentLiveTriggerLimiter {
	if clk == nil {
		clk = systemClockSource{}
	}
	return &agentLiveTriggerLimiter{
		clk: clk,
		m:   make(map[agentLiveTriggerKey]*agentLiveTriggerBucket),
	}
}

func (l *agentLiveTriggerLimiter) Allow(groupID entmoot.GroupID, nodeID entmoot.MemberID, limit rate.Limit, burst int) bool {
	if l == nil || limit <= 0 || burst <= 0 {
		return true
	}
	key := agentLiveTriggerKey{groupID: groupID, nodeID: nodeID}
	l.mu.Lock()
	bucket := l.m[key]
	if bucket == nil || bucket.rate != limit || bucket.burst != burst {
		bucket = &agentLiveTriggerBucket{
			rate:    limit,
			burst:   burst,
			limiter: rate.NewLimiter(limit, burst),
		}
		l.m[key] = bucket
	}
	allowed := bucket.limiter.AllowN(l.clk.Now(), 1)
	l.mu.Unlock()
	return allowed
}

func allowLiveTrigger(ctx context.Context, runCfg agentLiveRuntimeConfig, groupID entmoot.GroupID, nodeID entmoot.MemberID) (bool, error) {
	if runCfg.policies == nil || runCfg.triggerLimiter == nil {
		return true, nil
	}
	p, ok, err := runCfg.policies.Get(ctx, groupID)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	spec, err := entpolicy.ParseMessageRate(p.LiveTriggerRate)
	if err != nil {
		return false, fmt.Errorf("live_trigger_rate: %w", err)
	}
	if p.LiveTriggerBurst <= 0 {
		return false, fmt.Errorf("live_trigger_burst must be positive")
	}
	return runCfg.triggerLimiter.Allow(groupID, nodeID, spec.Limit(), p.LiveTriggerBurst), nil
}

type agentLiveScanResult struct {
	Status          string `json:"status,omitempty"`
	Seen            int    `json:"seen"`
	Matched         int    `json:"matched"`
	Proposed        int    `json:"proposed"`
	Applied         int    `json:"applied"`
	Rejected        int    `json:"rejected"`
	ErrorKind       string `json:"error_kind,omitempty"`
	Error           string `json:"error,omitempty"`
	NextAttemptAtMS int64  `json:"next_attempt_at_ms,omitempty"`
}

type liveAgentRunnerContext struct {
	GroupID        entmoot.GroupID          `json:"group_id"`
	MemberID       entmoot.MemberID         `json:"member_id"`
	Mode           string                   `json:"mode"`
	TopicFilters   []string                 `json:"topic_filters"`
	AllowedActions []string                 `json:"allowed_actions,omitempty"`
	Trigger        string                   `json:"trigger"`
	Events         []liveAgentRunnerMessage `json:"events"`
	Instructions   string                   `json:"instructions"`
}

type liveAgentRunnerMessage struct {
	MessageID      entmoot.MessageID `json:"message_id"`
	AuthorMemberID entmoot.MemberID  `json:"author_member_id"`
	Topics         []string          `json:"topics"`
	Content        string            `json:"content"`
	TimestampMS    int64             `json:"timestamp_ms"`
}

type liveAgentRunnerOutput struct {
	Actions []liveAgentAction `json:"actions"`
}

type liveAgentRunnerRawOutput struct {
	Actions json.RawMessage `json:"actions"`
	Output  string          `json:"output"`
	Status  string          `json:"status"`
}

type agentRuntimeProcessResult struct {
	stdout string
	stderr string
	err    error
}

type liveAgentAction struct {
	Kind     string          `json:"kind"`
	Message  string          `json:"message,omitempty"`
	Title    string          `json:"title,omitempty"`
	Content  string          `json:"content,omitempty"`
	Topic    string          `json:"topic,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

func runAgentLiveScan(ctx context.Context, gf *globalFlags, state esphttp.StateStore, msgStore store.MessageStore, cfg esphttp.LiveAgentConfig, runCfg agentLiveRuntimeConfig) (agentLiveScanResult, error) {
	result := agentLiveScanResult{Status: agentLiveScanStatusOK}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, cfg.GroupID, cfg.MemberID)
	if err != nil {
		return result, err
	}
	if !ok {
		cursor = esphttp.LiveAgentCursor{
			GroupID:       cfg.GroupID,
			MemberID:      cfg.MemberID,
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
			left := liveMessageAuthorMemberID(msgs[i])
			right := liveMessageAuthorMemberID(msgs[j])
			if left == right {
				return bytes.Compare(msgs[i].ID[:], msgs[j].ID[:]) < 0
			}
			return bytes.Compare(left[:], right[:]) < 0
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
		if msg.Author.MemberID != nil && *msg.Author.MemberID == cfg.MemberID {
			nextCursor = msgCursor
			continue
		}
		if !liveMessageMatchesTopics(cfg.TopicFilters, msg.Topics) {
			nextCursor = msgCursor
			continue
		}
		if cfg.Mode == esphttp.LiveModeReplyOnMention && !liveMessageMentionsAgent(msg, cfg.MemberID) {
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
	allowed, err := allowLiveTrigger(ctx, runCfg, cfg.GroupID, cfg.MemberID)
	if err != nil {
		return result, err
	}
	if !allowed {
		result.Status = agentLiveScanStatusError
		result.ErrorKind = liveRecoverableTriggerRateLimit
		result.Error = "live trigger rate limit reached"
		return result, nil
	}
	allowedActions := liveAllowedActionsForConfig(cfg)
	runnerCtx := liveAgentRunnerContext{
		GroupID:        cfg.GroupID,
		MemberID:       cfg.MemberID,
		Mode:           cfg.Mode,
		TopicFilters:   append([]string(nil), cfg.TopicFilters...),
		AllowedActions: append([]string(nil), allowedActions...),
		Trigger:        liveTriggerForMode(cfg.Mode),
		Events:         events,
		Instructions:   "Return JSON only: {\"actions\":[{\"kind\":\"reply\",\"message\":\"...\"}]}. Only use action kinds listed in allowed_actions. Entmoot will validate and apply allowed actions. Do not claim that you posted anything yourself.",
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
	if strings.EqualFold(runner, liveRunnerOpenClaw) {
		args := openClawLiveAgentArgs(cfg, string(data))
		cmd := exec.CommandContext(runnerCtx, openClawBinary(), args...)
		run := runLiveRuntimeProcess(cmd, nil, liveCtx)
		if runnerCtx.Err() != nil {
			return liveAgentRunnerOutput{}, liveRunnerTimeoutError("OpenClaw live interaction timed out", runnerCtx.Err(), run.stderr)
		}
		if run.err != nil {
			return liveAgentRunnerOutput{}, liveRunnerFailedError("OpenClaw live interaction failed", run)
		}
		return parseLiveRunnerOutput(openClawLiveFinalText(run.stdout))
	}
	cmd := exec.CommandContext(runnerCtx, runner)
	run := runLiveRuntimeProcess(cmd, data, liveCtx)
	if runnerCtx.Err() != nil {
		return liveAgentRunnerOutput{}, liveRunnerTimeoutError("live agent runtime timed out", runnerCtx.Err(), run.stderr)
	}
	if run.err != nil {
		return liveAgentRunnerOutput{}, liveRunnerFailedError("live agent runtime failed", run)
	}
	return parseLiveRunnerOutput(run.stdout)
}

func openClawLiveAgentArgs(cfg agentLiveRuntimeConfig, contextJSON string) []string {
	selectorFlag, selectorValue := openClawAgentSelector()
	return []string{
		"agent",
		selectorFlag, selectorValue,
		"--message", "Entmoot live interaction context JSON:\n" + contextJSON,
		"--json",
		"--timeout", strconv.Itoa(agentLiveTimeoutSeconds(cfg.timeout)),
	}
}

const liveRunnerOpenClaw = "openclaw"

type openClawAgentRunReport struct {
	Result json.RawMessage `json:"result"`
	Meta   openClawRunMeta `json:"meta"`
}

type openClawRunMeta struct {
	FinalAssistantVisibleText string `json:"finalAssistantVisibleText"`
	FinalAssistantRawText     string `json:"finalAssistantRawText"`
}

type openClawAgentRunResult struct {
	Meta openClawRunMeta `json:"meta"`
}

func openClawBinary() string {
	if bin := strings.TrimSpace(os.Getenv("OPENCLAW_BIN")); bin != "" {
		return bin
	}
	return "openclaw"
}

func openClawAgentSelector() (string, string) {
	if v := strings.TrimSpace(os.Getenv("ENTMOOT_OPENCLAW_SESSION_ID")); v != "" {
		return "--session-id", v
	}
	if v := strings.TrimSpace(os.Getenv("ENTMOOT_OPENCLAW_TO")); v != "" {
		return "--to", v
	}
	if v := strings.TrimSpace(os.Getenv("ENTMOOT_OPENCLAW_AGENT")); v != "" {
		return "--agent", v
	}
	if v := strings.TrimSpace(os.Getenv("OPENCLAW_SESSION_ID")); v != "" {
		return "--session-id", v
	}
	if v := strings.TrimSpace(os.Getenv("OPENCLAW_TO")); v != "" {
		return "--to", v
	}
	if v := strings.TrimSpace(os.Getenv("OPENCLAW_AGENT_ID")); v != "" {
		return "--agent", v
	}
	return "--agent", "main"
}

func openClawFinalText(report openClawAgentRunReport) string {
	if text := strings.TrimSpace(report.Meta.FinalAssistantVisibleText); text != "" {
		return text
	}
	if text := strings.TrimSpace(report.Meta.FinalAssistantRawText); text != "" {
		return text
	}
	var result openClawAgentRunResult
	if len(report.Result) > 0 && json.Unmarshal(report.Result, &result) == nil {
		if text := strings.TrimSpace(result.Meta.FinalAssistantVisibleText); text != "" {
			return text
		}
		if text := strings.TrimSpace(result.Meta.FinalAssistantRawText); text != "" {
			return text
		}
	}
	return ""
}

func liveRunnerTimeoutError(prefix string, ctxErr error, stderr string) error {
	output := strings.TrimSpace(stderr)
	if output == "" {
		return fmt.Errorf("%w: %w: %s", errLiveRunnerTimeout, ctxErr, prefix)
	}
	return fmt.Errorf("%w: %w: %s: %s", errLiveRunnerTimeout, ctxErr, prefix, output)
}

func liveRunnerFailedError(prefix string, run agentRuntimeProcessResult) error {
	output := strings.TrimSpace(run.stderr)
	if output == "" && run.err != nil {
		output = run.err.Error()
	}
	return fmt.Errorf("%w: %s: %s", errLiveRunnerFailed, prefix, addAgentRuntimeFailureAdvice(output))
}

func runLiveRuntimeProcess(cmd *exec.Cmd, stdin []byte, liveCtx liveAgentRunnerContext) agentRuntimeProcessResult {
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	cmd.Env = append(os.Environ(),
		"ENTMOOT_LIVE_GROUP_ID="+liveCtx.GroupID.String(),
		"ENTMOOT_LIVE_MEMBER_ID="+liveCtx.MemberID.String(),
		"ENTMOOT_LIVE_MODE="+liveCtx.Mode,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return agentRuntimeProcessResult{stdout: stdout.String(), stderr: stderr.String(), err: err}
}

func parseLiveRunnerOutput(stdout string) (liveAgentRunnerOutput, error) {
	return parseLiveRunnerJSONText(stdout, 0, "live agent runtime returned empty output", "live agent runtime returned invalid JSON")
}

func parseLiveRunnerJSONText(text string, depth int, emptyMessage, invalidMessage string) (liveAgentRunnerOutput, error) {
	raw := bytes.TrimSpace([]byte(text))
	if len(raw) == 0 {
		return liveAgentRunnerOutput{}, fmt.Errorf("%w: %s", errLiveRunnerInvalidJSON, emptyMessage)
	}
	if output, err := parseLiveRunnerJSON(raw, depth); err == nil {
		return output, nil
	}
	start := bytes.IndexByte(raw, '{')
	end := bytes.LastIndexByte(raw, '}')
	if start >= 0 && end > start {
		if output, err := parseLiveRunnerJSON(raw[start:end+1], depth); err == nil {
			return output, nil
		}
	}
	return liveAgentRunnerOutput{}, fmt.Errorf("%w: %s", errLiveRunnerInvalidJSON, invalidMessage)
}

func parseLiveRunnerJSON(raw []byte, depth int) (liveAgentRunnerOutput, error) {
	if depth > 4 {
		return liveAgentRunnerOutput{}, fmt.Errorf("%w: live agent runtime output nesting is too deep", errLiveRunnerInvalidJSON)
	}
	var envelope liveAgentRunnerRawOutput
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return liveAgentRunnerOutput{}, err
	}
	if len(envelope.Actions) > 0 {
		if !rawJSONStartsWith(envelope.Actions, '[') {
			return liveAgentRunnerOutput{}, fmt.Errorf("%w: live agent runtime actions must be an array", errLiveRunnerInvalidJSON)
		}
		var output liveAgentRunnerOutput
		if err := json.Unmarshal(raw, &output); err != nil {
			return liveAgentRunnerOutput{}, err
		}
		if err := validateLiveRunnerOutputActions(output.Actions); err != nil {
			return liveAgentRunnerOutput{}, err
		}
		return output, nil
	}
	if strings.TrimSpace(envelope.Output) != "" {
		if !liveRunnerOutputEnvelopeStatusAllowsActions(envelope.Status) {
			return liveAgentRunnerOutput{}, fmt.Errorf("%w: live agent runtime output envelope status %q is not completed", errLiveRunnerInvalidJSON, strings.TrimSpace(envelope.Status))
		}
		return parseLiveRunnerJSONText(envelope.Output, depth+1, "live agent runtime output envelope is empty", "live agent runtime output envelope missing actions")
	}
	return liveAgentRunnerOutput{}, fmt.Errorf("%w: live agent runtime output missing actions", errLiveRunnerInvalidJSON)
}

func rawJSONStartsWith(raw json.RawMessage, want byte) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && raw[0] == want
}

func liveRunnerOutputEnvelopeStatusAllowsActions(status string) bool {
	status = strings.TrimSpace(strings.ToLower(status))
	return status == "" || status == "completed"
}

func validateLiveRunnerOutputActions(actions []liveAgentAction) error {
	for _, action := range actions {
		if !knownLiveActionKind(action.Kind) {
			return fmt.Errorf("%w: live agent runtime action kind %q is not supported", errLiveRunnerInvalidJSON, strings.TrimSpace(action.Kind))
		}
	}
	return nil
}

func knownLiveActionKind(kind string) bool {
	switch strings.TrimSpace(strings.ToLower(kind)) {
	case liveActionReply,
		liveActionMessageSummarize,
		liveActionMetadataUpdate,
		liveActionAlertOwner:
		return true
	default:
		return false
	}
}

func openClawLiveFinalText(stdout string) string {
	var report openClawAgentRunReport
	if err := json.Unmarshal([]byte(stdout), &report); err == nil {
		if text := openClawFinalText(report); strings.TrimSpace(text) != "" {
			return text
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
	case liveActionMetadataUpdate:
		if state == nil {
			return false, errors.New("live action metadata.update requires state store")
		}
		return applyLiveAgentMetadataUpdate(ctx, gf, state, cfg, action)
	default:
		return false, fmt.Errorf("live action %q has no local executor yet", kind)
	}
}

func applyLiveAgentMetadataUpdate(ctx context.Context, gf *globalFlags, state esphttp.StateStore, cfg esphttp.LiveAgentConfig, action liveAgentAction) (bool, error) {
	if _, ok := state.(esphttp.GroupMetadataStore); !ok {
		return false, errors.New("live action metadata.update requires group metadata store")
	}
	if err := liveActionRequireGroupFounderPublisher(ctx, gf, cfg.GroupID, cfg.MemberID); err != nil {
		return false, err
	}
	raw := bytes.TrimSpace(action.Metadata)
	if len(raw) == 0 {
		return false, errors.New("live action metadata.update requires metadata")
	}
	metadata, err := esphttp.NormalizeGroupMetadata(raw)
	if err != nil {
		return false, err
	}
	if cfg.MaxActionBytes > 0 && len(metadata) > cfg.MaxActionBytes {
		return false, errors.New("live action metadata.update payload exceeds max_action_bytes")
	}
	exec := liveActionESPOperationExecutor(gf, state)
	if _, err := exec.updateGroup(ctx, esphttp.SignRequest{
		Kind:    "group_update",
		GroupID: cfg.GroupID,
		Payload: append(json.RawMessage(nil), metadata...),
	}); err != nil {
		return false, err
	}
	return true, nil
}

func liveActionESPOperationExecutor(gf *globalFlags, state esphttp.StateStore) espOperationExecutor {
	exec := espOperationExecutor{
		dataDir:    gf.data,
		socketPath: controlSocketPath(gf.data),
		timeout:    30 * time.Second,
		stateStore: state,
	}
	if metadataStore, ok := state.(esphttp.GroupMetadataStore); ok {
		exec.metadataStore = metadataStore
	}
	return exec
}

func liveActionRequireGroupFounderPublisher(ctx context.Context, gf *globalFlags, groupID entmoot.GroupID, nodeID entmoot.MemberID) error {
	info, err := infoOverIPCContext(ctx, controlSocketPath(gf.data))
	if err != nil {
		return fmt.Errorf("%w: %v", errLiveActionTransport, err)
	}
	infoMemberID, err := entmoot.MemberIDFromPublicKey(info.EntmootPubKey)
	if err != nil || infoMemberID != nodeID {
		return fmt.Errorf("live action metadata.update requires the local publisher to match live member %s", nodeID.String())
	}
	group, err := membership.Open(gf.data, groupID)
	if err != nil {
		return fmt.Errorf("live action metadata.update requires group membership: %w", err)
	}
	defer group.Close()
	founder := group.Founder()
	if !bytes.Equal(founder.EntmootPubKey, info.EntmootPubKey) {
		return errors.New("live action metadata.update requires the local publisher to match the group founder")
	}
	return nil
}

func liveMessageAuthorMemberID(msg entmoot.Message) entmoot.MemberID {
	if msg.Author.MemberID != nil {
		return *msg.Author.MemberID
	}
	memberID, _ := entmoot.MemberIDFromPublicKey(msg.Author.EntmootPubKey)
	return memberID
}

func liveMessageKeyAfterCursor(msg entmoot.Message, cursor esphttp.LiveAgentCursor) bool {
	if msg.Timestamp != cursor.LastSeenAtMS {
		return msg.Timestamp > cursor.LastSeenAtMS
	}
	authorID := liveMessageAuthorMemberID(msg)
	if authorID != cursor.LastSeenAuthorMemberID {
		return bytes.Compare(authorID[:], cursor.LastSeenAuthorMemberID[:]) > 0
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
		cursor.LastSeenAuthorMemberID = liveMessageAuthorMemberID(msg)
		cursor.LastSeenMessageID = msg.ID
	}
	return cursor
}

func cursorFromMessage(gid entmoot.GroupID, nodeID entmoot.MemberID, msg entmoot.Message) esphttp.LiveAgentCursor {
	return esphttp.LiveAgentCursor{
		GroupID:                gid,
		MemberID:               nodeID,
		LastSeenAtMS:           msg.Timestamp,
		LastSeenAuthorMemberID: liveMessageAuthorMemberID(msg),
		LastSeenMessageID:      msg.ID,
		SeenMessageIDs:         []entmoot.MessageID{msg.ID},
	}
}

func liveCursorsEqual(a, b esphttp.LiveAgentCursor) bool {
	return a.LastSeenAtMS == b.LastSeenAtMS &&
		a.LastSeenAuthorMemberID == b.LastSeenAuthorMemberID &&
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

func liveMessageMentionsAgent(msg entmoot.Message, memberID entmoot.MemberID) bool {
	content := strings.ToLower(string(msg.Content))
	id := strings.ToLower(memberID.String())
	return containsLiveMentionToken(content, "@"+id) ||
		containsLiveMentionToken(content, "@agent-"+id) ||
		containsLiveMentionToken(content, "member:"+id)
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
		MessageID:      msg.ID,
		AuthorMemberID: liveMessageAuthorMemberID(msg),
		Topics:         append([]string(nil), msg.Topics...),
		Content:        string(msg.Content),
		TimestampMS:    msg.Timestamp,
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
