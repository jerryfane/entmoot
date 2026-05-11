package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
)

type agentCommandsConfig struct {
	interval    time.Duration
	lease       time.Duration
	maxAttempts int
	runner      string
	once        bool
	json        bool
}

type agentInstructionRunnerResult struct {
	Status  string `json:"status"`
	Summary string `json:"summary"`
	Output  string `json:"output"`
}

type agentRuntimeProcessResult struct {
	stdout string
	stderr string
	err    error
}

type openClawAgentInstructionContext struct {
	CommandID      string                               `json:"command_id"`
	FleetID        string                               `json:"fleet_id"`
	ControlGroupID entmoot.GroupID                      `json:"control_group_id"`
	IssuerNodeID   entmoot.NodeID                       `json:"issuer_node_id"`
	Target         esphttp.FleetCommandTarget           `json:"target"`
	AgentNodeID    entmoot.NodeID                       `json:"agent_node_id"`
	Action         string                               `json:"action"`
	Context        map[string]interface{}               `json:"context"`
	Actions        []esphttp.FleetCommandExternalAction `json:"actions,omitempty"`
	Success        string                               `json:"success,omitempty"`
}

type openClawAgentRunReport struct {
	RunID   string          `json:"runId"`
	Status  string          `json:"status"`
	Summary string          `json:"summary"`
	Result  json.RawMessage `json:"result"`
	Meta    struct {
		FinalAssistantVisibleText string `json:"finalAssistantVisibleText"`
		FinalAssistantRawText     string `json:"finalAssistantRawText"`
	} `json:"meta"`
}

type openClawAgentRunResult struct {
	Payloads []openClawAgentPayload `json:"payloads"`
}

type openClawAgentPayload struct {
	Text     string `json:"text,omitempty"`
	MediaURL string `json:"mediaUrl,omitempty"`
}

type openClawAgentCompactOutput struct {
	Runner    string                   `json:"runner"`
	RunID     string                   `json:"run_id,omitempty"`
	Status    string                   `json:"status,omitempty"`
	Summary   string                   `json:"summary,omitempty"`
	FinalText string                   `json:"final_text,omitempty"`
	Payloads  []openClawAgentPayload   `json:"payloads,omitempty"`
	Actions   []openClawActionEvidence `json:"actions,omitempty"`
}

type openClawActionEvidence struct {
	ID               string          `json:"id,omitempty"`
	Kind             string          `json:"kind"`
	Channel          string          `json:"channel,omitempty"`
	Target           string          `json:"target,omitempty"`
	Required         bool            `json:"required,omitempty"`
	DeliveryRequired bool            `json:"delivery_required,omitempty"`
	Confirmed        bool            `json:"confirmed"`
	Receipt          json.RawMessage `json:"receipt,omitempty"`
}

const (
	agentCommandRunnerOpenClaw = "openclaw"
	openClawSelectorError      = "Pass --to <E.164>, --session-id, or --agent to choose a session"
)

func cmdAgentCommands(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd agent-commands <watch|run-once|status> [flags]")
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "agent-commands: missing op")
			return exitInvalidArgument
		}
		return exitOK
	}
	switch args[0] {
	case "watch":
		return cmdAgentCommandsWatch(gf, args[1:], false)
	case "run-once":
		return cmdAgentCommandsWatch(gf, args[1:], true)
	case "status":
		return cmdAgentCommandsStatus(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "agent-commands: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdAgentCommandsStatus(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("agent-commands status", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-commands status: %v\n", err)
		return exitTransport
	}
	defer state.Close()
	stats, err := state.AgentCommandStats(context.Background(), time.Now().UnixMilli())
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-commands status: %v\n", err)
		return exitTransport
	}
	return printJSON(stats)
}

func cmdAgentCommandsWatch(gf *globalFlags, args []string, once bool) int {
	fs := flag.NewFlagSet("agent-commands watch", flag.ContinueOnError)
	cfg := agentCommandsConfig{
		interval:    10 * time.Second,
		lease:       defaultAgentCommandLease(),
		maxAttempts: 3,
		runner:      firstNonEmpty(os.Getenv("ENTMOOT_AGENT_RUNNER"), os.Getenv("ENTMOOT_AGENT_COMMAND_HOOK")),
		once:        once,
	}
	fs.DurationVar(&cfg.interval, "interval", cfg.interval, "poll interval")
	fs.DurationVar(&cfg.lease, "lease", cfg.lease, "processing lease duration")
	fs.IntVar(&cfg.maxAttempts, "max-attempts", cfg.maxAttempts, "maximum attempts per command")
	fs.StringVar(&cfg.runner, "runner", cfg.runner, "agent runtime adapter executable, or \"openclaw\" for the built-in OpenClaw adapter")
	fs.BoolVar(&cfg.once, "once", cfg.once, "scan and process available commands once")
	fs.BoolVar(&cfg.json, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	cfg.runner = strings.TrimSpace(cfg.runner)
	if cfg.runner == "" {
		fmt.Fprintln(os.Stderr, "agent-commands watch: -runner or ENTMOOT_AGENT_RUNNER is required")
		return exitInvalidArgument
	}
	if cfg.interval <= 0 {
		fmt.Fprintln(os.Stderr, "agent-commands watch: -interval must be positive")
		return exitInvalidArgument
	}
	if cfg.lease <= 0 {
		fmt.Fprintln(os.Stderr, "agent-commands watch: -lease must be positive")
		return exitInvalidArgument
	}
	if cfg.maxAttempts <= 0 {
		fmt.Fprintln(os.Stderr, "agent-commands watch: -max-attempts must be positive")
		return exitInvalidArgument
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-commands watch: %v\n", err)
		return exitTransport
	}
	defer state.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	owner := agentCommandLeaseOwner()
	processed, err := runAgentCommandScan(ctx, gf, state, cfg, owner)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-commands watch: %v\n", err)
		return exitTransport
	}
	if cfg.once {
		if cfg.json {
			return printJSON(map[string]any{"processed": processed})
		}
		return exitOK
	}
	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return exitOK
		case <-ticker.C:
			if _, err := runAgentCommandScan(ctx, gf, state, cfg, owner); err != nil {
				fmt.Fprintf(os.Stderr, "agent-commands watch: %v\n", err)
			}
		}
	}
}

func runAgentCommandScan(ctx context.Context, gf *globalFlags, state *esphttp.SQLiteStateStore, cfg agentCommandsConfig, owner string) (int, error) {
	if err := importLegacyAgentCommandFiles(ctx, gf.data, state); err != nil {
		return 0, err
	}
	processed := 0
	for {
		now := time.Now().UnixMilli()
		rec, ok, err := state.ClaimNextAgentCommand(ctx, owner, now, now+cfg.lease.Milliseconds(), cfg.maxAttempts)
		if err != nil {
			return processed, err
		}
		if !ok {
			return processed, nil
		}
		processed++
		if err := processClaimedAgentCommand(ctx, gf, state, cfg, owner, rec); err != nil {
			return processed, err
		}
	}
}

func processClaimedAgentCommand(ctx context.Context, gf *globalFlags, state *esphttp.SQLiteStateStore, cfg agentCommandsConfig, owner string, rec esphttp.AgentCommandRecord) error {
	payload := rec.Payload
	startedAt := rec.StartedAtMS
	if startedAt == 0 {
		startedAt = time.Now().UnixMilli()
	}
	if rec.Status == esphttp.AgentCommandStatusResultPending {
		return publishPendingAgentCommandResult(ctx, gf, state, cfg, owner, rec)
	}
	if rec.RetryExhausted {
		result := agentCommandResult(payload, esphttp.FleetCommandStatusFailed, "Agent command retry limit exhausted", "", startedAt)
		return publishAndFinishAgentCommand(ctx, gf, state, cfg, owner, payload.ControlGroupID, result)
	}
	if payload.ExpiresAtMS > 0 && payload.ExpiresAtMS <= time.Now().UnixMilli() {
		result := agentCommandResult(payload, esphttp.FleetCommandStatusExpired, "Command expired before local agent runtime handled it", "", startedAt)
		return publishAndFinishAgentCommand(ctx, gf, state, cfg, owner, payload.ControlGroupID, result)
	}
	running := agentCommandResult(payload, esphttp.FleetCommandStatusRunning, "Agent runtime started", "", startedAt)
	_ = publishAgentCommandResult(ctx, gf, payload.ControlGroupID, running)
	timeout := time.Duration(payload.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = time.Duration(esphttp.DefaultFleetInstructionTimeoutMS) * time.Millisecond
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	execResult := runAgentCommandRunner(runCtx, cfg.runner, gf.data, payload)
	entry, _ := esphttp.FleetCommandCatalogLookup(esphttp.FleetCommandActionAgentInstruction)
	execResult.output = truncateCommandOutput(execResult.output, entry.MaxOutputBytes)
	result := agentCommandResult(payload, execResult.status, execResult.summary, execResult.output, startedAt)
	return publishAndFinishAgentCommand(ctx, gf, state, cfg, owner, payload.ControlGroupID, result)
}

func runAgentCommandRunner(ctx context.Context, runner, dataDir string, payload esphttp.AgentInstructionPayload) fleetCommandExecution {
	if strings.EqualFold(strings.TrimSpace(runner), agentCommandRunnerOpenClaw) {
		return runOpenClawAgentInstruction(ctx, dataDir, payload)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "Agent command encode failed", output: err.Error()}
	}
	cmd := exec.CommandContext(ctx, runner)
	run := runAgentRuntimeProcess(cmd, data, dataDir, payload)
	if ctx.Err() != nil {
		return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "Agent runtime timed out", output: strings.TrimSpace(run.stderr)}
	}
	if run.err != nil {
		output := strings.TrimSpace(run.stderr)
		if output == "" {
			output = run.err.Error()
		}
		output = addAgentRuntimeFailureAdvice(output)
		return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "Agent runtime failed", output: output}
	}
	return parseAgentInstructionRunnerResult(run.stdout)
}

func runOpenClawAgentInstruction(ctx context.Context, dataDir string, payload esphttp.AgentInstructionPayload) fleetCommandExecution {
	args, err := openClawAgentArgs(payload)
	if err != nil {
		return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "OpenClaw agent instruction context encode failed", output: err.Error()}
	}
	cmd := exec.CommandContext(ctx, openClawBinary(), args...)
	run := runAgentRuntimeProcess(cmd, nil, dataDir, payload)
	if ctx.Err() != nil {
		return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "OpenClaw agent instruction timed out", output: strings.TrimSpace(run.stderr)}
	}
	if run.err != nil {
		output := strings.TrimSpace(run.stderr)
		if output == "" {
			output = run.err.Error()
		}
		return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "OpenClaw agent instruction failed", output: addAgentRuntimeFailureAdvice(output)}
	}
	output := compactOpenClawAgentOutput(run.stdout, payload.Actions)
	if openClawRequiredActionMissing(run.stdout, payload.Actions) {
		return fleetCommandExecution{
			status:  esphttp.FleetCommandStatusFailed,
			summary: "OpenClaw completed without required external action evidence",
			output:  output,
		}
	}
	return fleetCommandExecution{
		status:  esphttp.FleetCommandStatusCompleted,
		summary: "OpenClaw handled agent instruction",
		output:  output,
	}
}

func runAgentRuntimeProcess(cmd *exec.Cmd, stdin []byte, dataDir string, payload esphttp.AgentInstructionPayload) agentRuntimeProcessResult {
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	cmd.Env = agentCommandRunnerEnv(dataDir, payload)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return agentRuntimeProcessResult{
		stdout: stdout.String(),
		stderr: stderr.String(),
		err:    err,
	}
}

func agentCommandRunnerEnv(dataDir string, payload esphttp.AgentInstructionPayload) []string {
	return append(os.Environ(),
		"ENTMOOT_AGENT_COMMAND_ID="+payload.CommandID,
		"ENTMOOT_AGENT_FLEET_ID="+payload.FleetID,
		"ENTMOOT_AGENT_CONTROL_GROUP_ID="+payload.ControlGroupID.String(),
		"ENTMOOT_AGENT_NODE_ID="+fmt.Sprintf("%d", payload.AgentNodeID),
		"ENTMOOT_AGENT_DATA_DIR="+dataDir,
	)
}

func parseAgentInstructionRunnerResult(stdout string) fleetCommandExecution {
	var result agentInstructionRunnerResult
	if len(bytes.TrimSpace([]byte(stdout))) > 0 {
		if err := json.Unmarshal([]byte(stdout), &result); err != nil {
			return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "Agent runtime returned invalid JSON", output: err.Error()}
		}
	}
	status := esphttp.NormalizeFleetCommandResultStatus(result.Status)
	if strings.TrimSpace(result.Status) == "" {
		status = esphttp.FleetCommandStatusCompleted
	} else if status == "" {
		return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "Agent runtime returned invalid status", output: result.Status}
	} else if !fleetCommandStatusIsTerminal(status) {
		return fleetCommandExecution{status: esphttp.FleetCommandStatusFailed, summary: "Agent runtime returned non-terminal status", output: result.Status}
	}
	summary := strings.TrimSpace(result.Summary)
	if summary == "" {
		summary = "Agent instruction handled"
	}
	return fleetCommandExecution{status: status, summary: summary, output: result.Output}
}

func openClawBinary() string {
	if bin := strings.TrimSpace(os.Getenv("OPENCLAW_BIN")); bin != "" {
		return bin
	}
	return "openclaw"
}

func openClawAgentArgs(payload esphttp.AgentInstructionPayload) ([]string, error) {
	selectorFlag, selectorValue := openClawAgentSelector()
	message, err := openClawAgentMessage(payload)
	if err != nil {
		return nil, err
	}
	args := []string{
		"agent",
		selectorFlag, selectorValue,
		"--message", message,
		"--json",
		"--timeout", fmt.Sprintf("%d", agentCommandTimeoutSeconds(payload.TimeoutMS)),
	}
	deliveryAction, ok, err := openClawDeliveryAction(payload.Actions)
	if err != nil {
		return nil, err
	}
	if ok {
		args = append(args, "--deliver", "--reply-channel", deliveryAction.Channel, "--reply-to", deliveryAction.Target)
	}
	return args, nil
}

func openClawAgentMessage(payload esphttp.AgentInstructionPayload) (string, error) {
	context := openClawAgentInstructionContext{
		CommandID:      payload.CommandID,
		FleetID:        payload.FleetID,
		ControlGroupID: payload.ControlGroupID,
		IssuerNodeID:   payload.IssuerNodeID,
		Target:         payload.Target,
		AgentNodeID:    payload.AgentNodeID,
		Action:         payload.Action,
		Context:        payload.Context,
		Actions:        payload.Actions,
		Success:        "External action success requires tool or delivery evidence, not agent prose.",
	}
	data, err := json.Marshal(context)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(payload.Instruction) + "\n\nEntmoot fleet command context JSON:\n" + string(data), nil
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

func openClawDeliveryAction(actions []esphttp.FleetCommandExternalAction) (esphttp.FleetCommandExternalAction, bool, error) {
	var selected esphttp.FleetCommandExternalAction
	found := false
	for _, action := range actions {
		if action.Kind != esphttp.FleetCommandExternalActionMessageSend || (!action.Required && !action.DeliveryRequired) {
			continue
		}
		if found {
			return esphttp.FleetCommandExternalAction{}, false, fmt.Errorf("OpenClaw adapter supports one delivery-required message.send action per instruction")
		}
		selected = action
		found = true
	}
	return selected, found, nil
}

func compactOpenClawAgentOutput(stdout string, actions []esphttp.FleetCommandExternalAction) string {
	stdout = strings.TrimSpace(stdout)
	if stdout == "" {
		return ""
	}
	var report openClawAgentRunReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		return stdout
	}
	if strings.TrimSpace(report.RunID) == "" && strings.TrimSpace(report.Status) == "" && strings.TrimSpace(report.Summary) == "" && len(report.Result) == 0 && strings.TrimSpace(report.Meta.FinalAssistantVisibleText) == "" && strings.TrimSpace(report.Meta.FinalAssistantRawText) == "" {
		return stdout
	}
	output := openClawAgentCompactOutput{
		Runner:    agentCommandRunnerOpenClaw,
		RunID:     strings.TrimSpace(report.RunID),
		Status:    strings.TrimSpace(report.Status),
		Summary:   strings.TrimSpace(report.Summary),
		FinalText: openClawFinalText(report),
		Payloads:  openClawPayloads(report.Result),
		Actions:   openClawActionEvidenceFromResult(report.Result, actions),
	}
	data, err := json.Marshal(output)
	if err != nil {
		return stdout
	}
	return string(data)
}

func openClawFinalText(report openClawAgentRunReport) string {
	if text := strings.TrimSpace(report.Meta.FinalAssistantVisibleText); text != "" {
		return text
	}
	return strings.TrimSpace(report.Meta.FinalAssistantRawText)
}

func openClawPayloads(result json.RawMessage) []openClawAgentPayload {
	var parsed openClawAgentRunResult
	if len(result) == 0 || json.Unmarshal(result, &parsed) != nil {
		return nil
	}
	payloads := make([]openClawAgentPayload, 0, len(parsed.Payloads))
	for _, payload := range parsed.Payloads {
		payload.Text = strings.TrimSpace(payload.Text)
		payload.MediaURL = strings.TrimSpace(payload.MediaURL)
		if payload.Text == "" && payload.MediaURL == "" {
			continue
		}
		payloads = append(payloads, payload)
	}
	return payloads
}

func openClawActionEvidenceFromResult(result json.RawMessage, actions []esphttp.FleetCommandExternalAction) []openClawActionEvidence {
	if len(actions) == 0 {
		return nil
	}
	receipt := openClawDeliveryReceipt(result)
	deliveredAction, deliverySelected, _ := openClawDeliveryAction(actions)
	evidence := make([]openClawActionEvidence, 0, len(actions))
	for _, action := range actions {
		confirmed := deliverySelected && openClawSameExternalAction(action, deliveredAction) && len(receipt) > 0
		ev := openClawActionEvidence{
			ID:               action.ID,
			Kind:             action.Kind,
			Channel:          action.Channel,
			Target:           action.Target,
			Required:         action.Required,
			DeliveryRequired: action.DeliveryRequired,
			Confirmed:        confirmed,
		}
		if ev.Confirmed {
			ev.Receipt = append(json.RawMessage(nil), receipt...)
		}
		evidence = append(evidence, ev)
	}
	return evidence
}

func openClawSameExternalAction(a, b esphttp.FleetCommandExternalAction) bool {
	return a.ID == b.ID &&
		a.Kind == b.Kind &&
		a.Channel == b.Channel &&
		a.Target == b.Target &&
		a.Required == b.Required &&
		a.DeliveryRequired == b.DeliveryRequired
}

func openClawRequiredActionMissing(stdout string, actions []esphttp.FleetCommandExternalAction) bool {
	required := false
	for _, action := range actions {
		if action.Required || action.DeliveryRequired {
			required = true
			break
		}
	}
	if !required {
		return false
	}
	var report openClawAgentRunReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &report); err != nil {
		return true
	}
	return len(openClawDeliveryReceipt(report.Result)) == 0
}

func openClawDeliveryReceipt(result json.RawMessage) json.RawMessage {
	if len(result) == 0 {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(result, &fields); err != nil {
		return nil
	}
	for _, key := range []string{"delivery", "deliveries", "delivery_receipts", "deliveryReceipts", "receipt", "receipts"} {
		raw := bytes.TrimSpace(fields[key])
		if openClawReceiptHasEvidence(raw) {
			return raw
		}
	}
	return nil
}

func openClawReceiptHasEvidence(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err == nil {
		for _, item := range arr {
			if openClawReceiptHasEvidence(item) {
				return true
			}
		}
		return false
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		return openClawReceiptObjectHasEvidence(obj)
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		switch strings.ToLower(strings.TrimSpace(text)) {
		case "", "false", "no", "none", "null":
			return false
		default:
			return true
		}
	}
	var delivered bool
	if err := json.Unmarshal(raw, &delivered); err == nil {
		return delivered
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err == nil {
		value, err := number.Float64()
		return err == nil && value != 0
	}
	return false
}

func openClawReceiptObjectHasEvidence(obj map[string]json.RawMessage) bool {
	if len(obj) == 0 {
		return false
	}
	success := false
	for _, key := range []string{"ok", "success", "sent", "delivered", "delivery_success", "deliverySuccess"} {
		if raw, ok := obj[key]; ok {
			value, parsed := openClawBoolReceiptField(raw)
			if parsed {
				if !value {
					return false
				}
				success = true
			}
		}
	}
	for _, key := range []string{"status", "state", "result"} {
		if raw, ok := obj[key]; ok {
			value, parsed := openClawStringReceiptField(raw)
			if !parsed {
				continue
			}
			switch value {
			case "failed", "failure", "error", "rejected", "blocked", "undelivered", "not_delivered", "not-delivered", "false", "no":
				return false
			case "ok", "success", "succeeded", "sent", "delivered", "completed", "true", "yes":
				success = true
			}
		}
	}
	for _, key := range []string{"error", "errors", "failure"} {
		if raw, ok := obj[key]; ok && openClawReceiptHasEvidence(raw) && !success {
			return false
		}
	}
	return true
}

func openClawBoolReceiptField(raw json.RawMessage) (bool, bool) {
	var value bool
	if err := json.Unmarshal(raw, &value); err == nil {
		return value, true
	}
	text, ok := openClawStringReceiptField(raw)
	if !ok {
		return false, false
	}
	switch text {
	case "true", "yes", "ok", "success", "sent", "delivered":
		return true, true
	case "false", "no", "failed", "failure", "error", "blocked", "undelivered", "not_delivered", "not-delivered":
		return false, true
	default:
		return false, false
	}
}

func openClawStringReceiptField(raw json.RawMessage) (string, bool) {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return "", false
	}
	return strings.ToLower(strings.TrimSpace(text)), true
}

func agentCommandTimeoutSeconds(timeoutMS int64) int64 {
	if timeoutMS <= 0 {
		return esphttp.DefaultFleetInstructionTimeoutMS / 1000
	}
	seconds := timeoutMS / 1000
	if seconds < 5 {
		return 5
	}
	if seconds > 1800 {
		return 1800
	}
	return seconds
}

func addAgentRuntimeFailureAdvice(output string) string {
	if !strings.Contains(output, openClawSelectorError) {
		return output
	}
	const advice = "Entmoot fix: use the built-in OpenClaw adapter with ENTMOOT_AGENT_RUNNER=openclaw and set ENTMOOT_OPENCLAW_AGENT, ENTMOOT_OPENCLAW_SESSION_ID, or ENTMOOT_OPENCLAW_TO as needed. Without an explicit selector, the built-in adapter defaults to ENTMOOT_OPENCLAW_AGENT=main."
	if strings.Contains(output, advice) {
		return output
	}
	if strings.TrimSpace(output) == "" {
		return advice
	}
	return strings.TrimSpace(output) + "\n\n" + advice
}

func publishPendingAgentCommandResult(ctx context.Context, gf *globalFlags, state *esphttp.SQLiteStateStore, cfg agentCommandsConfig, owner string, rec esphttp.AgentCommandRecord) error {
	var result esphttp.FleetCommandResultEnvelope
	if len(rec.Result) == 0 {
		result = agentCommandResult(rec.Payload, esphttp.FleetCommandStatusFailed, "Agent command result was missing", rec.LastError, rec.StartedAtMS)
	} else if err := json.Unmarshal(rec.Result, &result); err != nil {
		result = agentCommandResult(rec.Payload, esphttp.FleetCommandStatusFailed, "Agent command result was corrupt", err.Error(), rec.StartedAtMS)
	}
	return publishAndFinishAgentCommand(ctx, gf, state, cfg, owner, rec.Payload.ControlGroupID, result)
}

func publishAndFinishAgentCommand(ctx context.Context, gf *globalFlags, state *esphttp.SQLiteStateStore, cfg agentCommandsConfig, owner string, groupID entmoot.GroupID, result esphttp.FleetCommandResultEnvelope) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if err := publishAgentCommandResult(ctx, gf, groupID, result); err != nil {
		retryAt := time.Now().Add(agentCommandPublishRetryDelay(cfg)).UnixMilli()
		ok, deferErr := state.DeferAgentCommandResult(ctx, result.CommandID, owner, data, err.Error(), retryAt, time.Now().UnixMilli())
		if deferErr != nil {
			return deferErr
		}
		if !ok {
			return fmt.Errorf("agent command %s lease was not owned by this watcher", result.CommandID)
		}
		return nil
	}
	return finishAgentCommand(ctx, state, owner, result, "")
}

func finishAgentCommand(ctx context.Context, state *esphttp.SQLiteStateStore, owner string, result esphttp.FleetCommandResultEnvelope, lastErr string) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	ok, err := state.FinishAgentCommand(ctx, result.CommandID, owner, result.Status, data, lastErr, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("agent command %s lease was not owned by this watcher", result.CommandID)
	}
	return nil
}

func agentCommandPublishRetryDelay(cfg agentCommandsConfig) time.Duration {
	if cfg.interval > 0 {
		return cfg.interval
	}
	return time.Second
}

func defaultAgentCommandLease() time.Duration {
	return time.Duration(esphttp.MaxFleetInstructionTimeoutMS)*time.Millisecond + time.Minute
}

func agentCommandResult(payload esphttp.AgentInstructionPayload, status, summary, output string, startedAtMS int64) esphttp.FleetCommandResultEnvelope {
	if status == "" {
		status = esphttp.FleetCommandStatusCompleted
	}
	if summary == "" {
		summary = "Agent instruction handled"
	}
	result := esphttp.FleetCommandResultEnvelope{
		Type:        esphttp.FleetCommandResultType,
		Version:     1,
		CommandID:   payload.CommandID,
		FleetID:     payload.FleetID,
		AgentNodeID: payload.AgentNodeID,
		Action:      esphttp.FleetCommandActionAgentInstruction,
		Status:      status,
		Summary:     summary,
		Output:      output,
		StartedAtMS: startedAtMS,
	}
	if fleetCommandStatusIsTerminal(status) {
		result.CompletedAtMS = time.Now().UnixMilli()
	}
	return result
}

func publishAgentCommandResult(ctx context.Context, gf *globalFlags, groupID entmoot.GroupID, result esphttp.FleetCommandResultEnvelope) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return publishIPCMessage(ctx, gf, groupID, []string{"fleet/commands/results"}, data)
}

func importLegacyAgentCommandFiles(ctx context.Context, dataDir string, state *esphttp.SQLiteStateStore) error {
	commandDir := strings.TrimSpace(os.Getenv("ENTMOOT_AGENT_COMMAND_DIR"))
	if commandDir == "" {
		commandDir = filepath.Join(dataDir, "agent-commands")
	}
	inboxDir := filepath.Join(commandDir, "inbox")
	entries, err := os.ReadDir(inboxDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read legacy agent command inbox: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(inboxDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read legacy agent command %s: %w", path, err)
		}
		var payload esphttp.AgentInstructionPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return fmt.Errorf("decode legacy agent command %s: %w", path, err)
		}
		if payload.Type == "" {
			payload.Type = esphttp.AgentInstructionPayloadType
		}
		if payload.Action == "" {
			payload.Action = esphttp.FleetCommandActionAgentInstruction
		}
		if payload.ReceivedAtMS == 0 {
			payload.ReceivedAtMS = time.Now().UnixMilli()
		}
		if _, _, err := state.EnqueueAgentCommand(ctx, payload); err != nil {
			return fmt.Errorf("import legacy agent command %s: %w", path, err)
		}
		_ = os.Rename(path, path+".imported")
	}
	return nil
}

func agentCommandLeaseOwner() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d:%d", host, os.Getpid(), time.Now().UnixNano())
}
