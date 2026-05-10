package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
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
	CommandID      string                     `json:"command_id"`
	FleetID        string                     `json:"fleet_id"`
	ControlGroupID entmoot.GroupID            `json:"control_group_id"`
	IssuerNodeID   entmoot.NodeID             `json:"issuer_node_id"`
	Target         esphttp.FleetCommandTarget `json:"target"`
	AgentNodeID    entmoot.NodeID             `json:"agent_node_id"`
	Action         string                     `json:"action"`
	Context        map[string]interface{}     `json:"context"`
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
	return fleetCommandExecution{
		status:  esphttp.FleetCommandStatusCompleted,
		summary: "OpenClaw handled agent instruction",
		output:  strings.TrimSpace(run.stdout),
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
	return []string{
		"agent",
		selectorFlag, selectorValue,
		"--message", message,
		"--json",
		"--timeout", fmt.Sprintf("%d", agentCommandTimeoutSeconds(payload.TimeoutMS)),
	}, nil
}

func openClawAgentMessage(payload esphttp.AgentInstructionPayload) (string, error) {
	if len(payload.Context) == 0 {
		return payload.Instruction, nil
	}
	context := openClawAgentInstructionContext{
		CommandID:      payload.CommandID,
		FleetID:        payload.FleetID,
		ControlGroupID: payload.ControlGroupID,
		IssuerNodeID:   payload.IssuerNodeID,
		Target:         payload.Target,
		AgentNodeID:    payload.AgentNodeID,
		Action:         payload.Action,
		Context:        payload.Context,
	}
	data, err := json.Marshal(context)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(payload.Instruction) + "\n\nEntmoot agent command context JSON:\n" + string(data), nil
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
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "unix", controlSocketPath(gf.data))
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if err := ipc.EncodeAndWrite(conn, &ipc.PublishReq{
		GroupID: &groupID,
		Topics:  []string{"fleet/commands/results"},
		Content: data,
	}); err != nil {
		return err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return err
	}
	switch v := payload.(type) {
	case *ipc.PublishResp:
		return nil
	case *ipc.ErrorFrame:
		return fmt.Errorf("ipc error %s: %s", v.Code, v.Message)
	default:
		return fmt.Errorf("unexpected ipc response %T", payload)
	}
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
