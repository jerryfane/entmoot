package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
)

func TestRunAgentCommandRunnerHandlesCompletedJSON(t *testing.T) {
	runner := filepath.Join(t.TempDir(), "agent-runner.sh")
	script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"status\":\"completed\",\"summary\":\"done\",\"output\":\"ok\"}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	result := runAgentCommandRunner(context.Background(), runner, t.TempDir(), testCommandPayload("cmd-runner"))
	if result.status != esphttp.FleetCommandStatusCompleted || result.summary != "done" || result.output != "ok" {
		t.Fatalf("result = %+v, want completed/done/ok", result)
	}
}

func TestRunAgentCommandRunnerInvalidStatusFails(t *testing.T) {
	runner := filepath.Join(t.TempDir(), "agent-runner.sh")
	script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"status\":\"typo\"}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	result := runAgentCommandRunner(context.Background(), runner, t.TempDir(), testCommandPayload("cmd-invalid"))
	if result.status != esphttp.FleetCommandStatusFailed || result.summary != "Agent runtime returned invalid status" {
		t.Fatalf("result = %+v, want invalid-status failure", result)
	}
}

func TestRunAgentCommandRunnerNonTerminalStatusFails(t *testing.T) {
	runner := filepath.Join(t.TempDir(), "agent-runner.sh")
	script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"status\":\"running\"}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	result := runAgentCommandRunner(context.Background(), runner, t.TempDir(), testCommandPayload("cmd-non-terminal"))
	if result.status != esphttp.FleetCommandStatusFailed || result.summary != "Agent runtime returned non-terminal status" {
		t.Fatalf("result = %+v, want non-terminal failure", result)
	}
}

func TestAgentCommandRunOnceDefersResultWhenPublishFails(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	payload := testCommandPayload("cmd-run-once")
	if _, created, err := state.EnqueueAgentCommand(ctx, payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand created/err = %v/%v", created, err)
	}
	runner := filepath.Join(t.TempDir(), "agent-runner.sh")
	script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"status\":\"completed\",\"summary\":\"ran\",\"output\":\"ok\"}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	cfg := agentCommandsConfig{runner: runner, lease: time.Minute, maxAttempts: 3, once: true}
	processed, err := runAgentCommandScan(ctx, &globalFlags{data: dataDir}, state, cfg, "owner-a")
	if err != nil {
		t.Fatalf("runAgentCommandScan: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	rec, ok, err := state.GetAgentCommand(ctx, payload.CommandID)
	if err != nil || !ok {
		t.Fatalf("GetAgentCommand ok/err = %v/%v", ok, err)
	}
	if rec.Status != esphttp.AgentCommandStatusResultPending || len(rec.Result) == 0 || rec.LastError == "" {
		t.Fatalf("record = %+v, want result pending with stored result", rec)
	}
}

func TestAgentCommandRunOnceExecutesSingleAllowedAttempt(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	payload := testCommandPayload("cmd-single-attempt")
	if _, created, err := state.EnqueueAgentCommand(ctx, payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand created/err = %v/%v", created, err)
	}
	runner := filepath.Join(t.TempDir(), "agent-runner.sh")
	script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"status\":\"completed\",\"summary\":\"single attempt ran\"}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	cfg := agentCommandsConfig{runner: runner, lease: time.Minute, maxAttempts: 1, once: true}
	if _, err := runAgentCommandScan(ctx, &globalFlags{data: dataDir}, state, cfg, "owner-a"); err != nil {
		t.Fatalf("runAgentCommandScan: %v", err)
	}
	rec, ok, err := state.GetAgentCommand(ctx, payload.CommandID)
	if err != nil || !ok {
		t.Fatalf("GetAgentCommand ok/err = %v/%v", ok, err)
	}
	var result esphttp.FleetCommandResultEnvelope
	if err := json.Unmarshal(rec.Result, &result); err != nil {
		t.Fatalf("result JSON: %v", err)
	}
	if result.Status != esphttp.FleetCommandStatusCompleted || result.Summary != "single attempt ran" {
		t.Fatalf("result = %+v, want completed single attempt", result)
	}
}

func TestAgentCommandRunOnceFailsExhaustedStaleProcessingWithoutRunner(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	payload := testCommandPayload("cmd-exhausted")
	if _, created, err := state.EnqueueAgentCommand(ctx, payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand created/err = %v/%v", created, err)
	}
	now := time.Now().Add(-time.Minute).UnixMilli()
	if _, ok, err := state.ClaimNextAgentCommand(ctx, "owner-a", now, now, 1); err != nil || !ok {
		t.Fatalf("ClaimNextAgentCommand first ok/err = %v/%v", ok, err)
	}
	cfg := agentCommandsConfig{runner: filepath.Join(t.TempDir(), "missing-runner"), lease: time.Minute, maxAttempts: 1, once: true}
	processed, err := runAgentCommandScan(ctx, &globalFlags{data: dataDir}, state, cfg, "owner-b")
	if err != nil {
		t.Fatalf("runAgentCommandScan: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	rec, ok, err := state.GetAgentCommand(ctx, payload.CommandID)
	if err != nil || !ok {
		t.Fatalf("GetAgentCommand ok/err = %v/%v", ok, err)
	}
	if rec.Status != esphttp.AgentCommandStatusResultPending {
		t.Fatalf("status = %q, want result_pending", rec.Status)
	}
	var result esphttp.FleetCommandResultEnvelope
	if err := json.Unmarshal(rec.Result, &result); err != nil {
		t.Fatalf("result JSON: %v", err)
	}
	if result.Status != esphttp.FleetCommandStatusFailed || result.Summary != "Agent command retry limit exhausted" {
		t.Fatalf("result = %+v, want retry-limit failure", result)
	}
}

func TestAgentCommandRunOnceRepublishesPendingResultWithoutRunner(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	payload := testCommandPayload("cmd-pending-result")
	if _, created, err := state.EnqueueAgentCommand(ctx, payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand created/err = %v/%v", created, err)
	}
	if _, ok, err := state.ClaimNextAgentCommand(ctx, "owner-a", 2_000, 3_000, 3); err != nil || !ok {
		t.Fatalf("ClaimNextAgentCommand ok/err = %v/%v", ok, err)
	}
	storedResult := agentCommandResult(payload, esphttp.FleetCommandStatusCompleted, "stored result", "", 2_000)
	storedJSON, err := json.Marshal(storedResult)
	if err != nil {
		t.Fatalf("Marshal stored result: %v", err)
	}
	if ok, err := state.DeferAgentCommandResult(ctx, payload.CommandID, "owner-a", storedJSON, "publish failed", 1, 2_500); err != nil || !ok {
		t.Fatalf("DeferAgentCommandResult ok/err = %v/%v", ok, err)
	}
	marker := filepath.Join(t.TempDir(), "runner-called")
	runner := filepath.Join(t.TempDir(), "agent-runner.sh")
	script := "#!/bin/sh\ntouch " + marker + "\nprintf '%s' '{\"status\":\"completed\",\"summary\":\"reran\"}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	cfg := agentCommandsConfig{runner: runner, lease: time.Minute, maxAttempts: 3, once: true}
	if _, err := runAgentCommandScan(ctx, &globalFlags{data: dataDir}, state, cfg, "owner-b"); err != nil {
		t.Fatalf("runAgentCommandScan: %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("runner marker err = %v, want not exist", err)
	}
	rec, ok, err := state.GetAgentCommand(ctx, payload.CommandID)
	if err != nil || !ok {
		t.Fatalf("GetAgentCommand ok/err = %v/%v", ok, err)
	}
	var result esphttp.FleetCommandResultEnvelope
	if err := json.Unmarshal(rec.Result, &result); err != nil {
		t.Fatalf("result JSON: %v", err)
	}
	if result.Summary != "stored result" {
		t.Fatalf("result summary = %q, want stored result", result.Summary)
	}
}

func TestImportLegacyAgentCommandFiles(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	commandDir := filepath.Join(t.TempDir(), "agent-commands")
	t.Setenv("ENTMOOT_AGENT_COMMAND_DIR", commandDir)
	inbox := filepath.Join(commandDir, "inbox")
	if err := os.MkdirAll(inbox, 0o700); err != nil {
		t.Fatalf("MkdirAll inbox: %v", err)
	}
	payload := testCommandPayload("cmd-legacy")
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("jsonMarshal: %v", err)
	}
	path := filepath.Join(inbox, "cmd.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile legacy command: %v", err)
	}
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	if err := importLegacyAgentCommandFiles(ctx, dataDir, state); err != nil {
		t.Fatalf("importLegacyAgentCommandFiles: %v", err)
	}
	if _, ok, err := state.GetAgentCommand(ctx, payload.CommandID); err != nil || !ok {
		t.Fatalf("GetAgentCommand imported ok/err = %v/%v", ok, err)
	}
	if _, err := os.Stat(path + ".imported"); err != nil {
		t.Fatalf("imported marker missing: %v", err)
	}
}

func testCommandPayload(commandID string) esphttp.AgentInstructionPayload {
	var gid entmoot.GroupID
	gid[0] = 0x55
	cmd := esphttp.FleetCommandEnvelope{
		Type:           esphttp.FleetCommandMessageType,
		Version:        1,
		CommandID:      commandID,
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerNodeID:   45981,
		Target:         esphttp.FleetCommandTarget{Kind: esphttp.FleetCommandTargetNode, PilotNodeID: 133053},
		Action:         esphttp.FleetCommandActionAgentInstruction,
		CreatedAtMS:    1_000,
		Args: map[string]interface{}{
			"instruction": "report status",
			"timeout_ms":  float64(60_000),
		},
	}
	return esphttp.NewAgentInstructionPayload(cmd, 133053, "report status", nil, 60_000, 1_500)
}
