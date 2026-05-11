package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestRunAgentCommandRunnerOpenClawDefaultsToMainAgent(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	argsPath := filepath.Join(t.TempDir(), "args")
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\nprintf '%s\n' \"$@\" > \"$ARGS_PATH\"\nprintf '%s' '{\"ok\":true}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("ARGS_PATH", argsPath)
	t.Setenv("OPENCLAW_BIN", runner)
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), testCommandPayload("cmd-openclaw"))
	if result.status != esphttp.FleetCommandStatusCompleted || result.summary != "OpenClaw handled agent instruction" || result.output != `{"ok":true}` {
		t.Fatalf("result = %+v, want completed OpenClaw result", result)
	}
	data, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile args: %v", err)
	}
	got := strings.Split(strings.TrimSpace(string(data)), "\n")
	wantPrefix := []string{"agent", "--agent", "main", "--message", "report status"}
	if len(got) < len(wantPrefix) || !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("openclaw args prefix = %#v, want %#v", got, wantPrefix)
	}
	for _, want := range []string{"Entmoot fleet command context JSON:", `"command_id":"cmd-openclaw"`, "--json", "--timeout", "60"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("openclaw args = %q, want substring %q", string(data), want)
		}
	}
}

func TestRunAgentCommandRunnerOpenClawIncludesPayloadContext(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	messagePath := filepath.Join(t.TempDir(), "message")
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\nprintf '%s' \"$5\" > \"$MESSAGE_PATH\"\nprintf '%s' '{\"ok\":true}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("MESSAGE_PATH", messagePath)
	t.Setenv("OPENCLAW_BIN", runner)
	payload := testCommandPayload("cmd-openclaw-context")
	payload.Context = map[string]interface{}{
		"group":  "Mars Hub",
		"source": "fleet-command",
	}
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), payload)
	if result.status != esphttp.FleetCommandStatusCompleted {
		t.Fatalf("result = %+v, want completed OpenClaw result", result)
	}
	data, err := os.ReadFile(messagePath)
	if err != nil {
		t.Fatalf("ReadFile message: %v", err)
	}
	message := string(data)
	for _, want := range []string{
		"report status",
		"Entmoot fleet command context JSON:",
		`"command_id":"cmd-openclaw-context"`,
		`"agent_node_id":133053`,
		`"context":{"group":"Mars Hub","source":"fleet-command"}`,
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("message = %q, want substring %q", message, want)
		}
	}
}

func TestRunAgentCommandRunnerOpenClawAddsDeliveryArgsForRequiredMessageAction(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	argsPath := filepath.Join(t.TempDir(), "args")
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\nprintf '%s\n' \"$@\" > \"$ARGS_PATH\"\nprintf '%s' '{\"runId\":\"run-1\",\"status\":\"ok\",\"summary\":\"completed\",\"result\":{\"delivery\":{\"message_id\":\"m-1\"}}}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("ARGS_PATH", argsPath)
	t.Setenv("OPENCLAW_BIN", runner)
	payload := testCommandPayload("cmd-openclaw-delivery")
	payload.Actions = []esphttp.FleetCommandExternalAction{{
		ID:               "notify-owner",
		Kind:             esphttp.FleetCommandExternalActionMessageSend,
		Channel:          "telegram",
		Target:           "owner",
		Required:         true,
		DeliveryRequired: true,
	}}
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), payload)
	if result.status != esphttp.FleetCommandStatusCompleted {
		t.Fatalf("result = %+v, want completed OpenClaw result", result)
	}
	data, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile args: %v", err)
	}
	for _, want := range []string{"--deliver", "--reply-channel\ntelegram", "--reply-to\nowner"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("openclaw args = %q, want substring %q", string(data), want)
		}
	}
	var compact openClawAgentCompactOutput
	if err := json.Unmarshal([]byte(result.output), &compact); err != nil {
		t.Fatalf("compact output JSON: %v; output=%s", err, result.output)
	}
	if len(compact.Actions) != 1 || !compact.Actions[0].Confirmed {
		t.Fatalf("compact actions = %+v, want confirmed delivery evidence", compact.Actions)
	}
}

func TestRunAgentCommandRunnerOpenClawConfirmsOnlyDeliveredMessageAction(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\nprintf '%s' '{\"runId\":\"run-1\",\"status\":\"ok\",\"summary\":\"completed\",\"result\":{\"delivery\":{\"message_id\":\"m-1\"}}}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("OPENCLAW_BIN", runner)
	payload := testCommandPayload("cmd-openclaw-selected-delivery")
	payload.Actions = []esphttp.FleetCommandExternalAction{
		{
			ID:               "notify-owner",
			Kind:             esphttp.FleetCommandExternalActionMessageSend,
			Channel:          "telegram",
			Target:           "owner",
			DeliveryRequired: true,
		},
		{
			ID:      "optional-team-note",
			Kind:    esphttp.FleetCommandExternalActionMessageSend,
			Channel: "telegram",
			Target:  "team",
		},
	}
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), payload)
	if result.status != esphttp.FleetCommandStatusCompleted {
		t.Fatalf("result = %+v, want completed OpenClaw result", result)
	}
	var compact openClawAgentCompactOutput
	if err := json.Unmarshal([]byte(result.output), &compact); err != nil {
		t.Fatalf("compact output JSON: %v; output=%s", err, result.output)
	}
	if len(compact.Actions) != 2 {
		t.Fatalf("compact actions = %+v, want two action evidence entries", compact.Actions)
	}
	if !compact.Actions[0].Confirmed {
		t.Fatalf("compact actions = %+v, want selected delivery action confirmed", compact.Actions)
	}
	if compact.Actions[1].Confirmed {
		t.Fatalf("compact actions = %+v, want optional different target unconfirmed", compact.Actions)
	}
}

func TestRunAgentCommandRunnerOpenClawFailsRequiredActionWithoutEvidence(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\nprintf '%s' '{\"runId\":\"run-1\",\"status\":\"ok\",\"summary\":\"completed\",\"result\":{\"payloads\":[{\"text\":\"done\"}]}}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("OPENCLAW_BIN", runner)
	payload := testCommandPayload("cmd-openclaw-missing-evidence")
	payload.Actions = []esphttp.FleetCommandExternalAction{{
		Kind:             esphttp.FleetCommandExternalActionMessageSend,
		Channel:          "telegram",
		Target:           "owner",
		DeliveryRequired: true,
	}}
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), payload)
	if result.status != esphttp.FleetCommandStatusFailed || result.summary != "OpenClaw completed without required external action evidence" {
		t.Fatalf("result = %+v, want missing-evidence failure", result)
	}
	var compact openClawAgentCompactOutput
	if err := json.Unmarshal([]byte(result.output), &compact); err != nil {
		t.Fatalf("compact output JSON: %v; output=%s", err, result.output)
	}
	if len(compact.Actions) != 1 || compact.Actions[0].Confirmed {
		t.Fatalf("compact actions = %+v, want unconfirmed action evidence", compact.Actions)
	}
}

func TestRunAgentCommandRunnerOpenClawDoesNotTreatChatMessageAsDeliveryEvidence(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\nprintf '%s' '{\"runId\":\"run-1\",\"status\":\"ok\",\"summary\":\"completed\",\"result\":{\"message\":{\"text\":\"I sent it\"}}}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("OPENCLAW_BIN", runner)
	payload := testCommandPayload("cmd-openclaw-chat-message-not-evidence")
	payload.Actions = []esphttp.FleetCommandExternalAction{{
		Kind:             esphttp.FleetCommandExternalActionMessageSend,
		Channel:          "telegram",
		Target:           "owner",
		DeliveryRequired: true,
	}}
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), payload)
	if result.status != esphttp.FleetCommandStatusFailed || result.summary != "OpenClaw completed without required external action evidence" {
		t.Fatalf("result = %+v, want missing-evidence failure", result)
	}
}

func TestRunAgentCommandRunnerOpenClawDoesNotTreatFormattedEmptyReceiptAsEvidence(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\ncat <<'JSON'\n{\"runId\":\"run-1\",\"status\":\"ok\",\"summary\":\"completed\",\"result\":{\"deliveries\":[\n]}}\nJSON\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("OPENCLAW_BIN", runner)
	payload := testCommandPayload("cmd-openclaw-empty-delivery-not-evidence")
	payload.Actions = []esphttp.FleetCommandExternalAction{{
		Kind:             esphttp.FleetCommandExternalActionMessageSend,
		Channel:          "telegram",
		Target:           "owner",
		DeliveryRequired: true,
	}}
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), payload)
	if result.status != esphttp.FleetCommandStatusFailed || result.summary != "OpenClaw completed without required external action evidence" {
		t.Fatalf("result = %+v, want missing-evidence failure", result)
	}
}

func TestRunAgentCommandRunnerOpenClawDoesNotTreatFalseReceiptAsEvidence(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\nprintf '%s' '{\"runId\":\"run-1\",\"status\":\"ok\",\"summary\":\"completed\",\"result\":{\"delivery\":false}}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("OPENCLAW_BIN", runner)
	payload := testCommandPayload("cmd-openclaw-false-delivery-not-evidence")
	payload.Actions = []esphttp.FleetCommandExternalAction{{
		Kind:             esphttp.FleetCommandExternalActionMessageSend,
		Channel:          "telegram",
		Target:           "owner",
		DeliveryRequired: true,
	}}
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), payload)
	if result.status != esphttp.FleetCommandStatusFailed || result.summary != "OpenClaw completed without required external action evidence" {
		t.Fatalf("result = %+v, want missing-evidence failure", result)
	}
}

func TestRunAgentCommandRunnerOpenClawDoesNotTreatFailedReceiptAsEvidence(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	runner := filepath.Join(t.TempDir(), "openclaw")
	script := "#!/bin/sh\nprintf '%s' '{\"runId\":\"run-1\",\"status\":\"ok\",\"summary\":\"completed\",\"result\":{\"delivery\":{\"ok\":false,\"error\":\"blocked\"}}}'\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("OPENCLAW_BIN", runner)
	payload := testCommandPayload("cmd-openclaw-failed-delivery-not-evidence")
	payload.Actions = []esphttp.FleetCommandExternalAction{{
		Kind:             esphttp.FleetCommandExternalActionMessageSend,
		Channel:          "telegram",
		Target:           "owner",
		DeliveryRequired: true,
	}}
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), payload)
	if result.status != esphttp.FleetCommandStatusFailed || result.summary != "OpenClaw completed without required external action evidence" {
		t.Fatalf("result = %+v, want missing-evidence failure", result)
	}
}

func TestRunAgentCommandRunnerOpenClawCompactsRunReport(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	runner := filepath.Join(t.TempDir(), "openclaw")
	report := `{
  "runId": "run-1",
  "status": "ok",
  "summary": "completed",
  "result": {
    "payloads": [
      {
        "text": "message received",
        "mediaUrl": null
      }
    ]
  },
  "meta": {
    "finalAssistantVisibleText": "message received",
    "systemPromptReport": {
      "tools": {
        "schemaChars": 27210
      }
    },
    "agentMeta": {
      "promptTokens": 121336
    }
  }
}`
	script := "#!/bin/sh\ncat <<'JSON'\n" + report + "\nJSON\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	t.Setenv("OPENCLAW_BIN", runner)
	result := runAgentCommandRunner(context.Background(), "openclaw", t.TempDir(), testCommandPayload("cmd-openclaw-report"))
	if result.status != esphttp.FleetCommandStatusCompleted {
		t.Fatalf("result = %+v, want completed OpenClaw result", result)
	}
	if strings.Contains(result.output, "systemPromptReport") || strings.Contains(result.output, "promptTokens") || strings.Contains(result.output, "schemaChars") {
		t.Fatalf("output leaked verbose OpenClaw metadata: %s", result.output)
	}
	var compact openClawAgentCompactOutput
	if err := json.Unmarshal([]byte(result.output), &compact); err != nil {
		t.Fatalf("compact output JSON: %v; output=%s", err, result.output)
	}
	if compact.RunID != "run-1" || compact.Status != "ok" || compact.Summary != "completed" || compact.FinalText != "message received" {
		t.Fatalf("compact = %+v, want run/status/summary/final text", compact)
	}
	if len(compact.Payloads) != 1 || compact.Payloads[0].Text != "message received" || compact.Payloads[0].MediaURL != "" {
		t.Fatalf("compact payloads = %+v, want single text payload without media URL", compact.Payloads)
	}
}

func TestOpenClawAgentSelectorPrecedence(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{name: "default main agent", want: []string{"--agent", "main"}},
		{name: "agent", env: map[string]string{"ENTMOOT_OPENCLAW_AGENT": "ops"}, want: []string{"--agent", "ops"}},
		{name: "openclaw agent alias", env: map[string]string{"OPENCLAW_AGENT_ID": "alias"}, want: []string{"--agent", "alias"}},
		{name: "to", env: map[string]string{"ENTMOOT_OPENCLAW_AGENT": "ops", "ENTMOOT_OPENCLAW_TO": "+15555550123"}, want: []string{"--to", "+15555550123"}},
		{name: "openclaw to alias", env: map[string]string{"OPENCLAW_AGENT_ID": "alias", "OPENCLAW_TO": "+15555550123"}, want: []string{"--to", "+15555550123"}},
		{name: "session id", env: map[string]string{"ENTMOOT_OPENCLAW_AGENT": "ops", "ENTMOOT_OPENCLAW_TO": "+15555550123", "ENTMOOT_OPENCLAW_SESSION_ID": "sess-1"}, want: []string{"--session-id", "sess-1"}},
		{name: "openclaw session alias", env: map[string]string{"OPENCLAW_AGENT_ID": "alias", "OPENCLAW_TO": "+15555550123", "OPENCLAW_SESSION_ID": "sess-2"}, want: []string{"--session-id", "sess-2"}},
		{name: "entmoot to beats openclaw session alias", env: map[string]string{"ENTMOOT_OPENCLAW_TO": "+15555550123", "OPENCLAW_SESSION_ID": "sess-2"}, want: []string{"--to", "+15555550123"}},
		{name: "entmoot agent beats openclaw session alias", env: map[string]string{"ENTMOOT_OPENCLAW_AGENT": "ops", "OPENCLAW_SESSION_ID": "sess-2"}, want: []string{"--agent", "ops"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearOpenClawRunnerEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			gotFlag, gotValue := openClawAgentSelector()
			got := []string{gotFlag, gotValue}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("selector = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestAgentCommandTimeoutSecondsClampsOpenClawTimeout(t *testing.T) {
	tests := []struct {
		timeoutMS int64
		want      int64
	}{
		{timeoutMS: 0, want: 600},
		{timeoutMS: -1_000, want: 600},
		{timeoutMS: 4_000, want: 5},
		{timeoutMS: 60_000, want: 60},
		{timeoutMS: 1_900_000, want: 1800},
	}
	for _, tt := range tests {
		if got := agentCommandTimeoutSeconds(tt.timeoutMS); got != tt.want {
			t.Fatalf("agentCommandTimeoutSeconds(%d) = %d, want %d", tt.timeoutMS, got, tt.want)
		}
	}
}

func TestRunAgentCommandRunnerAddsOpenClawSelectorAdvice(t *testing.T) {
	runner := filepath.Join(t.TempDir(), "agent-runner.sh")
	script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' 'Error: " + openClawSelectorError + "' >&2\nexit 1\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	result := runAgentCommandRunner(context.Background(), runner, t.TempDir(), testCommandPayload("cmd-openclaw-advice"))
	if result.status != esphttp.FleetCommandStatusFailed || result.summary != "Agent runtime failed" {
		t.Fatalf("result = %+v, want failed external runner", result)
	}
	for _, want := range []string{openClawSelectorError, "ENTMOOT_AGENT_RUNNER=openclaw", "ENTMOOT_OPENCLAW_AGENT=main"} {
		if !strings.Contains(result.output, want) {
			t.Fatalf("output = %q, want substring %q", result.output, want)
		}
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

func clearOpenClawRunnerEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"ENTMOOT_OPENCLAW_AGENT",
		"ENTMOOT_OPENCLAW_SESSION_ID",
		"ENTMOOT_OPENCLAW_TO",
		"OPENCLAW_AGENT_ID",
		"OPENCLAW_SESSION_ID",
		"OPENCLAW_TO",
		"OPENCLAW_BIN",
	} {
		t.Setenv(name, "")
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
