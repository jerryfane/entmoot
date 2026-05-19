package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot/esphttp"
)

func TestBootstrapAgentDefaultDryRunJSON(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(testBootstrapGlobalFlags(t), []string{"--yes", "--dry-run", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	var report bootstrapAgentReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal report: %v\n%s", err, stdout)
	}
	if !report.DryRun || report.Applied {
		t.Fatalf("dry_run/applied = %t/%t, want true/false", report.DryRun, report.Applied)
	}
	if report.Runner != agentRunnerNone {
		t.Fatalf("runner = %q, want none", report.Runner)
	}
	if report.Live.Enabled {
		t.Fatalf("live enabled by default")
	}
	if len(report.Commands) != 1 || !strings.Contains(report.Commands[0], " serve") {
		t.Fatalf("commands = %#v, want serve command only", report.Commands)
	}
}

func TestBootstrapAgentDefaultYesJSONIsNotApplied(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(testBootstrapGlobalFlags(t), []string{"--yes", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	var report bootstrapAgentReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal report: %v\n%s", err, stdout)
	}
	if report.Applied {
		t.Fatalf("applied = true, want false for no-op bootstrap")
	}
	if !strings.Contains(stdout, `"commands"`) {
		t.Fatalf("stdout = %q, want command guidance", stdout)
	}
}

func TestBootstrapAgentCustomRunnerLiveDryRun(t *testing.T) {
	runner := testExecutableRunner(t)
	gid := testAgentLiveGroupID(0x71)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(testBootstrapGlobalFlags(t), []string{
			"--dry-run",
			"--json",
			"--runner", "custom",
			"--runner-command", runner,
			"--agent-instructions",
			"--live-mode", "operator",
			"--group", gid.String(),
			"--node", "155760",
			"--topic", "fleet/tasks",
			"--action", "task.assign_self",
			"--action", "task.update_own",
		})
	})
	if code != exitOK {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	var report bootstrapAgentReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal report: %v\n%s", err, stdout)
	}
	if report.Runner != agentRunnerCustom || report.RunnerCommand != runner {
		t.Fatalf("runner = %q command=%q", report.Runner, report.RunnerCommand)
	}
	if !report.AgentInstructions || len(report.Warnings) == 0 {
		t.Fatalf("instruction warning missing: %#v", report.Warnings)
	}
	if !report.Live.Enabled || report.Live.Mode != esphttp.LiveModeOperator {
		t.Fatalf("live report = %#v", report.Live)
	}
	if got := strings.Join(report.Live.AllowedActions, ","); got != "task.assign_self,task.update_own" {
		t.Fatalf("allowed actions = %q", got)
	}
	joined := strings.Join(report.Commands, "\n")
	for _, want := range []string{"ENTMOOT_AGENT_INSTRUCTIONS=1", "agent-commands watch", "agent-live run", runner} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands = %q, missing %q", joined, want)
		}
	}
}

func TestBootstrapAgentCommandsGateLiveRunWithStackHelper(t *testing.T) {
	gf := &globalFlags{
		socket:   agentPilotSocketPath,
		identity: "/data/.entmoot/custom-identity.json",
		data:     agentEntmootDataPath,
		hideIP:   true,
		logLevel: "info",
	}
	report := bootstrapAgentReport{
		Runner:            agentRunnerCustom,
		RunnerCommand:     "/tmp/runner",
		AgentInstructions: true,
		Runtime: runtimeReport{
			Binary:      "entmootd",
			StackHelper: "/data/.pilot/start-entmoot-stack.sh",
		},
		Live: bootstrapAgentLiveReport{
			Enabled: true,
			Group:   testAgentLiveGroupID(0x77).String(),
			NodeID:  155760,
		},
	}

	commands := bootstrapAgentCommands(gf, report)
	if len(commands) != 3 {
		t.Fatalf("commands = %#v, want serve, watcher, live", commands)
	}
	for _, command := range commands[:2] {
		if strings.Contains(command, "start-entmoot-stack.sh") {
			t.Fatalf("non-live command is stack gated: %q", command)
		}
	}
	live := commands[2]
	for _, want := range []string{
		"ENTMOOT_DATA=/data/.entmoot",
		"ENTMOOT_IDENTITY=/data/.entmoot/custom-identity.json",
		"PILOT_SOCKET=/data/.pilot/pilot.sock",
		"ENTMOOT_HIDE_IP=true",
		"ENTMOOT_AGENT_INSTRUCTIONS=1",
		"ENTMOOT_AGENT_RUNNER=/tmp/runner",
		"/data/.pilot/start-entmoot-stack.sh ensure",
		"/data/.pilot/start-entmoot-stack.sh check",
		"agent-live run",
		"-runner /tmp/runner",
	} {
		if !strings.Contains(live, want) {
			t.Fatalf("live command = %q, missing %q", live, want)
		}
	}
	if !strings.Contains(live, "start-entmoot-stack.sh ensure && ENTMOOT_DATA=/data/.entmoot") {
		t.Fatalf("live command does not gate before run: %q", live)
	}
}

func TestBootstrapAgentCommandsSkipStackGateForMismatchedRuntime(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	report := bootstrapAgentReport{
		Runner:        agentRunnerOpenClaw,
		RunnerCommand: agentRunnerOpenClaw,
		Runtime: runtimeReport{
			Binary:      "entmootd",
			StackHelper: "/data/.pilot/start-entmoot-stack.sh",
		},
		Live: bootstrapAgentLiveReport{
			Enabled: true,
			Group:   testAgentLiveGroupID(0x78).String(),
			NodeID:  155760,
		},
	}

	commands := bootstrapAgentCommands(gf, report)
	if len(commands) != 3 {
		t.Fatalf("commands = %#v, want serve, watcher, live", commands)
	}
	if strings.Contains(commands[2], "start-entmoot-stack.sh") {
		t.Fatalf("mismatched runtime command should not be stack gated: %q", commands[2])
	}
}

func TestStackGatedCommandQuotesHelperEnvValues(t *testing.T) {
	got := stackGatedCommand("/data/.pilot/start-entmoot-stack.sh", []string{
		shellEnvAssignment("ENTMOOT_AGENT_RUNNER", "/tmp/Hermes Runner/runner.sh"),
	}, "entmootd agent-live run")

	want := "ENTMOOT_AGENT_RUNNER='/tmp/Hermes Runner/runner.sh' /data/.pilot/start-entmoot-stack.sh ensure"
	if !strings.Contains(got, want) {
		t.Fatalf("stack command = %q, missing %q", got, want)
	}
}

func TestBootstrapAgentAppliesLiveConfig(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x72)
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(gf, []string{
			"--json",
			"--live-mode", "listen",
			"--group", gid.String(),
			"--node", "42",
			"--topic", "fleet/tasks",
		})
	})
	if code != exitOK {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	defer state.Close()
	cfg, ok, err := state.GetLiveAgentConfig(context.Background(), gid, 42)
	if err != nil {
		t.Fatalf("GetLiveAgentConfig: %v", err)
	}
	if !ok || !cfg.Enabled || cfg.Mode != esphttp.LiveModeListen {
		t.Fatalf("live config = %#v found=%t", cfg, ok)
	}
	if len(cfg.TopicFilters) != 1 || cfg.TopicFilters[0] != "fleet/tasks" {
		t.Fatalf("topics = %#v", cfg.TopicFilters)
	}
}

func TestBootstrapAgentRejectsCustomRunnerWithoutCommand(t *testing.T) {
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(testBootstrapGlobalFlags(t), []string{"--runner", "custom"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "--runner-command is required") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestBootstrapAgentRejectsRunnerCommandArgs(t *testing.T) {
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(testBootstrapGlobalFlags(t), []string{"--runner", "custom", "--runner-command", "runner --debug"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "without arguments") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestBootstrapAgentRejectsOpenClawRunnerCommand(t *testing.T) {
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(testBootstrapGlobalFlags(t), []string{
			"--runner", "openclaw",
			"--runner-command", "/tmp/runner",
		})
	})
	if code != exitInvalidArgument {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "--runner-command requires --runner custom") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestBootstrapAgentAcceptsRunnerPathWithSpaces(t *testing.T) {
	runner := testExecutableRunnerInDir(t, "Hermes Runner")
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(testBootstrapGlobalFlags(t), []string{
			"--dry-run",
			"--json",
			"--runner", "custom",
			"--runner-command", runner,
		})
	})
	if code != exitOK {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	var report bootstrapAgentReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal report: %v\n%s", err, stdout)
	}
	if report.RunnerCommand != runner {
		t.Fatalf("runner command = %q, want %q", report.RunnerCommand, runner)
	}
	if !strings.Contains(strings.Join(report.Commands, "\n"), "'") {
		t.Fatalf("commands = %#v, want quoted path with spaces", report.Commands)
	}
}

func TestBootstrapPromptWritesToStderr(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		value, err := promptString(bufio.NewReader(strings.NewReader("\n")), "runner", "none")
		if err != nil {
			t.Fatalf("promptString: %v", err)
		}
		if value != "none" {
			t.Fatalf("value = %q, want none", value)
		}
		return exitOK
	})
	if code != exitOK {
		t.Fatalf("code = %d", code)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty", stdout)
	}
	if !strings.Contains(stderr, "runner [none]:") {
		t.Fatalf("stderr = %q, want prompt", stderr)
	}
}

func testBootstrapGlobalFlags(t *testing.T) *globalFlags {
	t.Helper()
	dir := t.TempDir()
	return &globalFlags{
		socket:   filepath.Join(dir, "pilot.sock"),
		identity: filepath.Join(dir, "identity.json"),
		data:     filepath.Join(dir, "data"),
	}
}

func testExecutableRunner(t *testing.T) string {
	t.Helper()
	return testExecutableRunnerInDir(t, "")
}

func testExecutableRunnerInDir(t *testing.T, dirName string) string {
	t.Helper()
	dir := t.TempDir()
	if dirName != "" {
		dir = filepath.Join(dir, dirName)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir runner dir: %v", err)
		}
	}
	path := filepath.Join(dir, "runner.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write runner: %v", err)
	}
	return path
}
