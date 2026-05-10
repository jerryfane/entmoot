package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
)

func TestFleetCommandContextFallsBackToControlRoster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testFleetCommandGroupID(0x42)
	coordinatorID, coordinator := testFleetCommandIdentity(t, 45981)
	agentID, agent := testFleetCommandIdentity(t, 133053)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent)
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	testFleetCommandMetadata(t, ctx, state, gid, "fleet-a")
	runner := &fleetCommandRunner{
		server: &ipcServer{
			nodeID:   agent.PilotNodeID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: state,
	}
	commandCtx, ok, err := runner.commandContextForCommand(ctx, gid, esphttp.FleetCommandEnvelope{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerNodeID:   coordinator.PilotNodeID,
	})
	if err != nil {
		t.Fatalf("commandContextForCommand: %v", err)
	}
	if !ok {
		t.Fatal("commandContextForCommand ok=false, want true")
	}
	if commandCtx.source != "control_roster" {
		t.Fatalf("source = %q, want control_roster", commandCtx.source)
	}
	if commandCtx.fleet.Coordinator.PilotNodeID != coordinator.PilotNodeID {
		t.Fatalf("coordinator node = %d, want %d", commandCtx.fleet.Coordinator.PilotNodeID, coordinator.PilotNodeID)
	}
	if commandCtx.local.NodeID != agent.PilotNodeID || commandCtx.local.Role != esphttp.FleetRoleAgent || commandCtx.local.Status != esphttp.FleetMemberActive {
		t.Fatalf("local member = %+v, want active agent %d", commandCtx.local, agent.PilotNodeID)
	}
	if commandCtx.memberCount != 2 {
		t.Fatalf("memberCount = %d, want 2", commandCtx.memberCount)
	}
}

func TestFleetCommandContextDoesNotFallbackWithoutFleetMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testFleetCommandGroupID(0x44)
	coordinatorID, coordinator := testFleetCommandIdentity(t, 45981)
	agentID, agent := testFleetCommandIdentity(t, 133053)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent)
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	runner := &fleetCommandRunner{
		server: &ipcServer{
			nodeID:   agent.PilotNodeID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: state,
	}
	if _, ok, err := runner.commandContextForCommand(ctx, gid, esphttp.FleetCommandEnvelope{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerNodeID:   coordinator.PilotNodeID,
	}); err != nil {
		t.Fatalf("commandContextForCommand: %v", err)
	} else if ok {
		t.Fatal("commandContextForCommand ok=true without Fleet control metadata")
	}
}

func TestFleetCommandRunnerDoesNotMarkCommandBeforeContextExists(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testFleetCommandGroupID(0x46)
	coordinatorID, coordinator := testFleetCommandIdentity(t, 45981)
	agentID, agent := testFleetCommandIdentity(t, 133053)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent)
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	runner := &fleetCommandRunner{
		server: &ipcServer{
			nodeID:   agent.PilotNodeID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state:     state,
		processed: make(map[string]struct{}),
	}
	command := esphttp.FleetCommandEnvelope{
		Type:           esphttp.FleetCommandMessageType,
		Version:        1,
		CommandID:      "cmd_wait_for_metadata",
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerNodeID:   coordinator.PilotNodeID,
		Target:         esphttp.FleetCommandTarget{Kind: esphttp.FleetCommandTargetNode, PilotNodeID: agent.PilotNodeID},
		Action:         esphttp.FleetCommandActionEntmootVersion,
		AutoAccept:     true,
	}
	content, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("Marshal command: %v", err)
	}
	runner.handleMessage(ctx, entmoot.Message{
		GroupID: gid,
		Author:  coordinator,
		Topics:  []string{"fleet/commands"},
		Content: content,
	})
	if _, ok := runner.processed[command.CommandID]; ok {
		t.Fatal("command was marked processed before Fleet control metadata was available")
	}
}

func TestFleetCommandContextDoesNotFallbackForMismatchedFleetMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testFleetCommandGroupID(0x45)
	coordinatorID, coordinator := testFleetCommandIdentity(t, 45981)
	agentID, agent := testFleetCommandIdentity(t, 133053)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent)
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	testFleetCommandMetadata(t, ctx, state, gid, "other-fleet")
	runner := &fleetCommandRunner{
		server: &ipcServer{
			nodeID:   agent.PilotNodeID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: state,
	}
	if _, ok, err := runner.commandContextForCommand(ctx, gid, esphttp.FleetCommandEnvelope{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerNodeID:   coordinator.PilotNodeID,
	}); err != nil {
		t.Fatalf("commandContextForCommand: %v", err)
	} else if ok {
		t.Fatal("commandContextForCommand ok=true for mismatched Fleet control metadata")
	}
}

func TestFleetCommandContextDoesNotFallbackForArchivedFleetState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testFleetCommandGroupID(0x43)
	coordinatorID, coordinator := testFleetCommandIdentity(t, 45981)
	agentID, agent := testFleetCommandIdentity(t, 133053)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent)
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		Status:         esphttp.FleetStatusArchived,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	runner := &fleetCommandRunner{
		server: &ipcServer{
			nodeID:   agent.PilotNodeID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: state,
	}
	if _, ok, err := runner.commandContextForCommand(ctx, gid, esphttp.FleetCommandEnvelope{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerNodeID:   coordinator.PilotNodeID,
	}); err != nil {
		t.Fatalf("commandContextForCommand: %v", err)
	} else if ok {
		t.Fatal("commandContextForCommand ok=true for archived local fleet state")
	}
}

func TestFleetCommandAgentInstructionRequiresOptIn(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	result, err := runner.execute(context.Background(), commandCtx, cmd)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.status != esphttp.FleetCommandStatusRejected {
		t.Fatalf("status = %q, want rejected", result.status)
	}
	if !strings.Contains(result.summary, "not enabled") {
		t.Fatalf("summary = %q, want opt-in rejection", result.summary)
	}
}

func TestFleetCommandAgentInstructionQueuesInboxFile(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	commandDir := filepath.Join(t.TempDir(), "agent-commands")
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_AGENT_COMMAND_DIR", commandDir)
	result, err := runner.execute(context.Background(), commandCtx, cmd)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.status != esphttp.FleetCommandStatusRunning {
		t.Fatalf("status = %q, want running", result.status)
	}
	if result.output != `{"queued":true}` {
		t.Fatalf("output = %q, want queued JSON", result.output)
	}
	queuedPath, err := agentInstructionPath(filepath.Join(commandDir, "inbox"), cmd.CommandID)
	if err != nil {
		t.Fatalf("agentInstructionPath: %v", err)
	}
	data, err := os.ReadFile(queuedPath)
	if err != nil {
		t.Fatalf("ReadFile queued command: %v", err)
	}
	var queued agentInstructionFile
	if err := json.Unmarshal(data, &queued); err != nil {
		t.Fatalf("Unmarshal queued command: %v", err)
	}
	if queued.Instruction != "Send a status update to Mars Hub" {
		t.Fatalf("instruction = %q", queued.Instruction)
	}
	if queued.AgentNodeID != commandCtx.local.NodeID {
		t.Fatalf("agent node = %d, want %d", queued.AgentNodeID, commandCtx.local.NodeID)
	}
}

func TestFleetCommandAgentInstructionDoesNotRequeueExistingCommand(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	commandDir := filepath.Join(t.TempDir(), "agent-commands")
	inboxDir := filepath.Join(commandDir, "inbox")
	if err := os.MkdirAll(inboxDir, 0o700); err != nil {
		t.Fatalf("MkdirAll inbox: %v", err)
	}
	path, err := agentInstructionPath(inboxDir, cmd.CommandID)
	if err != nil {
		t.Fatalf("agentInstructionPath: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"existing":true}`), 0o600); err != nil {
		t.Fatalf("WriteFile existing command: %v", err)
	}
	hook := filepath.Join(t.TempDir(), "hook.sh")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 17\n"), 0o700); err != nil {
		t.Fatalf("WriteFile hook: %v", err)
	}
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_AGENT_COMMAND_DIR", commandDir)
	t.Setenv("ENTMOOT_AGENT_COMMAND_HOOK", hook)
	result, err := runner.execute(context.Background(), commandCtx, cmd)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.status != esphttp.FleetCommandStatusDuplicate {
		t.Fatalf("status = %q, want duplicate", result.status)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile existing command: %v", err)
	}
	if string(data) != `{"existing":true}` {
		t.Fatalf("existing command was overwritten: %s", string(data))
	}
}

func TestFleetCommandAgentInstructionHookResult(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	commandDir := filepath.Join(t.TempDir(), "agent-commands")
	hook := filepath.Join(t.TempDir(), "hook.sh")
	script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"status\":\"completed\",\"summary\":\"sent\",\"output\":\"ok\"}'\n"
	if err := os.WriteFile(hook, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile hook: %v", err)
	}
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_AGENT_COMMAND_DIR", commandDir)
	t.Setenv("ENTMOOT_AGENT_COMMAND_HOOK", hook)
	result, err := runner.execute(context.Background(), commandCtx, cmd)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.status != esphttp.FleetCommandStatusCompleted || result.summary != "sent" || result.output != "ok" {
		t.Fatalf("result = %+v, want completed/sent/ok", result)
	}
}

func TestFleetCommandAgentInstructionHookRejectsInvalidStatus(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	commandDir := filepath.Join(t.TempDir(), "agent-commands")
	hook := filepath.Join(t.TempDir(), "hook.sh")
	script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"status\":\"fail\",\"summary\":\"typo\"}'\n"
	if err := os.WriteFile(hook, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile hook: %v", err)
	}
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_AGENT_COMMAND_DIR", commandDir)
	t.Setenv("ENTMOOT_AGENT_COMMAND_HOOK", hook)
	if _, err := runner.execute(context.Background(), commandCtx, cmd); err == nil || !strings.Contains(err.Error(), "invalid status") {
		t.Fatalf("execute error = %v, want invalid status", err)
	}
}

func TestFleetCommandAgentInstructionEncodesUnsafeCommandID(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	cmd.CommandID = "../../somefile"
	commandDir := filepath.Join(t.TempDir(), "agent-commands")
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_AGENT_COMMAND_DIR", commandDir)
	result, err := runner.execute(context.Background(), commandCtx, cmd)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.status != esphttp.FleetCommandStatusRunning {
		t.Fatalf("status = %q, want running", result.status)
	}
	if _, err := os.Stat(filepath.Join(commandDir, "somefile")); !os.IsNotExist(err) {
		t.Fatalf("unsafe path write err = %v, want not exist", err)
	}
	if _, err := os.Stat(filepath.Join(commandDir, "inbox", "Li4vLi4vc29tZWZpbGU.json")); err != nil {
		t.Fatalf("encoded command file missing: %v", err)
	}
}

func TestFleetCommandStatusIsTerminal(t *testing.T) {
	if fleetCommandStatusIsTerminal(esphttp.FleetCommandStatusRunning) {
		t.Fatal("running status should not be terminal")
	}
	if fleetCommandStatusIsTerminal(esphttp.FleetCommandStatusAccepted) {
		t.Fatal("accepted status should not be terminal")
	}
	if !fleetCommandStatusIsTerminal(esphttp.FleetCommandStatusCompleted) {
		t.Fatal("completed status should be terminal")
	}
	if !fleetCommandStatusIsTerminal(esphttp.FleetCommandStatusRejected) {
		t.Fatal("rejected status should be terminal")
	}
	if !fleetCommandStatusIsTerminal(esphttp.FleetCommandStatusDuplicate) {
		t.Fatal("duplicate status should be terminal")
	}
	if !fleetCommandStatusIsTerminal(esphttp.FleetCommandStatusExpired) {
		t.Fatal("expired status should be terminal")
	}
}

func testFleetCommandInstructionRunner(t *testing.T) (*fleetCommandRunner, fleetCommandContext, esphttp.FleetCommandEnvelope) {
	t.Helper()
	dataDir := t.TempDir()
	agentID, agent := testFleetCommandIdentity(t, 133053)
	coordinator := entmoot.NodeInfo{PilotNodeID: 45981}
	gid := testFleetCommandGroupID(0x47)
	runner := &fleetCommandRunner{
		server: &ipcServer{
			nodeID:   agent.PilotNodeID,
			identity: agentID,
			dataDir:  dataDir,
		},
	}
	commandCtx := fleetCommandContext{
		fleet: esphttp.FleetRecord{
			FleetID:        "fleet-a",
			ControlGroupID: gid,
			Coordinator:    coordinator,
			Status:         esphttp.FleetStatusActive,
		},
		local: esphttp.FleetMemberRecord{
			FleetID:       "fleet-a",
			Role:          esphttp.FleetRoleAgent,
			Status:        esphttp.FleetMemberActive,
			NodeID:        agent.PilotNodeID,
			EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey),
		},
	}
	cmd := esphttp.FleetCommandEnvelope{
		Type:           esphttp.FleetCommandMessageType,
		Version:        1,
		CommandID:      "cmd_agent_instruction",
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerNodeID:   coordinator.PilotNodeID,
		Target:         esphttp.FleetCommandTarget{Kind: esphttp.FleetCommandTargetNode, PilotNodeID: agent.PilotNodeID},
		Action:         esphttp.FleetCommandActionAgentInstruction,
		AutoAccept:     true,
		CreatedAtMS:    1234,
		Args: map[string]interface{}{
			"instruction": "Send a status update to Mars Hub",
			"timeout_ms":  float64(60000),
			"context": map[string]interface{}{
				"source": "test",
			},
		},
	}
	return runner, commandCtx, cmd
}

func testFleetCommandIdentity(t *testing.T, nodeID entmoot.NodeID) (*keystore.Identity, entmoot.NodeInfo) {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate: %v", err)
	}
	return id, entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte(id.PublicKey)}
}

func testFleetCommandRoster(t *testing.T, dataDir string, gid entmoot.GroupID, coordinatorID *keystore.Identity, coordinator, agent entmoot.NodeInfo) {
	t.Helper()
	rlog, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	if err := rlog.Genesis(coordinatorID, coordinator, 1_700_000_000_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   agent,
		Actor:     coordinator.PilotNodeID,
		Timestamp: 1_700_000_001_000,
		Parents:   []entmoot.RosterEntryID{rlog.Head()},
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		t.Fatalf("canonical encode roster entry: %v", err)
	}
	entry.Signature = coordinatorID.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)
	if err := rlog.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

func testFleetCommandMetadata(t *testing.T, ctx context.Context, state *esphttp.SQLiteStateStore, gid entmoot.GroupID, fleetID string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"fleet_control": true,
		"fleet_id":      fleetID,
	})
	if err != nil {
		t.Fatalf("Marshal metadata: %v", err)
	}
	if err := state.SetGroupMetadata(ctx, gid, raw); err != nil {
		t.Fatalf("SetGroupMetadata: %v", err)
	}
}

func testFleetCommandGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = seed + byte(i)
	}
	return gid
}
