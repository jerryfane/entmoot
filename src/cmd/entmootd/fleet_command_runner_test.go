package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

func TestFleetCommandContextFallsBackToControlRoster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testFleetCommandGroupID(0x42)
	coordinatorID, coordinator := testFleetCommandIdentity(t)
	agentID, agent := testFleetCommandIdentity(t)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent, agentID)
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	testFleetCommandMetadata(t, ctx, state, gid, "fleet-a")
	runner := &fleetCommandRunner{
		server: &ipcServer{
			memberID: *agent.MemberID,
			peerID:   agent.PeerID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: state,
	}
	commandCtx, ok, err := runner.commandContextForCommand(ctx, gid, esphttp.FleetCommandEnvelope{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerMemberID: *coordinator.MemberID,
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
	if *commandCtx.fleet.Coordinator.MemberID != *coordinator.MemberID {
		t.Fatalf("coordinator member = %s, want %s", commandCtx.fleet.Coordinator.MemberID, coordinator.MemberID)
	}
	if commandCtx.local.MemberID != *agent.MemberID || commandCtx.local.Role != esphttp.FleetRoleAgent || commandCtx.local.Status != esphttp.FleetMemberActive {
		t.Fatalf("local member = %+v, want active agent %s", commandCtx.local, agent.MemberID)
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
	coordinatorID, coordinator := testFleetCommandIdentity(t)
	agentID, agent := testFleetCommandIdentity(t)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent, agentID)
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	runner := &fleetCommandRunner{
		server: &ipcServer{
			memberID: *agent.MemberID,
			peerID:   agent.PeerID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: state,
	}
	if _, ok, err := runner.commandContextForCommand(ctx, gid, esphttp.FleetCommandEnvelope{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerMemberID: *coordinator.MemberID,
	}); err != nil {
		t.Fatalf("commandContextForCommand: %v", err)
	} else if ok {
		t.Fatal("commandContextForCommand ok=true without Fleet control metadata")
	}
}

func TestFleetCommandRunnerProcessesAfterControlMetadataArrives(t *testing.T) {
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_ENABLE_FLEET", "1")
	t.Setenv("ENTMOOT_ENABLE_TASKS", "1")
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testFleetCommandGroupID(0x46)
	coordinatorID, coordinator := testFleetCommandIdentity(t)
	agentID, agent := testFleetCommandIdentity(t)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent, agentID)
	state := mustOpenFleetCommandState(t, dataDir)
	runner := &fleetCommandRunner{
		server: &ipcServer{memberID: *agent.MemberID, peerID: agent.PeerID, identity: agentID, dataDir: dataDir, runtime: &groupRuntime{}},
		state:  state, processed: make(map[string]struct{}), logger: slog.Default(),
	}
	command := esphttp.FleetCommandEnvelope{
		Type: esphttp.FleetCommandMessageType, Version: 2,
		CommandID: "cmd_wait_for_metadata", FleetID: "fleet-a", ControlGroupID: gid,
		IssuerMemberID: *coordinator.MemberID, IssuerPeerID: coordinator.PeerID,
		Target: esphttp.FleetCommandTarget{Kind: esphttp.FleetCommandTargetNode, MemberID: *agent.MemberID, PeerID: agent.PeerID},
		Action: esphttp.FleetCommandActionAgentInstruction,
		Args:   map[string]interface{}{"instruction": "Process after enrollment"},
	}
	content, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	message := entmoot.Message{Version: 2, GroupID: gid, Author: coordinator, Topics: []string{"fleet/commands"}, Content: content}
	message.ID = canonical.MessageID(message)
	payload, err := canonical.MessageSigningBytes(message)
	if err != nil {
		t.Fatal(err)
	}
	message.Signature = coordinatorID.Sign(payload)
	runner.handleMessage(ctx, message)
	if _, found, err := state.GetAgentCommand(ctx, command.CommandID); err != nil || found {
		t.Fatalf("queued before control metadata: found=%v err=%v", found, err)
	}
	testFleetCommandMetadata(t, ctx, state, gid, "fleet-a")
	runner.handleMessage(ctx, message)
	queued, found, err := state.GetAgentCommand(ctx, command.CommandID)
	if err != nil || !found || queued.Payload.Instruction != "Process after enrollment" {
		t.Fatalf("command was lost while awaiting metadata: %+v found=%v err=%v", queued, found, err)
	}
}

func TestFleetCommandContextDoesNotFallbackForMismatchedFleetMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testFleetCommandGroupID(0x45)
	coordinatorID, coordinator := testFleetCommandIdentity(t)
	agentID, agent := testFleetCommandIdentity(t)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent, agentID)
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	testFleetCommandMetadata(t, ctx, state, gid, "other-fleet")
	runner := &fleetCommandRunner{
		server: &ipcServer{
			memberID: *agent.MemberID,
			peerID:   agent.PeerID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: state,
	}
	if _, ok, err := runner.commandContextForCommand(ctx, gid, esphttp.FleetCommandEnvelope{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerMemberID: *coordinator.MemberID,
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
	coordinatorID, coordinator := testFleetCommandIdentity(t)
	agentID, agent := testFleetCommandIdentity(t)
	testFleetCommandRoster(t, dataDir, gid, coordinatorID, coordinator, agent, agentID)
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
			memberID: *agent.MemberID,
			peerID:   agent.PeerID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: state,
	}
	if _, ok, err := runner.commandContextForCommand(ctx, gid, esphttp.FleetCommandEnvelope{
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerMemberID: *coordinator.MemberID,
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
}

func TestFleetCommandAgentInstructionRequiresTaskFeature(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	result, err := runner.execute(context.Background(), commandCtx, cmd)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.status != esphttp.FleetCommandStatusRejected {
		t.Fatalf("status = %q, want rejected", result.status)
	}
}

func TestFleetCommandAgentInstructionQueuesSQLite(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_ENABLE_FLEET", "1")
	t.Setenv("ENTMOOT_ENABLE_TASKS", "1")
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
	queued, ok, err := runner.state.GetAgentCommand(context.Background(), cmd.CommandID)
	if err != nil || !ok {
		t.Fatalf("GetAgentCommand ok/err = %v/%v", ok, err)
	}
	if queued.Payload.Instruction != "Send a status update to Mars Hub" {
		t.Fatalf("instruction = %q", queued.Payload.Instruction)
	}
	if queued.Payload.AgentMemberID != commandCtx.local.MemberID {
		t.Fatalf("agent member = %s, want %s", queued.Payload.AgentMemberID, commandCtx.local.MemberID)
	}
	if queued.Status != esphttp.FleetCommandStatusRunning {
		t.Fatalf("queued status = %q, want running", queued.Status)
	}
}

func TestFleetCommandAgentInstructionDoesNotRequeueExistingCommand(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_ENABLE_FLEET", "1")
	t.Setenv("ENTMOOT_ENABLE_TASKS", "1")
	receivedAt := int64(9999)
	payload := esphttp.NewAgentInstructionPayload(cmd, commandCtx.local.MemberID, commandCtx.local.PeerID, "existing", nil, 60000, receivedAt)
	if _, created, err := runner.state.EnqueueAgentCommand(context.Background(), payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand created/err = %v/%v", created, err)
	}
	result, err := runner.execute(context.Background(), commandCtx, cmd)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.status != esphttp.FleetCommandStatusDuplicate {
		t.Fatalf("status = %q, want duplicate", result.status)
	}
	queued, ok, err := runner.state.GetAgentCommand(context.Background(), cmd.CommandID)
	if err != nil || !ok {
		t.Fatalf("GetAgentCommand ok/err = %v/%v", ok, err)
	}
	if queued.Payload.Instruction != "existing" {
		t.Fatalf("existing command was overwritten: %q", queued.Payload.Instruction)
	}
}

func TestFleetCommandAgentInstructionStoresUnsafeCommandIDWithoutPathWrite(t *testing.T) {
	runner, commandCtx, cmd := testFleetCommandInstructionRunner(t)
	cmd.CommandID = "../../somefile"
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "1")
	t.Setenv("ENTMOOT_ENABLE_FLEET", "1")
	t.Setenv("ENTMOOT_ENABLE_TASKS", "1")
	result, err := runner.execute(context.Background(), commandCtx, cmd)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.status != esphttp.FleetCommandStatusRunning {
		t.Fatalf("status = %q, want running", result.status)
	}
	if _, ok, err := runner.state.GetAgentCommand(context.Background(), cmd.CommandID); err != nil || !ok {
		t.Fatalf("GetAgentCommand unsafe id ok/err = %v/%v", ok, err)
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
	t.Setenv("ENTMOOT_AGENT_INSTRUCTIONS", "0")
	t.Setenv("ENTMOOT_ENABLE_FLEET", "0")
	t.Setenv("ENTMOOT_ENABLE_TASKS", "0")
	dataDir := t.TempDir()
	agentID, agent := testFleetCommandIdentity(t)
	_, coordinator := testFleetCommandIdentity(t)
	gid := testFleetCommandGroupID(0x47)
	runner := &fleetCommandRunner{
		server: &ipcServer{
			memberID: *agent.MemberID,
			peerID:   agent.PeerID,
			identity: agentID,
			dataDir:  dataDir,
		},
		state: mustOpenFleetCommandState(t, dataDir),
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
			MemberID:      *agent.MemberID,
			PeerID:        agent.PeerID,
			EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey),
		},
	}
	cmd := esphttp.FleetCommandEnvelope{
		Type:           esphttp.FleetCommandMessageType,
		Version:        2,
		CommandID:      "cmd_agent_instruction",
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerMemberID: *coordinator.MemberID,
		IssuerPeerID:   coordinator.PeerID,
		Target:         esphttp.FleetCommandTarget{Kind: esphttp.FleetCommandTargetNode, MemberID: *agent.MemberID, PeerID: agent.PeerID},
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

func mustOpenFleetCommandState(t *testing.T, dataDir string) *esphttp.SQLiteStateStore {
	t.Helper()
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })
	return state
}

func testFleetCommandIdentity(t *testing.T) (*keystore.Identity, entmoot.NodeInfo) {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate: %v", err)
	}
	memberID, err := entmoot.MemberIDFromPublicKey(id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := entmoot.PeerIDFromPublicKey(id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return id, entmoot.NodeInfo{MemberID: &memberID, PeerID: peerID, EntmootPubKey: id.PublicKey}
}

func testFleetCommandRoster(t *testing.T, dataDir string, gid entmoot.GroupID, coordinatorID *keystore.Identity, coordinator, agent entmoot.NodeInfo, agentID *keystore.Identity) {
	t.Helper()
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	group, err := membership.Create(dataDir, coordinatorID, coordinator, gid, policy, 1_700_000_000_000)
	if err != nil {
		t.Fatalf("membership.Create: %v", err)
	}
	defer func() {
		if err := group.Close(); err != nil {
			t.Fatalf("membership close: %v", err)
		}
	}()
	// The agent admits itself: under the open join rule a member's own signed
	// join is all it takes, which is what the daemon does at join time.
	if _, err := group.SignRecord(agentID, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("agent join: %v", err)
	}
	if !group.IsMemberID(*agent.MemberID) {
		t.Fatal("agent is not a member after its join record")
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
