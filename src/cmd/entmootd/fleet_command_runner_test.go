package main

import (
	"context"
	"encoding/json"
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
