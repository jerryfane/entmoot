package esphttp

import (
	"context"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

func TestSQLiteListFleetCommandsAgentFilterSortsByFilteredResult(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()

	for _, cmd := range []FleetCommandEnvelope{
		testFleetCommandEnvelope("cmd-global-newer", 100),
		testFleetCommandEnvelope("cmd-agent-newer", 150),
	} {
		if _, err := store.UpsertFleetCommand(ctx, cmd); err != nil {
			t.Fatalf("UpsertFleetCommand %s: %v", cmd.CommandID, err)
		}
	}
	for _, result := range []FleetCommandResultEnvelope{
		testFleetCommandResultEnvelope("cmd-global-newer", 45493, FleetCommandStatusCompleted, 200),
		testFleetCommandResultEnvelope("cmd-global-newer", 45494, FleetCommandStatusFailed, 500),
		testFleetCommandResultEnvelope("cmd-agent-newer", 45493, FleetCommandStatusCompleted, 300),
	} {
		if err := store.UpsertFleetCommandResult(ctx, result); err != nil {
			t.Fatalf("UpsertFleetCommandResult %s/%d: %v", result.CommandID, result.AgentNodeID, err)
		}
	}

	commands, err := store.ListFleetCommands(ctx, "fleet-a", FleetCommandListFilter{AgentNodeID: 45493, Limit: 1})
	if err != nil {
		t.Fatalf("ListFleetCommands: %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("commands len = %d, want 1: %+v", len(commands), commands)
	}
	if commands[0].Command.CommandID != "cmd-agent-newer" {
		t.Fatalf("command = %s, want cmd-agent-newer", commands[0].Command.CommandID)
	}
	if commands[0].LatestResult == nil || commands[0].LatestResult.AgentNodeID != 45493 || commands[0].UpdatedAtMS != 300 {
		t.Fatalf("filtered summary = %+v", commands[0])
	}
}

func TestSQLiteListFleetCommandsMarksNoResultExpired(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()

	cmd := testFleetCommandEnvelope("cmd-expired", time.Now().Add(-2*time.Minute).UnixMilli())
	cmd.ExpiresAtMS = time.Now().Add(-time.Minute).UnixMilli()
	if _, err := store.UpsertFleetCommand(ctx, cmd); err != nil {
		t.Fatalf("UpsertFleetCommand: %v", err)
	}

	expired, err := store.ListFleetCommands(ctx, "fleet-a", FleetCommandListFilter{Status: FleetCommandStatusExpired})
	if err != nil {
		t.Fatalf("ListFleetCommands expired: %v", err)
	}
	if len(expired) != 1 || expired[0].Command.CommandID != cmd.CommandID || expired[0].Status != FleetCommandStatusExpired {
		t.Fatalf("expired commands = %+v", expired)
	}

	sent, err := store.ListFleetCommands(ctx, "fleet-a", FleetCommandListFilter{Status: FleetCommandStatusSent})
	if err != nil {
		t.Fatalf("ListFleetCommands sent: %v", err)
	}
	if len(sent) != 0 {
		t.Fatalf("sent commands = %+v", sent)
	}
}

func testFleetCommandEnvelope(commandID string, createdAtMS int64) FleetCommandEnvelope {
	return FleetCommandEnvelope{
		Type:           FleetCommandMessageType,
		Version:        1,
		CommandID:      commandID,
		FleetID:        "fleet-a",
		ControlGroupID: entmoot.GroupID{0x94},
		IssuerNodeID:   45491,
		Target:         FleetCommandTarget{Kind: FleetCommandTargetAll},
		Action:         FleetCommandActionEntmootInfo,
		AutoAccept:     true,
		CreatedAtMS:    createdAtMS,
		ExpiresAtMS:    createdAtMS + 60_000,
	}
}

func testFleetCommandResultEnvelope(commandID string, agentNodeID entmoot.NodeID, status string, completedAtMS int64) FleetCommandResultEnvelope {
	return FleetCommandResultEnvelope{
		Type:          FleetCommandResultType,
		Version:       1,
		CommandID:     commandID,
		FleetID:       "fleet-a",
		AgentNodeID:   agentNodeID,
		Action:        FleetCommandActionEntmootInfo,
		Status:        status,
		Summary:       status,
		StartedAtMS:   completedAtMS - 1,
		CompletedAtMS: completedAtMS,
	}
}
