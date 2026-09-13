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
	memberA := entmoot.MemberID{0xA1}
	memberB := entmoot.MemberID{0xB2}
	for _, result := range []FleetCommandResultEnvelope{
		testFleetCommandResultEnvelope("cmd-global-newer", memberA, FleetCommandStatusCompleted, 200),
		testFleetCommandResultEnvelope("cmd-global-newer", memberB, FleetCommandStatusFailed, 500),
		testFleetCommandResultEnvelope("cmd-agent-newer", memberA, FleetCommandStatusCompleted, 300),
	} {
		if err := store.UpsertFleetCommandResult(ctx, result); err != nil {
			t.Fatalf("UpsertFleetCommandResult %s/%s: %v", result.CommandID, result.AgentMemberID, err)
		}
	}

	commands, err := store.ListFleetCommands(ctx, "fleet-a", FleetCommandListFilter{AgentMemberID: memberA, Limit: 1})
	if err != nil {
		t.Fatalf("ListFleetCommands: %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("commands len = %d, want 1: %+v", len(commands), commands)
	}
	if commands[0].Command.CommandID != "cmd-agent-newer" {
		t.Fatalf("command = %s, want cmd-agent-newer", commands[0].Command.CommandID)
	}
	if commands[0].LatestResult == nil || commands[0].LatestResult.AgentMemberID != memberA || commands[0].UpdatedAtMS != 300 {
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
		IssuerMemberID: entmoot.MemberID{0x91},
		Target:         FleetCommandTarget{Kind: FleetCommandTargetAll},
		Action:         FleetCommandActionEntmootInfo,
		AutoAccept:     true,
		CreatedAtMS:    createdAtMS,
		ExpiresAtMS:    createdAtMS + 60_000,
	}
}

func testFleetCommandResultEnvelope(commandID string, agentMemberID entmoot.MemberID, status string, completedAtMS int64) FleetCommandResultEnvelope {
	return FleetCommandResultEnvelope{
		Type:          FleetCommandResultType,
		Version:       1,
		CommandID:     commandID,
		FleetID:       "fleet-a",
		AgentMemberID: agentMemberID,
		AgentPeerID:   "12D3KooWTestAgent",
		Action:        FleetCommandActionEntmootInfo,
		Status:        status,
		Summary:       status,
		StartedAtMS:   completedAtMS - 1,
		CompletedAtMS: completedAtMS,
	}
}
