package esphttp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

func TestSQLiteAgentCommandQueuePersistsAndDedupes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	payload := testAgentInstructionPayload("cmd-1")
	if _, created, err := store.EnqueueAgentCommand(ctx, payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand #1 created/err = %v/%v", created, err)
	}
	if _, created, err := store.EnqueueAgentCommand(ctx, payload); err != nil || created {
		t.Fatalf("EnqueueAgentCommand #2 created/err = %v/%v, want false/nil", created, err)
	}
	_ = store.Close()

	store, err = OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore reopen: %v", err)
	}
	defer store.Close()
	rec, ok, err := store.GetAgentCommand(ctx, payload.CommandID)
	if err != nil || !ok {
		t.Fatalf("GetAgentCommand ok/err = %v/%v", ok, err)
	}
	if rec.Payload.Instruction != payload.Instruction || rec.Status != FleetCommandStatusRunning {
		t.Fatalf("record = %+v", rec)
	}
}

func TestSQLiteAgentCommandClaimUsesLease(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()
	payload := testAgentInstructionPayload("cmd-lease")
	if _, created, err := store.EnqueueAgentCommand(ctx, payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand created/err = %v/%v", created, err)
	}
	rec, ok, err := store.ClaimNextAgentCommand(ctx, "owner-a", 2_000, 12_000, 3)
	if err != nil || !ok {
		t.Fatalf("ClaimNextAgentCommand #1 ok/err = %v/%v", ok, err)
	}
	if rec.Status != AgentCommandStatusProcessing || rec.Attempts != 1 || rec.LeaseOwner != "owner-a" {
		t.Fatalf("claimed record = %+v", rec)
	}
	if _, ok, err := store.ClaimNextAgentCommand(ctx, "owner-b", 3_000, 13_000, 3); err != nil || ok {
		t.Fatalf("ClaimNextAgentCommand active lease ok/err = %v/%v, want false/nil", ok, err)
	}
	rec, ok, err = store.ClaimNextAgentCommand(ctx, "owner-b", 12_001, 22_001, 3)
	if err != nil || !ok {
		t.Fatalf("ClaimNextAgentCommand expired lease ok/err = %v/%v", ok, err)
	}
	if rec.Attempts != 2 || rec.LeaseOwner != "owner-b" {
		t.Fatalf("reclaimed record = %+v", rec)
	}
	result, _ := json.Marshal(FleetCommandResultEnvelope{CommandID: payload.CommandID, Status: FleetCommandStatusCompleted})
	finished, err := store.FinishAgentCommand(ctx, payload.CommandID, "owner-b", FleetCommandStatusCompleted, result, "", time.Now().UnixMilli())
	if err != nil || !finished {
		t.Fatalf("FinishAgentCommand finished/err = %v/%v", finished, err)
	}
	rec, ok, err = store.GetAgentCommand(ctx, payload.CommandID)
	if err != nil || !ok {
		t.Fatalf("GetAgentCommand after finish ok/err = %v/%v", ok, err)
	}
	if rec.Status != FleetCommandStatusCompleted || rec.CompletedAtMS == 0 {
		t.Fatalf("finished record = %+v", rec)
	}
}

func TestSQLiteAgentCommandClaimIncludesExpiredPendingCommand(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()
	payload := testAgentInstructionPayload("cmd-expired")
	payload.ExpiresAtMS = 1_000
	if _, created, err := store.EnqueueAgentCommand(ctx, payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand created/err = %v/%v", created, err)
	}
	rec, ok, err := store.ClaimNextAgentCommand(ctx, "owner-a", 2_000, 12_000, 3)
	if err != nil || !ok {
		t.Fatalf("ClaimNextAgentCommand expired pending ok/err = %v/%v", ok, err)
	}
	if rec.Payload.CommandID != payload.CommandID {
		t.Fatalf("claimed command = %q, want %q", rec.Payload.CommandID, payload.CommandID)
	}
}

func TestSQLiteAgentCommandClaimReclaimsExhaustedStaleProcessing(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()
	payload := testAgentInstructionPayload("cmd-exhausted")
	if _, created, err := store.EnqueueAgentCommand(ctx, payload); err != nil || !created {
		t.Fatalf("EnqueueAgentCommand created/err = %v/%v", created, err)
	}
	if _, ok, err := store.ClaimNextAgentCommand(ctx, "owner-a", 2_000, 3_000, 1); err != nil || !ok {
		t.Fatalf("ClaimNextAgentCommand first ok/err = %v/%v", ok, err)
	}
	rec, ok, err := store.ClaimNextAgentCommand(ctx, "owner-b", 3_001, 4_001, 1)
	if err != nil || !ok {
		t.Fatalf("ClaimNextAgentCommand exhausted stale ok/err = %v/%v", ok, err)
	}
	if rec.Payload.CommandID != payload.CommandID || rec.Attempts != 1 || rec.LeaseOwner != "owner-b" || !rec.RetryExhausted {
		t.Fatalf("reclaimed record = %+v", rec)
	}
}

func testAgentInstructionPayload(commandID string) AgentInstructionPayload {
	var gid entmoot.GroupID
	gid[0] = 0x42
	cmd := FleetCommandEnvelope{
		Type:           FleetCommandMessageType,
		Version:        1,
		CommandID:      commandID,
		FleetID:        "fleet-a",
		ControlGroupID: gid,
		IssuerNodeID:   45981,
		Target:         FleetCommandTarget{Kind: FleetCommandTargetNode, PilotNodeID: 133053},
		Action:         FleetCommandActionAgentInstruction,
		CreatedAtMS:    1_000,
		Args: map[string]interface{}{
			"instruction": "report status",
			"timeout_ms":  float64(60_000),
		},
	}
	return NewAgentInstructionPayload(cmd, 133053, "report status", nil, 60_000, 1_500)
}
