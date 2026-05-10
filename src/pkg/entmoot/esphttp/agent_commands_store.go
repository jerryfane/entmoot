package esphttp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"entmoot/pkg/entmoot"
)

const AgentInstructionPayloadType = "entmoot.agent_instruction.v1"

const (
	AgentCommandStatusProcessing    = "processing"
	AgentCommandStatusResultPending = "result_pending"
)

type AgentInstructionPayload struct {
	Type           string                 `json:"type"`
	Version        int                    `json:"version"`
	CommandID      string                 `json:"command_id"`
	FleetID        string                 `json:"fleet_id"`
	ControlGroupID entmoot.GroupID        `json:"control_group_id"`
	IssuerNodeID   entmoot.NodeID         `json:"issuer_node_id"`
	Target         FleetCommandTarget     `json:"target"`
	AgentNodeID    entmoot.NodeID         `json:"agent_node_id"`
	Action         string                 `json:"action"`
	Instruction    string                 `json:"instruction"`
	Context        map[string]interface{} `json:"context,omitempty"`
	TimeoutMS      int64                  `json:"timeout_ms"`
	CreatedAtMS    int64                  `json:"created_at_ms"`
	ExpiresAtMS    int64                  `json:"expires_at_ms,omitempty"`
	ReceivedAtMS   int64                  `json:"received_at_ms"`
	Args           map[string]interface{} `json:"args,omitempty"`
	Command        *FleetCommandEnvelope  `json:"command,omitempty"`
}

type AgentCommandRecord struct {
	Payload        AgentInstructionPayload
	Status         string
	Attempts       int
	LeaseOwner     string
	LeaseUntilMS   int64
	StartedAtMS    int64
	CompletedAtMS  int64
	UpdatedAtMS    int64
	Result         json.RawMessage
	LastError      string
	RetryExhausted bool
}

type AgentCommandQueueStats struct {
	Pending         int    `json:"pending"`
	Processing      int    `json:"processing"`
	StaleProcessing int    `json:"stale_processing"`
	ResultPending   int    `json:"result_pending"`
	Completed       int    `json:"completed"`
	Failed          int    `json:"failed"`
	Rejected        int    `json:"rejected"`
	Expired         int    `json:"expired"`
	LastClaimedID   string `json:"last_claimed_command_id,omitempty"`
	LastCompletedID string `json:"last_completed_command_id,omitempty"`
}

func NewAgentInstructionPayload(cmd FleetCommandEnvelope, agentNodeID entmoot.NodeID, instruction string, instructionContext map[string]interface{}, timeoutMS, receivedAtMS int64) AgentInstructionPayload {
	action := NormalizeFleetCommandAction(cmd.Action)
	if action == "" {
		action = FleetCommandActionAgentInstruction
	}
	return AgentInstructionPayload{
		Type:           AgentInstructionPayloadType,
		Version:        1,
		CommandID:      strings.TrimSpace(cmd.CommandID),
		FleetID:        strings.TrimSpace(cmd.FleetID),
		ControlGroupID: cmd.ControlGroupID,
		IssuerNodeID:   cmd.IssuerNodeID,
		Target:         cmd.Target,
		AgentNodeID:    agentNodeID,
		Action:         action,
		Instruction:    instruction,
		Context:        instructionContext,
		TimeoutMS:      timeoutMS,
		CreatedAtMS:    cmd.CreatedAtMS,
		ExpiresAtMS:    cmd.ExpiresAtMS,
		ReceivedAtMS:   receivedAtMS,
		Args:           cmd.Args,
		Command:        &cmd,
	}
}

func (s *SQLiteStateStore) EnqueueAgentCommand(ctx context.Context, payload AgentInstructionPayload) (AgentCommandRecord, bool, error) {
	if s == nil || s.db == nil {
		return AgentCommandRecord{}, false, errors.New("esphttp: nil SQLite state store")
	}
	rec, err := agentCommandRecordFromPayload(payload, FleetCommandStatusRunning, payload.ReceivedAtMS)
	if err != nil {
		return AgentCommandRecord{}, false, err
	}
	target, _ := json.Marshal(payload.Target)
	contextJSON, _ := json.Marshal(payload.Context)
	args, _ := json.Marshal(payload.Args)
	commandJSON, _ := json.Marshal(payload.Command)
	payloadJSON, _ := json.Marshal(payload)
	res, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO esp_agent_commands (
  command_id, fleet_id, control_group_id, issuer_node_id, agent_node_id, action,
  target, instruction, context, args, command, payload, status, created_at_ms,
  expires_at_ms, received_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		payload.CommandID, payload.FleetID, payload.ControlGroupID[:], payload.IssuerNodeID,
		payload.AgentNodeID, payload.Action, target, payload.Instruction, nullableJSON(contextJSON),
		nullableJSON(args), commandJSON, payloadJSON, rec.Status, payload.CreatedAtMS,
		payload.ExpiresAtMS, payload.ReceivedAtMS, rec.UpdatedAtMS)
	if err != nil {
		return AgentCommandRecord{}, false, fmt.Errorf("esphttp: enqueue agent command: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return AgentCommandRecord{}, false, fmt.Errorf("esphttp: enqueue agent command rows affected: %w", err)
	}
	if n == 0 {
		existing, ok, err := s.GetAgentCommand(ctx, payload.CommandID)
		return existing, false, errIfMissing(ok, err)
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) GetAgentCommand(ctx context.Context, commandID string) (AgentCommandRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT payload, status, attempts, lease_owner, lease_until_ms, started_at_ms,
       completed_at_ms, updated_at_ms, result, last_error
FROM esp_agent_commands
WHERE command_id = ?`, strings.TrimSpace(commandID))
	rec, err := scanAgentCommand(row)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentCommandRecord{}, false, nil
	}
	if err != nil {
		return AgentCommandRecord{}, false, err
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) ClaimNextAgentCommand(ctx context.Context, owner string, nowMS, leaseUntilMS int64, maxAttempts int) (AgentCommandRecord, bool, error) {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentCommandRecord{}, false, fmt.Errorf("esphttp: begin agent command claim: %w", err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `
SELECT payload, status, attempts, lease_owner, lease_until_ms, started_at_ms,
       completed_at_ms, updated_at_ms, result, last_error
FROM esp_agent_commands
WHERE (
    status = ?
    AND attempts < ?
  )
  OR (status = ? AND lease_until_ms <= ?)
  OR (status = ? AND lease_until_ms <= ?)
ORDER BY received_at_ms ASC
LIMIT 1`, FleetCommandStatusRunning, maxAttempts, AgentCommandStatusProcessing, nowMS, AgentCommandStatusResultPending, nowMS)
	rec, err := scanAgentCommand(row)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentCommandRecord{}, false, nil
	}
	if err != nil {
		return AgentCommandRecord{}, false, err
	}
	retryExhausted := rec.Status == AgentCommandStatusProcessing && rec.Attempts >= maxAttempts
	startedAt := rec.StartedAtMS
	if startedAt == 0 {
		startedAt = nowMS
	}
	res, err := tx.ExecContext(ctx, `
UPDATE esp_agent_commands
SET status = ?, attempts = attempts + CASE WHEN status = ? OR attempts >= ? THEN 0 ELSE 1 END,
    lease_owner = ?, lease_until_ms = ?,
    started_at_ms = ?, updated_at_ms = ?, last_error = ''
WHERE command_id = ?
  AND (
    (
      status = ?
      AND attempts < ?
    )
    OR (status = ? AND lease_until_ms <= ?)
    OR (status = ? AND lease_until_ms <= ?)
  )`, AgentCommandStatusProcessing, AgentCommandStatusResultPending, maxAttempts, owner, leaseUntilMS, startedAt, nowMS, rec.Payload.CommandID,
		FleetCommandStatusRunning, maxAttempts, AgentCommandStatusProcessing, nowMS, AgentCommandStatusResultPending, nowMS)
	if err != nil {
		return AgentCommandRecord{}, false, fmt.Errorf("esphttp: claim agent command: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return AgentCommandRecord{}, false, fmt.Errorf("esphttp: claim agent command rows affected: %w", err)
	}
	if n == 0 {
		return AgentCommandRecord{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return AgentCommandRecord{}, false, fmt.Errorf("esphttp: commit agent command claim: %w", err)
	}
	claimedStatus := rec.Status
	if claimedStatus != AgentCommandStatusResultPending {
		rec.Status = AgentCommandStatusProcessing
	}
	if claimedStatus != AgentCommandStatusResultPending && rec.Attempts < maxAttempts {
		rec.Attempts++
	}
	rec.RetryExhausted = retryExhausted
	rec.LeaseOwner = owner
	rec.LeaseUntilMS = leaseUntilMS
	rec.StartedAtMS = startedAt
	rec.UpdatedAtMS = nowMS
	return rec, true, nil
}

func (s *SQLiteStateStore) FinishAgentCommand(ctx context.Context, commandID, owner, status string, result json.RawMessage, lastError string, nowMS int64) (bool, error) {
	status = NormalizeFleetCommandResultStatus(status)
	if status == "" {
		return false, errors.New("esphttp: invalid agent command status")
	}
	if !fleetCommandStatusIsTerminalStore(status) {
		return false, errors.New("esphttp: agent command status must be terminal")
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE esp_agent_commands
SET status = ?, lease_owner = '', lease_until_ms = 0, completed_at_ms = ?,
    updated_at_ms = ?, result = ?, last_error = ?
WHERE command_id = ?
  AND (lease_owner = ? OR ? = '')`,
		status, nowMS, nowMS, nullableJSON(result), strings.TrimSpace(lastError),
		strings.TrimSpace(commandID), owner, owner)
	if err != nil {
		return false, fmt.Errorf("esphttp: finish agent command: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("esphttp: finish agent command rows affected: %w", err)
	}
	return n > 0, nil
}

func (s *SQLiteStateStore) DeferAgentCommandResult(ctx context.Context, commandID, owner string, result json.RawMessage, lastError string, retryAfterMS, nowMS int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE esp_agent_commands
SET status = ?, lease_owner = '', lease_until_ms = ?, updated_at_ms = ?,
    result = ?, last_error = ?
WHERE command_id = ?
  AND (lease_owner = ? OR ? = '')`,
		AgentCommandStatusResultPending, retryAfterMS, nowMS, nullableJSON(result), strings.TrimSpace(lastError),
		strings.TrimSpace(commandID), owner, owner)
	if err != nil {
		return false, fmt.Errorf("esphttp: defer agent command result: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("esphttp: defer agent command result rows affected: %w", err)
	}
	return n > 0, nil
}

func (s *SQLiteStateStore) AgentCommandStats(ctx context.Context, nowMS int64) (AgentCommandQueueStats, error) {
	var stats AgentCommandQueueStats
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM esp_agent_commands GROUP BY status`)
	if err != nil {
		return stats, fmt.Errorf("esphttp: agent command stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return stats, fmt.Errorf("esphttp: scan agent command stats: %w", err)
		}
		switch status {
		case FleetCommandStatusRunning:
			stats.Pending = count
		case AgentCommandStatusProcessing:
			stats.Processing = count
		case AgentCommandStatusResultPending:
			stats.ResultPending = count
		case FleetCommandStatusCompleted:
			stats.Completed = count
		case FleetCommandStatusFailed:
			stats.Failed = count
		case FleetCommandStatusRejected:
			stats.Rejected = count
		case FleetCommandStatusExpired:
			stats.Expired = count
		}
	}
	if err := rows.Err(); err != nil {
		return stats, err
	}
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM esp_agent_commands WHERE status = ? AND lease_until_ms <= ?`, "processing", nowMS).Scan(&stats.StaleProcessing)
	_ = s.db.QueryRowContext(ctx, `SELECT command_id FROM esp_agent_commands WHERE started_at_ms > 0 ORDER BY started_at_ms DESC LIMIT 1`).Scan(&stats.LastClaimedID)
	_ = s.db.QueryRowContext(ctx, `SELECT command_id FROM esp_agent_commands WHERE completed_at_ms > 0 ORDER BY completed_at_ms DESC LIMIT 1`).Scan(&stats.LastCompletedID)
	return stats, nil
}

func scanAgentCommand(row interface {
	Scan(dest ...any) error
}) (AgentCommandRecord, error) {
	var payloadJSON, result []byte
	var rec AgentCommandRecord
	err := row.Scan(&payloadJSON, &rec.Status, &rec.Attempts, &rec.LeaseOwner, &rec.LeaseUntilMS,
		&rec.StartedAtMS, &rec.CompletedAtMS, &rec.UpdatedAtMS, &result, &rec.LastError)
	if err != nil {
		return AgentCommandRecord{}, err
	}
	if err := json.Unmarshal(payloadJSON, &rec.Payload); err != nil {
		return AgentCommandRecord{}, fmt.Errorf("esphttp: decode agent command payload: %w", err)
	}
	if len(result) > 0 {
		rec.Result = append(json.RawMessage(nil), result...)
	}
	return rec, nil
}

func agentCommandRecordFromPayload(payload AgentInstructionPayload, status string, updatedAtMS int64) (AgentCommandRecord, error) {
	if strings.TrimSpace(payload.CommandID) == "" {
		return AgentCommandRecord{}, errors.New("esphttp: agent command id is required")
	}
	if strings.TrimSpace(payload.FleetID) == "" {
		return AgentCommandRecord{}, errors.New("esphttp: agent command fleet id is required")
	}
	if strings.TrimSpace(payload.Instruction) == "" {
		return AgentCommandRecord{}, errors.New("esphttp: agent command instruction is required")
	}
	if payload.Type == "" {
		payload.Type = AgentInstructionPayloadType
	}
	if payload.Version == 0 {
		payload.Version = 1
	}
	if payload.Action == "" {
		payload.Action = FleetCommandActionAgentInstruction
	}
	if status == "" {
		status = FleetCommandStatusRunning
	}
	if updatedAtMS == 0 {
		updatedAtMS = payload.ReceivedAtMS
	}
	return AgentCommandRecord{Payload: payload, Status: status, UpdatedAtMS: updatedAtMS}, nil
}

func nullableJSON(data []byte) any {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	return data
}

func errIfMissing(ok bool, err error) error {
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrNoRows
	}
	return nil
}

func fleetCommandStatusIsTerminalStore(status string) bool {
	switch NormalizeFleetCommandResultStatus(status) {
	case FleetCommandStatusCompleted, FleetCommandStatusFailed, FleetCommandStatusRejected, FleetCommandStatusExpired, FleetCommandStatusDuplicate:
		return true
	default:
		return false
	}
}
