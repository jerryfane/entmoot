package esphttp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
)

type FleetCommandListFilter struct {
	Status      string
	Action      string
	AgentNodeID entmoot.NodeID
	Limit       int
}

func (s *MemoryStateStore) UpsertFleetCommand(_ context.Context, cmd FleetCommandEnvelope) (FleetCommandSummaryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd = normalizeFleetCommandEnvelope(cmd)
	if err := validateFleetCommandRecord(cmd); err != nil {
		return FleetCommandSummaryRecord{}, err
	}
	if s.fleetCommands[cmd.FleetID] == nil {
		s.fleetCommands[cmd.FleetID] = make(map[string]FleetCommandEnvelope)
	}
	s.fleetCommands[cmd.FleetID][cmd.CommandID] = cloneFleetCommandEnvelope(cmd)
	return s.fleetCommandSummaryLocked(cmd.FleetID, cmd.CommandID), nil
}

func (s *MemoryStateStore) UpsertFleetCommandResult(_ context.Context, result FleetCommandResultEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	result = normalizeFleetCommandResultEnvelope(result)
	if err := validateFleetCommandResultRecord(result); err != nil {
		return err
	}
	if s.fleetCommandResults[result.FleetID] == nil {
		s.fleetCommandResults[result.FleetID] = make(map[string]map[entmoot.NodeID]FleetCommandResultEnvelope)
	}
	if s.fleetCommandResults[result.FleetID][result.CommandID] == nil {
		s.fleetCommandResults[result.FleetID][result.CommandID] = make(map[entmoot.NodeID]FleetCommandResultEnvelope)
	}
	s.fleetCommandResults[result.FleetID][result.CommandID][result.AgentNodeID] = cloneFleetCommandResultEnvelope(result)
	return nil
}

func (s *MemoryStateStore) GetFleetCommandDetail(_ context.Context, fleetID, commandID string) (FleetCommandDetailRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.fleetCommands[fleetID][strings.TrimSpace(commandID)]
	if !ok {
		return FleetCommandDetailRecord{}, false, nil
	}
	results := s.fleetCommandResultsForLocked(fleetID, cmd.CommandID)
	return FleetCommandDetailRecord{
		Command:     cloneFleetCommandEnvelope(cmd),
		Status:      fleetCommandAggregateStatus(cmd, results),
		Results:     results,
		UpdatedAtMS: fleetCommandUpdatedAt(cmd, results),
	}, true, nil
}

func (s *MemoryStateStore) ListFleetCommands(_ context.Context, fleetID string, filter FleetCommandListFilter) ([]FleetCommandSummaryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]FleetCommandSummaryRecord, 0, len(s.fleetCommands[fleetID]))
	filter.Action = NormalizeFleetCommandAction(filter.Action)
	filter.Status = strings.TrimSpace(strings.ToLower(filter.Status))
	for commandID, cmd := range s.fleetCommands[fleetID] {
		results := fleetCommandResultsForFilter(s.fleetCommandResultsForLocked(fleetID, commandID), filter)
		summary := fleetCommandSummaryFromCommand(cmd, results)
		if !fleetCommandSummaryMatches(summary, results, filter) {
			continue
		}
		if filter.Action != "" && NormalizeFleetCommandAction(cmd.Action) != filter.Action {
			continue
		}
		out = append(out, summary)
	}
	sortFleetCommandSummaries(out)
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (s *MemoryStateStore) fleetCommandSummaryLocked(fleetID, commandID string) FleetCommandSummaryRecord {
	cmd := s.fleetCommands[fleetID][commandID]
	results := s.fleetCommandResultsForLocked(fleetID, commandID)
	return fleetCommandSummaryFromCommand(cmd, results)
}

func (s *MemoryStateStore) fleetCommandResultsForLocked(fleetID, commandID string) []FleetCommandResultEnvelope {
	resultsByNode := s.fleetCommandResults[fleetID][commandID]
	out := make([]FleetCommandResultEnvelope, 0, len(resultsByNode))
	for _, result := range resultsByNode {
		out = append(out, cloneFleetCommandResultEnvelope(result))
	}
	sortFleetCommandResults(out)
	return out
}

func (s *SQLiteStateStore) UpsertFleetCommand(ctx context.Context, cmd FleetCommandEnvelope) (FleetCommandSummaryRecord, error) {
	cmd = normalizeFleetCommandEnvelope(cmd)
	if err := validateFleetCommandRecord(cmd); err != nil {
		return FleetCommandSummaryRecord{}, err
	}
	targetJSON, _ := json.Marshal(cmd.Target)
	argsJSON, _ := json.Marshal(cmd.Args)
	commandJSON, _ := json.Marshal(cmd)
	autoAccept := 0
	if cmd.AutoAccept {
		autoAccept = 1
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO esp_fleet_commands (
  command_id, fleet_id, issuer_node_id, target, action, args, auto_accept,
  created_at_ms, expires_at_ms, command, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(command_id) DO UPDATE SET
  fleet_id = excluded.fleet_id,
  issuer_node_id = excluded.issuer_node_id,
  target = excluded.target,
  action = excluded.action,
  args = excluded.args,
  auto_accept = excluded.auto_accept,
  created_at_ms = excluded.created_at_ms,
  expires_at_ms = excluded.expires_at_ms,
  command = excluded.command,
  updated_at_ms = excluded.updated_at_ms`,
		cmd.CommandID, cmd.FleetID, cmd.IssuerNodeID, targetJSON, cmd.Action, nullableJSON(argsJSON),
		autoAccept, cmd.CreatedAtMS, cmd.ExpiresAtMS, commandJSON, cmd.CreatedAtMS)
	if err != nil {
		return FleetCommandSummaryRecord{}, fmt.Errorf("esphttp: upsert fleet command: %w", err)
	}
	detail, found, err := s.GetFleetCommandDetail(ctx, cmd.FleetID, cmd.CommandID)
	if err != nil {
		return FleetCommandSummaryRecord{}, err
	}
	if !found {
		return FleetCommandSummaryRecord{}, fmt.Errorf("esphttp: upserted fleet command not found")
	}
	return fleetCommandSummaryFromDetail(detail), nil
}

func (s *SQLiteStateStore) UpsertFleetCommandResult(ctx context.Context, result FleetCommandResultEnvelope) error {
	result = normalizeFleetCommandResultEnvelope(result)
	if err := validateFleetCommandResultRecord(result); err != nil {
		return err
	}
	resultJSON, _ := json.Marshal(result)
	updatedAtMS := result.CompletedAtMS
	if updatedAtMS == 0 {
		updatedAtMS = result.StartedAtMS
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO esp_fleet_command_results (
  command_id, fleet_id, agent_node_id, action, status, summary, output,
  started_at_ms, completed_at_ms, result, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(command_id, agent_node_id) DO UPDATE SET
  fleet_id = excluded.fleet_id,
  action = excluded.action,
  status = excluded.status,
  summary = excluded.summary,
  output = excluded.output,
  started_at_ms = excluded.started_at_ms,
  completed_at_ms = excluded.completed_at_ms,
  result = excluded.result,
  updated_at_ms = excluded.updated_at_ms`,
		result.CommandID, result.FleetID, result.AgentNodeID, result.Action, result.Status,
		result.Summary, result.Output, result.StartedAtMS, result.CompletedAtMS, resultJSON, updatedAtMS)
	if err != nil {
		return fmt.Errorf("esphttp: upsert fleet command result: %w", err)
	}
	if updatedAtMS > 0 {
		_, _ = s.db.ExecContext(ctx, `
UPDATE esp_fleet_commands
SET updated_at_ms = CASE WHEN updated_at_ms < ? THEN ? ELSE updated_at_ms END
WHERE fleet_id = ? AND command_id = ?`, updatedAtMS, updatedAtMS, result.FleetID, result.CommandID)
	}
	return nil
}

func (s *SQLiteStateStore) GetFleetCommandDetail(ctx context.Context, fleetID, commandID string) (FleetCommandDetailRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT command FROM esp_fleet_commands WHERE fleet_id = ? AND command_id = ?`, fleetID, strings.TrimSpace(commandID))
	cmd, err := scanFleetCommandEnvelope(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetCommandDetailRecord{}, false, nil
	}
	if err != nil {
		return FleetCommandDetailRecord{}, false, err
	}
	results, err := s.listFleetCommandResults(ctx, fleetID, cmd.CommandID, FleetCommandListFilter{})
	if err != nil {
		return FleetCommandDetailRecord{}, false, err
	}
	return FleetCommandDetailRecord{
		Command:     cmd,
		Status:      fleetCommandAggregateStatus(cmd, results),
		Results:     results,
		UpdatedAtMS: fleetCommandUpdatedAt(cmd, results),
	}, true, nil
}

func (s *SQLiteStateStore) ListFleetCommands(ctx context.Context, fleetID string, filter FleetCommandListFilter) ([]FleetCommandSummaryRecord, error) {
	filter.Action = NormalizeFleetCommandAction(filter.Action)
	query := `SELECT command FROM esp_fleet_commands WHERE fleet_id = ?`
	args := []any{fleetID}
	if filter.Action != "" {
		query += ` AND action = ?`
		args = append(args, filter.Action)
	}
	query += ` ORDER BY updated_at_ms DESC, command_id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list fleet commands: %w", err)
	}
	defer rows.Close()
	out := make([]FleetCommandSummaryRecord, 0)
	for rows.Next() {
		cmd, err := scanFleetCommandEnvelope(rows)
		if err != nil {
			return nil, err
		}
		results, err := s.listFleetCommandResults(ctx, fleetID, cmd.CommandID, filter)
		if err != nil {
			return nil, err
		}
		summary := fleetCommandSummaryFromDetail(FleetCommandDetailRecord{
			Command:     cmd,
			Status:      fleetCommandAggregateStatus(cmd, results),
			Results:     results,
			UpdatedAtMS: fleetCommandUpdatedAt(cmd, results),
		})
		if !fleetCommandSummaryMatches(summary, results, filter) {
			continue
		}
		out = append(out, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sortFleetCommandSummaries(out)
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (s *SQLiteStateStore) listFleetCommandResults(ctx context.Context, fleetID, commandID string, filter FleetCommandListFilter) ([]FleetCommandResultEnvelope, error) {
	query := `SELECT result FROM esp_fleet_command_results WHERE fleet_id = ? AND command_id = ?`
	args := []any{fleetID, commandID}
	if filter.AgentNodeID != 0 {
		query += ` AND agent_node_id = ?`
		args = append(args, filter.AgentNodeID)
	}
	query += ` ORDER BY updated_at_ms DESC, agent_node_id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list fleet command results: %w", err)
	}
	defer rows.Close()
	out := make([]FleetCommandResultEnvelope, 0)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var result FleetCommandResultEnvelope
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("esphttp: decode fleet command result: %w", err)
		}
		out = append(out, cloneFleetCommandResultEnvelope(result))
	}
	return out, rows.Err()
}

func scanFleetCommandEnvelope(row interface{ Scan(dest ...any) error }) (FleetCommandEnvelope, error) {
	var data []byte
	if err := row.Scan(&data); err != nil {
		return FleetCommandEnvelope{}, err
	}
	var cmd FleetCommandEnvelope
	if err := json.Unmarshal(data, &cmd); err != nil {
		return FleetCommandEnvelope{}, fmt.Errorf("esphttp: decode fleet command: %w", err)
	}
	return cloneFleetCommandEnvelope(cmd), nil
}

func fleetCommandSummaryFromDetail(detail FleetCommandDetailRecord) FleetCommandSummaryRecord {
	return fleetCommandSummaryFromCommand(detail.Command, detail.Results)
}

func fleetCommandSummaryFromCommand(cmd FleetCommandEnvelope, results []FleetCommandResultEnvelope) FleetCommandSummaryRecord {
	return FleetCommandSummaryRecord{
		Command:      cloneFleetCommandEnvelope(cmd),
		Status:       fleetCommandAggregateStatus(cmd, results),
		ResultCount:  len(results),
		LatestResult: latestFleetCommandResult(results),
		UpdatedAtMS:  fleetCommandUpdatedAt(cmd, results),
	}
}

func fleetCommandSummaryMatches(summary FleetCommandSummaryRecord, results []FleetCommandResultEnvelope, filter FleetCommandListFilter) bool {
	if filter.Status != "" && strings.TrimSpace(strings.ToLower(summary.Status)) != filter.Status {
		return false
	}
	if filter.AgentNodeID == 0 {
		return true
	}
	for _, result := range results {
		if result.AgentNodeID == filter.AgentNodeID {
			return true
		}
	}
	return false
}

func fleetCommandResultsForFilter(results []FleetCommandResultEnvelope, filter FleetCommandListFilter) []FleetCommandResultEnvelope {
	if filter.AgentNodeID == 0 {
		return results
	}
	out := make([]FleetCommandResultEnvelope, 0, len(results))
	for _, result := range results {
		if result.AgentNodeID == filter.AgentNodeID {
			out = append(out, result)
		}
	}
	return out
}

func normalizeFleetCommandEnvelope(cmd FleetCommandEnvelope) FleetCommandEnvelope {
	cmd.CommandID = strings.TrimSpace(cmd.CommandID)
	cmd.FleetID = strings.TrimSpace(cmd.FleetID)
	cmd.Type = strings.TrimSpace(cmd.Type)
	if cmd.Type == "" {
		cmd.Type = FleetCommandMessageType
	}
	if cmd.Version == 0 {
		cmd.Version = 1
	}
	cmd.Action = NormalizeFleetCommandAction(cmd.Action)
	cmd.Target.Kind = NormalizeFleetCommandTarget(cmd.Target.Kind)
	return cmd
}

func normalizeFleetCommandResultEnvelope(result FleetCommandResultEnvelope) FleetCommandResultEnvelope {
	result.CommandID = strings.TrimSpace(result.CommandID)
	result.FleetID = strings.TrimSpace(result.FleetID)
	result.Type = strings.TrimSpace(result.Type)
	if result.Type == "" {
		result.Type = FleetCommandResultType
	}
	if result.Version == 0 {
		result.Version = 1
	}
	result.Action = NormalizeFleetCommandAction(result.Action)
	result.Status = NormalizeFleetCommandResultStatus(result.Status)
	result.Summary = strings.TrimSpace(result.Summary)
	return result
}

func validateFleetCommandRecord(cmd FleetCommandEnvelope) error {
	if cmd.CommandID == "" {
		return errors.New("esphttp: fleet command id is required")
	}
	if cmd.FleetID == "" {
		return errors.New("esphttp: fleet command fleet id is required")
	}
	if cmd.Action == "" {
		return errors.New("esphttp: fleet command action is required")
	}
	if cmd.CreatedAtMS == 0 {
		return errors.New("esphttp: fleet command created_at_ms is required")
	}
	return nil
}

func validateFleetCommandResultRecord(result FleetCommandResultEnvelope) error {
	if result.CommandID == "" {
		return errors.New("esphttp: fleet command result id is required")
	}
	if result.FleetID == "" {
		return errors.New("esphttp: fleet command result fleet id is required")
	}
	if result.AgentNodeID == 0 {
		return errors.New("esphttp: fleet command result agent node id is required")
	}
	if result.Status == "" {
		return errors.New("esphttp: fleet command result status is required")
	}
	return nil
}

func fleetCommandAggregateStatus(cmd FleetCommandEnvelope, results []FleetCommandResultEnvelope) string {
	if len(results) == 0 {
		if cmd.ExpiresAtMS > 0 && cmd.ExpiresAtMS <= time.Now().UnixMilli() {
			return FleetCommandStatusExpired
		}
		return FleetCommandStatusSent
	}
	hasRunning := false
	hasAccepted := false
	hasFailed := false
	hasRejected := false
	hasExpired := false
	hasDuplicate := false
	hasCompleted := false
	for _, result := range results {
		switch NormalizeFleetCommandResultStatus(result.Status) {
		case FleetCommandStatusRunning:
			hasRunning = true
		case FleetCommandStatusAccepted:
			hasAccepted = true
		case FleetCommandStatusFailed:
			hasFailed = true
		case FleetCommandStatusRejected:
			hasRejected = true
		case FleetCommandStatusExpired:
			hasExpired = true
		case FleetCommandStatusDuplicate:
			hasDuplicate = true
		case FleetCommandStatusCompleted:
			hasCompleted = true
		}
	}
	switch {
	case hasRunning:
		return FleetCommandStatusRunning
	case hasAccepted:
		return FleetCommandStatusAccepted
	case hasFailed:
		return FleetCommandStatusFailed
	case hasRejected:
		return FleetCommandStatusRejected
	case hasExpired:
		return FleetCommandStatusExpired
	case hasDuplicate:
		return FleetCommandStatusDuplicate
	case hasCompleted:
		return FleetCommandStatusCompleted
	default:
		return FleetCommandStatusSent
	}
}

func fleetCommandUpdatedAt(cmd FleetCommandEnvelope, results []FleetCommandResultEnvelope) int64 {
	updated := cmd.CreatedAtMS
	for _, result := range results {
		for _, candidate := range []int64{result.CompletedAtMS, result.StartedAtMS} {
			if candidate > updated {
				updated = candidate
			}
		}
	}
	return updated
}

func latestFleetCommandResult(results []FleetCommandResultEnvelope) *FleetCommandResultEnvelope {
	if len(results) == 0 {
		return nil
	}
	out := cloneFleetCommandResultEnvelope(results[0])
	return &out
}

func sortFleetCommandSummaries(commands []FleetCommandSummaryRecord) {
	sort.Slice(commands, func(i, j int) bool {
		if commands[i].UpdatedAtMS == commands[j].UpdatedAtMS {
			return commands[i].Command.CommandID < commands[j].Command.CommandID
		}
		return commands[i].UpdatedAtMS > commands[j].UpdatedAtMS
	})
}

func sortFleetCommandResults(results []FleetCommandResultEnvelope) {
	sort.Slice(results, func(i, j int) bool {
		left := fleetCommandResultUpdatedAt(results[i])
		right := fleetCommandResultUpdatedAt(results[j])
		if left == right {
			return results[i].AgentNodeID < results[j].AgentNodeID
		}
		return left > right
	})
}

func fleetCommandResultUpdatedAt(result FleetCommandResultEnvelope) int64 {
	if result.CompletedAtMS != 0 {
		return result.CompletedAtMS
	}
	return result.StartedAtMS
}

func cloneFleetCommandEnvelope(cmd FleetCommandEnvelope) FleetCommandEnvelope {
	data, _ := json.Marshal(cmd)
	var out FleetCommandEnvelope
	_ = json.Unmarshal(data, &out)
	return out
}

func cloneFleetCommandResultEnvelope(result FleetCommandResultEnvelope) FleetCommandResultEnvelope {
	data, _ := json.Marshal(result)
	var out FleetCommandResultEnvelope
	_ = json.Unmarshal(data, &out)
	return out
}
