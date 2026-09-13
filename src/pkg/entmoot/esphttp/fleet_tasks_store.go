package esphttp

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
)

func (s *SQLiteStateStore) UpsertFleetTask(ctx context.Context, rec FleetTaskRecord) (FleetTaskRecord, error) {
	now := time.Now().UnixMilli()
	var err error
	rec, err = normalizeFleetTaskRecord(rec, now)
	if err != nil {
		return FleetTaskRecord{}, err
	}
	var assigneeMember []byte
	var assigneePeer, assigneePub string
	if rec.Assignee != nil {
		memberID := nodeInfoMemberID(*rec.Assignee)
		assigneeMember = memberID[:]
		assigneePeer = rec.Assignee.PeerID
		assigneePub = base64.StdEncoding.EncodeToString(rec.Assignee.EntmootPubKey)
	}
	creatorMember := nodeInfoMemberID(rec.Creator)
	_, err = s.db.ExecContext(ctx, `
INSERT INTO esp_fleet_tasks
  (task_id, fleet_id, title, description, mode, status, creator_member_id, creator_peer_id, creator_pubkey,
   assignee_member_id, assignee_peer_id, assignee_pubkey, created_at_ms, updated_at_ms, completed_at_ms, rejected_at_ms, canceled_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(task_id) DO UPDATE SET
  title = excluded.title,
  description = excluded.description,
  mode = excluded.mode,
  status = excluded.status,
  assignee_member_id = excluded.assignee_member_id,
  assignee_peer_id = excluded.assignee_peer_id,
  assignee_pubkey = excluded.assignee_pubkey,
  updated_at_ms = excluded.updated_at_ms,
  completed_at_ms = excluded.completed_at_ms,
  rejected_at_ms = excluded.rejected_at_ms,
  canceled_at_ms = excluded.canceled_at_ms`,
		rec.TaskID, rec.FleetID, rec.Title, rec.Description, rec.Mode, rec.Status,
		creatorMember[:], rec.Creator.PeerID, base64.StdEncoding.EncodeToString(rec.Creator.EntmootPubKey),
		assigneeMember, assigneePeer, assigneePub, rec.CreatedAtMS, rec.UpdatedAtMS, rec.CompletedAtMS, rec.RejectedAtMS, rec.CanceledAtMS)
	if err != nil {
		return FleetTaskRecord{}, fmt.Errorf("esphttp: upsert fleet task: %w", err)
	}
	return cloneFleetTaskRecord(rec), nil
}

func (s *SQLiteStateStore) ClaimFleetTask(ctx context.Context, rec FleetTaskRecord) (FleetTaskRecord, bool, error) {
	now := time.Now().UnixMilli()
	var err error
	rec, err = normalizeFleetTaskRecord(rec, now)
	if err != nil {
		return FleetTaskRecord{}, false, err
	}
	if rec.Assignee == nil {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: claim fleet task: assignee is required")
	}
	assigneeMember := nodeInfoMemberID(*rec.Assignee)
	res, err := s.db.ExecContext(ctx, `
UPDATE esp_fleet_tasks
SET status = ?, assignee_member_id = ?, assignee_pubkey = ?, updated_at_ms = ?
WHERE fleet_id = ? AND task_id = ? AND mode = ? AND status = ? AND assignee_member_id IS NULL AND assignee_pubkey = ''`,
		rec.Status, assigneeMember[:], base64.StdEncoding.EncodeToString(rec.Assignee.EntmootPubKey), rec.UpdatedAtMS,
		rec.FleetID, rec.TaskID, FleetTaskModeFirstClaim, FleetTaskStatusOpen)
	if err != nil {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: claim fleet task: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: claim fleet task rows affected: %w", err)
	}
	if affected == 0 {
		return FleetTaskRecord{}, false, nil
	}
	claimed, found, err := s.GetFleetTask(ctx, rec.FleetID, rec.TaskID)
	if err != nil {
		return FleetTaskRecord{}, false, err
	}
	if !found {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: claimed fleet task not found")
	}
	return claimed, true, nil
}

func (s *SQLiteStateStore) UpdateFleetTaskIfCurrent(ctx context.Context, rec FleetTaskRecord, expectedUpdatedAtMS int64) (FleetTaskRecord, bool, error) {
	now := time.Now().UnixMilli()
	var err error
	rec, err = normalizeFleetTaskRecord(rec, now)
	if err != nil {
		return FleetTaskRecord{}, false, err
	}
	var assigneeMember []byte
	var assigneePub string
	if rec.Assignee != nil {
		memberID := nodeInfoMemberID(*rec.Assignee)
		assigneeMember = memberID[:]
		assigneePub = base64.StdEncoding.EncodeToString(rec.Assignee.EntmootPubKey)
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE esp_fleet_tasks
SET title = ?, description = ?, mode = ?, status = ?, assignee_member_id = ?, assignee_pubkey = ?,
    updated_at_ms = ?, completed_at_ms = ?, rejected_at_ms = ?, canceled_at_ms = ?
WHERE fleet_id = ? AND task_id = ? AND updated_at_ms = ?`,
		rec.Title, rec.Description, rec.Mode, rec.Status, assigneeMember, assigneePub,
		rec.UpdatedAtMS, rec.CompletedAtMS, rec.RejectedAtMS, rec.CanceledAtMS,
		rec.FleetID, rec.TaskID, expectedUpdatedAtMS)
	if err != nil {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: update fleet task: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: update fleet task rows affected: %w", err)
	}
	if affected == 0 {
		return FleetTaskRecord{}, false, nil
	}
	updated, found, err := s.GetFleetTask(ctx, rec.FleetID, rec.TaskID)
	if err != nil {
		return FleetTaskRecord{}, false, err
	}
	if !found {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: updated fleet task not found")
	}
	return updated, true, nil
}

func (s *SQLiteStateStore) SubmitFleetTask(ctx context.Context, rec FleetTaskRecord, expectedUpdatedAtMS int64, submission FleetTaskSubmissionRecord) (FleetTaskRecord, FleetTaskSubmissionRecord, bool, error) {
	now := time.Now().UnixMilli()
	var err error
	rec, err = normalizeFleetTaskRecord(rec, now)
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
	}
	submission, err = normalizeFleetTaskSubmissionRecord(submission, now)
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, fmt.Errorf("esphttp: submit fleet task begin: %w", err)
	}
	defer tx.Rollback()
	if rec.Mode == FleetTaskModeOpenSubmission {
		current, found, err := getFleetTaskTx(ctx, tx, rec.FleetID, rec.TaskID)
		if err != nil {
			return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
		}
		if !found || current.Mode != FleetTaskModeOpenSubmission || !fleetTaskCanAcceptSubmission(current) {
			return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, nil
		}
		if err := insertFleetTaskSubmission(ctx, tx, submission); err != nil {
			return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, fmt.Errorf("esphttp: submit fleet task commit: %w", err)
		}
		return current, cloneFleetTaskSubmissionRecord(submission), true, nil
	}
	var assigneeMember []byte
	var assigneePub string
	if rec.Assignee != nil {
		memberID := nodeInfoMemberID(*rec.Assignee)
		assigneeMember = memberID[:]
		assigneePub = base64.StdEncoding.EncodeToString(rec.Assignee.EntmootPubKey)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE esp_fleet_tasks
SET title = ?, description = ?, mode = ?, status = ?, assignee_member_id = ?, assignee_pubkey = ?,
    updated_at_ms = ?, completed_at_ms = ?, rejected_at_ms = ?, canceled_at_ms = ?
WHERE fleet_id = ? AND task_id = ? AND updated_at_ms = ?`,
		rec.Title, rec.Description, rec.Mode, rec.Status, assigneeMember, assigneePub,
		rec.UpdatedAtMS, rec.CompletedAtMS, rec.RejectedAtMS, rec.CanceledAtMS,
		rec.FleetID, rec.TaskID, expectedUpdatedAtMS)
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, fmt.Errorf("esphttp: submit fleet task update: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, fmt.Errorf("esphttp: submit fleet task rows affected: %w", err)
	}
	if affected == 0 {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, nil
	}
	if err := insertFleetTaskSubmission(ctx, tx, submission); err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, fmt.Errorf("esphttp: submit fleet task commit: %w", err)
	}
	updated, found, err := s.GetFleetTask(ctx, rec.FleetID, rec.TaskID)
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
	}
	if !found {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, fmt.Errorf("esphttp: submitted fleet task not found")
	}
	return updated, cloneFleetTaskSubmissionRecord(submission), true, nil
}

func getFleetTaskTx(ctx context.Context, tx *sql.Tx, fleetID, taskID string) (FleetTaskRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT task_id, fleet_id, title, description, mode, status, creator_member_id, creator_peer_id, creator_pubkey, assignee_member_id, assignee_peer_id, assignee_pubkey, created_at_ms, updated_at_ms, completed_at_ms, rejected_at_ms, canceled_at_ms FROM esp_fleet_tasks WHERE fleet_id = ? AND task_id = ?`, fleetID, taskID)
	rec, err := scanFleetTaskRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetTaskRecord{}, false, nil
	}
	if err != nil {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: get fleet task: %w", err)
	}
	return rec, true, nil
}

func insertFleetTaskSubmission(ctx context.Context, execer fleetTaskSubmissionExecer, rec FleetTaskSubmissionRecord) error {
	authorMember := nodeInfoMemberID(rec.Author)
	_, err := execer.ExecContext(ctx, `
INSERT INTO esp_fleet_task_submissions
  (submission_id, fleet_id, task_id, author_member_id, author_peer_id, author_pubkey, content, status, created_at_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(submission_id) DO UPDATE SET
  content = excluded.content,
  status = excluded.status,
  updated_at_ms = excluded.updated_at_ms`,
		rec.SubmissionID, rec.FleetID, rec.TaskID, authorMember[:], rec.Author.PeerID,
		base64.StdEncoding.EncodeToString(rec.Author.EntmootPubKey), rec.Content, rec.Status, rec.CreatedAtMS, rec.UpdatedAtMS)
	if err != nil {
		return fmt.Errorf("esphttp: upsert fleet task submission: %w", err)
	}
	return nil
}

type fleetTaskSubmissionExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *SQLiteStateStore) GetFleetTask(ctx context.Context, fleetID, taskID string) (FleetTaskRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT task_id, fleet_id, title, description, mode, status, creator_member_id, creator_peer_id, creator_pubkey, assignee_member_id, assignee_peer_id, assignee_pubkey, created_at_ms, updated_at_ms, completed_at_ms, rejected_at_ms, canceled_at_ms FROM esp_fleet_tasks WHERE fleet_id = ? AND task_id = ?`, fleetID, taskID)
	rec, err := scanFleetTaskRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetTaskRecord{}, false, nil
	}
	if err != nil {
		return FleetTaskRecord{}, false, fmt.Errorf("esphttp: get fleet task: %w", err)
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) ListFleetTasks(ctx context.Context, fleetID, status string) ([]FleetTaskRecord, error) {
	query := `SELECT task_id, fleet_id, title, description, mode, status, creator_member_id, creator_peer_id, creator_pubkey, assignee_member_id, assignee_peer_id, assignee_pubkey, created_at_ms, updated_at_ms, completed_at_ms, rejected_at_ms, canceled_at_ms FROM esp_fleet_tasks WHERE fleet_id = ?`
	args := []any{fleetID}
	if strings.TrimSpace(status) != "" {
		query += ` AND status = ?`
		args = append(args, strings.TrimSpace(status))
	}
	query += ` ORDER BY updated_at_ms DESC, task_id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list fleet tasks: %w", err)
	}
	defer rows.Close()
	out := make([]FleetTaskRecord, 0)
	for rows.Next() {
		rec, err := scanFleetTaskRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) DeleteFleetTask(ctx context.Context, fleetID, taskID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_task_submissions WHERE fleet_id = ? AND task_id = ?`, fleetID, taskID); err != nil {
		return fmt.Errorf("esphttp: delete fleet task submissions: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_tasks WHERE fleet_id = ? AND task_id = ?`, fleetID, taskID); err != nil {
		return fmt.Errorf("esphttp: delete fleet task: %w", err)
	}
	return nil
}

func (s *SQLiteStateStore) UpsertFleetTaskSubmission(ctx context.Context, rec FleetTaskSubmissionRecord) (FleetTaskSubmissionRecord, error) {
	now := time.Now().UnixMilli()
	var err error
	rec, err = normalizeFleetTaskSubmissionRecord(rec, now)
	if err != nil {
		return FleetTaskSubmissionRecord{}, err
	}
	if err := insertFleetTaskSubmission(ctx, s.db, rec); err != nil {
		return FleetTaskSubmissionRecord{}, err
	}
	return cloneFleetTaskSubmissionRecord(rec), nil
}

func (s *SQLiteStateStore) GetFleetTaskSubmission(ctx context.Context, fleetID, taskID, submissionID string) (FleetTaskSubmissionRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT submission_id, fleet_id, task_id, author_member_id, author_peer_id, author_pubkey, content, status, created_at_ms, updated_at_ms FROM esp_fleet_task_submissions WHERE fleet_id = ? AND task_id = ? AND submission_id = ?`, fleetID, taskID, submissionID)
	rec, err := scanFleetTaskSubmissionRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetTaskSubmissionRecord{}, false, nil
	}
	if err != nil {
		return FleetTaskSubmissionRecord{}, false, fmt.Errorf("esphttp: get fleet task submission: %w", err)
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) ListFleetTaskSubmissions(ctx context.Context, fleetID, taskID string) ([]FleetTaskSubmissionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT submission_id, fleet_id, task_id, author_member_id, author_peer_id, author_pubkey, content, status, created_at_ms, updated_at_ms FROM esp_fleet_task_submissions WHERE fleet_id = ? AND task_id = ? ORDER BY created_at_ms DESC, submission_id ASC`, fleetID, taskID)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list fleet task submissions: %w", err)
	}
	defer rows.Close()
	out := make([]FleetTaskSubmissionRecord, 0)
	for rows.Next() {
		rec, err := scanFleetTaskSubmissionRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

type fleetTaskScanner interface {
	Scan(...any) error
}

func scanFleetTaskRecord(row fleetTaskScanner) (FleetTaskRecord, error) {
	var rec FleetTaskRecord
	var creatorBytes, assigneeBytes []byte
	var creatorPeer, creatorPub, assigneePeer, assigneePub string
	if err := row.Scan(&rec.TaskID, &rec.FleetID, &rec.Title, &rec.Description, &rec.Mode, &rec.Status,
		&creatorBytes, &creatorPeer, &creatorPub, &assigneeBytes, &assigneePeer, &assigneePub,
		&rec.CreatedAtMS, &rec.UpdatedAtMS, &rec.CompletedAtMS, &rec.RejectedAtMS, &rec.CanceledAtMS); err != nil {
		return FleetTaskRecord{}, err
	}
	if len(creatorBytes) != len(entmoot.MemberID{}) {
		return FleetTaskRecord{}, errors.New("esphttp: invalid fleet task creator member id")
	}
	creatorID := entmoot.MemberID{}
	copy(creatorID[:], creatorBytes)
	pub, err := base64.StdEncoding.DecodeString(creatorPub)
	if err != nil {
		return FleetTaskRecord{}, fmt.Errorf("esphttp: decode fleet task creator pubkey: %w", err)
	}
	rec.Creator = entmoot.NodeInfo{MemberID: &creatorID, PeerID: creatorPeer, EntmootPubKey: pub}
	if len(assigneeBytes) != 0 {
		if len(assigneeBytes) != len(entmoot.MemberID{}) {
			return FleetTaskRecord{}, errors.New("esphttp: invalid fleet task assignee member id")
		}
		assigneeID := entmoot.MemberID{}
		copy(assigneeID[:], assigneeBytes)
		pub, err := base64.StdEncoding.DecodeString(assigneePub)
		if err != nil {
			return FleetTaskRecord{}, fmt.Errorf("esphttp: decode fleet task assignee pubkey: %w", err)
		}
		rec.Assignee = &entmoot.NodeInfo{MemberID: &assigneeID, PeerID: assigneePeer, EntmootPubKey: pub}
	}
	return rec, nil
}

func scanFleetTaskSubmissionRecord(row fleetTaskScanner) (FleetTaskSubmissionRecord, error) {
	var rec FleetTaskSubmissionRecord
	var authorBytes []byte
	var authorPeer, authorPub string
	if err := row.Scan(&rec.SubmissionID, &rec.FleetID, &rec.TaskID, &authorBytes, &authorPeer, &authorPub, &rec.Content, &rec.Status, &rec.CreatedAtMS, &rec.UpdatedAtMS); err != nil {
		return FleetTaskSubmissionRecord{}, err
	}
	if len(authorBytes) != len(entmoot.MemberID{}) {
		return FleetTaskSubmissionRecord{}, errors.New("esphttp: invalid fleet task submission author member id")
	}
	authorID := entmoot.MemberID{}
	copy(authorID[:], authorBytes)
	pub, err := base64.StdEncoding.DecodeString(authorPub)
	if err != nil {
		return FleetTaskSubmissionRecord{}, fmt.Errorf("esphttp: decode fleet task submission author pubkey: %w", err)
	}
	rec.Author = entmoot.NodeInfo{MemberID: &authorID, PeerID: authorPeer, EntmootPubKey: pub}
	return rec, nil
}
