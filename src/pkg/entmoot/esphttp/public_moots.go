package esphttp

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/publicmoot"
)

const (
	PublicMootStatusListed   = "listed"
	PublicMootStatusPending  = "pending"
	PublicMootStatusDelisted = "delisted"
	PublicMootStatusBlocked  = "blocked"

	PublicMootMirrorNone = "none"
)

var (
	ErrPublicMootBlocked         = errors.New("esphttp: public moot is blocked")
	ErrPublicMootFounderMismatch = errors.New("esphttp: public moot founder mismatch")
)

type PublicMootRecord struct {
	Descriptor        publicmoot.Descriptor
	Status            string
	IndexedAtMS       int64
	StatusUpdatedAtMS int64
}

type PublicMootListFilter struct {
	Statuses []string
}

type PublicMootDirectoryEntry struct {
	Descriptor              publicmoot.Descriptor `json:"descriptor"`
	Status                  string                `json:"status"`
	PolicySummary           string                `json:"policy_summary"`
	MirrorState             string                `json:"mirror_state"`
	MessageHistoryAvailable bool                  `json:"message_history_available"`
	IndexedAtMS             int64                 `json:"indexed_at_ms"`
	StatusUpdatedAtMS       int64                 `json:"status_updated_at_ms"`
}

func PublicMootEntryFromRecord(rec PublicMootRecord) PublicMootDirectoryEntry {
	return PublicMootDirectoryEntry{
		Descriptor:              clonePublicMootDescriptor(rec.Descriptor),
		Status:                  normalizePublicMootStatus(rec.Status),
		PolicySummary:           policy.Summary(rec.Descriptor.Policy),
		MirrorState:             PublicMootMirrorNone,
		MessageHistoryAvailable: false,
		IndexedAtMS:             rec.IndexedAtMS,
		StatusUpdatedAtMS:       rec.StatusUpdatedAtMS,
	}
}

func validPublicMootStatus(status string) bool {
	switch normalizePublicMootStatus(status) {
	case PublicMootStatusListed, PublicMootStatusPending, PublicMootStatusDelisted, PublicMootStatusBlocked:
		return true
	default:
		return false
	}
}

func normalizePublicMootStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}

func defaultPublicMootStatus(status string) string {
	status = normalizePublicMootStatus(status)
	if status == "" {
		return PublicMootStatusListed
	}
	return status
}

func clonePublicMootRecord(rec PublicMootRecord) PublicMootRecord {
	rec.Descriptor = clonePublicMootDescriptor(rec.Descriptor)
	rec.Status = defaultPublicMootStatus(rec.Status)
	return rec
}

func clonePublicMootDescriptor(desc publicmoot.Descriptor) publicmoot.Descriptor {
	desc.Tags = append([]string(nil), desc.Tags...)
	desc.Founder.EntmootPubKey = append([]byte(nil), desc.Founder.EntmootPubKey...)
	desc.Signature = append([]byte(nil), desc.Signature...)
	if desc.OpenInvite != nil {
		openInvite := *desc.OpenInvite
		desc.OpenInvite = &openInvite
	}
	return desc
}

func publicMootFounderKey(desc publicmoot.Descriptor) string {
	return base64.StdEncoding.EncodeToString(desc.Founder.EntmootPubKey)
}

func publicMootStatusSet(filter PublicMootListFilter) map[string]struct{} {
	if len(filter.Statuses) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(filter.Statuses))
	for _, status := range filter.Statuses {
		status = defaultPublicMootStatus(status)
		if status != "" {
			out[status] = struct{}{}
		}
	}
	return out
}

func publicMootMatchesFilter(rec PublicMootRecord, filter PublicMootListFilter) bool {
	statuses := publicMootStatusSet(filter)
	if len(statuses) == 0 {
		return true
	}
	_, ok := statuses[normalizePublicMootStatus(rec.Status)]
	return ok
}

func (s *MemoryStateStore) UpsertPublicMoot(_ context.Context, rec PublicMootRecord, nowMS int64) (PublicMootRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := publicmoot.Verify(rec.Descriptor); err != nil {
		return PublicMootRecord{}, false, err
	}
	if nowMS <= 0 {
		nowMS = s.nowMS()
	}
	rec.Descriptor = clonePublicMootDescriptor(rec.Descriptor)
	rec.Status = defaultPublicMootStatus(rec.Status)
	if !validPublicMootStatus(rec.Status) {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: invalid public moot status %q", rec.Status)
	}
	existing, exists := s.publicMoots[rec.Descriptor.GroupID]
	if exists && normalizePublicMootStatus(existing.Status) == PublicMootStatusBlocked {
		return clonePublicMootRecord(existing), false, ErrPublicMootBlocked
	}
	founder := publicMootFounderKey(rec.Descriptor)
	if exists && publicMootFounderKey(existing.Descriptor) != founder {
		return clonePublicMootRecord(existing), false, ErrPublicMootFounderMismatch
	}
	for _, other := range s.publicMoots {
		if publicMootFounderKey(other.Descriptor) == founder && normalizePublicMootStatus(other.Status) == PublicMootStatusBlocked {
			return clonePublicMootRecord(other), false, ErrPublicMootBlocked
		}
	}
	if exists && existing.Descriptor.UpdatedAtMS >= rec.Descriptor.UpdatedAtMS {
		return clonePublicMootRecord(existing), false, nil
	}
	if exists {
		rec.Status = defaultPublicMootStatus(existing.Status)
		rec.IndexedAtMS = existing.IndexedAtMS
		rec.StatusUpdatedAtMS = existing.StatusUpdatedAtMS
	}
	if rec.IndexedAtMS == 0 {
		rec.IndexedAtMS = nowMS
	}
	if rec.StatusUpdatedAtMS == 0 {
		rec.StatusUpdatedAtMS = nowMS
	}
	s.publicMoots[rec.Descriptor.GroupID] = clonePublicMootRecord(rec)
	return clonePublicMootRecord(rec), true, nil
}

func (s *MemoryStateStore) ListPublicMoots(_ context.Context, filter PublicMootListFilter) ([]PublicMootRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PublicMootRecord, 0, len(s.publicMoots))
	for _, rec := range s.publicMoots {
		if publicMootMatchesFilter(rec, filter) {
			out = append(out, clonePublicMootRecord(rec))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Descriptor.UpdatedAtMS > out[j].Descriptor.UpdatedAtMS
	})
	return out, nil
}

func (s *MemoryStateStore) GetPublicMoot(_ context.Context, groupID entmoot.GroupID) (PublicMootRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.publicMoots[groupID]
	return clonePublicMootRecord(rec), ok, nil
}

func (s *MemoryStateStore) UpdatePublicMootIndexStatus(_ context.Context, groupID entmoot.GroupID, status string, nowMS int64) (PublicMootRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status = normalizePublicMootStatus(status)
	if !validPublicMootStatus(status) {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: invalid public moot status %q", status)
	}
	rec, ok := s.publicMoots[groupID]
	if !ok {
		return PublicMootRecord{}, false, nil
	}
	if nowMS <= 0 {
		nowMS = s.nowMS()
	}
	rec.Status = status
	rec.StatusUpdatedAtMS = nowMS
	s.publicMoots[groupID] = clonePublicMootRecord(rec)
	return clonePublicMootRecord(rec), true, nil
}

func (s *SQLiteStateStore) UpsertPublicMoot(ctx context.Context, rec PublicMootRecord, nowMS int64) (PublicMootRecord, bool, error) {
	if err := publicmoot.Verify(rec.Descriptor); err != nil {
		return PublicMootRecord{}, false, err
	}
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	rec.Descriptor = clonePublicMootDescriptor(rec.Descriptor)
	rec.Status = defaultPublicMootStatus(rec.Status)
	if !validPublicMootStatus(rec.Status) {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: invalid public moot status %q", rec.Status)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: begin public moot upsert: %w", err)
	}
	defer tx.Rollback()
	existing, exists, err := getPublicMootTx(ctx, tx, rec.Descriptor.GroupID)
	if err != nil {
		return PublicMootRecord{}, false, err
	}
	if exists && normalizePublicMootStatus(existing.Status) == PublicMootStatusBlocked {
		return existing, false, ErrPublicMootBlocked
	}
	founder := publicMootFounderKey(rec.Descriptor)
	if exists && publicMootFounderKey(existing.Descriptor) != founder {
		return existing, false, ErrPublicMootFounderMismatch
	}
	blockedFounder, err := publicMootBlockedFounderTx(ctx, tx, founder)
	if err != nil {
		return PublicMootRecord{}, false, err
	}
	if blockedFounder != nil {
		return *blockedFounder, false, ErrPublicMootBlocked
	}
	if exists && existing.Descriptor.UpdatedAtMS >= rec.Descriptor.UpdatedAtMS {
		return existing, false, tx.Commit()
	}
	if exists {
		rec.Status = defaultPublicMootStatus(existing.Status)
		rec.IndexedAtMS = existing.IndexedAtMS
		rec.StatusUpdatedAtMS = existing.StatusUpdatedAtMS
	}
	if rec.IndexedAtMS == 0 {
		rec.IndexedAtMS = nowMS
	}
	if rec.StatusUpdatedAtMS == 0 {
		rec.StatusUpdatedAtMS = nowMS
	}
	raw, err := json.Marshal(rec.Descriptor)
	if err != nil {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: marshal public moot descriptor: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO esp_public_moots
  (group_id, founder_pubkey, descriptor, descriptor_updated_at_ms, status, indexed_at_ms, status_updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(group_id) DO UPDATE SET
  founder_pubkey = excluded.founder_pubkey,
  descriptor = excluded.descriptor,
  descriptor_updated_at_ms = excluded.descriptor_updated_at_ms,
  status = excluded.status,
  indexed_at_ms = excluded.indexed_at_ms,
  status_updated_at_ms = excluded.status_updated_at_ms`,
		rec.Descriptor.GroupID[:], publicMootFounderKey(rec.Descriptor), raw, rec.Descriptor.UpdatedAtMS, rec.Status, rec.IndexedAtMS, rec.StatusUpdatedAtMS)
	if err != nil {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: upsert public moot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PublicMootRecord{}, false, err
	}
	return clonePublicMootRecord(rec), true, nil
}

func (s *SQLiteStateStore) ListPublicMoots(ctx context.Context, filter PublicMootListFilter) ([]PublicMootRecord, error) {
	statuses := publicMootStatusSet(filter)
	query := `SELECT descriptor, status, indexed_at_ms, status_updated_at_ms FROM esp_public_moots`
	args := []any{}
	if len(statuses) > 0 {
		parts := make([]string, 0, len(statuses))
		keys := make([]string, 0, len(statuses))
		for status := range statuses {
			keys = append(keys, status)
		}
		sort.Strings(keys)
		for _, status := range keys {
			parts = append(parts, "?")
			args = append(args, status)
		}
		query += ` WHERE status IN (` + strings.Join(parts, ",") + `)`
	}
	query += ` ORDER BY descriptor_updated_at_ms DESC, rowid DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list public moots: %w", err)
	}
	defer rows.Close()
	var out []PublicMootRecord
	for rows.Next() {
		rec, err := scanPublicMootRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) GetPublicMoot(ctx context.Context, groupID entmoot.GroupID) (PublicMootRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT descriptor, status, indexed_at_ms, status_updated_at_ms FROM esp_public_moots WHERE group_id = ?`, groupID[:])
	return scanPublicMootRow(row)
}

func (s *SQLiteStateStore) UpdatePublicMootIndexStatus(ctx context.Context, groupID entmoot.GroupID, status string, nowMS int64) (PublicMootRecord, bool, error) {
	status = normalizePublicMootStatus(status)
	if !validPublicMootStatus(status) {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: invalid public moot status %q", status)
	}
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE esp_public_moots SET status = ?, status_updated_at_ms = ? WHERE group_id = ?`, status, nowMS, groupID[:])
	if err != nil {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: update public moot status: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return PublicMootRecord{}, false, err
	}
	if changed == 0 {
		return PublicMootRecord{}, false, nil
	}
	rec, ok, err := s.GetPublicMoot(ctx, groupID)
	return rec, ok, err
}

type publicMootScanner interface {
	Scan(dest ...any) error
}

func scanPublicMootRow(row publicMootScanner) (PublicMootRecord, bool, error) {
	rec, err := scanPublicMootRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return PublicMootRecord{}, false, nil
	}
	if err != nil {
		return PublicMootRecord{}, false, err
	}
	return rec, true, nil
}

func scanPublicMootRecord(row publicMootScanner) (PublicMootRecord, error) {
	var raw []byte
	var rec PublicMootRecord
	if err := row.Scan(&raw, &rec.Status, &rec.IndexedAtMS, &rec.StatusUpdatedAtMS); err != nil {
		return PublicMootRecord{}, err
	}
	desc, err := publicmoot.Parse(raw)
	if err != nil {
		return PublicMootRecord{}, err
	}
	rec.Descriptor = desc
	return clonePublicMootRecord(rec), nil
}

func getPublicMootTx(ctx context.Context, tx *sql.Tx, groupID entmoot.GroupID) (PublicMootRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT descriptor, status, indexed_at_ms, status_updated_at_ms FROM esp_public_moots WHERE group_id = ?`, groupID[:])
	return scanPublicMootRow(row)
}

func publicMootBlockedFounderTx(ctx context.Context, tx *sql.Tx, founder string) (*PublicMootRecord, error) {
	row := tx.QueryRowContext(ctx, `SELECT descriptor, status, indexed_at_ms, status_updated_at_ms FROM esp_public_moots WHERE founder_pubkey = ? AND status = ? LIMIT 1`, founder, PublicMootStatusBlocked)
	rec, ok, err := scanPublicMootRow(row)
	if err != nil || !ok {
		return nil, err
	}
	return &rec, nil
}
