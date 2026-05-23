package esphttp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/publicmoot"
)

const (
	PublicMootStatusListed   = "listed"
	PublicMootStatusPending  = "pending"
	PublicMootStatusDelisted = "delisted"
	PublicMootStatusBlocked  = "blocked"
)

var ErrPublicMootFounderMismatch = errors.New("esphttp: public moot founder mismatch")

type PublicMootRecord struct {
	Descriptor        publicmoot.Descriptor
	Status            string
	CreatedAtMS       int64
	UpdatedAtMS       int64
	StatusUpdatedAtMS int64
}

func NormalizePublicMootStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", PublicMootStatusListed:
		return PublicMootStatusListed
	case PublicMootStatusPending:
		return PublicMootStatusPending
	case PublicMootStatusDelisted:
		return PublicMootStatusDelisted
	case PublicMootStatusBlocked:
		return PublicMootStatusBlocked
	default:
		return ""
	}
}

func PublicMootRecordHasDescriptor(rec PublicMootRecord) bool {
	return rec.Descriptor.Type == publicmoot.DescriptorType && rec.Descriptor.GroupID != (entmoot.GroupID{})
}

func publicMootFounderMatches(rec PublicMootRecord, desc publicmoot.Descriptor) bool {
	return bytes.Equal(rec.Descriptor.Founder.EntmootPubKey, desc.Founder.EntmootPubKey)
}

func (s *MemoryStateStore) UpsertPublicMootDescriptor(_ context.Context, rec PublicMootRecord, nowMS int64) (PublicMootRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nowMS <= 0 {
		nowMS = s.nowMS()
	}
	if err := publicmoot.Verify(rec.Descriptor); err != nil {
		return PublicMootRecord{}, false, err
	}
	current, ok := s.publicMoots[rec.Descriptor.GroupID]
	if ok && PublicMootRecordHasDescriptor(current) {
		if !publicMootFounderMatches(current, rec.Descriptor) {
			return clonePublicMootRecord(current), false, ErrPublicMootFounderMismatch
		}
		if rec.Descriptor.UpdatedAtMS <= current.Descriptor.UpdatedAtMS {
			return clonePublicMootRecord(current), false, nil
		}
	}
	status := NormalizePublicMootStatus(current.Status)
	if status == "" {
		status = PublicMootStatusListed
	}
	if current.CreatedAtMS == 0 {
		current.CreatedAtMS = nowMS
	}
	if current.StatusUpdatedAtMS == 0 {
		current.StatusUpdatedAtMS = nowMS
	}
	current.Descriptor = clonePublicMootDescriptor(rec.Descriptor)
	current.Status = status
	current.UpdatedAtMS = nowMS
	s.publicMoots[rec.Descriptor.GroupID] = clonePublicMootRecord(current)
	return clonePublicMootRecord(current), true, nil
}

func (s *MemoryStateStore) GetPublicMoot(_ context.Context, gid entmoot.GroupID) (PublicMootRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.publicMoots[gid]
	return clonePublicMootRecord(rec), ok, nil
}

func (s *MemoryStateStore) ListPublicMoots(_ context.Context, status string) ([]PublicMootRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status = normalizePublicMootListFilter(status)
	out := make([]PublicMootRecord, 0, len(s.publicMoots))
	for _, rec := range s.publicMoots {
		if status != "" && NormalizePublicMootStatus(rec.Status) != status {
			continue
		}
		out = append(out, clonePublicMootRecord(rec))
	}
	sortPublicMootRecords(out)
	return out, nil
}

func (s *MemoryStateStore) SetPublicMootIndexStatus(_ context.Context, gid entmoot.GroupID, status string, nowMS int64) (PublicMootRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status = NormalizePublicMootStatus(status)
	if status == "" {
		return PublicMootRecord{}, false, errors.New("esphttp: invalid public moot status")
	}
	if nowMS <= 0 {
		nowMS = s.nowMS()
	}
	rec, ok := s.publicMoots[gid]
	if !ok && status != PublicMootStatusBlocked {
		return PublicMootRecord{}, false, nil
	}
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = nowMS
	}
	if rec.Descriptor.GroupID == (entmoot.GroupID{}) {
		rec.Descriptor.GroupID = gid
	}
	rec.Status = status
	rec.UpdatedAtMS = nowMS
	rec.StatusUpdatedAtMS = nowMS
	s.publicMoots[gid] = clonePublicMootRecord(rec)
	return clonePublicMootRecord(rec), true, nil
}

func (s *SQLiteStateStore) UpsertPublicMootDescriptor(ctx context.Context, rec PublicMootRecord, nowMS int64) (PublicMootRecord, bool, error) {
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	if err := publicmoot.Verify(rec.Descriptor); err != nil {
		return PublicMootRecord{}, false, err
	}
	body, err := json.Marshal(rec.Descriptor)
	if err != nil {
		return PublicMootRecord{}, false, err
	}
	res, err := s.db.ExecContext(ctx, `
INSERT INTO esp_public_moots (
  group_id, founder_pubkey, status, descriptor, descriptor_updated_at_ms,
  created_at_ms, updated_at_ms, status_updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(group_id) DO UPDATE SET
  founder_pubkey = CASE
    WHEN esp_public_moots.founder_pubkey = x'' THEN excluded.founder_pubkey
    ELSE esp_public_moots.founder_pubkey
  END,
  descriptor = excluded.descriptor,
  descriptor_updated_at_ms = excluded.descriptor_updated_at_ms,
  updated_at_ms = excluded.updated_at_ms
WHERE excluded.descriptor_updated_at_ms > esp_public_moots.descriptor_updated_at_ms
  AND (esp_public_moots.founder_pubkey = x'' OR esp_public_moots.founder_pubkey = excluded.founder_pubkey)`,
		rec.Descriptor.GroupID[:], []byte(rec.Descriptor.Founder.EntmootPubKey), PublicMootStatusListed, body,
		rec.Descriptor.UpdatedAtMS, nowMS, nowMS, nowMS)
	if err != nil {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: upsert public moot: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: upsert public moot rows affected: %w", err)
	}
	stored, ok, err := s.GetPublicMoot(ctx, rec.Descriptor.GroupID)
	if err := errIfMissing(ok, err); err != nil {
		return PublicMootRecord{}, false, err
	}
	if n == 0 && PublicMootRecordHasDescriptor(stored) && !publicMootFounderMatches(stored, rec.Descriptor) {
		return stored, false, ErrPublicMootFounderMismatch
	}
	return stored, n > 0, nil
}

func (s *SQLiteStateStore) GetPublicMoot(ctx context.Context, gid entmoot.GroupID) (PublicMootRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT descriptor, status, created_at_ms, updated_at_ms, status_updated_at_ms
FROM esp_public_moots
WHERE group_id = ?`, gid[:])
	rec, err := scanPublicMootRecord(row, gid)
	if errors.Is(err, sql.ErrNoRows) {
		return PublicMootRecord{}, false, nil
	}
	if err != nil {
		return PublicMootRecord{}, false, err
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) ListPublicMoots(ctx context.Context, status string) ([]PublicMootRecord, error) {
	status = normalizePublicMootListFilter(status)
	var (
		rows *sql.Rows
		err  error
	)
	if status == "" {
		rows, err = s.db.QueryContext(ctx, `
SELECT group_id, descriptor, status, created_at_ms, updated_at_ms, status_updated_at_ms
FROM esp_public_moots
ORDER BY descriptor_updated_at_ms DESC, group_id`)
	} else {
		rows, err = s.db.QueryContext(ctx, `
SELECT group_id, descriptor, status, created_at_ms, updated_at_ms, status_updated_at_ms
FROM esp_public_moots
WHERE status = ?
ORDER BY descriptor_updated_at_ms DESC, group_id`, status)
	}
	if err != nil {
		return nil, fmt.Errorf("esphttp: list public moots: %w", err)
	}
	defer rows.Close()
	var out []PublicMootRecord
	for rows.Next() {
		var gid entmoot.GroupID
		var groupRaw []byte
		var descriptor []byte
		var rec PublicMootRecord
		if err := rows.Scan(&groupRaw, &descriptor, &rec.Status, &rec.CreatedAtMS, &rec.UpdatedAtMS, &rec.StatusUpdatedAtMS); err != nil {
			return nil, fmt.Errorf("esphttp: scan public moot: %w", err)
		}
		if len(groupRaw) != len(gid) {
			return nil, fmt.Errorf("esphttp: public moot group_id length %d", len(groupRaw))
		}
		copy(gid[:], groupRaw)
		if len(bytes.TrimSpace(descriptor)) != 0 {
			desc, err := publicmoot.Parse(descriptor)
			if err != nil {
				return nil, err
			}
			rec.Descriptor = desc
		} else {
			rec.Descriptor.GroupID = gid
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("esphttp: list public moots: %w", err)
	}
	return out, nil
}

func normalizePublicMootListFilter(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return ""
	}
	return NormalizePublicMootStatus(status)
}

func (s *SQLiteStateStore) SetPublicMootIndexStatus(ctx context.Context, gid entmoot.GroupID, status string, nowMS int64) (PublicMootRecord, bool, error) {
	status = NormalizePublicMootStatus(status)
	if status == "" {
		return PublicMootRecord{}, false, errors.New("esphttp: invalid public moot status")
	}
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	current, ok, err := s.GetPublicMoot(ctx, gid)
	if err != nil {
		return PublicMootRecord{}, false, err
	}
	if !ok && status != PublicMootStatusBlocked {
		return PublicMootRecord{}, false, nil
	}
	if current.CreatedAtMS == 0 {
		current.CreatedAtMS = nowMS
	}
	descriptorUpdatedAt := int64(0)
	var descriptor any
	founder := []byte{}
	if PublicMootRecordHasDescriptor(current) {
		body, err := json.Marshal(current.Descriptor)
		if err != nil {
			return PublicMootRecord{}, false, err
		}
		descriptor = body
		descriptorUpdatedAt = current.Descriptor.UpdatedAtMS
		founder = []byte(current.Descriptor.Founder.EntmootPubKey)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO esp_public_moots (
  group_id, founder_pubkey, status, descriptor, descriptor_updated_at_ms,
  created_at_ms, updated_at_ms, status_updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(group_id) DO UPDATE SET
  status = excluded.status,
  updated_at_ms = excluded.updated_at_ms,
  status_updated_at_ms = excluded.status_updated_at_ms`,
		gid[:], founder, status, descriptor, descriptorUpdatedAt, current.CreatedAtMS, nowMS, nowMS)
	if err != nil {
		return PublicMootRecord{}, false, fmt.Errorf("esphttp: set public moot status: %w", err)
	}
	stored, ok, err := s.GetPublicMoot(ctx, gid)
	return stored, ok, errIfMissing(ok, err)
}

type publicMootScanner interface {
	Scan(dest ...any) error
}

func scanPublicMootRecord(row publicMootScanner, gid entmoot.GroupID) (PublicMootRecord, error) {
	var descriptor []byte
	var rec PublicMootRecord
	if err := row.Scan(&descriptor, &rec.Status, &rec.CreatedAtMS, &rec.UpdatedAtMS, &rec.StatusUpdatedAtMS); err != nil {
		return PublicMootRecord{}, err
	}
	if len(bytes.TrimSpace(descriptor)) != 0 {
		desc, err := publicmoot.Parse(descriptor)
		if err != nil {
			return PublicMootRecord{}, err
		}
		rec.Descriptor = desc
	} else if gid != (entmoot.GroupID{}) {
		rec.Descriptor.GroupID = gid
	}
	return rec, nil
}

func sortPublicMootRecords(records []PublicMootRecord) {
	sort.Slice(records, func(i, j int) bool {
		left := records[i].Descriptor.UpdatedAtMS
		right := records[j].Descriptor.UpdatedAtMS
		if left == right {
			return records[i].Descriptor.GroupID.String() < records[j].Descriptor.GroupID.String()
		}
		return left > right
	})
}

func clonePublicMootRecord(rec PublicMootRecord) PublicMootRecord {
	rec.Descriptor = clonePublicMootDescriptor(rec.Descriptor)
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
