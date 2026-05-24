package esphttp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
)

const (
	NodeProfileSourceMemberProfile = "member_profile"
	NodeProfileSourceFleetMember   = "fleet_member"
	NodeProfileSourceFleetInvite   = "fleet_invite"
	NodeProfileSourcePilotInfo     = "pilot_info"

	NodeProfileConfidencePilotInfo     = 10
	NodeProfileConfidenceFleetInvite   = 20
	NodeProfileConfidenceFleetMember   = 30
	NodeProfileConfidenceMemberProfile = 40

	MaxNodeProfileHostnameBytes = 255
)

// NodeProfileRecord is the ESP-local best known global display profile for a node.
type NodeProfileRecord struct {
	NodeID        entmoot.NodeID   `json:"node_id"`
	Hostname      string           `json:"hostname"`
	Source        string           `json:"source"`
	Confidence    int              `json:"confidence"`
	ObservedAtMS  int64            `json:"observed_at_ms"`
	ExpiresAtMS   int64            `json:"expires_at_ms"`
	SourceGroupID *entmoot.GroupID `json:"source_group_id,omitempty"`
}

// NormalizeNodeProfileHostname canonicalizes hostnames accepted for ESP node display names.
func NormalizeNodeProfileHostname(hostname string) (string, bool) {
	normalized := strings.TrimSpace(hostname)
	if normalized == "" || len(normalized) > MaxNodeProfileHostnameBytes {
		return "", false
	}
	for _, r := range normalized {
		if r < 0x20 || r == 0x7f {
			return "", false
		}
	}
	return normalized, true
}

// NodeDisplayName returns the stable user-facing label for a node.
func NodeDisplayName(nodeID entmoot.NodeID, hostname string) string {
	if normalized, ok := NormalizeNodeProfileHostname(hostname); ok {
		return fmt.Sprintf("%s#%d", normalized, nodeID)
	}
	return fmt.Sprintf("node-%d", nodeID)
}

func nodeProfileConfidenceForSource(source string) (int, bool) {
	switch source {
	case NodeProfileSourceMemberProfile:
		return NodeProfileConfidenceMemberProfile, true
	case NodeProfileSourceFleetMember:
		return NodeProfileConfidenceFleetMember, true
	case NodeProfileSourceFleetInvite:
		return NodeProfileConfidenceFleetInvite, true
	case NodeProfileSourcePilotInfo:
		return NodeProfileConfidencePilotInfo, true
	default:
		return 0, false
	}
}

func normalizeNodeProfileRecord(rec NodeProfileRecord, nowMS int64) (NodeProfileRecord, bool, error) {
	hostname, ok := NormalizeNodeProfileHostname(rec.Hostname)
	if !ok {
		return NodeProfileRecord{}, false, nil
	}
	if rec.NodeID == 0 {
		return NodeProfileRecord{}, false, errors.New("esphttp: node profile node_id is required")
	}
	sourceConfidence, ok := nodeProfileConfidenceForSource(rec.Source)
	if !ok {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: unknown node profile source %q", rec.Source)
	}
	rec.Confidence = sourceConfidence
	if rec.ObservedAtMS == 0 {
		rec.ObservedAtMS = nowMS
	}
	if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS {
		return NodeProfileRecord{}, false, nil
	}
	rec.Hostname = hostname
	rec.SourceGroupID = cloneGroupIDPtr(rec.SourceGroupID)
	return rec, true, nil
}

func nodeProfileExpired(rec NodeProfileRecord, nowMS int64) bool {
	return rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS
}

func shouldReplaceNodeProfile(existing NodeProfileRecord, incoming NodeProfileRecord, nowMS int64) bool {
	if existing.NodeID == 0 || nodeProfileExpired(existing, nowMS) {
		return true
	}
	if incoming.Confidence != existing.Confidence {
		return incoming.Confidence > existing.Confidence
	}
	return incoming.ObservedAtMS > existing.ObservedAtMS
}

func cloneNodeProfileRecord(rec NodeProfileRecord) NodeProfileRecord {
	rec.SourceGroupID = cloneGroupIDPtr(rec.SourceGroupID)
	return rec
}

func cloneGroupIDPtr(groupID *entmoot.GroupID) *entmoot.GroupID {
	if groupID == nil {
		return nil
	}
	copied := *groupID
	return &copied
}

func (s *MemoryStateStore) UpsertNodeProfile(_ context.Context, rec NodeProfileRecord) (NodeProfileRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMS := s.nowMS()
	normalized, valid, err := normalizeNodeProfileRecord(rec, nowMS)
	if err != nil || !valid {
		return NodeProfileRecord{}, false, err
	}
	existing := s.nodeProfiles[normalized.NodeID]
	if !shouldReplaceNodeProfile(existing, normalized, nowMS) {
		return cloneNodeProfileRecord(existing), false, nil
	}
	s.nodeProfiles[normalized.NodeID] = cloneNodeProfileRecord(normalized)
	return cloneNodeProfileRecord(normalized), true, nil
}

func (s *MemoryStateStore) GetNodeProfile(_ context.Context, nodeID entmoot.NodeID) (NodeProfileRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.nodeProfiles[nodeID]
	if !ok || nodeProfileExpired(rec, s.nowMS()) {
		return NodeProfileRecord{}, false, nil
	}
	return cloneNodeProfileRecord(rec), true, nil
}

func (s *MemoryStateStore) ListNodeProfiles(_ context.Context, nodeIDs []entmoot.NodeID) (map[entmoot.NodeID]NodeProfileRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMS := s.nowMS()
	out := make(map[entmoot.NodeID]NodeProfileRecord)
	for _, nodeID := range nodeIDs {
		if nodeID == 0 {
			continue
		}
		rec, ok := s.nodeProfiles[nodeID]
		if !ok || nodeProfileExpired(rec, nowMS) {
			continue
		}
		out[nodeID] = cloneNodeProfileRecord(rec)
	}
	return out, nil
}

func (s *SQLiteStateStore) UpsertNodeProfile(ctx context.Context, rec NodeProfileRecord) (NodeProfileRecord, bool, error) {
	nowMS := time.Now().UnixMilli()
	normalized, valid, err := normalizeNodeProfileRecord(rec, nowMS)
	if err != nil || !valid {
		return NodeProfileRecord{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: begin node profile upsert: %w", err)
	}
	defer tx.Rollback()
	existing, ok, err := getNodeProfileTx(ctx, tx, normalized.NodeID)
	if err != nil {
		return NodeProfileRecord{}, false, err
	}
	if ok && !shouldReplaceNodeProfile(existing, normalized, nowMS) {
		if err := tx.Commit(); err != nil {
			return NodeProfileRecord{}, false, fmt.Errorf("esphttp: commit node profile noop: %w", err)
		}
		return existing, false, nil
	}
	var sourceGroupID []byte
	if normalized.SourceGroupID != nil {
		sourceGroupID = normalized.SourceGroupID[:]
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO esp_node_profiles (node_id, hostname, source, confidence, observed_at_ms, expires_at_ms, source_group_id)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(node_id) DO UPDATE SET
  hostname = excluded.hostname,
  source = excluded.source,
  confidence = excluded.confidence,
  observed_at_ms = excluded.observed_at_ms,
  expires_at_ms = excluded.expires_at_ms,
  source_group_id = excluded.source_group_id
`, int64(normalized.NodeID), normalized.Hostname, normalized.Source, normalized.Confidence, normalized.ObservedAtMS, normalized.ExpiresAtMS, sourceGroupID); err != nil {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: upsert node profile: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: commit node profile upsert: %w", err)
	}
	return cloneNodeProfileRecord(normalized), true, nil
}

func (s *SQLiteStateStore) GetNodeProfile(ctx context.Context, nodeID entmoot.NodeID) (NodeProfileRecord, bool, error) {
	return getNodeProfile(ctx, s.db, nodeID, time.Now().UnixMilli())
}

func (s *SQLiteStateStore) ListNodeProfiles(ctx context.Context, nodeIDs []entmoot.NodeID) (map[entmoot.NodeID]NodeProfileRecord, error) {
	out := make(map[entmoot.NodeID]NodeProfileRecord)
	seen := make(map[entmoot.NodeID]struct{}, len(nodeIDs))
	nowMS := time.Now().UnixMilli()
	for _, nodeID := range nodeIDs {
		if nodeID == 0 {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		rec, ok, err := getNodeProfile(ctx, s.db, nodeID, nowMS)
		if err != nil {
			return nil, err
		}
		if ok {
			out[nodeID] = rec
		}
	}
	return out, nil
}

type nodeProfileQuerier interface {
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func getNodeProfile(ctx context.Context, q nodeProfileQuerier, nodeID entmoot.NodeID, nowMS int64) (NodeProfileRecord, bool, error) {
	rec, ok, err := scanNodeProfileRow(q.QueryRowContext(ctx, `
SELECT node_id, hostname, source, confidence, observed_at_ms, expires_at_ms, source_group_id
FROM esp_node_profiles
WHERE node_id = ?
`, int64(nodeID)))
	if err != nil || !ok {
		return NodeProfileRecord{}, ok, err
	}
	if nodeProfileExpired(rec, nowMS) {
		return NodeProfileRecord{}, false, nil
	}
	return rec, true, nil
}

func getNodeProfileTx(ctx context.Context, tx *sql.Tx, nodeID entmoot.NodeID) (NodeProfileRecord, bool, error) {
	return scanNodeProfileRow(tx.QueryRowContext(ctx, `
SELECT node_id, hostname, source, confidence, observed_at_ms, expires_at_ms, source_group_id
FROM esp_node_profiles
WHERE node_id = ?
`, int64(nodeID)))
}

func scanNodeProfileRow(row *sql.Row) (NodeProfileRecord, bool, error) {
	var nodeID int64
	var sourceGroupBytes []byte
	var rec NodeProfileRecord
	if err := row.Scan(&nodeID, &rec.Hostname, &rec.Source, &rec.Confidence, &rec.ObservedAtMS, &rec.ExpiresAtMS, &sourceGroupBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NodeProfileRecord{}, false, nil
		}
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: scan node profile: %w", err)
	}
	rec.NodeID = entmoot.NodeID(nodeID)
	if len(sourceGroupBytes) == len(entmoot.GroupID{}) {
		var groupID entmoot.GroupID
		copy(groupID[:], sourceGroupBytes)
		rec.SourceGroupID = &groupID
	}
	return rec, true, nil
}
