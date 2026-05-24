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
	EntmootPubKey string           `json:"entmoot_pubkey,omitempty"`
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

// EnrichMemberDisplayNames applies ESP-local global hostname fallbacks and stable display names.
func EnrichMemberDisplayNames(ctx context.Context, state StateStore, groupID entmoot.GroupID, members []MemberSummary) ([]MemberSummary, error) {
	out := append([]MemberSummary(nil), members...)
	if len(out) == 0 {
		return out, nil
	}
	needGlobal := make([]entmoot.NodeID, 0, len(out))
	memberPubKeys := make(map[entmoot.NodeID]string, len(out))
	for i := range out {
		if hostname, ok := NormalizeNodeProfileHostname(out[i].Hostname); ok {
			out[i].Hostname = hostname
			out[i].GlobalHostname = ""
			out[i].DisplayName = NodeDisplayName(out[i].NodeID, hostname)
			continue
		}
		out[i].Hostname = ""
		needGlobal = append(needGlobal, out[i].NodeID)
		memberPubKeys[out[i].NodeID] = strings.TrimSpace(out[i].EntmootPubKey)
	}
	profiles := map[entmoot.NodeID]NodeProfileRecord{}
	if state != nil && len(needGlobal) > 0 {
		var err error
		if memberLister, ok := state.(interface {
			ListNodeProfilesForMembers(context.Context, entmoot.GroupID, map[entmoot.NodeID]string) (map[entmoot.NodeID]NodeProfileRecord, error)
		}); ok {
			profiles, err = memberLister.ListNodeProfilesForMembers(ctx, groupID, memberPubKeys)
		} else if groupLister, ok := state.(interface {
			ListNodeProfilesForGroup(context.Context, entmoot.GroupID, []entmoot.NodeID) (map[entmoot.NodeID]NodeProfileRecord, error)
		}); ok {
			profiles, err = groupLister.ListNodeProfilesForGroup(ctx, groupID, needGlobal)
		} else {
			profiles, err = state.ListNodeProfiles(ctx, needGlobal)
		}
		if err != nil {
			return nil, err
		}
	}
	for i := range out {
		if out[i].DisplayName != "" {
			continue
		}
		if profile, ok := profiles[out[i].NodeID]; ok && nodeProfileVisibleForMember(profile, groupID, out[i].EntmootPubKey) {
			out[i].GlobalHostname = profile.Hostname
			out[i].DisplayName = NodeDisplayName(out[i].NodeID, profile.Hostname)
			continue
		}
		out[i].DisplayName = NodeDisplayName(out[i].NodeID, "")
	}
	return out, nil
}

func nodeProfileVisibleForMember(profile NodeProfileRecord, _ entmoot.GroupID, entmootPubKey string) bool {
	if profile.Source != NodeProfileSourceMemberProfile {
		return true
	}
	return strings.TrimSpace(profile.EntmootPubKey) != "" &&
		strings.TrimSpace(profile.EntmootPubKey) == strings.TrimSpace(entmootPubKey)
}

// ObserveMemberProfileNodeProfile records a verified group-local member profile observation.
func ObserveMemberProfileNodeProfile(ctx context.Context, state StateStore, groupID entmoot.GroupID, nodeID entmoot.NodeID, entmootPubKey string, hostname string, observedAtMS int64, expiresAtMS int64) error {
	if state == nil || nodeID == 0 {
		return nil
	}
	if _, ok := NormalizeNodeProfileHostname(hostname); !ok {
		return nil
	}
	_, _, err := state.UpsertNodeProfile(ctx, NodeProfileRecord{
		NodeID:        nodeID,
		EntmootPubKey: strings.TrimSpace(entmootPubKey),
		Hostname:      hostname,
		Source:        NodeProfileSourceMemberProfile,
		ObservedAtMS:  observedAtMS,
		ExpiresAtMS:   expiresAtMS,
		SourceGroupID: &groupID,
	})
	return err
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
	rec.EntmootPubKey = strings.TrimSpace(rec.EntmootPubKey)
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

func nodeProfileFromFleetMember(rec FleetMemberRecord) (NodeProfileRecord, bool) {
	if rec.NodeID == 0 {
		return NodeProfileRecord{}, false
	}
	if NormalizeFleetMemberStatus(rec.Status) != FleetMemberActive {
		return NodeProfileRecord{}, false
	}
	if _, ok := NormalizeNodeProfileHostname(rec.Hostname); !ok {
		return NodeProfileRecord{}, false
	}
	return NodeProfileRecord{
		NodeID:       rec.NodeID,
		Hostname:     rec.Hostname,
		Source:       NodeProfileSourceFleetMember,
		ObservedAtMS: rec.UpdatedAtMS,
	}, true
}

func nodeProfileFromFleetInvite(rec FleetInviteRecord) (NodeProfileRecord, bool) {
	if rec.NodeID == 0 {
		return NodeProfileRecord{}, false
	}
	if NormalizeFleetMemberStatus(rec.Status) != FleetMemberInvited {
		return NodeProfileRecord{}, false
	}
	if _, ok := NormalizeNodeProfileHostname(rec.Hostname); !ok {
		return NodeProfileRecord{}, false
	}
	return NodeProfileRecord{
		NodeID:       rec.NodeID,
		Hostname:     rec.Hostname,
		Source:       NodeProfileSourceFleetInvite,
		ObservedAtMS: rec.UpdatedAtMS,
		ExpiresAtMS:  rec.ExpiresAtMS,
	}, true
}

func nodeProfileSourceKey(rec NodeProfileRecord) string {
	if rec.Source == NodeProfileSourceMemberProfile && rec.SourceGroupID != nil {
		return rec.Source + ":" + rec.SourceGroupID.String() + ":" + strings.TrimSpace(rec.EntmootPubKey)
	}
	return rec.Source
}

func bestNodeProfile(records map[string]NodeProfileRecord, nowMS int64, groupID *entmoot.GroupID, entmootPubKey string) (NodeProfileRecord, bool) {
	var best NodeProfileRecord
	for _, rec := range records {
		if nodeProfileExpired(rec, nowMS) {
			continue
		}
		if groupID != nil && !nodeProfileVisibleForMember(rec, *groupID, entmootPubKey) {
			continue
		}
		if groupID == nil && rec.Source == NodeProfileSourceMemberProfile {
			continue
		}
		if shouldReplaceNodeProfile(best, rec, nowMS) {
			best = rec
		}
	}
	if best.NodeID == 0 {
		return NodeProfileRecord{}, false
	}
	return cloneNodeProfileRecord(best), true
}

func (s *MemoryStateStore) upsertNodeProfileLocked(rec NodeProfileRecord, nowMS int64) (NodeProfileRecord, bool, error) {
	normalized, valid, err := normalizeNodeProfileRecord(rec, nowMS)
	if err != nil || !valid {
		return NodeProfileRecord{}, false, err
	}
	records := s.nodeProfiles[normalized.NodeID]
	if records == nil {
		records = make(map[string]NodeProfileRecord)
		s.nodeProfiles[normalized.NodeID] = records
	}
	key := nodeProfileSourceKey(normalized)
	existing := records[key]
	if existing.NodeID != 0 && !shouldReplaceNodeProfile(existing, normalized, nowMS) {
		return cloneNodeProfileRecord(existing), false, nil
	}
	records[key] = cloneNodeProfileRecord(normalized)
	best, ok := bestNodeProfile(records, nowMS, nil, "")
	if !ok {
		return cloneNodeProfileRecord(normalized), true, nil
	}
	return best, nodeProfileSourceKey(best) == key, nil
}

func (s *MemoryStateStore) observeFleetMemberNodeProfileLocked(rec FleetMemberRecord) error {
	return s.refreshFleetMemberNodeProfileLocked(rec.NodeID)
}

func (s *MemoryStateStore) refreshFleetMemberNodeProfileLocked(nodeID entmoot.NodeID) error {
	if nodeID == 0 {
		return nil
	}
	nowMS := s.nowMS()
	var best NodeProfileRecord
	for fleetID, members := range s.fleetMembers {
		if !s.fleetActiveLocked(fleetID) {
			continue
		}
		rec, ok := members[nodeID]
		if !ok {
			continue
		}
		profile, ok := nodeProfileFromFleetMember(rec)
		if ok && shouldReplaceNodeProfile(best, profile, nowMS) {
			best = profile
		}
	}
	return s.replaceNodeProfileSourceLocked(nodeID, NodeProfileSourceFleetMember, best)
}

func (s *MemoryStateStore) clearNodeProfileSourceLocked(nodeID entmoot.NodeID, source string) {
	if nodeID == 0 {
		return
	}
	records := s.nodeProfiles[nodeID]
	for key, rec := range records {
		if rec.Source == source {
			delete(records, key)
		}
	}
	if len(records) == 0 {
		delete(s.nodeProfiles, nodeID)
	}
}

func (s *MemoryStateStore) replaceNodeProfileSourceLocked(nodeID entmoot.NodeID, source string, rec NodeProfileRecord) error {
	if nodeID == 0 {
		return nil
	}
	s.clearNodeProfileSourceLocked(nodeID, source)
	if rec.NodeID == 0 {
		return nil
	}
	_, _, err := s.upsertNodeProfileLocked(rec, s.nowMS())
	return err
}

func (s *MemoryStateStore) observeFleetInviteNodeProfileLocked(rec FleetInviteRecord) error {
	return s.refreshFleetInviteNodeProfileLocked(rec.NodeID)
}

func (s *MemoryStateStore) refreshFleetInviteNodeProfileLocked(nodeID entmoot.NodeID) error {
	if nodeID == 0 {
		return nil
	}
	nowMS := s.nowMS()
	var best NodeProfileRecord
	for fleetID, invites := range s.fleetInvites {
		if !s.fleetActiveLocked(fleetID) {
			continue
		}
		for _, rec := range invites {
			if rec.NodeID != nodeID {
				continue
			}
			profile, ok := nodeProfileFromFleetInvite(rec)
			if ok && !nodeProfileExpired(profile, nowMS) && shouldReplaceNodeProfile(best, profile, nowMS) {
				best = profile
			}
		}
	}
	return s.replaceNodeProfileSourceLocked(nodeID, NodeProfileSourceFleetInvite, best)
}

func (s *MemoryStateStore) UpsertNodeProfile(_ context.Context, rec NodeProfileRecord) (NodeProfileRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsertNodeProfileLocked(rec, s.nowMS())
}

func (s *MemoryStateStore) GetNodeProfile(_ context.Context, nodeID entmoot.NodeID) (NodeProfileRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := bestNodeProfile(s.nodeProfiles[nodeID], s.nowMS(), nil, "")
	if !ok {
		return NodeProfileRecord{}, false, nil
	}
	return rec, true, nil
}

func (s *MemoryStateStore) ListNodeProfiles(_ context.Context, nodeIDs []entmoot.NodeID) (map[entmoot.NodeID]NodeProfileRecord, error) {
	return s.listNodeProfilesForGroup(nodeIDs, nil, nil), nil
}

func (s *MemoryStateStore) ListNodeProfilesForGroup(_ context.Context, groupID entmoot.GroupID, nodeIDs []entmoot.NodeID) (map[entmoot.NodeID]NodeProfileRecord, error) {
	return s.listNodeProfilesForGroup(nodeIDs, &groupID, nil), nil
}

func (s *MemoryStateStore) ListNodeProfilesForMembers(_ context.Context, groupID entmoot.GroupID, members map[entmoot.NodeID]string) (map[entmoot.NodeID]NodeProfileRecord, error) {
	nodeIDs := make([]entmoot.NodeID, 0, len(members))
	for nodeID := range members {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return s.listNodeProfilesForGroup(nodeIDs, &groupID, members), nil
}

func (s *MemoryStateStore) listNodeProfilesForGroup(nodeIDs []entmoot.NodeID, groupID *entmoot.GroupID, memberPubKeys map[entmoot.NodeID]string) map[entmoot.NodeID]NodeProfileRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMS := s.nowMS()
	out := make(map[entmoot.NodeID]NodeProfileRecord)
	for _, nodeID := range nodeIDs {
		if nodeID == 0 {
			continue
		}
		if rec, ok := bestNodeProfile(s.nodeProfiles[nodeID], nowMS, groupID, memberPubKeys[nodeID]); ok {
			out[nodeID] = rec
		}
	}
	return out
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
	existing, ok, err := getNodeProfileSourceTx(ctx, tx, normalized)
	if err != nil {
		return NodeProfileRecord{}, false, err
	}
	changed := true
	if ok && !shouldReplaceNodeProfile(existing, normalized, nowMS) {
		changed = false
	}
	if !changed {
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
INSERT INTO esp_node_profile_sources (node_id, entmoot_pubkey, source, source_key, hostname, confidence, observed_at_ms, expires_at_ms, source_group_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(node_id, source_key) DO UPDATE SET
  hostname = excluded.hostname,
  entmoot_pubkey = excluded.entmoot_pubkey,
  source = excluded.source,
  confidence = excluded.confidence,
  observed_at_ms = excluded.observed_at_ms,
  expires_at_ms = excluded.expires_at_ms,
  source_group_id = excluded.source_group_id
`, int64(normalized.NodeID), normalized.EntmootPubKey, normalized.Source, nodeProfileSourceKey(normalized), normalized.Hostname, normalized.Confidence, normalized.ObservedAtMS, normalized.ExpiresAtMS, sourceGroupID); err != nil {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: upsert node profile: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: commit node profile upsert: %w", err)
	}
	best, ok, err := getNodeProfile(ctx, s.db, normalized.NodeID, nowMS, nil, "")
	if err != nil {
		return NodeProfileRecord{}, false, err
	}
	if !ok {
		return cloneNodeProfileRecord(normalized), true, nil
	}
	return best, nodeProfileSourceKey(best) == nodeProfileSourceKey(normalized), nil
}

func (s *SQLiteStateStore) observeFleetMemberNodeProfile(ctx context.Context, rec FleetMemberRecord) error {
	return s.refreshFleetMemberNodeProfile(ctx, rec.NodeID)
}

func (s *SQLiteStateStore) backfillFleetNodeProfiles(ctx context.Context) error {
	memberNodeIDs := make(map[entmoot.NodeID]struct{})
	for _, query := range []string{
		`SELECT DISTINCT node_id
		FROM esp_node_profile_sources
		WHERE node_id != 0 AND source = 'fleet_member'`,
		`SELECT DISTINCT m.node_id
		FROM esp_fleet_members m
		JOIN esp_fleets f ON f.fleet_id = m.fleet_id
		WHERE m.node_id != 0 AND f.status = 'active'`,
	} {
		nodeIDs, err := s.nodeProfileNodeIDs(ctx, query)
		if err != nil {
			return err
		}
		for _, nodeID := range nodeIDs {
			memberNodeIDs[nodeID] = struct{}{}
		}
	}
	for nodeID := range memberNodeIDs {
		if err := s.refreshFleetMemberNodeProfile(ctx, nodeID); err != nil {
			return err
		}
	}
	inviteNodeIDs := make(map[entmoot.NodeID]struct{})
	for _, query := range []string{
		`SELECT DISTINCT node_id
		FROM esp_node_profile_sources
		WHERE node_id != 0 AND source = 'fleet_invite'`,
		`SELECT DISTINCT i.node_id
		FROM esp_fleet_invites i
		JOIN esp_fleets f ON f.fleet_id = i.fleet_id
		WHERE i.node_id != 0 AND f.status = 'active'`,
	} {
		nodeIDs, err := s.nodeProfileNodeIDs(ctx, query)
		if err != nil {
			return err
		}
		for _, nodeID := range nodeIDs {
			inviteNodeIDs[nodeID] = struct{}{}
		}
	}
	for nodeID := range inviteNodeIDs {
		if err := s.refreshFleetInviteNodeProfile(ctx, nodeID); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStateStore) nodeProfileNodeIDs(ctx context.Context, query string) ([]entmoot.NodeID, error) {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list node profile ids: %w", err)
	}
	defer rows.Close()
	var out []entmoot.NodeID
	for rows.Next() {
		var nodeID int64
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("esphttp: scan node profile id: %w", err)
		}
		if nodeID != 0 {
			out = append(out, entmoot.NodeID(nodeID))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("esphttp: list node profile id rows: %w", err)
	}
	return out, nil
}

func (s *SQLiteStateStore) refreshFleetMemberNodeProfile(ctx context.Context, nodeID entmoot.NodeID) error {
	if nodeID == 0 {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT m.fleet_id, m.node_id, m.entmoot_pubkey, m.hostname, m.role, m.status, m.invited_at_ms, m.accepted_at_ms, m.removed_at_ms, m.updated_at_ms
FROM esp_fleet_members m
JOIN esp_fleets f ON f.fleet_id = m.fleet_id
WHERE m.node_id = ? AND f.status = ?`, int64(nodeID), FleetStatusActive)
	if err != nil {
		return fmt.Errorf("esphttp: refresh fleet member node profile: %w", err)
	}
	defer rows.Close()
	nowMS := time.Now().UnixMilli()
	var best NodeProfileRecord
	for rows.Next() {
		rec, err := scanFleetMemberRecord(rows)
		if err != nil {
			return err
		}
		profile, ok := nodeProfileFromFleetMember(rec)
		if ok && shouldReplaceNodeProfile(best, profile, nowMS) {
			best = profile
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("esphttp: refresh fleet member node profile rows: %w", err)
	}
	return s.replaceNodeProfileSource(ctx, nodeID, NodeProfileSourceFleetMember, best)
}

func (s *SQLiteStateStore) clearNodeProfileSource(ctx context.Context, nodeID entmoot.NodeID, source string) error {
	if nodeID == 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_node_profile_sources WHERE node_id = ? AND source = ?`, int64(nodeID), source); err != nil {
		return fmt.Errorf("esphttp: clear node profile source: %w", err)
	}
	return nil
}

func (s *SQLiteStateStore) replaceNodeProfileSource(ctx context.Context, nodeID entmoot.NodeID, source string, rec NodeProfileRecord) error {
	if err := s.clearNodeProfileSource(ctx, nodeID, source); err != nil {
		return err
	}
	if rec.NodeID == 0 {
		return nil
	}
	_, _, err := s.UpsertNodeProfile(ctx, rec)
	return err
}

func (s *SQLiteStateStore) observeFleetInviteNodeProfile(ctx context.Context, rec FleetInviteRecord) error {
	return s.refreshFleetInviteNodeProfile(ctx, rec.NodeID)
}

func (s *SQLiteStateStore) refreshFleetInviteNodeProfile(ctx context.Context, nodeID entmoot.NodeID) error {
	if nodeID == 0 {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT i.invite_id, i.fleet_id, i.node_id, i.entmoot_pubkey, i.hostname, i.status, i.invite, i.created_at_ms, i.updated_at_ms, i.expires_at_ms
FROM esp_fleet_invites i
JOIN esp_fleets f ON f.fleet_id = i.fleet_id
WHERE i.node_id = ? AND f.status = ?`, int64(nodeID), FleetStatusActive)
	if err != nil {
		return fmt.Errorf("esphttp: refresh fleet invite node profile: %w", err)
	}
	defer rows.Close()
	nowMS := time.Now().UnixMilli()
	var best NodeProfileRecord
	for rows.Next() {
		rec, err := scanFleetInviteRecord(rows)
		if err != nil {
			return err
		}
		profile, ok := nodeProfileFromFleetInvite(rec)
		if ok && !nodeProfileExpired(profile, nowMS) && shouldReplaceNodeProfile(best, profile, nowMS) {
			best = profile
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("esphttp: refresh fleet invite node profile rows: %w", err)
	}
	return s.replaceNodeProfileSource(ctx, nodeID, NodeProfileSourceFleetInvite, best)
}

func (s *SQLiteStateStore) GetNodeProfile(ctx context.Context, nodeID entmoot.NodeID) (NodeProfileRecord, bool, error) {
	return getNodeProfile(ctx, s.db, nodeID, time.Now().UnixMilli(), nil, "")
}

func (s *SQLiteStateStore) ListNodeProfiles(ctx context.Context, nodeIDs []entmoot.NodeID) (map[entmoot.NodeID]NodeProfileRecord, error) {
	return s.listNodeProfilesForGroup(ctx, nodeIDs, nil, nil)
}

func (s *SQLiteStateStore) ListNodeProfilesForGroup(ctx context.Context, groupID entmoot.GroupID, nodeIDs []entmoot.NodeID) (map[entmoot.NodeID]NodeProfileRecord, error) {
	return s.listNodeProfilesForGroup(ctx, nodeIDs, &groupID, nil)
}

func (s *SQLiteStateStore) ListNodeProfilesForMembers(ctx context.Context, groupID entmoot.GroupID, members map[entmoot.NodeID]string) (map[entmoot.NodeID]NodeProfileRecord, error) {
	nodeIDs := make([]entmoot.NodeID, 0, len(members))
	for nodeID := range members {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return s.listNodeProfilesForGroup(ctx, nodeIDs, &groupID, members)
}

func (s *SQLiteStateStore) listNodeProfilesForGroup(ctx context.Context, nodeIDs []entmoot.NodeID, groupID *entmoot.GroupID, memberPubKeys map[entmoot.NodeID]string) (map[entmoot.NodeID]NodeProfileRecord, error) {
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
		rec, ok, err := getNodeProfile(ctx, s.db, nodeID, nowMS, groupID, memberPubKeys[nodeID])
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
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

func getNodeProfile(ctx context.Context, q nodeProfileQuerier, nodeID entmoot.NodeID, nowMS int64, groupID *entmoot.GroupID, entmootPubKey string) (NodeProfileRecord, bool, error) {
	rows, err := q.QueryContext(ctx, `
SELECT node_id, entmoot_pubkey, hostname, source, confidence, observed_at_ms, expires_at_ms, source_group_id
FROM esp_node_profile_sources
WHERE node_id = ?
`, int64(nodeID))
	if err != nil {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: query node profiles: %w", err)
	}
	defer rows.Close()
	records := make(map[string]NodeProfileRecord)
	for rows.Next() {
		rec, err := scanNodeProfileRow(rows)
		if err != nil {
			return NodeProfileRecord{}, false, err
		}
		records[nodeProfileSourceKey(rec)] = rec
	}
	if err := rows.Err(); err != nil {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: query node profiles rows: %w", err)
	}
	rec, ok := bestNodeProfile(records, nowMS, groupID, entmootPubKey)
	return rec, ok, nil
}

func getNodeProfileSourceTx(ctx context.Context, tx *sql.Tx, rec NodeProfileRecord) (NodeProfileRecord, bool, error) {
	return scanNodeProfileRowMaybe(tx.QueryRowContext(ctx, `
SELECT node_id, entmoot_pubkey, hostname, source, confidence, observed_at_ms, expires_at_ms, source_group_id
FROM esp_node_profile_sources
WHERE node_id = ? AND source_key = ?
`, int64(rec.NodeID), nodeProfileSourceKey(rec)))
}

type nodeProfileScanner interface {
	Scan(dest ...any) error
}

func scanNodeProfileRowMaybe(row nodeProfileScanner) (NodeProfileRecord, bool, error) {
	rec, err := scanNodeProfileRow(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NodeProfileRecord{}, false, nil
		}
		return NodeProfileRecord{}, false, err
	}
	return rec, true, nil
}

func scanNodeProfileRow(row nodeProfileScanner) (NodeProfileRecord, error) {
	var nodeID int64
	var sourceGroupBytes []byte
	var rec NodeProfileRecord
	if err := row.Scan(&nodeID, &rec.EntmootPubKey, &rec.Hostname, &rec.Source, &rec.Confidence, &rec.ObservedAtMS, &rec.ExpiresAtMS, &sourceGroupBytes); err != nil {
		return NodeProfileRecord{}, fmt.Errorf("esphttp: scan node profile: %w", err)
	}
	rec.NodeID = entmoot.NodeID(nodeID)
	if len(sourceGroupBytes) == len(entmoot.GroupID{}) {
		var groupID entmoot.GroupID
		copy(groupID[:], sourceGroupBytes)
		rec.SourceGroupID = &groupID
	}
	return rec, nil
}
