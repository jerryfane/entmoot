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

	NodeProfileConfidenceFleetInvite   = 20
	NodeProfileConfidenceFleetMember   = 30
	NodeProfileConfidenceMemberProfile = 40

	MaxNodeProfileHostnameBytes = 255
)

type NodeProfileRecord struct {
	MemberID      entmoot.MemberID `json:"member_id"`
	EntmootPubKey string           `json:"entmoot_pubkey,omitempty"`
	Hostname      string           `json:"hostname"`
	Source        string           `json:"source"`
	Confidence    int              `json:"confidence"`
	ObservedAtMS  int64            `json:"observed_at_ms"`
	ExpiresAtMS   int64            `json:"expires_at_ms"`
	SourceGroupID *entmoot.GroupID `json:"source_group_id,omitempty"`
}

func NormalizeNodeProfileHostname(hostname string) (string, bool) {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" || len(hostname) > MaxNodeProfileHostnameBytes {
		return "", false
	}
	for _, r := range hostname {
		if r < 0x20 || r == 0x7f {
			return "", false
		}
	}
	return hostname, true
}

func NodeDisplayName(memberID entmoot.MemberID, hostname string) string {
	if normalized, ok := NormalizeNodeProfileHostname(hostname); ok {
		return normalized + "#" + memberID.String()
	}
	return "member-" + memberID.String()
}

func EnrichMemberDisplayNames(ctx context.Context, state StateStore, groupID entmoot.GroupID, members []MemberSummary) ([]MemberSummary, error) {
	out := append([]MemberSummary(nil), members...)
	memberPubKeys := make(map[entmoot.MemberID]string, len(out))
	for i := range out {
		if hostname, ok := NormalizeNodeProfileHostname(out[i].Hostname); ok {
			out[i].Hostname = hostname
			out[i].DisplayName = NodeDisplayName(out[i].MemberID, hostname)
			continue
		}
		out[i].Hostname = ""
		memberPubKeys[out[i].MemberID] = strings.TrimSpace(out[i].EntmootPubKey)
	}
	profiles := map[entmoot.MemberID]NodeProfileRecord{}
	if state != nil && len(memberPubKeys) != 0 {
		var err error
		if lister, ok := state.(interface {
			ListNodeProfilesForMembers(context.Context, entmoot.GroupID, map[entmoot.MemberID]string) (map[entmoot.MemberID]NodeProfileRecord, error)
		}); ok {
			profiles, err = lister.ListNodeProfilesForMembers(ctx, groupID, memberPubKeys)
		} else {
			ids := make([]entmoot.MemberID, 0, len(memberPubKeys))
			for id := range memberPubKeys {
				ids = append(ids, id)
			}
			profiles, err = state.ListNodeProfiles(ctx, ids)
		}
		if err != nil {
			return nil, err
		}
	}
	for i := range out {
		if out[i].DisplayName != "" {
			continue
		}
		if profile, ok := profiles[out[i].MemberID]; ok && nodeProfileVisibleForMember(profile, out[i].EntmootPubKey) {
			out[i].GlobalHostname = profile.Hostname
			out[i].DisplayName = NodeDisplayName(out[i].MemberID, profile.Hostname)
		} else {
			out[i].DisplayName = NodeDisplayName(out[i].MemberID, "")
		}
	}
	return out, nil
}

func nodeProfileVisibleForMember(profile NodeProfileRecord, publicKey string) bool {
	return profile.Source != NodeProfileSourceMemberProfile ||
		(strings.TrimSpace(profile.EntmootPubKey) != "" && strings.TrimSpace(profile.EntmootPubKey) == strings.TrimSpace(publicKey))
}

func ObserveMemberProfileNodeProfile(ctx context.Context, state StateStore, groupID entmoot.GroupID, memberID entmoot.MemberID, publicKey, hostname string, observedAtMS, expiresAtMS int64) error {
	if state == nil || memberID == (entmoot.MemberID{}) {
		return nil
	}
	if _, ok := NormalizeNodeProfileHostname(hostname); !ok {
		return nil
	}
	_, _, err := state.UpsertNodeProfile(ctx, NodeProfileRecord{MemberID: memberID, EntmootPubKey: strings.TrimSpace(publicKey), Hostname: hostname, Source: NodeProfileSourceMemberProfile, ObservedAtMS: observedAtMS, ExpiresAtMS: expiresAtMS, SourceGroupID: &groupID})
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
	default:
		return 0, false
	}
}

func normalizeNodeProfileRecord(rec NodeProfileRecord, nowMS int64) (NodeProfileRecord, bool, error) {
	hostname, ok := NormalizeNodeProfileHostname(rec.Hostname)
	if !ok {
		return NodeProfileRecord{}, false, nil
	}
	if rec.MemberID == (entmoot.MemberID{}) {
		return NodeProfileRecord{}, false, errors.New("esphttp: node profile member_id is required")
	}
	confidence, ok := nodeProfileConfidenceForSource(rec.Source)
	if !ok {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: unknown node profile source %q", rec.Source)
	}
	rec.Hostname, rec.Confidence = hostname, confidence
	if rec.ObservedAtMS == 0 {
		rec.ObservedAtMS = nowMS
	}
	if rec.Source == NodeProfileSourceMemberProfile && (rec.SourceGroupID == nil || strings.TrimSpace(rec.EntmootPubKey) == "") {
		return NodeProfileRecord{}, false, errors.New("esphttp: member profile requires source group and public key")
	}
	return rec, true, nil
}

func nodeProfileExpired(rec NodeProfileRecord, nowMS int64) bool {
	return rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS
}
func shouldReplaceNodeProfile(existing, incoming NodeProfileRecord, nowMS int64) bool {
	if existing.MemberID == (entmoot.MemberID{}) || nodeProfileExpired(existing, nowMS) {
		return true
	}
	if incoming.Confidence != existing.Confidence {
		return incoming.Confidence > existing.Confidence
	}
	return incoming.ObservedAtMS > existing.ObservedAtMS
}
func cloneNodeProfileRecord(rec NodeProfileRecord) NodeProfileRecord {
	if rec.SourceGroupID != nil {
		gid := *rec.SourceGroupID
		rec.SourceGroupID = &gid
	}
	return rec
}
func nodeProfileSourceKey(rec NodeProfileRecord) string {
	if rec.Source == NodeProfileSourceMemberProfile && rec.SourceGroupID != nil {
		return rec.Source + ":" + rec.SourceGroupID.String() + ":" + strings.TrimSpace(rec.EntmootPubKey)
	}
	return rec.Source
}
func bestNodeProfile(records map[string]NodeProfileRecord, nowMS int64, groupID *entmoot.GroupID, publicKey string) (NodeProfileRecord, bool) {
	var best NodeProfileRecord
	for _, rec := range records {
		if nodeProfileExpired(rec, nowMS) {
			continue
		}
		if rec.Source == NodeProfileSourceMemberProfile && groupID != nil {
			if rec.SourceGroupID == nil || *rec.SourceGroupID != *groupID || strings.TrimSpace(rec.EntmootPubKey) != strings.TrimSpace(publicKey) {
				continue
			}
		}
		if shouldReplaceNodeProfile(best, rec, nowMS) {
			best = rec
		}
	}
	return cloneNodeProfileRecord(best), best.MemberID != (entmoot.MemberID{})
}

func nodeProfileFromFleetMember(rec FleetMemberRecord) (NodeProfileRecord, bool) {
	if rec.MemberID == (entmoot.MemberID{}) || NormalizeFleetMemberStatus(rec.Status) != FleetMemberActive {
		return NodeProfileRecord{}, false
	}
	if _, ok := NormalizeNodeProfileHostname(rec.Hostname); !ok {
		return NodeProfileRecord{}, false
	}
	return NodeProfileRecord{MemberID: rec.MemberID, EntmootPubKey: rec.EntmootPubKey, Hostname: rec.Hostname, Source: NodeProfileSourceFleetMember, ObservedAtMS: rec.UpdatedAtMS}, true
}
func nodeProfileFromFleetInvite(rec FleetInviteRecord) (NodeProfileRecord, bool) {
	if rec.MemberID == (entmoot.MemberID{}) || NormalizeFleetMemberStatus(rec.Status) != FleetMemberInvited {
		return NodeProfileRecord{}, false
	}
	if _, ok := NormalizeNodeProfileHostname(rec.Hostname); !ok {
		return NodeProfileRecord{}, false
	}
	return NodeProfileRecord{MemberID: rec.MemberID, EntmootPubKey: rec.EntmootPubKey, Hostname: rec.Hostname, Source: NodeProfileSourceFleetInvite, ObservedAtMS: rec.UpdatedAtMS, ExpiresAtMS: rec.ExpiresAtMS}, true
}

func (s *MemoryStateStore) upsertNodeProfileLocked(rec NodeProfileRecord, nowMS int64) (NodeProfileRecord, bool, error) {
	rec, valid, err := normalizeNodeProfileRecord(rec, nowMS)
	if err != nil || !valid {
		return NodeProfileRecord{}, false, err
	}
	records := s.nodeProfiles[rec.MemberID]
	if records == nil {
		records = make(map[string]NodeProfileRecord)
		s.nodeProfiles[rec.MemberID] = records
	}
	key := nodeProfileSourceKey(rec)
	if existing := records[key]; existing.MemberID != (entmoot.MemberID{}) && !shouldReplaceNodeProfile(existing, rec, nowMS) {
		return cloneNodeProfileRecord(existing), false, nil
	}
	records[key] = cloneNodeProfileRecord(rec)
	best, _ := bestNodeProfile(records, nowMS, nil, "")
	return best, true, nil
}
func (s *MemoryStateStore) UpsertNodeProfile(_ context.Context, rec NodeProfileRecord) (NodeProfileRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsertNodeProfileLocked(rec, s.nowMS())
}
func (s *MemoryStateStore) GetNodeProfile(_ context.Context, id entmoot.MemberID) (NodeProfileRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := bestNodeProfile(s.nodeProfiles[id], s.nowMS(), nil, "")
	return rec, ok, nil
}
func (s *MemoryStateStore) ListNodeProfiles(_ context.Context, ids []entmoot.MemberID) (map[entmoot.MemberID]NodeProfileRecord, error) {
	return s.listNodeProfilesForGroup(ids, nil, nil), nil
}
func (s *MemoryStateStore) ListNodeProfilesForGroup(_ context.Context, gid entmoot.GroupID, ids []entmoot.MemberID) (map[entmoot.MemberID]NodeProfileRecord, error) {
	return s.listNodeProfilesForGroup(ids, &gid, nil), nil
}
func (s *MemoryStateStore) ListNodeProfilesForMembers(_ context.Context, gid entmoot.GroupID, members map[entmoot.MemberID]string) (map[entmoot.MemberID]NodeProfileRecord, error) {
	ids := make([]entmoot.MemberID, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	return s.listNodeProfilesForGroup(ids, &gid, members), nil
}
func (s *MemoryStateStore) listNodeProfilesForGroup(ids []entmoot.MemberID, gid *entmoot.GroupID, keys map[entmoot.MemberID]string) map[entmoot.MemberID]NodeProfileRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[entmoot.MemberID]NodeProfileRecord)
	for _, id := range ids {
		if rec, ok := bestNodeProfile(s.nodeProfiles[id], s.nowMS(), gid, keys[id]); ok {
			out[id] = rec
		}
	}
	return out
}
func (s *MemoryStateStore) observeFleetMemberNodeProfileLocked(rec FleetMemberRecord) error {
	return s.refreshFleetMemberNodeProfileLocked(rec.MemberID)
}
func (s *MemoryStateStore) observeFleetInviteNodeProfileLocked(rec FleetInviteRecord) error {
	return s.refreshFleetInviteNodeProfileLocked(rec.MemberID)
}
func (s *MemoryStateStore) refreshFleetMemberNodeProfileLocked(id entmoot.MemberID) error {
	var best NodeProfileRecord
	now := s.nowMS()
	for fleetID, members := range s.fleetMembers {
		if !s.fleetActiveLocked(fleetID) {
			continue
		}
		if profile, ok := nodeProfileFromFleetMember(members[id]); ok && shouldReplaceNodeProfile(best, profile, now) {
			best = profile
		}
	}
	return s.replaceFleetNodeProfileLocked(id, NodeProfileSourceFleetMember, best)
}

func (s *MemoryStateStore) refreshFleetInviteNodeProfileLocked(id entmoot.MemberID) error {
	var best NodeProfileRecord
	now := s.nowMS()
	for fleetID, invites := range s.fleetInvites {
		if !s.fleetActiveLocked(fleetID) {
			continue
		}
		for _, invite := range invites {
			if invite.MemberID != id {
				continue
			}
			if profile, ok := nodeProfileFromFleetInvite(invite); ok && !nodeProfileExpired(profile, now) && shouldReplaceNodeProfile(best, profile, now) {
				best = profile
			}
		}
	}
	return s.replaceFleetNodeProfileLocked(id, NodeProfileSourceFleetInvite, best)
}

func (s *MemoryStateStore) replaceFleetNodeProfileLocked(id entmoot.MemberID, source string, best NodeProfileRecord) error {
	records := s.nodeProfiles[id]
	delete(records, source)
	if len(records) == 0 {
		delete(s.nodeProfiles, id)
	}
	if best.MemberID == (entmoot.MemberID{}) {
		return nil
	}
	_, _, err := s.upsertNodeProfileLocked(best, s.nowMS())
	return err
}

// Call before dropping a fleet's rows, after changing its active status.
func (s *MemoryStateStore) refreshFleetNodeProfilesLocked(fleetID string) {
	for id := range s.fleetMembers[fleetID] {
		_ = s.refreshFleetMemberNodeProfileLocked(id)
	}
	for _, invite := range s.fleetInvites[fleetID] {
		_ = s.refreshFleetInviteNodeProfileLocked(invite.MemberID)
	}
}

func (s *SQLiteStateStore) UpsertNodeProfile(ctx context.Context, rec NodeProfileRecord) (NodeProfileRecord, bool, error) {
	nowMS := time.Now().UnixMilli()
	rec, valid, err := normalizeNodeProfileRecord(rec, nowMS)
	if err != nil || !valid {
		return NodeProfileRecord{}, false, err
	}
	var sourceGroup []byte
	if rec.SourceGroupID != nil {
		sourceGroup = rec.SourceGroupID[:]
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO esp_node_profile_sources (member_id, entmoot_pubkey, source, source_key, hostname, confidence, observed_at_ms, expires_at_ms, source_group_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(member_id, source_key) DO UPDATE SET entmoot_pubkey=excluded.entmoot_pubkey, source=excluded.source, hostname=excluded.hostname, confidence=excluded.confidence, observed_at_ms=excluded.observed_at_ms, expires_at_ms=excluded.expires_at_ms, source_group_id=excluded.source_group_id WHERE excluded.confidence > esp_node_profile_sources.confidence OR (excluded.confidence = esp_node_profile_sources.confidence AND excluded.observed_at_ms > esp_node_profile_sources.observed_at_ms)`, rec.MemberID[:], rec.EntmootPubKey, rec.Source, nodeProfileSourceKey(rec), rec.Hostname, rec.Confidence, rec.ObservedAtMS, rec.ExpiresAtMS, sourceGroup)
	if err != nil {
		return NodeProfileRecord{}, false, fmt.Errorf("esphttp: upsert node profile: %w", err)
	}
	best, ok, err := s.GetNodeProfile(ctx, rec.MemberID)
	if err != nil {
		return NodeProfileRecord{}, false, err
	}
	if !ok {
		return rec, true, nil
	}
	return best, nodeProfileSourceKey(best) == nodeProfileSourceKey(rec), nil
}
func (s *SQLiteStateStore) GetNodeProfile(ctx context.Context, id entmoot.MemberID) (NodeProfileRecord, bool, error) {
	return getNodeProfile(ctx, s.db, id, time.Now().UnixMilli(), nil, "")
}
func (s *SQLiteStateStore) ListNodeProfiles(ctx context.Context, ids []entmoot.MemberID) (map[entmoot.MemberID]NodeProfileRecord, error) {
	return s.listNodeProfilesForGroup(ctx, ids, nil, nil)
}
func (s *SQLiteStateStore) ListNodeProfilesForGroup(ctx context.Context, gid entmoot.GroupID, ids []entmoot.MemberID) (map[entmoot.MemberID]NodeProfileRecord, error) {
	return s.listNodeProfilesForGroup(ctx, ids, &gid, nil)
}
func (s *SQLiteStateStore) ListNodeProfilesForMembers(ctx context.Context, gid entmoot.GroupID, members map[entmoot.MemberID]string) (map[entmoot.MemberID]NodeProfileRecord, error) {
	ids := make([]entmoot.MemberID, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	return s.listNodeProfilesForGroup(ctx, ids, &gid, members)
}
func (s *SQLiteStateStore) listNodeProfilesForGroup(ctx context.Context, ids []entmoot.MemberID, gid *entmoot.GroupID, keys map[entmoot.MemberID]string) (map[entmoot.MemberID]NodeProfileRecord, error) {
	out := make(map[entmoot.MemberID]NodeProfileRecord)
	for _, id := range ids {
		rec, ok, err := getNodeProfile(ctx, s.db, id, time.Now().UnixMilli(), gid, keys[id])
		if err != nil {
			return nil, err
		}
		if ok {
			out[id] = rec
		}
	}
	return out, nil
}
func (s *SQLiteStateStore) observeFleetMemberNodeProfile(ctx context.Context, rec FleetMemberRecord) error {
	return s.refreshFleetMemberNodeProfile(ctx, rec.MemberID)
}
func (s *SQLiteStateStore) observeFleetInviteNodeProfile(ctx context.Context, rec FleetInviteRecord) error {
	return s.refreshFleetInviteNodeProfile(ctx, rec.MemberID)
}
func (s *SQLiteStateStore) refreshFleetMemberNodeProfile(ctx context.Context, id entmoot.MemberID) error {
	return s.refreshFleetNodeProfile(ctx, id, NodeProfileSourceFleetMember, `
SELECT m.entmoot_pubkey, m.hostname, m.updated_at_ms, 0
FROM esp_fleet_members m JOIN esp_fleets f ON f.fleet_id = m.fleet_id
WHERE m.member_id = ? AND f.status = 'active' AND m.status = 'active'`)
}

func (s *SQLiteStateStore) refreshFleetInviteNodeProfile(ctx context.Context, id entmoot.MemberID) error {
	return s.refreshFleetNodeProfile(ctx, id, NodeProfileSourceFleetInvite, `
SELECT i.entmoot_pubkey, i.hostname, i.updated_at_ms, i.expires_at_ms
FROM esp_fleet_invites i JOIN esp_fleets f ON f.fleet_id = i.fleet_id
WHERE i.member_id = ? AND f.status = 'active' AND i.status = 'invited'`)
}

func (s *SQLiteStateStore) refreshFleetNodeProfile(ctx context.Context, id entmoot.MemberID, source, query string) error {
	if id == (entmoot.MemberID{}) {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Take the SQLite writer lock before reading authoritative Fleet rows.
	// Concurrent refreshers then cannot reinsert an older membership snapshot.
	if _, err := tx.ExecContext(ctx, `DELETE FROM esp_node_profile_sources WHERE member_id = ? AND source = ?`, id[:], source); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, query, id[:])
	if err != nil {
		return err
	}
	defer rows.Close()
	now := time.Now().UnixMilli()
	var best NodeProfileRecord
	for rows.Next() {
		profile := NodeProfileRecord{MemberID: id, Source: source}
		if err := rows.Scan(&profile.EntmootPubKey, &profile.Hostname, &profile.ObservedAtMS, &profile.ExpiresAtMS); err != nil {
			return err
		}
		profile, valid, err := normalizeNodeProfileRecord(profile, now)
		if err != nil {
			return err
		}
		if valid && !nodeProfileExpired(profile, now) && shouldReplaceNodeProfile(best, profile, now) {
			best = profile
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if best.MemberID != (entmoot.MemberID{}) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO esp_node_profile_sources
(member_id, entmoot_pubkey, source, source_key, hostname, confidence, observed_at_ms, expires_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, id[:], best.EntmootPubKey, source, source, best.Hostname, best.Confidence, best.ObservedAtMS, best.ExpiresAtMS); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func nodeProfileMemberIDs(ctx context.Context, q nodeProfileQuerier, query string, args ...any) ([]entmoot.MemberID, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []entmoot.MemberID
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var id entmoot.MemberID
		if len(raw) != len(id) {
			return nil, errors.New("esphttp: profile member id must be full-width")
		}
		copy(id[:], raw)
		if id != (entmoot.MemberID{}) {
			ids = append(ids, id)
		}
	}
	return ids, rows.Err()
}

func fleetProfileMemberIDs(ctx context.Context, q nodeProfileQuerier, fleetID string) ([]entmoot.MemberID, error) {
	return nodeProfileMemberIDs(ctx, q, `
SELECT member_id FROM esp_fleet_members WHERE fleet_id = ?
UNION SELECT member_id FROM esp_fleet_invites WHERE fleet_id = ?`, fleetID, fleetID)
}

func (s *SQLiteStateStore) refreshFleetNodeProfiles(ctx context.Context, ids []entmoot.MemberID) error {
	for _, id := range ids {
		if err := s.refreshFleetMemberNodeProfile(ctx, id); err != nil {
			return err
		}
		if err := s.refreshFleetInviteNodeProfile(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStateStore) backfillFleetNodeProfiles(ctx context.Context) error {
	ids, err := nodeProfileMemberIDs(ctx, s.db, `
SELECT member_id FROM esp_node_profile_sources WHERE source IN ('fleet_member', 'fleet_invite')
UNION SELECT m.member_id FROM esp_fleet_members m JOIN esp_fleets f ON f.fleet_id = m.fleet_id
WHERE f.status = 'active' AND m.status = 'active' AND trim(m.hostname) != ''
UNION SELECT i.member_id FROM esp_fleet_invites i JOIN esp_fleets f ON f.fleet_id = i.fleet_id
WHERE f.status = 'active' AND i.status = 'invited' AND trim(i.hostname) != ''`)
	if err != nil {
		return err
	}
	return s.refreshFleetNodeProfiles(ctx, ids)
}

type nodeProfileQuerier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

func getNodeProfile(ctx context.Context, q nodeProfileQuerier, id entmoot.MemberID, nowMS int64, gid *entmoot.GroupID, publicKey string) (NodeProfileRecord, bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT member_id, entmoot_pubkey, hostname, source, confidence, observed_at_ms, expires_at_ms, source_group_id FROM esp_node_profile_sources WHERE member_id = ?`, id[:])
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
	rec, ok := bestNodeProfile(records, nowMS, gid, publicKey)
	return rec, ok, nil
}

type nodeProfileScanner interface{ Scan(...any) error }

func scanNodeProfileRow(row nodeProfileScanner) (NodeProfileRecord, error) {
	var memberBytes, groupBytes []byte
	var rec NodeProfileRecord
	if err := row.Scan(&memberBytes, &rec.EntmootPubKey, &rec.Hostname, &rec.Source, &rec.Confidence, &rec.ObservedAtMS, &rec.ExpiresAtMS, &groupBytes); err != nil {
		return NodeProfileRecord{}, fmt.Errorf("esphttp: scan node profile: %w", err)
	}
	if len(memberBytes) != len(rec.MemberID) {
		return NodeProfileRecord{}, errors.New("esphttp: invalid stored member id")
	}
	copy(rec.MemberID[:], memberBytes)
	if len(groupBytes) == len(entmoot.GroupID{}) {
		gid := entmoot.GroupID{}
		copy(gid[:], groupBytes)
		rec.SourceGroupID = &gid
	}
	return rec, nil
}
