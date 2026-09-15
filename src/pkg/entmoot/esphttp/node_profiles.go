package esphttp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
)

const (
	NodeProfileSourceMemberProfile = "member_profile"

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

// WithdrawnNodeProfileHostname is the placeholder a withdrawal record holds.
// A withdrawal must be a record rather than a delete, because a delete carries
// no timestamp: an older profile arriving afterwards — from history catch-up,
// or a peer re-gossiping — would then win and resurrect the withdrawn name,
// and nodes would disagree permanently. The record is written already expired,
// so no reader ever shows the placeholder.
const WithdrawnNodeProfileHostname = "-"

// WithdrawMemberProfileNodeProfile withdraws a member's published name for a
// group by recording a tombstone at the withdrawal's own issue time. Ordering
// is by that time, so a profile issued earlier cannot undo it however late it
// arrives.
// MemberProfileRecord is the record a published profile becomes. An empty
// displayName is a withdrawal, stored as a tombstone: ObservedAtMS carries the
// withdrawal's own issue time, which is what orders it against profiles, and
// the expiry is a fixed point in the past so the tombstone is expired for
// every reader's clock, however far ahead or behind. Using the issue time as
// the expiry would leave a withdrawal briefly unexpired and the placeholder
// briefly visible.
//
// Everything that publishes, withdraws or ranks a member profile builds the
// record here, so there is one mapping from a claim to a stored row.
func MemberProfileRecord(groupID entmoot.GroupID, memberID entmoot.MemberID, publicKey, displayName string, issuedAtMS, expiresAtMS int64) NodeProfileRecord {
	rec := NodeProfileRecord{
		MemberID:      memberID,
		EntmootPubKey: strings.TrimSpace(publicKey),
		Hostname:      displayName,
		Source:        NodeProfileSourceMemberProfile,
		ObservedAtMS:  issuedAtMS,
		ExpiresAtMS:   expiresAtMS,
		SourceGroupID: &groupID,
	}
	if displayName == "" {
		rec.Hostname = WithdrawnNodeProfileHostname
		rec.ExpiresAtMS = 1
	}
	return rec
}

// BetterMemberProfileRecord reports whether a would replace b. It is the same
// comparison every StateStore applies on upsert, exported so a caller holding
// several claims for one member picks the winner without inventing a second
// ordering rule — which is exactly the defect that made history catch-up
// re-show retracted names.
func BetterMemberProfileRecord(a, b NodeProfileRecord) bool {
	return shouldReplaceNodeProfile(b, a)
}

func WithdrawMemberProfileNodeProfile(ctx context.Context, state StateStore, groupID entmoot.GroupID, memberID entmoot.MemberID, publicKey string, issuedAtMS int64) error {
	if state == nil || memberID == (entmoot.MemberID{}) {
		return nil
	}
	_, _, err := state.UpsertNodeProfile(ctx, MemberProfileRecord(groupID, memberID, publicKey, "", issuedAtMS, 0))
	return err
}

func ObserveMemberProfileNodeProfile(ctx context.Context, state StateStore, groupID entmoot.GroupID, memberID entmoot.MemberID, publicKey, hostname string, observedAtMS, expiresAtMS int64) error {
	if state == nil || memberID == (entmoot.MemberID{}) {
		return nil
	}
	if _, ok := NormalizeNodeProfileHostname(hostname); !ok {
		return nil
	}
	_, _, err := state.UpsertNodeProfile(ctx, MemberProfileRecord(groupID, memberID, publicKey, hostname, observedAtMS, expiresAtMS))
	return err
}

func nodeProfileConfidenceForSource(source string) (int, bool) {
	switch source {
	case NodeProfileSourceMemberProfile:
		return NodeProfileConfidenceMemberProfile, true
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

// nodeProfileExpiryRank orders expiries with any non-positive value meaning
// "never expires", so it sorts above every finite one. Callers must compare
// ranks rather than raw expiries, or two never-expires claims written with
// different sentinels compare as different lifetimes.
func nodeProfileExpiryRank(rec NodeProfileRecord) int64 {
	if rec.ExpiresAtMS <= 0 {
		return math.MaxInt64
	}
	return rec.ExpiresAtMS
}

func nodeProfileExpired(rec NodeProfileRecord, nowMS int64) bool {
	return rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS
}

// isWithdrawalRecord reports the tombstone a withdrawal writes. It is stored
// permanently expired, so the expiry clause in the tie-break must not judge
// it: a withdrawal loses to nothing but a later issue time. The tombstone has
// its own clause, ahead of the expiry one.
func isWithdrawalRecord(rec NodeProfileRecord) bool {
	return rec.Source == NodeProfileSourceMemberProfile && rec.Hostname == WithdrawnNodeProfileHostname
}

func shouldReplaceNodeProfile(existing, incoming NodeProfileRecord) bool {
	if existing.MemberID == (entmoot.MemberID{}) {
		return true
	}
	if incoming.Confidence != existing.Confidence {
		return incoming.Confidence > existing.Confidence
	}
	if incoming.ObservedAtMS != existing.ObservedAtMS {
		return incoming.ObservedAtMS > existing.ObservedAtMS
	}
	// Equal issue times must not resolve by arrival order, or two nodes that
	// saw the same pair in opposite orders keep different names indefinitely.
	// Everything below is a total order over the pair, so the outcome is the
	// same whichever arrived first.
	//
	// A withdrawal wins first — the safe direction, since the alternative is
	// showing a name its owner asked to retract. Then the longer-lived claim
	// wins, which is the clock-free way to prefer the usable record: asking
	// whether a record is expired RIGHT NOW made the stored winner depend on
	// when the loser arrived, so two nodes holding the same two claims could
	// disagree until the nearer expiry passed. Comparing the expiries decides
	// the same cases without consulting any clock. Hostname order settles the
	// rest; every clause is a total order, so the winner is the maximum of a
	// total order and arrival cannot change it.
	if isWithdrawalRecord(incoming) != isWithdrawalRecord(existing) {
		return isWithdrawalRecord(incoming)
	}
	// Compare the RANKS, not the raw expiries: every non-positive value means
	// never-expires, so 0 and -1 are the same claim about lifetime. Guarding
	// on the raw values made two such claims mutually non-replacing — the
	// first one stored won, and the relation was not even transitive.
	if incomingRank, existingRank := nodeProfileExpiryRank(incoming), nodeProfileExpiryRank(existing); incomingRank != existingRank {
		return incomingRank > existingRank
	}
	return incoming.Hostname < existing.Hostname
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
		if shouldReplaceNodeProfile(best, rec) {
			best = rec
		}
	}
	return cloneNodeProfileRecord(best), best.MemberID != (entmoot.MemberID{})
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
	if existing := records[key]; existing.MemberID != (entmoot.MemberID{}) && !shouldReplaceNodeProfile(existing, rec) {
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
	// The predicates below mirror shouldReplaceNodeProfile clause for clause.
	expiryRank := func(table string) string {
		return "(CASE WHEN " + table + ".expires_at_ms <= 0 THEN 9223372036854775807 ELSE " + table + ".expires_at_ms END)"
	}
	tombstone := func(table string) string {
		return "(" + table + ".source = :member_profile AND " + table + ".hostname = :tombstone)"
	}
	existingExpiryRank, incomingExpiryRank := expiryRank("esp_node_profile_sources"), expiryRank("excluded")
	existingTombstone, incomingTombstone := tombstone("esp_node_profile_sources"), tombstone("excluded")
	_, err = s.db.ExecContext(ctx, `INSERT INTO esp_node_profile_sources
			(member_id, entmoot_pubkey, source, source_key, hostname, confidence, observed_at_ms, expires_at_ms, source_group_id)
		VALUES (:member_id, :pubkey, :source, :source_key, :hostname, :confidence, :observed_at, :expires_at, :source_group)
		ON CONFLICT(member_id, source_key) DO UPDATE SET
			entmoot_pubkey=excluded.entmoot_pubkey, source=excluded.source, hostname=excluded.hostname,
			confidence=excluded.confidence, observed_at_ms=excluded.observed_at_ms,
			expires_at_ms=excluded.expires_at_ms, source_group_id=excluded.source_group_id
		WHERE excluded.confidence > esp_node_profile_sources.confidence
				OR (excluded.confidence = esp_node_profile_sources.confidence AND excluded.observed_at_ms > esp_node_profile_sources.observed_at_ms)
				OR (excluded.confidence = esp_node_profile_sources.confidence AND excluded.observed_at_ms = esp_node_profile_sources.observed_at_ms
					AND (CASE
						WHEN `+incomingTombstone+` <> `+existingTombstone+` THEN `+incomingTombstone+`
						WHEN `+incomingExpiryRank+` <> `+existingExpiryRank+`
							THEN `+incomingExpiryRank+` > `+existingExpiryRank+`
						ELSE excluded.hostname < esp_node_profile_sources.hostname END))`,
		sql.Named("member_id", rec.MemberID[:]),
		sql.Named("pubkey", rec.EntmootPubKey),
		sql.Named("source", rec.Source),
		sql.Named("source_key", nodeProfileSourceKey(rec)),
		sql.Named("hostname", rec.Hostname),
		sql.Named("confidence", rec.Confidence),
		sql.Named("observed_at", rec.ObservedAtMS),
		sql.Named("expires_at", rec.ExpiresAtMS),
		sql.Named("source_group", sourceGroup),
		sql.Named("tombstone", WithdrawnNodeProfileHostname),
		sql.Named("member_profile", NodeProfileSourceMemberProfile),
	)
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
