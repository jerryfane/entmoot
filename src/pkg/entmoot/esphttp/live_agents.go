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
	topicmatch "entmoot/pkg/entmoot/topic"
)

const (
	LiveModeListen         = "listen"
	LiveModeReplyOnMention = "reply_on_mention"
	LiveModeConverse       = "converse"
	LiveModeOperator       = "operator"

	LiveStatusOnline  = "online"
	LiveStatusOffline = "offline"
)

var defaultLiveActions = []string{
	"reply",
	"message.summarize",
	"task.create",
	"task.comment",
	"task.assign_self",
	"task.update_own",
	"task.assign_others",
	"command.request",
	"command.send",
	"alert.owner",
	"invite.create",
	"member.remove",
	"metadata.update",
	"external.message.send",
	"webhook.call",
	"shell.run",
}

type LiveAgentState struct {
	Enabled        bool     `json:"enabled"`
	Status         string   `json:"status,omitempty"`
	Mode           string   `json:"mode,omitempty"`
	TopicFilters   []string `json:"topic_filters,omitempty"`
	AllowedActions []string `json:"allowed_actions,omitempty"`
	LastSeenAtMS   int64    `json:"last_seen_at_ms,omitempty"`
	LeaseUntilMS   int64    `json:"lease_until_ms,omitempty"`
	UpdatedAtMS    int64    `json:"updated_at_ms,omitempty"`
}

type LiveAgentConfig struct {
	GroupID        entmoot.GroupID `json:"group_id"`
	NodeID         entmoot.NodeID  `json:"node_id"`
	Enabled        bool            `json:"enabled"`
	Mode           string          `json:"mode"`
	TopicFilters   []string        `json:"topic_filters,omitempty"`
	AllowedActions []string        `json:"allowed_actions,omitempty"`
	UpdatedAtMS    int64           `json:"updated_at_ms"`
}

type LiveAgentPresence struct {
	GroupID      entmoot.GroupID `json:"group_id"`
	NodeID       entmoot.NodeID  `json:"node_id"`
	Status       string          `json:"status"`
	Mode         string          `json:"mode"`
	TopicFilters []string        `json:"topic_filters,omitempty"`
	LastSeenAtMS int64           `json:"last_seen_at_ms"`
	LeaseUntilMS int64           `json:"lease_until_ms"`
	UpdatedAtMS  int64           `json:"updated_at_ms"`
}

type LiveAgentCursor struct {
	GroupID              entmoot.GroupID     `json:"group_id"`
	NodeID               entmoot.NodeID      `json:"node_id"`
	ScanFloorAtMS        int64               `json:"scan_floor_at_ms,omitempty"`
	LastSeenAtMS         int64               `json:"last_seen_at_ms"`
	LastSeenAuthorNodeID entmoot.NodeID      `json:"last_seen_author_node_id,omitempty"`
	LastSeenMessageID    entmoot.MessageID   `json:"last_seen_message_id,omitempty"`
	SeenMessageIDs       []entmoot.MessageID `json:"seen_message_ids,omitempty"`
	UpdatedAtMS          int64               `json:"updated_at_ms"`
}

func DefaultLiveActions() []string {
	return append([]string(nil), defaultLiveActions...)
}

func NormalizeLiveMode(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case LiveModeListen:
		return LiveModeListen
	case LiveModeReplyOnMention:
		return LiveModeReplyOnMention
	case LiveModeConverse:
		return LiveModeConverse
	case LiveModeOperator:
		return LiveModeOperator
	default:
		return ""
	}
}

func NormalizeLiveStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "", LiveStatusOnline:
		return LiveStatusOnline
	case LiveStatusOffline:
		return LiveStatusOffline
	default:
		return strings.TrimSpace(strings.ToLower(status))
	}
}

func NormalizeLiveTopicFilters(filters []string) []string {
	out := make([]string, 0, len(filters))
	seen := make(map[string]struct{}, len(filters))
	for _, filter := range filters {
		filter = strings.TrimSpace(filter)
		if filter == "" {
			continue
		}
		if _, ok := seen[filter]; ok {
			continue
		}
		seen[filter] = struct{}{}
		out = append(out, filter)
	}
	sort.Strings(out)
	return out
}

func NormalizeLiveActions(actions []string) []string {
	if len(actions) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(defaultLiveActions))
	for _, action := range defaultLiveActions {
		allowed[action] = struct{}{}
	}
	out := make([]string, 0, len(actions))
	seen := make(map[string]struct{}, len(actions))
	for _, action := range actions {
		action = strings.TrimSpace(strings.ToLower(action))
		if _, ok := allowed[action]; !ok {
			continue
		}
		if _, ok := seen[action]; ok {
			continue
		}
		seen[action] = struct{}{}
		out = append(out, action)
	}
	sort.Strings(out)
	return out
}

func UnknownLiveActions(actions []string) []string {
	if len(actions) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(defaultLiveActions))
	for _, action := range defaultLiveActions {
		allowed[action] = struct{}{}
	}
	out := make([]string, 0)
	seen := make(map[string]struct{}, len(actions))
	for _, action := range actions {
		action = strings.TrimSpace(strings.ToLower(action))
		if action == "" {
			continue
		}
		if _, ok := allowed[action]; ok {
			continue
		}
		if _, ok := seen[action]; ok {
			continue
		}
		seen[action] = struct{}{}
		out = append(out, action)
	}
	sort.Strings(out)
	return out
}

func ValidateLiveConfig(cfg LiveAgentConfig) error {
	if cfg.GroupID == (entmoot.GroupID{}) {
		return errors.New("esphttp: live group id is required")
	}
	if cfg.NodeID == 0 {
		return errors.New("esphttp: live node id is required")
	}
	if NormalizeLiveMode(cfg.Mode) == "" {
		return errors.New("esphttp: live mode is invalid")
	}
	return nil
}

func LiveTopicMatches(filter, topic string) bool {
	filter = strings.TrimSpace(filter)
	topic = strings.TrimSpace(topic)
	if filter == "" || topic == "" || topicmatch.ValidPattern(filter) != nil {
		return false
	}
	return topicmatch.Match(filter, topic)
}

func (s *MemoryStateStore) UpsertLiveAgentConfig(_ context.Context, cfg LiveAgentConfig) (LiveAgentConfig, error) {
	cfg.Mode = NormalizeLiveMode(cfg.Mode)
	cfg.TopicFilters = NormalizeLiveTopicFilters(cfg.TopicFilters)
	if unknown := UnknownLiveActions(cfg.AllowedActions); len(unknown) > 0 {
		return LiveAgentConfig{}, fmt.Errorf("esphttp: unknown live actions: %s", strings.Join(unknown, ", "))
	}
	cfg.AllowedActions = NormalizeLiveActions(cfg.AllowedActions)
	if cfg.Mode == LiveModeOperator && len(cfg.AllowedActions) == 0 {
		cfg.AllowedActions = DefaultLiveActions()
	}
	if err := ValidateLiveConfig(cfg); err != nil {
		return LiveAgentConfig{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.liveAgentConfigs[cfg.GroupID] == nil {
		s.liveAgentConfigs[cfg.GroupID] = make(map[entmoot.NodeID]LiveAgentConfig)
	}
	if cfg.UpdatedAtMS == 0 {
		cfg.UpdatedAtMS = s.nowMS()
	}
	s.liveAgentConfigs[cfg.GroupID][cfg.NodeID] = cloneLiveAgentConfig(cfg)
	return cloneLiveAgentConfig(cfg), nil
}

func (s *MemoryStateStore) GetLiveAgentConfig(_ context.Context, gid entmoot.GroupID, nodeID entmoot.NodeID) (LiveAgentConfig, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, ok := s.liveAgentConfigs[gid][nodeID]
	if !ok {
		return LiveAgentConfig{}, false, nil
	}
	return cloneLiveAgentConfig(cfg), true, nil
}

func (s *MemoryStateStore) ListLiveAgentConfigs(_ context.Context, gid entmoot.GroupID) ([]LiveAgentConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LiveAgentConfig, 0, len(s.liveAgentConfigs[gid]))
	for _, cfg := range s.liveAgentConfigs[gid] {
		out = append(out, cloneLiveAgentConfig(cfg))
	}
	sortLiveAgentConfigs(out)
	return out, nil
}

func (s *MemoryStateStore) DeleteLiveAgentConfig(_ context.Context, gid entmoot.GroupID, nodeID entmoot.NodeID, updatedAtMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.liveAgentConfigs[gid] != nil {
		delete(s.liveAgentConfigs[gid], nodeID)
	}
	if s.liveAgentPresence[gid] != nil {
		if p, ok := s.liveAgentPresence[gid][nodeID]; ok {
			p.Status = LiveStatusOffline
			p.UpdatedAtMS = updatedAtMS
			if p.UpdatedAtMS == 0 {
				p.UpdatedAtMS = s.nowMS()
			}
			s.liveAgentPresence[gid][nodeID] = p
		}
	}
	return nil
}

func (s *MemoryStateStore) UpsertLiveAgentPresence(_ context.Context, p LiveAgentPresence) (LiveAgentPresence, error) {
	p.Status = NormalizeLiveStatus(p.Status)
	p.Mode = NormalizeLiveMode(p.Mode)
	p.TopicFilters = NormalizeLiveTopicFilters(p.TopicFilters)
	if p.GroupID == (entmoot.GroupID{}) {
		return LiveAgentPresence{}, errors.New("esphttp: live presence group id is required")
	}
	if p.NodeID == 0 {
		return LiveAgentPresence{}, errors.New("esphttp: live presence node id is required")
	}
	if p.Mode == "" {
		return LiveAgentPresence{}, errors.New("esphttp: live presence mode is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	if p.LastSeenAtMS == 0 {
		p.LastSeenAtMS = now
	}
	if p.UpdatedAtMS == 0 {
		p.UpdatedAtMS = now
	}
	if s.liveAgentPresence[p.GroupID] == nil {
		s.liveAgentPresence[p.GroupID] = make(map[entmoot.NodeID]LiveAgentPresence)
	}
	s.liveAgentPresence[p.GroupID][p.NodeID] = cloneLiveAgentPresence(p)
	return cloneLiveAgentPresence(p), nil
}

func (s *MemoryStateStore) ListLiveAgentPresence(_ context.Context, gid entmoot.GroupID) ([]LiveAgentPresence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LiveAgentPresence, 0, len(s.liveAgentPresence[gid]))
	for _, p := range s.liveAgentPresence[gid] {
		out = append(out, cloneLiveAgentPresence(p))
	}
	sortLiveAgentPresence(out)
	return out, nil
}

func (s *MemoryStateStore) GetLiveAgentCursor(_ context.Context, gid entmoot.GroupID, nodeID entmoot.NodeID) (LiveAgentCursor, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cursor, ok := s.liveAgentCursors[gid][nodeID]
	if !ok {
		return LiveAgentCursor{}, false, nil
	}
	return cloneLiveAgentCursor(cursor), true, nil
}

func (s *MemoryStateStore) UpsertLiveAgentCursor(_ context.Context, cursor LiveAgentCursor) (LiveAgentCursor, error) {
	if cursor.GroupID == (entmoot.GroupID{}) {
		return LiveAgentCursor{}, errors.New("esphttp: live cursor group id is required")
	}
	if cursor.NodeID == 0 {
		return LiveAgentCursor{}, errors.New("esphttp: live cursor node id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor.UpdatedAtMS == 0 {
		cursor.UpdatedAtMS = s.nowMS()
	}
	if s.liveAgentCursors[cursor.GroupID] == nil {
		s.liveAgentCursors[cursor.GroupID] = make(map[entmoot.NodeID]LiveAgentCursor)
	}
	s.liveAgentCursors[cursor.GroupID][cursor.NodeID] = cloneLiveAgentCursor(cursor)
	return cloneLiveAgentCursor(cursor), nil
}

func (s *SQLiteStateStore) UpsertLiveAgentConfig(ctx context.Context, cfg LiveAgentConfig) (LiveAgentConfig, error) {
	cfg.Mode = NormalizeLiveMode(cfg.Mode)
	cfg.TopicFilters = NormalizeLiveTopicFilters(cfg.TopicFilters)
	if unknown := UnknownLiveActions(cfg.AllowedActions); len(unknown) > 0 {
		return LiveAgentConfig{}, fmt.Errorf("esphttp: unknown live actions: %s", strings.Join(unknown, ", "))
	}
	cfg.AllowedActions = NormalizeLiveActions(cfg.AllowedActions)
	if cfg.Mode == LiveModeOperator && len(cfg.AllowedActions) == 0 {
		cfg.AllowedActions = DefaultLiveActions()
	}
	if err := ValidateLiveConfig(cfg); err != nil {
		return LiveAgentConfig{}, err
	}
	if cfg.UpdatedAtMS == 0 {
		cfg.UpdatedAtMS = nowMS()
	}
	topics, _ := json.Marshal(cfg.TopicFilters)
	actions, _ := json.Marshal(cfg.AllowedActions)
	enabled := 0
	if cfg.Enabled {
		enabled = 1
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO esp_live_agent_configs (group_id, node_id, enabled, mode, topic_filters, allowed_actions, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(group_id, node_id) DO UPDATE SET
  enabled=excluded.enabled,
  mode=excluded.mode,
  topic_filters=excluded.topic_filters,
  allowed_actions=excluded.allowed_actions,
  updated_at_ms=excluded.updated_at_ms`,
		cfg.GroupID[:], uint64(cfg.NodeID), enabled, cfg.Mode, topics, actions, cfg.UpdatedAtMS)
	if err != nil {
		return LiveAgentConfig{}, fmt.Errorf("esphttp: upsert live agent config: %w", err)
	}
	return cfg, nil
}

func (s *SQLiteStateStore) GetLiveAgentConfig(ctx context.Context, gid entmoot.GroupID, nodeID entmoot.NodeID) (LiveAgentConfig, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT group_id, node_id, enabled, mode, topic_filters, allowed_actions, updated_at_ms FROM esp_live_agent_configs WHERE group_id = ? AND node_id = ?`, gid[:], uint64(nodeID))
	cfg, err := scanLiveAgentConfig(row)
	if errors.Is(err, sql.ErrNoRows) {
		return LiveAgentConfig{}, false, nil
	}
	if err != nil {
		return LiveAgentConfig{}, false, err
	}
	return cfg, true, nil
}

func (s *SQLiteStateStore) ListLiveAgentConfigs(ctx context.Context, gid entmoot.GroupID) ([]LiveAgentConfig, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT group_id, node_id, enabled, mode, topic_filters, allowed_actions, updated_at_ms FROM esp_live_agent_configs WHERE group_id = ? ORDER BY node_id`, gid[:])
	if err != nil {
		return nil, fmt.Errorf("esphttp: list live agent configs: %w", err)
	}
	defer rows.Close()
	out := []LiveAgentConfig{}
	for rows.Next() {
		cfg, err := scanLiveAgentConfig(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cfg)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) DeleteLiveAgentConfig(ctx context.Context, gid entmoot.GroupID, nodeID entmoot.NodeID, updatedAtMS int64) error {
	if updatedAtMS == 0 {
		updatedAtMS = nowMS()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("esphttp: begin delete live agent config: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM esp_live_agent_configs WHERE group_id = ? AND node_id = ?`, gid[:], uint64(nodeID)); err != nil {
		return fmt.Errorf("esphttp: delete live agent config: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE esp_live_agent_presence SET status = ?, updated_at_ms = ? WHERE group_id = ? AND node_id = ?`, LiveStatusOffline, updatedAtMS, gid[:], uint64(nodeID)); err != nil {
		return fmt.Errorf("esphttp: mark live agent offline: %w", err)
	}
	return tx.Commit()
}

func (s *SQLiteStateStore) UpsertLiveAgentPresence(ctx context.Context, p LiveAgentPresence) (LiveAgentPresence, error) {
	p.Status = NormalizeLiveStatus(p.Status)
	p.Mode = NormalizeLiveMode(p.Mode)
	p.TopicFilters = NormalizeLiveTopicFilters(p.TopicFilters)
	if p.GroupID == (entmoot.GroupID{}) {
		return LiveAgentPresence{}, errors.New("esphttp: live presence group id is required")
	}
	if p.NodeID == 0 {
		return LiveAgentPresence{}, errors.New("esphttp: live presence node id is required")
	}
	if p.Mode == "" {
		return LiveAgentPresence{}, errors.New("esphttp: live presence mode is invalid")
	}
	now := nowMS()
	if p.LastSeenAtMS == 0 {
		p.LastSeenAtMS = now
	}
	if p.UpdatedAtMS == 0 {
		p.UpdatedAtMS = now
	}
	topics, _ := json.Marshal(p.TopicFilters)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO esp_live_agent_presence (group_id, node_id, status, mode, topic_filters, last_seen_at_ms, lease_until_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(group_id, node_id) DO UPDATE SET
  status=excluded.status,
  mode=excluded.mode,
  topic_filters=excluded.topic_filters,
  last_seen_at_ms=excluded.last_seen_at_ms,
  lease_until_ms=excluded.lease_until_ms,
  updated_at_ms=excluded.updated_at_ms`,
		p.GroupID[:], uint64(p.NodeID), p.Status, p.Mode, topics, p.LastSeenAtMS, p.LeaseUntilMS, p.UpdatedAtMS)
	if err != nil {
		return LiveAgentPresence{}, fmt.Errorf("esphttp: upsert live agent presence: %w", err)
	}
	return p, nil
}

func (s *SQLiteStateStore) ListLiveAgentPresence(ctx context.Context, gid entmoot.GroupID) ([]LiveAgentPresence, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT group_id, node_id, status, mode, topic_filters, last_seen_at_ms, lease_until_ms, updated_at_ms FROM esp_live_agent_presence WHERE group_id = ? ORDER BY node_id`, gid[:])
	if err != nil {
		return nil, fmt.Errorf("esphttp: list live agent presence: %w", err)
	}
	defer rows.Close()
	out := []LiveAgentPresence{}
	for rows.Next() {
		p, err := scanLiveAgentPresence(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) GetLiveAgentCursor(ctx context.Context, gid entmoot.GroupID, nodeID entmoot.NodeID) (LiveAgentCursor, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT group_id, node_id, scan_floor_at_ms, last_seen_at_ms, last_seen_author_node_id, last_seen_message_id, seen_message_ids, updated_at_ms FROM esp_live_agent_cursors WHERE group_id = ? AND node_id = ?`, gid[:], uint64(nodeID))
	cursor, err := scanLiveAgentCursor(row)
	if errors.Is(err, sql.ErrNoRows) {
		return LiveAgentCursor{}, false, nil
	}
	if err != nil {
		return LiveAgentCursor{}, false, err
	}
	return cursor, true, nil
}

func (s *SQLiteStateStore) UpsertLiveAgentCursor(ctx context.Context, cursor LiveAgentCursor) (LiveAgentCursor, error) {
	if cursor.GroupID == (entmoot.GroupID{}) {
		return LiveAgentCursor{}, errors.New("esphttp: live cursor group id is required")
	}
	if cursor.NodeID == 0 {
		return LiveAgentCursor{}, errors.New("esphttp: live cursor node id is required")
	}
	if cursor.UpdatedAtMS == 0 {
		cursor.UpdatedAtMS = nowMS()
	}
	seenIDs, err := json.Marshal(cursor.SeenMessageIDs)
	if err != nil {
		return LiveAgentCursor{}, fmt.Errorf("esphttp: encode live agent cursor seen ids: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO esp_live_agent_cursors (group_id, node_id, scan_floor_at_ms, last_seen_at_ms, last_seen_author_node_id, last_seen_message_id, seen_message_ids, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(group_id, node_id) DO UPDATE SET
  scan_floor_at_ms=excluded.scan_floor_at_ms,
  last_seen_at_ms=excluded.last_seen_at_ms,
  last_seen_author_node_id=excluded.last_seen_author_node_id,
  last_seen_message_id=excluded.last_seen_message_id,
  seen_message_ids=excluded.seen_message_ids,
  updated_at_ms=excluded.updated_at_ms`,
		cursor.GroupID[:], uint64(cursor.NodeID), cursor.ScanFloorAtMS, cursor.LastSeenAtMS, uint64(cursor.LastSeenAuthorNodeID), cursor.LastSeenMessageID[:], seenIDs, cursor.UpdatedAtMS)
	if err != nil {
		return LiveAgentCursor{}, fmt.Errorf("esphttp: upsert live agent cursor: %w", err)
	}
	return cursor, nil
}

func LiveAgentStatesByNode(configs []LiveAgentConfig, presences []LiveAgentPresence, nowMS int64) map[entmoot.NodeID]LiveAgentState {
	out := make(map[entmoot.NodeID]LiveAgentState, len(configs))
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		out[cfg.NodeID] = LiveAgentState{
			Enabled:        cfg.Enabled,
			Status:         LiveStatusOffline,
			Mode:           cfg.Mode,
			TopicFilters:   append([]string(nil), cfg.TopicFilters...),
			AllowedActions: append([]string(nil), cfg.AllowedActions...),
			UpdatedAtMS:    cfg.UpdatedAtMS,
		}
	}
	for _, p := range presences {
		state, ok := out[p.NodeID]
		if !ok {
			continue
		}
		status := p.Status
		if p.LeaseUntilMS > 0 && nowMS > 0 && p.LeaseUntilMS <= nowMS {
			status = LiveStatusOffline
		}
		state.Status = status
		state.Mode = firstNonEmptyString(p.Mode, state.Mode)
		if len(p.TopicFilters) > 0 {
			state.TopicFilters = append([]string(nil), p.TopicFilters...)
		}
		state.LastSeenAtMS = p.LastSeenAtMS
		state.LeaseUntilMS = p.LeaseUntilMS
		if p.UpdatedAtMS > state.UpdatedAtMS {
			state.UpdatedAtMS = p.UpdatedAtMS
		}
		out[p.NodeID] = state
	}
	return out
}

func scanLiveAgentConfig(row interface {
	Scan(dest ...interface{}) error
}) (LiveAgentConfig, error) {
	var cfg LiveAgentConfig
	var groupBytes []byte
	var nodeID uint64
	var enabled int
	var topics, actions []byte
	if err := row.Scan(&groupBytes, &nodeID, &enabled, &cfg.Mode, &topics, &actions, &cfg.UpdatedAtMS); err != nil {
		return LiveAgentConfig{}, err
	}
	if err := decodeGroupIDBytes(groupBytes, &cfg.GroupID); err != nil {
		return LiveAgentConfig{}, err
	}
	cfg.NodeID = entmoot.NodeID(nodeID)
	cfg.Enabled = enabled != 0
	_ = json.Unmarshal(topics, &cfg.TopicFilters)
	_ = json.Unmarshal(actions, &cfg.AllowedActions)
	return cfg, nil
}

func scanLiveAgentPresence(row interface {
	Scan(dest ...interface{}) error
}) (LiveAgentPresence, error) {
	var p LiveAgentPresence
	var groupBytes []byte
	var nodeID uint64
	var topics []byte
	if err := row.Scan(&groupBytes, &nodeID, &p.Status, &p.Mode, &topics, &p.LastSeenAtMS, &p.LeaseUntilMS, &p.UpdatedAtMS); err != nil {
		return LiveAgentPresence{}, err
	}
	if err := decodeGroupIDBytes(groupBytes, &p.GroupID); err != nil {
		return LiveAgentPresence{}, err
	}
	p.NodeID = entmoot.NodeID(nodeID)
	_ = json.Unmarshal(topics, &p.TopicFilters)
	return p, nil
}

func scanLiveAgentCursor(row interface {
	Scan(dest ...interface{}) error
}) (LiveAgentCursor, error) {
	var cursor LiveAgentCursor
	var groupBytes []byte
	var nodeID, authorNodeID uint64
	var msgID, seenIDs []byte
	if err := row.Scan(&groupBytes, &nodeID, &cursor.ScanFloorAtMS, &cursor.LastSeenAtMS, &authorNodeID, &msgID, &seenIDs, &cursor.UpdatedAtMS); err != nil {
		return LiveAgentCursor{}, err
	}
	if err := decodeGroupIDBytes(groupBytes, &cursor.GroupID); err != nil {
		return LiveAgentCursor{}, err
	}
	cursor.NodeID = entmoot.NodeID(nodeID)
	cursor.LastSeenAuthorNodeID = entmoot.NodeID(authorNodeID)
	if len(msgID) == len(cursor.LastSeenMessageID) {
		copy(cursor.LastSeenMessageID[:], msgID)
	}
	_ = json.Unmarshal(seenIDs, &cursor.SeenMessageIDs)
	return cursor, nil
}

func cloneLiveAgentConfig(cfg LiveAgentConfig) LiveAgentConfig {
	cfg.TopicFilters = append([]string(nil), cfg.TopicFilters...)
	cfg.AllowedActions = append([]string(nil), cfg.AllowedActions...)
	return cfg
}

func cloneLiveAgentPresence(p LiveAgentPresence) LiveAgentPresence {
	p.TopicFilters = append([]string(nil), p.TopicFilters...)
	return p
}

func cloneLiveAgentCursor(cursor LiveAgentCursor) LiveAgentCursor {
	cursor.SeenMessageIDs = append([]entmoot.MessageID(nil), cursor.SeenMessageIDs...)
	return cursor
}

func sortLiveAgentConfigs(items []LiveAgentConfig) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].NodeID < items[j].NodeID
	})
}

func sortLiveAgentPresence(items []LiveAgentPresence) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].NodeID < items[j].NodeID
	})
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func nowMS() int64 {
	return time.Now().UnixMilli()
}

func decodeGroupIDBytes(raw []byte, dst *entmoot.GroupID) error {
	if len(raw) != len(dst[:]) {
		return fmt.Errorf("esphttp: invalid group id length %d", len(raw))
	}
	copy(dst[:], raw)
	return nil
}
