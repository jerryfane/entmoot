package esphttp

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"entmoot/pkg/entmoot"

	_ "modernc.org/sqlite"
)

// GroupSummary is the mobile/API projection for one locally served group.
type GroupSummary struct {
	GroupID    entmoot.GroupID        `json:"group_id"`
	Name       string                 `json:"name,omitempty"`
	Members    int                    `json:"members,omitempty"`
	RosterHead entmoot.RosterEntryID  `json:"roster_head,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// MemberSummary is the mobile/API projection for one group member.
type MemberSummary struct {
	NodeID        entmoot.NodeID `json:"node_id"`
	EntmootPubKey string         `json:"entmoot_pubkey"`
	Founder       bool           `json:"founder,omitempty"`
}

// GroupCatalog reads local group/roster state for mobile API requests.
type GroupCatalog interface {
	ListGroups(context.Context) ([]GroupSummary, error)
	GetGroup(context.Context, entmoot.GroupID) (GroupSummary, bool, error)
	ListMembers(context.Context, entmoot.GroupID) ([]MemberSummary, error)
}

// SignRequest is durable ESP-local state for phone-held signing workflows.
type SignRequest struct {
	ID                   string          `json:"id"`
	DeviceID             string          `json:"device_id,omitempty"`
	Kind                 string          `json:"kind"`
	Status               string          `json:"status"`
	GroupID              entmoot.GroupID `json:"group_id,omitempty"`
	Payload              json.RawMessage `json:"payload,omitempty"`
	CanonicalType        string          `json:"canonical_type,omitempty"`
	SignatureAlgorithm   string          `json:"signature_algorithm,omitempty"`
	SigningPayload       string          `json:"signing_payload,omitempty"`
	SigningPayloadSHA256 string          `json:"signing_payload_sha256,omitempty"`
	Signature            string          `json:"signature,omitempty"`
	PublishResult        *PublishResult  `json:"publish_result,omitempty"`
	CreatedAtMS          int64           `json:"created_at_ms"`
	UpdatedAtMS          int64           `json:"updated_at_ms"`
	ExpiresAtMS          int64           `json:"expires_at_ms,omitempty"`
}

// NotificationPreferences are ESP-local device notification settings.
type NotificationPreferences struct {
	Enabled bool     `json:"enabled"`
	Topics  []string `json:"topics,omitempty"`
}

// DeviceState is ESP-local state associated with one mobile device.
type DeviceState struct {
	DeviceID                string                  `json:"device_id"`
	PushToken               string                  `json:"push_token,omitempty"`
	PushPlatform            string                  `json:"push_platform,omitempty"`
	NotificationPreferences NotificationPreferences `json:"notification_preferences"`
	UpdatedAtMS             int64                   `json:"updated_at_ms"`
}

// IdempotencyRecord stores one completed ESP HTTP mutation response.
type IdempotencyRecord struct {
	Scope       string          `json:"scope"`
	Key         string          `json:"key"`
	RequestHash string          `json:"request_hash"`
	StatusCode  int             `json:"status_code"`
	Response    json.RawMessage `json:"response"`
	CreatedAtMS int64           `json:"created_at_ms"`
	UpdatedAtMS int64           `json:"updated_at_ms"`
	ExpiresAtMS int64           `json:"expires_at_ms"`
}

// StateStore persists ESP-local mobile service state.
type StateStore interface {
	CreateSignRequest(context.Context, SignRequest) (SignRequest, error)
	ListSignRequests(context.Context, string) ([]SignRequest, error)
	GetSignRequest(context.Context, string) (SignRequest, bool, error)
	CompleteSignRequest(context.Context, string, string, *PublishResult) (SignRequest, error)
	RejectSignRequest(context.Context, string) (SignRequest, error)
	UpsertPushToken(context.Context, string, string, string) (DeviceState, error)
	ClearPushToken(context.Context, string) (DeviceState, error)
	GetDeviceState(context.Context, string) (DeviceState, error)
	PatchNotificationPreferences(context.Context, string, NotificationPreferences) (DeviceState, error)
	GetIdempotencyRecord(context.Context, string, string) (IdempotencyRecord, bool, error)
	SaveIdempotencyRecord(context.Context, IdempotencyRecord) error
	Close() error
}

const (
	signRequestPending    = "pending"
	signRequestCompleted  = "completed"
	signRequestRejected   = "rejected"
	defaultSignRequestTTL = 15 * time.Minute
	defaultIdempotencyTTL = 24 * time.Hour
)

// MemoryStateStore is useful for tests and dev-mode ESP handlers.
type MemoryStateStore struct {
	mu       sync.Mutex
	requests map[string]SignRequest
	devices  map[string]DeviceState
	idem     map[string]IdempotencyRecord
	clock    func() time.Time
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		requests: make(map[string]SignRequest),
		devices:  make(map[string]DeviceState),
		idem:     make(map[string]IdempotencyRecord),
		clock:    time.Now,
	}
}

func (s *MemoryStateStore) CreateSignRequest(_ context.Context, req SignRequest) (SignRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	if req.ID == "" {
		req.ID = newSignRequestID()
	}
	if req.Kind == "" {
		return SignRequest{}, errors.New("esphttp: sign request kind is required")
	}
	if req.Status == "" {
		req.Status = signRequestPending
	}
	if req.CreatedAtMS == 0 {
		req.CreatedAtMS = now
	}
	req.UpdatedAtMS = now
	if req.ExpiresAtMS == 0 {
		req.ExpiresAtMS = time.UnixMilli(now).Add(defaultSignRequestTTL).UnixMilli()
	}
	if err := ensureSignRequestSigningFields(&req); err != nil {
		return SignRequest{}, err
	}
	s.requests[req.ID] = cloneSignRequest(req)
	return cloneSignRequest(req), nil
}

func (s *MemoryStateStore) ListSignRequests(_ context.Context, deviceID string) ([]SignRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SignRequest, 0, len(s.requests))
	for _, req := range s.requests {
		if deviceID != "" && req.DeviceID != "" && req.DeviceID != deviceID {
			continue
		}
		out = append(out, cloneSignRequest(req))
	}
	return out, nil
}

func (s *MemoryStateStore) GetSignRequest(_ context.Context, id string) (SignRequest, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.requests[id]
	return cloneSignRequest(req), ok, nil
}

func (s *MemoryStateStore) CompleteSignRequest(_ context.Context, id, signature string, publishResult *PublishResult) (SignRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.requests[id]
	if !ok {
		return SignRequest{}, sql.ErrNoRows
	}
	req.Status = signRequestCompleted
	req.Signature = signature
	req.PublishResult = clonePublishResultPtr(publishResult)
	req.UpdatedAtMS = s.nowMS()
	s.requests[id] = req
	return cloneSignRequest(req), nil
}

func (s *MemoryStateStore) RejectSignRequest(_ context.Context, id string) (SignRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.requests[id]
	if !ok {
		return SignRequest{}, sql.ErrNoRows
	}
	req.Status = signRequestRejected
	req.UpdatedAtMS = s.nowMS()
	s.requests[id] = req
	return cloneSignRequest(req), nil
}

func (s *MemoryStateStore) UpsertPushToken(_ context.Context, deviceID, platform, token string) (DeviceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.deviceStateLocked(deviceID)
	state.PushPlatform = platform
	state.PushToken = token
	state.UpdatedAtMS = s.nowMS()
	s.devices[deviceID] = state
	return state, nil
}

func (s *MemoryStateStore) ClearPushToken(_ context.Context, deviceID string) (DeviceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.deviceStateLocked(deviceID)
	state.PushPlatform = ""
	state.PushToken = ""
	state.UpdatedAtMS = s.nowMS()
	s.devices[deviceID] = state
	return state, nil
}

func (s *MemoryStateStore) GetDeviceState(_ context.Context, deviceID string) (DeviceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.deviceStateLocked(deviceID)
	s.devices[deviceID] = state
	return state, nil
}

func (s *MemoryStateStore) PatchNotificationPreferences(_ context.Context, deviceID string, prefs NotificationPreferences) (DeviceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.deviceStateLocked(deviceID)
	state.NotificationPreferences = normalizePrefs(prefs)
	state.UpdatedAtMS = s.nowMS()
	s.devices[deviceID] = state
	return state, nil
}

func (s *MemoryStateStore) GetIdempotencyRecord(_ context.Context, scope, key string) (IdempotencyRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.idem[idempotencyMapKey(scope, key)]
	if !ok || (rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= s.nowMS()) {
		return IdempotencyRecord{}, false, nil
	}
	return cloneIdempotencyRecord(rec), true, nil
}

func (s *MemoryStateStore) SaveIdempotencyRecord(_ context.Context, rec IdempotencyRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	if rec.ExpiresAtMS == 0 {
		rec.ExpiresAtMS = time.UnixMilli(now).Add(defaultIdempotencyTTL).UnixMilli()
	}
	s.idem[idempotencyMapKey(rec.Scope, rec.Key)] = cloneIdempotencyRecord(rec)
	return nil
}

func (s *MemoryStateStore) Close() error {
	return nil
}

func (s *MemoryStateStore) deviceStateLocked(deviceID string) DeviceState {
	state := s.devices[deviceID]
	if state.DeviceID == "" {
		state.DeviceID = deviceID
		state.NotificationPreferences = NotificationPreferences{Enabled: true}
		state.UpdatedAtMS = s.nowMS()
	}
	state.NotificationPreferences = normalizePrefs(state.NotificationPreferences)
	return state
}

func (s *MemoryStateStore) nowMS() int64 {
	if s.clock == nil {
		return time.Now().UnixMilli()
	}
	return s.clock().UnixMilli()
}

// SQLiteStateStore persists ESP-local state in <data>/esp.sqlite.
type SQLiteStateStore struct {
	db *sql.DB
}

const sqliteStateSchema = `
CREATE TABLE IF NOT EXISTS sign_requests (
  id            TEXT PRIMARY KEY,
  device_id     TEXT NOT NULL,
  kind          TEXT NOT NULL,
  status        TEXT NOT NULL,
  group_id      BLOB,
  payload       BLOB NOT NULL,
  canonical_type TEXT NOT NULL DEFAULT '',
  signature_algorithm TEXT NOT NULL DEFAULT '',
  signing_payload TEXT NOT NULL DEFAULT '',
  signing_payload_sha256 TEXT NOT NULL DEFAULT '',
  signature     TEXT NOT NULL DEFAULT '',
  publish_result BLOB,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sign_requests_device
  ON sign_requests(device_id, created_at_ms DESC);

CREATE TABLE IF NOT EXISTS esp_devices_state (
  device_id     TEXT PRIMARY KEY,
  push_platform TEXT NOT NULL DEFAULT '',
  push_token    TEXT NOT NULL DEFAULT '',
  prefs         BLOB NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS esp_idempotency (
  scope         TEXT NOT NULL,
  key           TEXT NOT NULL,
  request_hash  TEXT NOT NULL,
  status_code   INTEGER NOT NULL,
  response      BLOB NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  PRIMARY KEY(scope, key)
);
`

func OpenSQLiteStateStore(dataDir string) (*SQLiteStateStore, error) {
	if dataDir == "" {
		return nil, errors.New("esphttp: state data dir is empty")
	}
	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("esphttp: resolve data dir %q: %w", dataDir, err)
	}
	if err := os.MkdirAll(absDir, 0o700); err != nil {
		return nil, fmt.Errorf("esphttp: mkdir data dir %q: %w", absDir, err)
	}
	dbPath := filepath.Join(absDir, "esp.sqlite")
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	db, err := sql.Open("sqlite", "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("esphttp: open state sqlite: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("esphttp: ping state sqlite: %w", err)
	}
	if _, err := db.Exec(sqliteStateSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("esphttp: apply state schema: %w", err)
	}
	if err := migrateSQLiteState(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLiteStateStore{db: db}, nil
}

func (s *SQLiteStateStore) CreateSignRequest(ctx context.Context, req SignRequest) (SignRequest, error) {
	now := time.Now().UnixMilli()
	if req.ID == "" {
		req.ID = newSignRequestID()
	}
	if req.Kind == "" {
		return SignRequest{}, errors.New("esphttp: sign request kind is required")
	}
	if req.Status == "" {
		req.Status = signRequestPending
	}
	if req.CreatedAtMS == 0 {
		req.CreatedAtMS = now
	}
	req.UpdatedAtMS = now
	if req.ExpiresAtMS == 0 {
		req.ExpiresAtMS = time.UnixMilli(now).Add(defaultSignRequestTTL).UnixMilli()
	}
	if err := ensureSignRequestSigningFields(&req); err != nil {
		return SignRequest{}, err
	}
	var groupBytes []byte
	if req.GroupID != (entmoot.GroupID{}) {
		groupBytes = req.GroupID[:]
	}
	payload := []byte(req.Payload)
	if payload == nil {
		payload = []byte("{}")
	}
	var publishResult []byte
	if req.PublishResult != nil {
		var err error
		publishResult, err = json.Marshal(req.PublishResult)
		if err != nil {
			return SignRequest{}, fmt.Errorf("esphttp: marshal publish result: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO sign_requests
  (id, device_id, kind, status, group_id, payload, canonical_type, signature_algorithm, signing_payload,
   signing_payload_sha256, signature, publish_result, created_at_ms, updated_at_ms, expires_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.ID, req.DeviceID, req.Kind, req.Status, groupBytes, payload, req.CanonicalType,
		req.SignatureAlgorithm, req.SigningPayload, req.SigningPayloadSHA256, req.Signature, publishResult,
		req.CreatedAtMS, req.UpdatedAtMS, req.ExpiresAtMS); err != nil {
		return SignRequest{}, fmt.Errorf("esphttp: create sign request: %w", err)
	}
	return cloneSignRequest(req), nil
}

func (s *SQLiteStateStore) ListSignRequests(ctx context.Context, deviceID string) ([]SignRequest, error) {
	query := `SELECT id, device_id, kind, status, group_id, payload, canonical_type, signature_algorithm, signing_payload,
signing_payload_sha256, signature, publish_result, created_at_ms, updated_at_ms, expires_at_ms
FROM sign_requests`
	args := []interface{}{}
	if deviceID != "" {
		query += ` WHERE device_id = ? OR device_id = ''`
		args = append(args, deviceID)
	}
	query += ` ORDER BY created_at_ms DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list sign requests: %w", err)
	}
	defer rows.Close()
	var out []SignRequest
	for rows.Next() {
		req, err := scanSignRequest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, req)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) GetSignRequest(ctx context.Context, id string) (SignRequest, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, device_id, kind, status, group_id, payload, canonical_type, signature_algorithm, signing_payload,
signing_payload_sha256, signature, publish_result, created_at_ms, updated_at_ms, expires_at_ms
FROM sign_requests WHERE id = ?`, id)
	req, err := scanSignRequest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return SignRequest{}, false, nil
	}
	return req, err == nil, err
}

func (s *SQLiteStateStore) CompleteSignRequest(ctx context.Context, id, signature string, publishResult *PublishResult) (SignRequest, error) {
	var publishResultJSON []byte
	if publishResult != nil {
		var err error
		publishResultJSON, err = json.Marshal(publishResult)
		if err != nil {
			return SignRequest{}, fmt.Errorf("esphttp: marshal publish result: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE sign_requests SET status = ?, signature = ?, publish_result = ?, updated_at_ms = ? WHERE id = ?`,
		signRequestCompleted, signature, publishResultJSON, time.Now().UnixMilli(), id); err != nil {
		return SignRequest{}, fmt.Errorf("esphttp: complete sign request: %w", err)
	}
	req, ok, err := s.GetSignRequest(ctx, id)
	if err != nil {
		return SignRequest{}, err
	}
	if !ok {
		return SignRequest{}, sql.ErrNoRows
	}
	return req, nil
}

func (s *SQLiteStateStore) RejectSignRequest(ctx context.Context, id string) (SignRequest, error) {
	if _, err := s.db.ExecContext(ctx, `UPDATE sign_requests SET status = ?, updated_at_ms = ? WHERE id = ?`,
		signRequestRejected, time.Now().UnixMilli(), id); err != nil {
		return SignRequest{}, fmt.Errorf("esphttp: reject sign request: %w", err)
	}
	req, ok, err := s.GetSignRequest(ctx, id)
	if err != nil {
		return SignRequest{}, err
	}
	if !ok {
		return SignRequest{}, sql.ErrNoRows
	}
	return req, nil
}

func (s *SQLiteStateStore) UpsertPushToken(ctx context.Context, deviceID, platform, token string) (DeviceState, error) {
	state, err := s.GetDeviceState(ctx, deviceID)
	if err != nil {
		return DeviceState{}, err
	}
	state.PushPlatform = platform
	state.PushToken = token
	state.UpdatedAtMS = time.Now().UnixMilli()
	if err := s.saveDeviceState(ctx, state); err != nil {
		return DeviceState{}, err
	}
	return state, nil
}

func (s *SQLiteStateStore) ClearPushToken(ctx context.Context, deviceID string) (DeviceState, error) {
	state, err := s.GetDeviceState(ctx, deviceID)
	if err != nil {
		return DeviceState{}, err
	}
	state.PushPlatform = ""
	state.PushToken = ""
	state.UpdatedAtMS = time.Now().UnixMilli()
	if err := s.saveDeviceState(ctx, state); err != nil {
		return DeviceState{}, err
	}
	return state, nil
}

func (s *SQLiteStateStore) GetDeviceState(ctx context.Context, deviceID string) (DeviceState, error) {
	row := s.db.QueryRowContext(ctx, `SELECT device_id, push_platform, push_token, prefs, updated_at_ms FROM esp_devices_state WHERE device_id = ?`, deviceID)
	var state DeviceState
	var prefs []byte
	if err := row.Scan(&state.DeviceID, &state.PushPlatform, &state.PushToken, &prefs, &state.UpdatedAtMS); errors.Is(err, sql.ErrNoRows) {
		return DeviceState{DeviceID: deviceID, NotificationPreferences: NotificationPreferences{Enabled: true}, UpdatedAtMS: time.Now().UnixMilli()}, nil
	} else if err != nil {
		return DeviceState{}, fmt.Errorf("esphttp: get device state: %w", err)
	}
	if err := json.Unmarshal(prefs, &state.NotificationPreferences); err != nil {
		return DeviceState{}, fmt.Errorf("esphttp: parse notification preferences: %w", err)
	}
	state.NotificationPreferences = normalizePrefs(state.NotificationPreferences)
	return state, nil
}

func (s *SQLiteStateStore) PatchNotificationPreferences(ctx context.Context, deviceID string, prefs NotificationPreferences) (DeviceState, error) {
	state, err := s.GetDeviceState(ctx, deviceID)
	if err != nil {
		return DeviceState{}, err
	}
	state.NotificationPreferences = normalizePrefs(prefs)
	state.UpdatedAtMS = time.Now().UnixMilli()
	if err := s.saveDeviceState(ctx, state); err != nil {
		return DeviceState{}, err
	}
	return state, nil
}

func (s *SQLiteStateStore) GetIdempotencyRecord(ctx context.Context, scope, key string) (IdempotencyRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT scope, key, request_hash, status_code, response, created_at_ms, updated_at_ms, expires_at_ms
FROM esp_idempotency WHERE scope = ? AND key = ? AND expires_at_ms > ?`, scope, key, time.Now().UnixMilli())
	var rec IdempotencyRecord
	var response []byte
	if err := row.Scan(&rec.Scope, &rec.Key, &rec.RequestHash, &rec.StatusCode, &response, &rec.CreatedAtMS, &rec.UpdatedAtMS, &rec.ExpiresAtMS); errors.Is(err, sql.ErrNoRows) {
		return IdempotencyRecord{}, false, nil
	} else if err != nil {
		return IdempotencyRecord{}, false, fmt.Errorf("esphttp: get idempotency record: %w", err)
	}
	rec.Response = append(json.RawMessage(nil), response...)
	return rec, true, nil
}

func (s *SQLiteStateStore) SaveIdempotencyRecord(ctx context.Context, rec IdempotencyRecord) error {
	now := time.Now().UnixMilli()
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	if rec.ExpiresAtMS == 0 {
		rec.ExpiresAtMS = time.UnixMilli(now).Add(defaultIdempotencyTTL).UnixMilli()
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO esp_idempotency (scope, key, request_hash, status_code, response, created_at_ms, updated_at_ms, expires_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(scope, key) DO UPDATE SET
  request_hash = excluded.request_hash,
  status_code = excluded.status_code,
  response = excluded.response,
  updated_at_ms = excluded.updated_at_ms,
  expires_at_ms = excluded.expires_at_ms`,
		rec.Scope, rec.Key, rec.RequestHash, rec.StatusCode, []byte(rec.Response), rec.CreatedAtMS, rec.UpdatedAtMS, rec.ExpiresAtMS); err != nil {
		return fmt.Errorf("esphttp: save idempotency record: %w", err)
	}
	return nil
}

func (s *SQLiteStateStore) Close() error {
	if _, err := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE);"); err != nil {
		return fmt.Errorf("esphttp: state wal_checkpoint: %w", err)
	}
	return s.db.Close()
}

func (s *SQLiteStateStore) saveDeviceState(ctx context.Context, state DeviceState) error {
	prefs, err := json.Marshal(normalizePrefs(state.NotificationPreferences))
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO esp_devices_state (device_id, push_platform, push_token, prefs, updated_at_ms)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(device_id) DO UPDATE SET
  push_platform = excluded.push_platform,
  push_token = excluded.push_token,
  prefs = excluded.prefs,
  updated_at_ms = excluded.updated_at_ms`,
		state.DeviceID, state.PushPlatform, state.PushToken, prefs, state.UpdatedAtMS)
	if err != nil {
		return fmt.Errorf("esphttp: save device state: %w", err)
	}
	return nil
}

func migrateSQLiteState(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(sign_requests)`)
	if err != nil {
		return fmt.Errorf("esphttp: inspect state schema: %w", err)
	}
	defer rows.Close()
	cols := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue interface{}
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("esphttp: scan state schema: %w", err)
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("esphttp: inspect state schema: %w", err)
	}
	for _, stmt := range []struct {
		name string
		sql  string
	}{
		{"canonical_type", `ALTER TABLE sign_requests ADD COLUMN canonical_type TEXT NOT NULL DEFAULT ''`},
		{"signature_algorithm", `ALTER TABLE sign_requests ADD COLUMN signature_algorithm TEXT NOT NULL DEFAULT ''`},
		{"signing_payload", `ALTER TABLE sign_requests ADD COLUMN signing_payload TEXT NOT NULL DEFAULT ''`},
		{"signing_payload_sha256", `ALTER TABLE sign_requests ADD COLUMN signing_payload_sha256 TEXT NOT NULL DEFAULT ''`},
		{"publish_result", `ALTER TABLE sign_requests ADD COLUMN publish_result BLOB`},
	} {
		if cols[stmt.name] {
			continue
		}
		if _, err := db.Exec(stmt.sql); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add %s: %w", stmt.name, err)
		}
	}
	return nil
}

type signRequestScanner interface {
	Scan(...interface{}) error
}

func scanSignRequest(row signRequestScanner) (SignRequest, error) {
	var req SignRequest
	var groupBytes []byte
	var payload []byte
	var publishResult []byte
	if err := row.Scan(&req.ID, &req.DeviceID, &req.Kind, &req.Status, &groupBytes, &payload,
		&req.CanonicalType, &req.SignatureAlgorithm, &req.SigningPayload, &req.SigningPayloadSHA256,
		&req.Signature, &publishResult, &req.CreatedAtMS, &req.UpdatedAtMS, &req.ExpiresAtMS); err != nil {
		return SignRequest{}, err
	}
	if len(groupBytes) == len(req.GroupID) {
		copy(req.GroupID[:], groupBytes)
	}
	req.Payload = append(json.RawMessage(nil), payload...)
	if len(publishResult) > 0 {
		var result PublishResult
		if err := json.Unmarshal(publishResult, &result); err != nil {
			return SignRequest{}, fmt.Errorf("esphttp: parse publish result: %w", err)
		}
		req.PublishResult = &result
	}
	return req, nil
}

func cloneSignRequest(req SignRequest) SignRequest {
	req.Payload = append(json.RawMessage(nil), req.Payload...)
	req.PublishResult = clonePublishResultPtr(req.PublishResult)
	return req
}

func clonePublishResultPtr(in *PublishResult) *PublishResult {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneIdempotencyRecord(rec IdempotencyRecord) IdempotencyRecord {
	rec.Response = append(json.RawMessage(nil), rec.Response...)
	return rec
}

func idempotencyMapKey(scope, key string) string {
	return scope + "\x00" + key
}

func normalizePrefs(prefs NotificationPreferences) NotificationPreferences {
	if prefs.Topics == nil {
		prefs.Topics = []string{}
	}
	return prefs
}

func newSignRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

func deviceIDForRequest(auth authContext) string {
	if auth.device == nil {
		return ""
	}
	return strings.TrimSpace(auth.device.ID)
}
