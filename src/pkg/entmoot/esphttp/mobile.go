package esphttp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
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
	GroupID     entmoot.GroupID        `json:"group_id"`
	Name        string                 `json:"name,omitempty"`
	Description string                 `json:"description,omitempty"`
	Tags        []string               `json:"tags,omitempty"`
	Members     int                    `json:"members,omitempty"`
	RosterHead  entmoot.RosterEntryID  `json:"roster_head,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// MemberSummary is the mobile/API projection for one group member.
type MemberSummary struct {
	NodeID        entmoot.NodeID `json:"node_id"`
	EntmootPubKey string         `json:"entmoot_pubkey"`
	Founder       bool           `json:"founder,omitempty"`
	Hostname      string         `json:"hostname,omitempty"`
}

// GroupCatalog reads local group/roster state for mobile API requests.
type GroupCatalog interface {
	ListGroups(context.Context) ([]GroupSummary, error)
	GetGroup(context.Context, entmoot.GroupID) (GroupSummary, bool, error)
	ListMembers(context.Context, entmoot.GroupID) ([]MemberSummary, error)
}

// GroupMetadataStore persists ESP-local group display metadata.
type GroupMetadataStore interface {
	GetGroupMetadata(context.Context, entmoot.GroupID) (json.RawMessage, bool, error)
	SetGroupMetadata(context.Context, entmoot.GroupID, json.RawMessage) error
	DeleteGroupMetadata(context.Context, entmoot.GroupID) error
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
	OperationResult      json.RawMessage `json:"result,omitempty"`
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

type OpenInviteRecord struct {
	TokenHash      string           `json:"-"`
	GroupID        entmoot.GroupID  `json:"group_id"`
	DeviceID       string           `json:"device_id,omitempty"`
	MaxUses        int              `json:"max_uses"`
	UseCount       int              `json:"use_count"`
	Revoked        bool             `json:"revoked"`
	BootstrapPeers []entmoot.NodeID `json:"bootstrap_peers,omitempty"`
	CreatedAtMS    int64            `json:"created_at_ms"`
	UpdatedAtMS    int64            `json:"updated_at_ms"`
	ExpiresAtMS    int64            `json:"expires_at_ms"`
}

type OpenInviteRedemption struct {
	TokenHash     string          `json:"-"`
	RedeemerKey   string          `json:"redeemer_key"`
	PilotNodeID   entmoot.NodeID  `json:"pilot_node_id"`
	EntmootPubKey string          `json:"entmoot_pubkey"`
	Result        json.RawMessage `json:"result,omitempty"`
	RedeemedAtMS  int64           `json:"redeemed_at_ms"`
}

type OpenInviteChallenge struct {
	ChallengeID    string          `json:"challenge_id"`
	TokenHash      string          `json:"-"`
	GroupID        entmoot.GroupID `json:"group_id"`
	PilotNodeID    entmoot.NodeID  `json:"pilot_node_id"`
	PilotPubKey    string          `json:"pilot_pubkey"`
	EntmootPubKey  string          `json:"entmoot_pubkey"`
	Nonce          string          `json:"nonce"`
	SigningPayload string          `json:"signing_payload"`
	CreatedAtMS    int64           `json:"created_at_ms"`
	ExpiresAtMS    int64           `json:"expires_at_ms"`
	UsedAtMS       int64           `json:"used_at_ms,omitempty"`
}

// StateStore persists ESP-local mobile service state.
type StateStore interface {
	CreateSignRequest(context.Context, SignRequest) (SignRequest, error)
	ListSignRequests(context.Context, string) ([]SignRequest, error)
	GetSignRequest(context.Context, string) (SignRequest, bool, error)
	CompleteSignRequest(context.Context, string, string, *PublishResult, json.RawMessage) (SignRequest, error)
	RejectSignRequest(context.Context, string) (SignRequest, error)
	UpsertPushToken(context.Context, string, string, string) (DeviceState, error)
	ClearPushToken(context.Context, string) (DeviceState, error)
	GetDeviceState(context.Context, string) (DeviceState, error)
	PatchNotificationPreferences(context.Context, string, NotificationPreferences) (DeviceState, error)
	GetIdempotencyRecord(context.Context, string, string) (IdempotencyRecord, bool, error)
	SaveIdempotencyRecord(context.Context, IdempotencyRecord) error
	CreateOpenInvite(context.Context, OpenInviteRecord) (OpenInviteRecord, error)
	GetOpenInviteByTokenHash(context.Context, string) (OpenInviteRecord, bool, error)
	GetOpenInviteRedemption(context.Context, string, string) (OpenInviteRedemption, bool, error)
	RedeemOpenInvite(context.Context, string, OpenInviteRedemption, int64) (OpenInviteRecord, OpenInviteRedemption, bool, error)
	CompleteOpenInviteRedemption(context.Context, string, string, json.RawMessage, int64) error
	ReleaseOpenInviteRedemption(context.Context, string, string, int64) error
	CreateOpenInviteChallenge(context.Context, OpenInviteChallenge) (OpenInviteChallenge, error)
	CreateOrReuseOpenInviteChallenge(context.Context, OpenInviteChallenge, int, int64) (OpenInviteChallenge, error)
	GetOpenInviteChallenge(context.Context, string) (OpenInviteChallenge, bool, error)
	ConsumeOpenInviteChallenge(context.Context, string, int64) (OpenInviteChallenge, error)
	Close() error
}

const (
	signRequestPending    = "pending"
	signRequestCompleted  = "completed"
	signRequestRejected   = "rejected"
	defaultSignRequestTTL = 15 * time.Minute
	defaultIdempotencyTTL = 24 * time.Hour
)

var (
	ErrOpenInviteExpired          = errors.New("esphttp: open invite expired")
	ErrOpenInviteRevoked          = errors.New("esphttp: open invite revoked")
	ErrOpenInviteExhausted        = errors.New("esphttp: open invite exhausted")
	ErrOpenInviteChallengeExpired = errors.New("esphttp: open invite challenge expired")
	ErrOpenInviteChallengeUsed    = errors.New("esphttp: open invite challenge used")
	ErrOpenInviteChallengeLimit   = errors.New("esphttp: open invite challenge limit reached")
)

// MemoryStateStore is useful for tests and dev-mode ESP handlers.
type MemoryStateStore struct {
	mu       sync.Mutex
	requests map[string]SignRequest
	devices  map[string]DeviceState
	idem     map[string]IdempotencyRecord
	groups   map[entmoot.GroupID]json.RawMessage
	invites  map[string]OpenInviteRecord
	redeems  map[string]map[string]OpenInviteRedemption
	chals    map[string]OpenInviteChallenge
	clock    func() time.Time
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		requests: make(map[string]SignRequest),
		devices:  make(map[string]DeviceState),
		idem:     make(map[string]IdempotencyRecord),
		groups:   make(map[entmoot.GroupID]json.RawMessage),
		invites:  make(map[string]OpenInviteRecord),
		redeems:  make(map[string]map[string]OpenInviteRedemption),
		chals:    make(map[string]OpenInviteChallenge),
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

func (s *MemoryStateStore) CompleteSignRequest(_ context.Context, id, signature string, publishResult *PublishResult, operationResult json.RawMessage) (SignRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.requests[id]
	if !ok {
		return SignRequest{}, sql.ErrNoRows
	}
	req.Status = signRequestCompleted
	req.Signature = signature
	req.PublishResult = clonePublishResultPtr(publishResult)
	req.OperationResult = append(json.RawMessage(nil), operationResult...)
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

func (s *MemoryStateStore) CreateOpenInvite(_ context.Context, rec OpenInviteRecord) (OpenInviteRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	if rec.MaxUses <= 0 {
		return OpenInviteRecord{}, errors.New("esphttp: open invite max_uses must be positive")
	}
	s.invites[rec.TokenHash] = cloneOpenInviteRecord(rec)
	if s.redeems[rec.TokenHash] == nil {
		s.redeems[rec.TokenHash] = make(map[string]OpenInviteRedemption)
	}
	return cloneOpenInviteRecord(rec), nil
}

func (s *MemoryStateStore) GetOpenInviteByTokenHash(_ context.Context, tokenHash string) (OpenInviteRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.invites[tokenHash]
	return cloneOpenInviteRecord(rec), ok, nil
}

func (s *MemoryStateStore) GetOpenInviteRedemption(_ context.Context, tokenHash string, redeemerKey string) (OpenInviteRedemption, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	redeemers := s.redeems[tokenHash]
	if redeemers == nil {
		return OpenInviteRedemption{}, false, nil
	}
	redemption, ok := redeemers[redeemerKey]
	return cloneOpenInviteRedemption(redemption), ok, nil
}

func (s *MemoryStateStore) RedeemOpenInvite(_ context.Context, tokenHash string, redemption OpenInviteRedemption, nowMS int64) (OpenInviteRecord, OpenInviteRedemption, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.invites[tokenHash]
	if !ok {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, sql.ErrNoRows
	}
	if rec.Revoked {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, ErrOpenInviteRevoked
	}
	if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, ErrOpenInviteExpired
	}
	redeemers := s.redeems[tokenHash]
	if redeemers == nil {
		redeemers = make(map[string]OpenInviteRedemption)
		s.redeems[tokenHash] = redeemers
	}
	if existing, ok := redeemers[redemption.RedeemerKey]; ok {
		return cloneOpenInviteRecord(rec), cloneOpenInviteRedemption(existing), true, nil
	}
	if rec.MaxUses > 0 && rec.UseCount >= rec.MaxUses {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, ErrOpenInviteExhausted
	}
	rec.UseCount++
	rec.UpdatedAtMS = nowMS
	redemption.TokenHash = tokenHash
	redemption.RedeemedAtMS = nowMS
	redeemers[redemption.RedeemerKey] = redemption
	s.invites[tokenHash] = rec
	return cloneOpenInviteRecord(rec), cloneOpenInviteRedemption(redemption), false, nil
}

func (s *MemoryStateStore) CompleteOpenInviteRedemption(_ context.Context, tokenHash string, redeemerKey string, result json.RawMessage, nowMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	redeemers, ok := s.redeems[tokenHash]
	if !ok {
		return sql.ErrNoRows
	}
	redemption, ok := redeemers[redeemerKey]
	if !ok {
		return sql.ErrNoRows
	}
	redemption.Result = append(json.RawMessage(nil), result...)
	redemption.RedeemedAtMS = nowMS
	redeemers[redeemerKey] = redemption
	if rec, ok := s.invites[tokenHash]; ok {
		rec.UpdatedAtMS = nowMS
		s.invites[tokenHash] = rec
	}
	return nil
}

func (s *MemoryStateStore) ReleaseOpenInviteRedemption(_ context.Context, tokenHash string, redeemerKey string, nowMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	redeemers, ok := s.redeems[tokenHash]
	if !ok {
		return nil
	}
	if _, ok := redeemers[redeemerKey]; !ok {
		return nil
	}
	delete(redeemers, redeemerKey)
	if len(redeemers) == 0 {
		delete(s.redeems, tokenHash)
	}
	rec, ok := s.invites[tokenHash]
	if !ok {
		return nil
	}
	if rec.UseCount > 0 {
		rec.UseCount--
		rec.UpdatedAtMS = nowMS
		s.invites[tokenHash] = rec
	}
	return nil
}

func (s *MemoryStateStore) CreateOpenInviteChallenge(_ context.Context, ch OpenInviteChallenge) (OpenInviteChallenge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	if ch.ChallengeID == "" {
		ch.ChallengeID = newOpenInviteChallengeID()
	}
	if ch.CreatedAtMS == 0 {
		ch.CreatedAtMS = now
	}
	s.chals[ch.ChallengeID] = cloneOpenInviteChallenge(ch)
	return cloneOpenInviteChallenge(ch), nil
}

func (s *MemoryStateStore) CreateOrReuseOpenInviteChallenge(_ context.Context, ch OpenInviteChallenge, activeCap int, nowMS int64) (OpenInviteChallenge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	if nowMS > 0 {
		now = nowMS
	}
	for id, existing := range s.chals {
		if existing.TokenHash == ch.TokenHash && existing.UsedAtMS == 0 && existing.ExpiresAtMS > 0 && existing.ExpiresAtMS <= now {
			delete(s.chals, id)
		}
	}
	for _, existing := range s.chals {
		if openInviteChallengeMatchesRedeemer(existing, ch, now) {
			return cloneOpenInviteChallenge(existing), nil
		}
	}
	active := 0
	for _, existing := range s.chals {
		if existing.TokenHash == ch.TokenHash && existing.UsedAtMS == 0 && (existing.ExpiresAtMS == 0 || existing.ExpiresAtMS > now) {
			active++
		}
	}
	if activeCap > 0 && active >= activeCap {
		return OpenInviteChallenge{}, ErrOpenInviteChallengeLimit
	}
	if ch.ChallengeID == "" {
		ch.ChallengeID = newOpenInviteChallengeID()
	}
	if ch.CreatedAtMS == 0 {
		ch.CreatedAtMS = now
	}
	s.chals[ch.ChallengeID] = cloneOpenInviteChallenge(ch)
	return cloneOpenInviteChallenge(ch), nil
}

func (s *MemoryStateStore) GetOpenInviteChallenge(_ context.Context, challengeID string) (OpenInviteChallenge, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.chals[challengeID]
	return cloneOpenInviteChallenge(ch), ok, nil
}

func (s *MemoryStateStore) ConsumeOpenInviteChallenge(_ context.Context, challengeID string, nowMS int64) (OpenInviteChallenge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.chals[challengeID]
	if !ok {
		return OpenInviteChallenge{}, sql.ErrNoRows
	}
	if ch.UsedAtMS != 0 {
		return OpenInviteChallenge{}, ErrOpenInviteChallengeUsed
	}
	if ch.ExpiresAtMS > 0 && ch.ExpiresAtMS <= nowMS {
		return OpenInviteChallenge{}, ErrOpenInviteChallengeExpired
	}
	ch.UsedAtMS = nowMS
	s.chals[challengeID] = ch
	return cloneOpenInviteChallenge(ch), nil
}

func (s *MemoryStateStore) GetGroupMetadata(_ context.Context, groupID entmoot.GroupID) (json.RawMessage, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.groups[groupID]
	return append(json.RawMessage(nil), meta...), ok, nil
}

func (s *MemoryStateStore) SetGroupMetadata(_ context.Context, groupID entmoot.GroupID, metadata json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	metadata, err := NormalizeGroupMetadata(metadata)
	if err != nil {
		return err
	}
	s.groups[groupID] = append(json.RawMessage(nil), metadata...)
	return nil
}

func (s *MemoryStateStore) DeleteGroupMetadata(_ context.Context, groupID entmoot.GroupID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.groups, groupID)
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
  operation_result BLOB,
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

CREATE TABLE IF NOT EXISTS esp_group_metadata (
  group_id      BLOB PRIMARY KEY,
  metadata      BLOB NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS esp_open_invites (
  token_hash    TEXT PRIMARY KEY,
  group_id      BLOB NOT NULL,
  device_id     TEXT NOT NULL DEFAULT '',
  max_uses      INTEGER NOT NULL,
  use_count     INTEGER NOT NULL DEFAULT 0,
  revoked       INTEGER NOT NULL DEFAULT 0,
  bootstrap_peers BLOB,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS esp_open_invite_redemptions (
  token_hash      TEXT NOT NULL,
  redeemer_key    TEXT NOT NULL,
  pilot_node_id   INTEGER NOT NULL,
  entmoot_pubkey  TEXT NOT NULL,
  result          BLOB,
  redeemed_at_ms  INTEGER NOT NULL,
  PRIMARY KEY(token_hash, redeemer_key)
);

CREATE TABLE IF NOT EXISTS esp_open_invite_challenges (
  challenge_id    TEXT PRIMARY KEY,
  token_hash      TEXT NOT NULL,
  group_id        BLOB NOT NULL,
  pilot_node_id   INTEGER NOT NULL,
  pilot_pubkey    TEXT NOT NULL,
  entmoot_pubkey  TEXT NOT NULL,
  nonce           TEXT NOT NULL,
  signing_payload TEXT NOT NULL,
  created_at_ms   INTEGER NOT NULL,
  expires_at_ms   INTEGER NOT NULL,
  used_at_ms      INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_open_invite_challenges_token_active
  ON esp_open_invite_challenges(token_hash, used_at_ms, expires_at_ms);

CREATE INDEX IF NOT EXISTS idx_open_invite_challenges_redeemer
  ON esp_open_invite_challenges(token_hash, pilot_node_id, pilot_pubkey, entmoot_pubkey, used_at_ms, expires_at_ms);
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
	operationResult := []byte(req.OperationResult)
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO sign_requests
  (id, device_id, kind, status, group_id, payload, canonical_type, signature_algorithm, signing_payload,
   signing_payload_sha256, signature, publish_result, operation_result, created_at_ms, updated_at_ms, expires_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.ID, req.DeviceID, req.Kind, req.Status, groupBytes, payload, req.CanonicalType,
		req.SignatureAlgorithm, req.SigningPayload, req.SigningPayloadSHA256, req.Signature, publishResult, operationResult,
		req.CreatedAtMS, req.UpdatedAtMS, req.ExpiresAtMS); err != nil {
		return SignRequest{}, fmt.Errorf("esphttp: create sign request: %w", err)
	}
	return cloneSignRequest(req), nil
}

func (s *SQLiteStateStore) ListSignRequests(ctx context.Context, deviceID string) ([]SignRequest, error) {
	query := `SELECT id, device_id, kind, status, group_id, payload, canonical_type, signature_algorithm, signing_payload,
signing_payload_sha256, signature, publish_result, operation_result, created_at_ms, updated_at_ms, expires_at_ms
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
signing_payload_sha256, signature, publish_result, operation_result, created_at_ms, updated_at_ms, expires_at_ms
FROM sign_requests WHERE id = ?`, id)
	req, err := scanSignRequest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return SignRequest{}, false, nil
	}
	return req, err == nil, err
}

func (s *SQLiteStateStore) CompleteSignRequest(ctx context.Context, id, signature string, publishResult *PublishResult, operationResult json.RawMessage) (SignRequest, error) {
	var publishResultJSON []byte
	if publishResult != nil {
		var err error
		publishResultJSON, err = json.Marshal(publishResult)
		if err != nil {
			return SignRequest{}, fmt.Errorf("esphttp: marshal publish result: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE sign_requests SET status = ?, signature = ?, publish_result = ?, operation_result = ?, updated_at_ms = ? WHERE id = ?`,
		signRequestCompleted, signature, publishResultJSON, []byte(operationResult), time.Now().UnixMilli(), id); err != nil {
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

func (s *SQLiteStateStore) CreateOpenInvite(ctx context.Context, rec OpenInviteRecord) (OpenInviteRecord, error) {
	now := time.Now().UnixMilli()
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	if rec.MaxUses <= 0 {
		return OpenInviteRecord{}, errors.New("esphttp: open invite max_uses must be positive")
	}
	bootstrapPeers, err := encodeBootstrapPeers(rec.BootstrapPeers)
	if err != nil {
		return OpenInviteRecord{}, err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO esp_open_invites (token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_peers, created_at_ms, updated_at_ms, expires_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.TokenHash, rec.GroupID[:], rec.DeviceID, rec.MaxUses, rec.UseCount, boolInt(rec.Revoked), bootstrapPeers, rec.CreatedAtMS, rec.UpdatedAtMS, rec.ExpiresAtMS)
	if err != nil {
		return OpenInviteRecord{}, fmt.Errorf("esphttp: create open invite: %w", err)
	}
	return cloneOpenInviteRecord(rec), nil
}

func (s *SQLiteStateStore) GetOpenInviteByTokenHash(ctx context.Context, tokenHash string) (OpenInviteRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_peers, created_at_ms, updated_at_ms, expires_at_ms FROM esp_open_invites WHERE token_hash = ?`, tokenHash)
	rec, err := scanOpenInviteRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return OpenInviteRecord{}, false, nil
	}
	if err != nil {
		return OpenInviteRecord{}, false, fmt.Errorf("esphttp: get open invite: %w", err)
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) GetOpenInviteRedemption(ctx context.Context, tokenHash string, redeemerKey string) (OpenInviteRedemption, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT token_hash, redeemer_key, pilot_node_id, entmoot_pubkey, result, redeemed_at_ms FROM esp_open_invite_redemptions WHERE token_hash = ? AND redeemer_key = ?`, tokenHash, redeemerKey)
	redemption, err := scanOpenInviteRedemption(row)
	if errors.Is(err, sql.ErrNoRows) {
		return OpenInviteRedemption{}, false, nil
	}
	if err != nil {
		return OpenInviteRedemption{}, false, fmt.Errorf("esphttp: get open invite redemption: %w", err)
	}
	return redemption, true, nil
}

func (s *SQLiteStateStore) RedeemOpenInvite(ctx context.Context, tokenHash string, redemption OpenInviteRedemption, nowMS int64) (OpenInviteRecord, OpenInviteRedemption, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `SELECT token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_peers, created_at_ms, updated_at_ms, expires_at_ms FROM esp_open_invites WHERE token_hash = ?`, tokenHash)
	rec, err := scanOpenInviteRecord(row)
	if err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	if rec.Revoked {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, ErrOpenInviteRevoked
	}
	if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, ErrOpenInviteExpired
	}
	existing, ok, err := getOpenInviteRedemptionTx(ctx, tx, tokenHash, redemption.RedeemerKey)
	if err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	if ok {
		if err := tx.Commit(); err != nil {
			return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
		}
		return rec, existing, true, nil
	}
	res, err := tx.ExecContext(ctx, `
UPDATE esp_open_invites
SET use_count = use_count + 1, updated_at_ms = ?
WHERE token_hash = ?
  AND revoked = 0
  AND (expires_at_ms = 0 OR expires_at_ms > ?)
  AND (max_uses <= 0 OR use_count < max_uses)`, nowMS, tokenHash, nowMS)
	if err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	if rows == 0 {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, ErrOpenInviteExhausted
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO esp_open_invite_redemptions (token_hash, redeemer_key, pilot_node_id, entmoot_pubkey, redeemed_at_ms) VALUES (?, ?, ?, ?, ?)`,
		tokenHash, redemption.RedeemerKey, redemption.PilotNodeID, redemption.EntmootPubKey, nowMS); err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	row = tx.QueryRowContext(ctx, `SELECT token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_peers, created_at_ms, updated_at_ms, expires_at_ms FROM esp_open_invites WHERE token_hash = ?`, tokenHash)
	rec, err = scanOpenInviteRecord(row)
	if err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	redemption.TokenHash = tokenHash
	redemption.RedeemedAtMS = nowMS
	return rec, cloneOpenInviteRedemption(redemption), false, nil
}

func (s *SQLiteStateStore) CompleteOpenInviteRedemption(ctx context.Context, tokenHash string, redeemerKey string, result json.RawMessage, nowMS int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE esp_open_invite_redemptions SET result = ?, redeemed_at_ms = ? WHERE token_hash = ? AND redeemer_key = ?`, []byte(result), nowMS, tokenHash, redeemerKey)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	_, err = s.db.ExecContext(ctx, `UPDATE esp_open_invites SET updated_at_ms = ? WHERE token_hash = ?`, nowMS, tokenHash)
	return err
}

func (s *SQLiteStateStore) ReleaseOpenInviteRedemption(ctx context.Context, tokenHash string, redeemerKey string, nowMS int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `DELETE FROM esp_open_invite_redemptions WHERE token_hash = ? AND redeemer_key = ?`, tokenHash, redeemerKey)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE esp_open_invites SET use_count = CASE WHEN use_count > 0 THEN use_count - 1 ELSE 0 END, updated_at_ms = ? WHERE token_hash = ?`, nowMS, tokenHash); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLiteStateStore) CreateOpenInviteChallenge(ctx context.Context, ch OpenInviteChallenge) (OpenInviteChallenge, error) {
	now := time.Now().UnixMilli()
	if ch.ChallengeID == "" {
		ch.ChallengeID = newOpenInviteChallengeID()
	}
	if ch.CreatedAtMS == 0 {
		ch.CreatedAtMS = now
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO esp_open_invite_challenges
  (challenge_id, token_hash, group_id, pilot_node_id, pilot_pubkey, entmoot_pubkey, nonce, signing_payload, created_at_ms, expires_at_ms, used_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ch.ChallengeID, ch.TokenHash, ch.GroupID[:], ch.PilotNodeID, ch.PilotPubKey, ch.EntmootPubKey, ch.Nonce, ch.SigningPayload, ch.CreatedAtMS, ch.ExpiresAtMS, ch.UsedAtMS); err != nil {
		return OpenInviteChallenge{}, fmt.Errorf("esphttp: create open invite challenge: %w", err)
	}
	return cloneOpenInviteChallenge(ch), nil
}

func (s *SQLiteStateStore) CreateOrReuseOpenInviteChallenge(ctx context.Context, ch OpenInviteChallenge, activeCap int, nowMS int64) (OpenInviteChallenge, error) {
	now := time.Now().UnixMilli()
	if nowMS > 0 {
		now = nowMS
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OpenInviteChallenge{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM esp_open_invite_challenges WHERE token_hash = ? AND used_at_ms = 0 AND expires_at_ms > 0 AND expires_at_ms <= ?`, ch.TokenHash, now); err != nil {
		return OpenInviteChallenge{}, err
	}
	row := tx.QueryRowContext(ctx, `SELECT challenge_id, token_hash, group_id, pilot_node_id, pilot_pubkey, entmoot_pubkey, nonce, signing_payload, created_at_ms, expires_at_ms, used_at_ms
FROM esp_open_invite_challenges
WHERE token_hash = ? AND pilot_node_id = ? AND pilot_pubkey = ? AND entmoot_pubkey = ? AND used_at_ms = 0 AND (expires_at_ms = 0 OR expires_at_ms > ?)
ORDER BY created_at_ms DESC
LIMIT 1`, ch.TokenHash, ch.PilotNodeID, ch.PilotPubKey, ch.EntmootPubKey, now)
	existing, err := scanOpenInviteChallenge(row)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return OpenInviteChallenge{}, err
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return OpenInviteChallenge{}, err
	}
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM esp_open_invite_challenges WHERE token_hash = ? AND used_at_ms = 0 AND (expires_at_ms = 0 OR expires_at_ms > ?)`, ch.TokenHash, now).Scan(&active); err != nil {
		return OpenInviteChallenge{}, err
	}
	if activeCap > 0 && active >= activeCap {
		return OpenInviteChallenge{}, ErrOpenInviteChallengeLimit
	}
	if ch.ChallengeID == "" {
		ch.ChallengeID = newOpenInviteChallengeID()
	}
	if ch.CreatedAtMS == 0 {
		ch.CreatedAtMS = now
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO esp_open_invite_challenges
  (challenge_id, token_hash, group_id, pilot_node_id, pilot_pubkey, entmoot_pubkey, nonce, signing_payload, created_at_ms, expires_at_ms, used_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ch.ChallengeID, ch.TokenHash, ch.GroupID[:], ch.PilotNodeID, ch.PilotPubKey, ch.EntmootPubKey, ch.Nonce, ch.SigningPayload, ch.CreatedAtMS, ch.ExpiresAtMS, ch.UsedAtMS); err != nil {
		return OpenInviteChallenge{}, fmt.Errorf("esphttp: create open invite challenge: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return OpenInviteChallenge{}, err
	}
	return cloneOpenInviteChallenge(ch), nil
}

func (s *SQLiteStateStore) GetOpenInviteChallenge(ctx context.Context, challengeID string) (OpenInviteChallenge, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT challenge_id, token_hash, group_id, pilot_node_id, pilot_pubkey, entmoot_pubkey, nonce, signing_payload, created_at_ms, expires_at_ms, used_at_ms FROM esp_open_invite_challenges WHERE challenge_id = ?`, challengeID)
	ch, err := scanOpenInviteChallenge(row)
	if errors.Is(err, sql.ErrNoRows) {
		return OpenInviteChallenge{}, false, nil
	}
	if err != nil {
		return OpenInviteChallenge{}, false, fmt.Errorf("esphttp: get open invite challenge: %w", err)
	}
	return ch, true, nil
}

func (s *SQLiteStateStore) ConsumeOpenInviteChallenge(ctx context.Context, challengeID string, nowMS int64) (OpenInviteChallenge, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OpenInviteChallenge{}, err
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `SELECT challenge_id, token_hash, group_id, pilot_node_id, pilot_pubkey, entmoot_pubkey, nonce, signing_payload, created_at_ms, expires_at_ms, used_at_ms FROM esp_open_invite_challenges WHERE challenge_id = ?`, challengeID)
	ch, err := scanOpenInviteChallenge(row)
	if err != nil {
		return OpenInviteChallenge{}, err
	}
	if ch.UsedAtMS != 0 {
		return OpenInviteChallenge{}, ErrOpenInviteChallengeUsed
	}
	if ch.ExpiresAtMS > 0 && ch.ExpiresAtMS <= nowMS {
		return OpenInviteChallenge{}, ErrOpenInviteChallengeExpired
	}
	res, err := tx.ExecContext(ctx, `UPDATE esp_open_invite_challenges SET used_at_ms = ? WHERE challenge_id = ? AND used_at_ms = 0 AND (expires_at_ms = 0 OR expires_at_ms > ?)`, nowMS, challengeID, nowMS)
	if err != nil {
		return OpenInviteChallenge{}, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return OpenInviteChallenge{}, err
	}
	if rows == 0 {
		return OpenInviteChallenge{}, ErrOpenInviteChallengeUsed
	}
	ch.UsedAtMS = nowMS
	if err := tx.Commit(); err != nil {
		return OpenInviteChallenge{}, err
	}
	return ch, nil
}

func (s *SQLiteStateStore) GetGroupMetadata(ctx context.Context, groupID entmoot.GroupID) (json.RawMessage, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT metadata FROM esp_group_metadata WHERE group_id = ?`, groupID[:])
	var metadata []byte
	if err := row.Scan(&metadata); errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, fmt.Errorf("esphttp: get group metadata: %w", err)
	}
	return append(json.RawMessage(nil), metadata...), true, nil
}

func (s *SQLiteStateStore) SetGroupMetadata(ctx context.Context, groupID entmoot.GroupID, metadata json.RawMessage) error {
	metadata, err := NormalizeGroupMetadata(metadata)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO esp_group_metadata (group_id, metadata, updated_at_ms)
VALUES (?, ?, ?)
ON CONFLICT(group_id) DO UPDATE SET
  metadata = excluded.metadata,
  updated_at_ms = excluded.updated_at_ms`,
		groupID[:], []byte(metadata), time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("esphttp: set group metadata: %w", err)
	}
	return nil
}

func (s *SQLiteStateStore) DeleteGroupMetadata(ctx context.Context, groupID entmoot.GroupID) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_group_metadata WHERE group_id = ?`, groupID[:]); err != nil {
		return fmt.Errorf("esphttp: delete group metadata: %w", err)
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
	signRequestCols, err := tableColumns(db, "sign_requests")
	if err != nil {
		return err
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
		{"operation_result", `ALTER TABLE sign_requests ADD COLUMN operation_result BLOB`},
	} {
		if signRequestCols[stmt.name] {
			continue
		}
		if _, err := db.Exec(stmt.sql); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add %s: %w", stmt.name, err)
		}
	}

	openInviteCols, err := tableColumns(db, "esp_open_invites")
	if err != nil {
		return err
	}
	if !openInviteCols["bootstrap_peers"] {
		if _, err := db.Exec(`ALTER TABLE esp_open_invites ADD COLUMN bootstrap_peers BLOB`); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add bootstrap_peers: %w", err)
		}
	}
	redemptionCols, err := tableColumns(db, "esp_open_invite_redemptions")
	if err != nil {
		return err
	}
	if !redemptionCols["result"] {
		if _, err := db.Exec(`ALTER TABLE esp_open_invite_redemptions ADD COLUMN result BLOB`); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add result: %w", err)
		}
	}
	for _, stmt := range []string{
		`CREATE INDEX IF NOT EXISTS idx_open_invite_challenges_token_active ON esp_open_invite_challenges(token_hash, used_at_ms, expires_at_ms)`,
		`CREATE INDEX IF NOT EXISTS idx_open_invite_challenges_redeemer ON esp_open_invite_challenges(token_hash, pilot_node_id, pilot_pubkey, entmoot_pubkey, used_at_ms, expires_at_ms)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add open invite challenge index: %w", err)
		}
	}
	return nil
}

func tableColumns(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return nil, fmt.Errorf("esphttp: inspect state schema %s: %w", table, err)
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
			return nil, fmt.Errorf("esphttp: scan state schema %s: %w", table, err)
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("esphttp: inspect state schema %s: %w", table, err)
	}
	return cols, nil
}

type signRequestScanner interface {
	Scan(...interface{}) error
}

type openInviteScanner interface {
	Scan(...interface{}) error
}

func scanSignRequest(row signRequestScanner) (SignRequest, error) {
	var req SignRequest
	var groupBytes []byte
	var payload []byte
	var publishResult []byte
	var operationResult []byte
	if err := row.Scan(&req.ID, &req.DeviceID, &req.Kind, &req.Status, &groupBytes, &payload,
		&req.CanonicalType, &req.SignatureAlgorithm, &req.SigningPayload, &req.SigningPayloadSHA256,
		&req.Signature, &publishResult, &operationResult, &req.CreatedAtMS, &req.UpdatedAtMS, &req.ExpiresAtMS); err != nil {
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
	req.OperationResult = append(json.RawMessage(nil), operationResult...)
	return req, nil
}

func scanOpenInviteRecord(row openInviteScanner) (OpenInviteRecord, error) {
	var rec OpenInviteRecord
	var groupBytes []byte
	var revoked int
	var bootstrapPeers []byte
	if err := row.Scan(&rec.TokenHash, &groupBytes, &rec.DeviceID, &rec.MaxUses, &rec.UseCount, &revoked, &bootstrapPeers, &rec.CreatedAtMS, &rec.UpdatedAtMS, &rec.ExpiresAtMS); err != nil {
		return OpenInviteRecord{}, err
	}
	if len(groupBytes) == len(rec.GroupID) {
		copy(rec.GroupID[:], groupBytes)
	}
	rec.Revoked = revoked != 0
	if len(bootstrapPeers) > 0 {
		if err := json.Unmarshal(bootstrapPeers, &rec.BootstrapPeers); err != nil {
			return OpenInviteRecord{}, err
		}
	}
	return rec, nil
}

func scanOpenInviteChallenge(row openInviteScanner) (OpenInviteChallenge, error) {
	var ch OpenInviteChallenge
	var groupBytes []byte
	if err := row.Scan(&ch.ChallengeID, &ch.TokenHash, &groupBytes, &ch.PilotNodeID, &ch.PilotPubKey, &ch.EntmootPubKey, &ch.Nonce, &ch.SigningPayload, &ch.CreatedAtMS, &ch.ExpiresAtMS, &ch.UsedAtMS); err != nil {
		return OpenInviteChallenge{}, err
	}
	if len(groupBytes) == len(ch.GroupID) {
		copy(ch.GroupID[:], groupBytes)
	}
	return ch, nil
}

func getOpenInviteRedemptionTx(ctx context.Context, tx *sql.Tx, tokenHash string, redeemerKey string) (OpenInviteRedemption, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT token_hash, redeemer_key, pilot_node_id, entmoot_pubkey, result, redeemed_at_ms FROM esp_open_invite_redemptions WHERE token_hash = ? AND redeemer_key = ?`, tokenHash, redeemerKey)
	red, err := scanOpenInviteRedemption(row)
	if errors.Is(err, sql.ErrNoRows) {
		return OpenInviteRedemption{}, false, nil
	}
	if err != nil {
		return OpenInviteRedemption{}, false, err
	}
	return red, true, nil
}

func scanOpenInviteRedemption(row interface {
	Scan(dest ...any) error
}) (OpenInviteRedemption, error) {
	var red OpenInviteRedemption
	var result []byte
	if err := row.Scan(&red.TokenHash, &red.RedeemerKey, &red.PilotNodeID, &red.EntmootPubKey, &result, &red.RedeemedAtMS); err != nil {
		return OpenInviteRedemption{}, err
	}
	red.Result = append(json.RawMessage(nil), result...)
	return red, nil
}

func cloneSignRequest(req SignRequest) SignRequest {
	req.Payload = append(json.RawMessage(nil), req.Payload...)
	req.PublishResult = clonePublishResultPtr(req.PublishResult)
	req.OperationResult = append(json.RawMessage(nil), req.OperationResult...)
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

func cloneOpenInviteRecord(rec OpenInviteRecord) OpenInviteRecord {
	rec.BootstrapPeers = append([]entmoot.NodeID(nil), rec.BootstrapPeers...)
	return rec
}

func cloneOpenInviteRedemption(red OpenInviteRedemption) OpenInviteRedemption {
	red.Result = append(json.RawMessage(nil), red.Result...)
	return red
}

func cloneOpenInviteChallenge(ch OpenInviteChallenge) OpenInviteChallenge {
	return ch
}

func openInviteChallengeMatchesRedeemer(existing OpenInviteChallenge, want OpenInviteChallenge, nowMS int64) bool {
	return existing.TokenHash == want.TokenHash &&
		existing.PilotNodeID == want.PilotNodeID &&
		existing.PilotPubKey == want.PilotPubKey &&
		existing.EntmootPubKey == want.EntmootPubKey &&
		existing.UsedAtMS == 0 &&
		(existing.ExpiresAtMS == 0 || existing.ExpiresAtMS > nowMS)
}

func encodeBootstrapPeers(peers []entmoot.NodeID) ([]byte, error) {
	if len(peers) == 0 {
		return nil, nil
	}
	return json.Marshal(peers)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
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

func NewOpenInviteToken() (string, string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw[:])
	return token, HashOpenInviteToken(token), nil
}

func HashOpenInviteToken(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func NewOpenInviteChallengeNonce() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func newOpenInviteChallengeID() string {
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
