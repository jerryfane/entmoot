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
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
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
	MemberID       entmoot.MemberID `json:"member_id"`
	PeerID         string           `json:"peer_id"`
	EntmootPubKey  string           `json:"entmoot_pubkey"`
	Founder        bool             `json:"founder,omitempty"`
	Hostname       string           `json:"hostname,omitempty"`
	GlobalHostname string           `json:"global_hostname,omitempty"`
	DisplayName    string           `json:"display_name"`
	Live           *LiveAgentState  `json:"live,omitempty"`
}

// GroupCatalog reads local group/roster state for mobile API requests.
type GroupCatalog interface {
	ListGroups(context.Context) ([]GroupSummary, error)
	GetGroup(context.Context, entmoot.GroupID) (GroupSummary, bool, error)
	ListMembers(context.Context, entmoot.GroupID) ([]MemberSummary, error)
}

type GroupListOptions struct {
	IncludeHidden bool
}

type GroupCatalogWithOptions interface {
	ListGroupsWithOptions(context.Context, GroupListOptions) ([]GroupSummary, error)
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
	TokenHash           string          `json:"-"`
	GroupID             entmoot.GroupID `json:"group_id"`
	DeviceID            string          `json:"device_id,omitempty"`
	MaxUses             int             `json:"max_uses"`
	UseCount            int             `json:"use_count"`
	Revoked             bool            `json:"revoked"`
	BootstrapMultiaddrs []string        `json:"bootstrap_multiaddrs,omitempty"`
	CreatedAtMS         int64           `json:"created_at_ms"`
	UpdatedAtMS         int64           `json:"updated_at_ms"`
	ExpiresAtMS         int64           `json:"expires_at_ms"`
}

type OpenInviteSummary struct {
	ID                  string          `json:"id"`
	GroupID             entmoot.GroupID `json:"group_id"`
	DeviceID            string          `json:"device_id,omitempty"`
	MaxUses             int             `json:"max_uses"`
	UseCount            int             `json:"use_count"`
	Revoked             bool            `json:"revoked"`
	BootstrapMultiaddrs []string        `json:"bootstrap_multiaddrs,omitempty"`
	CreatedAtMS         int64           `json:"created_at_ms"`
	UpdatedAtMS         int64           `json:"updated_at_ms"`
	ExpiresAtMS         int64           `json:"expires_at_ms"`
	Status              string          `json:"status"`
}

func OpenInviteSummaryFromRecord(rec OpenInviteRecord, nowMS int64) OpenInviteSummary {
	status := "active"
	if rec.Revoked {
		status = "revoked"
	} else if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS {
		status = "expired"
	} else if OpenInviteUseLimitReached(rec) {
		status = "exhausted"
	}
	return OpenInviteSummary{
		ID:                  rec.TokenHash,
		GroupID:             rec.GroupID,
		DeviceID:            rec.DeviceID,
		MaxUses:             rec.MaxUses,
		UseCount:            rec.UseCount,
		Revoked:             rec.Revoked,
		BootstrapMultiaddrs: append([]string(nil), rec.BootstrapMultiaddrs...),
		CreatedAtMS:         rec.CreatedAtMS,
		UpdatedAtMS:         rec.UpdatedAtMS,
		ExpiresAtMS:         rec.ExpiresAtMS,
		Status:              status,
	}
}

type OpenInviteRedemption struct {
	TokenHash     string           `json:"-"`
	RedeemerKey   string           `json:"redeemer_key"`
	MemberID      entmoot.MemberID `json:"member_id"`
	PeerID        string           `json:"peer_id"`
	EntmootPubKey string           `json:"entmoot_pubkey"`
	Result        json.RawMessage  `json:"result,omitempty"`
	RedeemedAtMS  int64            `json:"redeemed_at_ms"`
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
	ListOpenInvitesByGroup(context.Context, entmoot.GroupID) ([]OpenInviteRecord, error)
	GetOpenInviteByTokenHash(context.Context, string) (OpenInviteRecord, bool, error)
	RevokeOpenInvite(context.Context, string, int64) (OpenInviteRecord, bool, error)
	GetOpenInviteRedemption(context.Context, string, string) (OpenInviteRedemption, bool, error)
	RedeemOpenInvite(context.Context, string, OpenInviteRedemption, int64) (OpenInviteRecord, OpenInviteRedemption, bool, error)
	CompleteOpenInviteRedemption(context.Context, string, string, json.RawMessage, int64) error
	ReleaseOpenInviteRedemption(context.Context, string, string, int64) error
	UpsertLiveAgentConfig(context.Context, LiveAgentConfig) (LiveAgentConfig, error)
	GetLiveAgentConfig(context.Context, entmoot.GroupID, entmoot.MemberID) (LiveAgentConfig, bool, error)
	ListLiveAgentConfigs(context.Context, entmoot.GroupID) ([]LiveAgentConfig, error)
	ListLiveAgentConfigsForMember(context.Context, entmoot.MemberID) ([]LiveAgentConfig, error)
	DeleteLiveAgentConfig(context.Context, entmoot.GroupID, entmoot.MemberID, int64) error
	UpsertLiveAgentPresence(context.Context, LiveAgentPresence) (LiveAgentPresence, error)
	ListLiveAgentPresence(context.Context, entmoot.GroupID) ([]LiveAgentPresence, error)
	GetLiveAgentCursor(context.Context, entmoot.GroupID, entmoot.MemberID) (LiveAgentCursor, bool, error)
	UpsertLiveAgentCursor(context.Context, LiveAgentCursor) (LiveAgentCursor, error)
	UpsertPublicMoot(context.Context, PublicMootRecord, int64) (PublicMootRecord, bool, error)
	ListPublicMoots(context.Context, PublicMootListFilter) ([]PublicMootRecord, error)
	GetPublicMoot(context.Context, entmoot.GroupID) (PublicMootRecord, bool, error)
	UpdatePublicMootIndexStatus(context.Context, entmoot.GroupID, string, int64) (PublicMootRecord, bool, error)
	UpsertNodeProfile(context.Context, NodeProfileRecord) (NodeProfileRecord, bool, error)
	GetNodeProfile(context.Context, entmoot.MemberID) (NodeProfileRecord, bool, error)
	ListNodeProfiles(context.Context, []entmoot.MemberID) (map[entmoot.MemberID]NodeProfileRecord, error)
	Close() error
}

const (
	signRequestPending             = "pending"
	signRequestCompleted           = "completed"
	signRequestRejected            = "rejected"
	defaultSignRequestTTL          = 15 * time.Minute
	defaultIdempotencyTTL          = 24 * time.Hour
	idempotencyCleanupInterval     = 15 * time.Minute
	idempotencyCleanupBatchSize    = 256
	memoryIdempotencyCleanupChecks = 64

	// OpenInviteUnlimitedMaxUses marks an open invite as unlimited.
	OpenInviteUnlimitedMaxUses = 0
)

var (
	ErrOpenInviteExpired   = errors.New("esphttp: open invite expired")
	ErrOpenInviteRevoked   = errors.New("esphttp: open invite revoked")
	ErrOpenInviteExhausted = errors.New("esphttp: open invite exhausted")
)

// ValidateOpenInviteMaxUses accepts zero as unlimited and rejects negatives.
func ValidateOpenInviteMaxUses(maxUses int) error {
	if maxUses < OpenInviteUnlimitedMaxUses {
		return errors.New("esphttp: open invite max_uses must be non-negative")
	}
	return nil
}

// OpenInviteUseLimitReached reports whether a capped open invite is exhausted.
func OpenInviteUseLimitReached(rec OpenInviteRecord) bool {
	return rec.MaxUses > OpenInviteUnlimitedMaxUses && rec.UseCount >= rec.MaxUses
}

// MemoryStateStore is useful for tests and dev-mode ESP handlers.
type MemoryStateStore struct {
	mu                sync.Mutex
	requests          map[string]SignRequest
	devices           map[string]DeviceState
	idem              map[string]IdempotencyRecord
	groups            map[entmoot.GroupID]json.RawMessage
	invites           map[string]OpenInviteRecord
	redeems           map[string]map[string]OpenInviteRedemption
	liveAgentConfigs  map[entmoot.GroupID]map[entmoot.MemberID]LiveAgentConfig
	liveAgentPresence map[entmoot.GroupID]map[entmoot.MemberID]LiveAgentPresence
	liveAgentCursors  map[entmoot.GroupID]map[entmoot.MemberID]LiveAgentCursor
	publicMoots       map[entmoot.GroupID]PublicMootRecord
	nodeProfiles      map[entmoot.MemberID]map[string]NodeProfileRecord
	clock             func() time.Time
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		requests:          make(map[string]SignRequest),
		devices:           make(map[string]DeviceState),
		idem:              make(map[string]IdempotencyRecord),
		groups:            make(map[entmoot.GroupID]json.RawMessage),
		invites:           make(map[string]OpenInviteRecord),
		redeems:           make(map[string]map[string]OpenInviteRedemption),
		liveAgentConfigs:  make(map[entmoot.GroupID]map[entmoot.MemberID]LiveAgentConfig),
		liveAgentPresence: make(map[entmoot.GroupID]map[entmoot.MemberID]LiveAgentPresence),
		liveAgentCursors:  make(map[entmoot.GroupID]map[entmoot.MemberID]LiveAgentCursor),
		publicMoots:       make(map[entmoot.GroupID]PublicMootRecord),
		nodeProfiles:      make(map[entmoot.MemberID]map[string]NodeProfileRecord),
		clock:             time.Now,
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
	mapKey := idempotencyMapKey(scope, key)
	rec, ok := s.idem[mapKey]
	if !ok {
		return IdempotencyRecord{}, false, nil
	}
	if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= s.nowMS() {
		delete(s.idem, mapKey)
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
	s.cleanupExpiredIdempotencyLocked(now, memoryIdempotencyCleanupChecks)
	s.idem[idempotencyMapKey(rec.Scope, rec.Key)] = cloneIdempotencyRecord(rec)
	return nil
}
func (s *MemoryStateStore) cleanupExpiredIdempotencyLocked(now int64, maxChecks int) {
	checked := 0
	for key, rec := range s.idem {
		if checked >= maxChecks {
			return
		}
		checked++
		if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= now {
			delete(s.idem, key)
		}
	}
}

func (s *MemoryStateStore) CreateOpenInvite(_ context.Context, rec OpenInviteRecord) (OpenInviteRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	if err := ValidateOpenInviteMaxUses(rec.MaxUses); err != nil {
		return OpenInviteRecord{}, err
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

func (s *MemoryStateStore) ListOpenInvitesByGroup(_ context.Context, groupID entmoot.GroupID) ([]OpenInviteRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OpenInviteRecord, 0)
	for _, rec := range s.invites {
		if rec.GroupID == groupID {
			out = append(out, cloneOpenInviteRecord(rec))
		}
	}
	sortOpenInviteRecords(out)
	return out, nil
}

func (s *MemoryStateStore) RevokeOpenInvite(_ context.Context, tokenHash string, nowMS int64) (OpenInviteRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.invites[tokenHash]
	if !ok {
		return OpenInviteRecord{}, false, nil
	}
	rec.Revoked = true
	rec.UpdatedAtMS = nowMS
	s.invites[tokenHash] = rec
	return cloneOpenInviteRecord(rec), true, nil
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
	if OpenInviteUseLimitReached(rec) {
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
	db            *sql.DB
	cleanupCancel context.CancelFunc
	cleanupWG     sync.WaitGroup
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
  bootstrap_multiaddrs BLOB,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS esp_open_invite_redemptions (
  token_hash      TEXT NOT NULL,
  redeemer_key    TEXT NOT NULL,
  member_id       BLOB NOT NULL,
  peer_id         TEXT NOT NULL,
  entmoot_pubkey  TEXT NOT NULL,
  result          BLOB,
  redeemed_at_ms  INTEGER NOT NULL,
  PRIMARY KEY(token_hash, redeemer_key)
);

CREATE TABLE IF NOT EXISTS esp_public_moots (
  group_id             BLOB PRIMARY KEY,
  founder_pubkey       TEXT NOT NULL DEFAULT '',
  descriptor           BLOB,
  descriptor_updated_at_ms INTEGER NOT NULL DEFAULT 0,
  status               TEXT NOT NULL,
  indexed_at_ms        INTEGER NOT NULL,
  status_updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_public_moots_status_updated
  ON esp_public_moots(status, descriptor_updated_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_public_moots_founder_status
  ON esp_public_moots(founder_pubkey, status);

CREATE TABLE IF NOT EXISTS esp_node_profiles (
  member_id BLOB PRIMARY KEY,
  entmoot_pubkey TEXT NOT NULL DEFAULT '',
  hostname TEXT NOT NULL,
  source TEXT NOT NULL,
  confidence INTEGER NOT NULL,
  observed_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0,
  source_group_id BLOB
);

CREATE INDEX IF NOT EXISTS idx_node_profiles_expires
  ON esp_node_profiles(expires_at_ms);

CREATE TABLE IF NOT EXISTS esp_node_profile_sources (
  member_id BLOB NOT NULL,
  entmoot_pubkey TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL,
  source_key TEXT NOT NULL,
  hostname TEXT NOT NULL,
  confidence INTEGER NOT NULL,
  observed_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0,
  source_group_id BLOB,
  PRIMARY KEY(member_id, source_key)
);

CREATE INDEX IF NOT EXISTS idx_node_profile_sources_node
  ON esp_node_profile_sources(member_id);

CREATE INDEX IF NOT EXISTS idx_node_profile_sources_expires
  ON esp_node_profile_sources(expires_at_ms);

CREATE TABLE IF NOT EXISTS esp_live_agent_configs (
  group_id BLOB NOT NULL,
  member_id BLOB NOT NULL,
  enabled INTEGER NOT NULL,
  mode TEXT NOT NULL,
  topic_filters BLOB NOT NULL,
  allowed_actions BLOB NOT NULL,
  max_actions_per_scan INTEGER NOT NULL DEFAULT 0,
  max_action_bytes INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY(group_id, member_id)
);

CREATE INDEX IF NOT EXISTS idx_live_agent_configs_group
  ON esp_live_agent_configs(group_id, enabled, member_id);

CREATE TABLE IF NOT EXISTS esp_live_agent_presence (
  group_id BLOB NOT NULL,
  member_id BLOB NOT NULL,
  status TEXT NOT NULL,
  mode TEXT NOT NULL,
  topic_filters BLOB NOT NULL,
  last_seen_at_ms INTEGER NOT NULL,
  lease_until_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY(group_id, member_id)
);

CREATE INDEX IF NOT EXISTS idx_live_agent_presence_group
  ON esp_live_agent_presence(group_id, lease_until_ms, member_id);

CREATE TABLE IF NOT EXISTS esp_live_agent_cursors (
  group_id BLOB NOT NULL,
  member_id BLOB NOT NULL,
  scan_floor_at_ms INTEGER NOT NULL DEFAULT 0,
  last_seen_at_ms INTEGER NOT NULL,
  last_seen_author_member_id BLOB,
  last_seen_message_id BLOB NOT NULL DEFAULT x'',
  seen_message_ids BLOB NOT NULL DEFAULT '[]',
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY(group_id, member_id)
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
	retireFleetTables(db, dbPath)
	store := &SQLiteStateStore{db: db}
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	store.cleanupCancel = cleanupCancel
	_, _ = store.deleteExpiredIdempotency(cleanupCtx, idempotencyCleanupBatchSize)
	store.cleanupWG.Add(1)
	go store.runIdempotencyCleanup(cleanupCtx)
	return store, nil
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
func (s *SQLiteStateStore) runIdempotencyCleanup(ctx context.Context) {
	defer s.cleanupWG.Done()
	ticker := time.NewTicker(idempotencyCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.deleteExpiredIdempotency(ctx, idempotencyCleanupBatchSize)
		}
	}
}

func (s *SQLiteStateStore) deleteExpiredIdempotency(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	result, err := s.db.ExecContext(ctx, `
DELETE FROM esp_idempotency
WHERE rowid IN (
  SELECT rowid
  FROM esp_idempotency
  WHERE expires_at_ms <= ?
  ORDER BY expires_at_ms
  LIMIT ?
)`, time.Now().UnixMilli(), limit)
	if err != nil {
		return 0, fmt.Errorf("esphttp: delete expired idempotency records: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("esphttp: count deleted idempotency records: %w", err)
	}
	return deleted, nil
}

func (s *SQLiteStateStore) CreateOpenInvite(ctx context.Context, rec OpenInviteRecord) (OpenInviteRecord, error) {
	now := time.Now().UnixMilli()
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	if err := ValidateOpenInviteMaxUses(rec.MaxUses); err != nil {
		return OpenInviteRecord{}, err
	}
	bootstrapMultiaddrs, err := json.Marshal(rec.BootstrapMultiaddrs)
	if err != nil {
		return OpenInviteRecord{}, err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO esp_open_invites (token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_multiaddrs, created_at_ms, updated_at_ms, expires_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.TokenHash, rec.GroupID[:], rec.DeviceID, rec.MaxUses, rec.UseCount, boolInt(rec.Revoked), bootstrapMultiaddrs, rec.CreatedAtMS, rec.UpdatedAtMS, rec.ExpiresAtMS)
	if err != nil {
		return OpenInviteRecord{}, fmt.Errorf("esphttp: create open invite: %w", err)
	}
	return cloneOpenInviteRecord(rec), nil
}

func (s *SQLiteStateStore) GetOpenInviteByTokenHash(ctx context.Context, tokenHash string) (OpenInviteRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_multiaddrs, created_at_ms, updated_at_ms, expires_at_ms FROM esp_open_invites WHERE token_hash = ?`, tokenHash)
	rec, err := scanOpenInviteRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return OpenInviteRecord{}, false, nil
	}
	if err != nil {
		return OpenInviteRecord{}, false, fmt.Errorf("esphttp: get open invite: %w", err)
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) ListOpenInvitesByGroup(ctx context.Context, groupID entmoot.GroupID) ([]OpenInviteRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_multiaddrs, created_at_ms, updated_at_ms, expires_at_ms FROM esp_open_invites WHERE group_id = ? ORDER BY created_at_ms DESC`, groupID[:])
	if err != nil {
		return nil, fmt.Errorf("esphttp: list open invites: %w", err)
	}
	defer rows.Close()
	out := make([]OpenInviteRecord, 0)
	for rows.Next() {
		rec, err := scanOpenInviteRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("esphttp: scan open invite: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("esphttp: list open invites: %w", err)
	}
	return out, nil
}

func (s *SQLiteStateStore) RevokeOpenInvite(ctx context.Context, tokenHash string, nowMS int64) (OpenInviteRecord, bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE esp_open_invites SET revoked = 1, updated_at_ms = ? WHERE token_hash = ?`, nowMS, tokenHash)
	if err != nil {
		return OpenInviteRecord{}, false, fmt.Errorf("esphttp: revoke open invite: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return OpenInviteRecord{}, false, fmt.Errorf("esphttp: revoke open invite: %w", err)
	}
	if rows == 0 {
		return OpenInviteRecord{}, false, nil
	}
	rec, ok, err := s.GetOpenInviteByTokenHash(ctx, tokenHash)
	return rec, ok, err
}

func (s *SQLiteStateStore) GetOpenInviteRedemption(ctx context.Context, tokenHash string, redeemerKey string) (OpenInviteRedemption, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT token_hash, redeemer_key, member_id, peer_id, entmoot_pubkey, result, redeemed_at_ms FROM esp_open_invite_redemptions WHERE token_hash = ? AND redeemer_key = ?`, tokenHash, redeemerKey)
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

	row := tx.QueryRowContext(ctx, `SELECT token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_multiaddrs, created_at_ms, updated_at_ms, expires_at_ms FROM esp_open_invites WHERE token_hash = ?`, tokenHash)
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
  AND (max_uses = 0 OR use_count < max_uses)`, nowMS, tokenHash, nowMS)
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO esp_open_invite_redemptions (token_hash, redeemer_key, member_id, peer_id, entmoot_pubkey, redeemed_at_ms) VALUES (?, ?, ?, ?, ?, ?)`,
		tokenHash, redemption.RedeemerKey, redemption.MemberID[:], redemption.PeerID, redemption.EntmootPubKey, nowMS); err != nil {
		return OpenInviteRecord{}, OpenInviteRedemption{}, false, err
	}
	row = tx.QueryRowContext(ctx, `SELECT token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_multiaddrs, created_at_ms, updated_at_ms, expires_at_ms FROM esp_open_invites WHERE token_hash = ?`, tokenHash)
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
	if s.cleanupCancel != nil {
		s.cleanupCancel()
		s.cleanupWG.Wait()
	}
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
	liveConfigCols, err := tableColumns(db, "esp_live_agent_configs")
	if err != nil {
		return err
	}
	for _, stmt := range []struct {
		name string
		sql  string
	}{
		{"max_actions_per_scan", `ALTER TABLE esp_live_agent_configs ADD COLUMN max_actions_per_scan INTEGER NOT NULL DEFAULT 0`},
		{"max_action_bytes", `ALTER TABLE esp_live_agent_configs ADD COLUMN max_action_bytes INTEGER NOT NULL DEFAULT 0`},
	} {
		if liveConfigCols[stmt.name] {
			continue
		}
		if _, err := db.Exec(stmt.sql); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add live config %s: %w", stmt.name, err)
		}
	}
	liveCursorCols, err := tableColumns(db, "esp_live_agent_cursors")
	if err != nil {
		return err
	}
	for _, stmt := range []struct {
		name string
		sql  string
	}{
		{"scan_floor_at_ms", `ALTER TABLE esp_live_agent_cursors ADD COLUMN scan_floor_at_ms INTEGER NOT NULL DEFAULT 0`},
		{"last_seen_author_member_id", `ALTER TABLE esp_live_agent_cursors ADD COLUMN last_seen_author_member_id BLOB`},
		{"last_seen_message_id", `ALTER TABLE esp_live_agent_cursors ADD COLUMN last_seen_message_id BLOB NOT NULL DEFAULT x''`},
		{"seen_message_ids", `ALTER TABLE esp_live_agent_cursors ADD COLUMN seen_message_ids BLOB NOT NULL DEFAULT '[]'`},
	} {
		if liveCursorCols[stmt.name] {
			continue
		}
		if _, err := db.Exec(stmt.sql); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add live cursor %s: %w", stmt.name, err)
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
	var bootstrapMultiaddrs []byte
	if err := row.Scan(&rec.TokenHash, &groupBytes, &rec.DeviceID, &rec.MaxUses, &rec.UseCount, &revoked, &bootstrapMultiaddrs, &rec.CreatedAtMS, &rec.UpdatedAtMS, &rec.ExpiresAtMS); err != nil {
		return OpenInviteRecord{}, err
	}
	if len(groupBytes) == len(rec.GroupID) {
		copy(rec.GroupID[:], groupBytes)
	}
	rec.Revoked = revoked != 0
	if len(bootstrapMultiaddrs) > 0 {
		if err := json.Unmarshal(bootstrapMultiaddrs, &rec.BootstrapMultiaddrs); err != nil {
			return OpenInviteRecord{}, err
		}
	}
	return rec, nil
}

func getOpenInviteRedemptionTx(ctx context.Context, tx *sql.Tx, tokenHash string, redeemerKey string) (OpenInviteRedemption, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT token_hash, redeemer_key, member_id, peer_id, entmoot_pubkey, result, redeemed_at_ms FROM esp_open_invite_redemptions WHERE token_hash = ? AND redeemer_key = ?`, tokenHash, redeemerKey)
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
	var memberID, result []byte
	if err := row.Scan(&red.TokenHash, &red.RedeemerKey, &memberID, &red.PeerID, &red.EntmootPubKey, &result, &red.RedeemedAtMS); err != nil {
		return OpenInviteRedemption{}, err
	}
	if len(memberID) != len(red.MemberID) {
		return OpenInviteRedemption{}, errors.New("esphttp: invalid redemption member id")
	}
	copy(red.MemberID[:], memberID)
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
	rec.BootstrapMultiaddrs = append([]string(nil), rec.BootstrapMultiaddrs...)
	return rec
}

func cloneOpenInviteRedemption(red OpenInviteRedemption) OpenInviteRedemption {
	red.Result = append(json.RawMessage(nil), red.Result...)
	return red
}

func sortOpenInviteRecords(records []OpenInviteRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAtMS == records[j].CreatedAtMS {
			return records[i].TokenHash < records[j].TokenHash
		}
		return records[i].CreatedAtMS > records[j].CreatedAtMS
	})
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

func deviceIDForRequest(auth authContext) string {
	if auth.device == nil {
		return ""
	}
	return strings.TrimSpace(auth.device.ID)
}

// retiredFleetTables are the tables of the removed Fleet/tasks/agent-commands
// feature. They are dropped rather than left in place so an upgraded node stops
// carrying rows nothing can read, and so a later reader cannot mistake stale
// fleet state for something live.
var retiredFleetTables = []string{
	"esp_fleet_command_results",
	"esp_fleet_commands",
	"esp_fleet_task_submissions",
	"esp_fleet_tasks",
	"esp_fleet_activity",
	"esp_fleet_invites",
	"esp_fleet_members",
	"esp_fleets",
	"esp_agent_commands",
}

// retireFleetTables drops the removed feature's tables, best-effort.
//
// DROP TABLE needs a write transaction while the schema block above needs
// none, so this runs outside it and never fails the open: this fleet runs
// `serve` and `esp serve` against one data root, and an unrelated writer
// holding the lock must not stop the ESP starting. busy_timeout is lowered for
// the attempt and restored afterwards so a contended open is not stalled for
// the path's full 5s; whatever is left is retired on a later open.
func retireFleetTables(db *sql.DB, dbPath string) {
	if _, err := db.Exec(`PRAGMA busy_timeout = 200`); err != nil {
		slog.Debug("esphttp: retire fleet tables deferred: set busy_timeout",
			slog.String("path", dbPath), slog.String("err", err.Error()))
		return
	}
	defer func() {
		if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
			slog.Warn("esphttp: restore busy_timeout after fleet retirement",
				slog.String("path", dbPath), slog.String("err", err.Error()))
		}
	}()
	for _, table := range retiredFleetTables {
		if _, err := db.Exec(`DROP TABLE IF EXISTS ` + table); err != nil {
			slog.Debug("esphttp: retire fleet table deferred to a later open",
				slog.String("path", dbPath), slog.String("table", table), slog.String("err", err.Error()))
			return
		}
	}
}
