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

type OpenInviteSummary struct {
	ID             string           `json:"id"`
	GroupID        entmoot.GroupID  `json:"group_id"`
	DeviceID       string           `json:"device_id,omitempty"`
	MaxUses        int              `json:"max_uses"`
	UseCount       int              `json:"use_count"`
	Revoked        bool             `json:"revoked"`
	BootstrapPeers []entmoot.NodeID `json:"bootstrap_peers,omitempty"`
	CreatedAtMS    int64            `json:"created_at_ms"`
	UpdatedAtMS    int64            `json:"updated_at_ms"`
	ExpiresAtMS    int64            `json:"expires_at_ms"`
	Status         string           `json:"status"`
}

func OpenInviteSummaryFromRecord(rec OpenInviteRecord, nowMS int64) OpenInviteSummary {
	status := "active"
	if rec.Revoked {
		status = "revoked"
	} else if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS {
		status = "expired"
	} else if rec.MaxUses > 0 && rec.UseCount >= rec.MaxUses {
		status = "exhausted"
	}
	return OpenInviteSummary{
		ID:             rec.TokenHash,
		GroupID:        rec.GroupID,
		DeviceID:       rec.DeviceID,
		MaxUses:        rec.MaxUses,
		UseCount:       rec.UseCount,
		Revoked:        rec.Revoked,
		BootstrapPeers: append([]entmoot.NodeID(nil), rec.BootstrapPeers...),
		CreatedAtMS:    rec.CreatedAtMS,
		UpdatedAtMS:    rec.UpdatedAtMS,
		ExpiresAtMS:    rec.ExpiresAtMS,
		Status:         status,
	}
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
	ListOpenInvitesByGroup(context.Context, entmoot.GroupID) ([]OpenInviteRecord, error)
	GetOpenInviteByTokenHash(context.Context, string) (OpenInviteRecord, bool, error)
	RevokeOpenInvite(context.Context, string, int64) (OpenInviteRecord, bool, error)
	GetOpenInviteRedemption(context.Context, string, string) (OpenInviteRedemption, bool, error)
	RedeemOpenInvite(context.Context, string, OpenInviteRedemption, int64) (OpenInviteRecord, OpenInviteRedemption, bool, error)
	CompleteOpenInviteRedemption(context.Context, string, string, json.RawMessage, int64) error
	ReleaseOpenInviteRedemption(context.Context, string, string, int64) error
	CreateOpenInviteChallenge(context.Context, OpenInviteChallenge) (OpenInviteChallenge, error)
	CreateOrReuseOpenInviteChallenge(context.Context, OpenInviteChallenge, int, int64) (OpenInviteChallenge, error)
	GetOpenInviteChallenge(context.Context, string) (OpenInviteChallenge, bool, error)
	ConsumeOpenInviteChallenge(context.Context, string, int64) (OpenInviteChallenge, error)
	CreateFleet(context.Context, FleetRecord) (FleetRecord, error)
	ListFleets(context.Context) ([]FleetRecord, error)
	GetFleet(context.Context, string) (FleetRecord, bool, error)
	GetFleetByControlGroup(context.Context, entmoot.GroupID) (FleetRecord, bool, error)
	ArchiveFleet(context.Context, string, int64) (FleetRecord, bool, error)
	RestoreFleet(context.Context, string, int64) (FleetRecord, bool, error)
	DeleteFleet(context.Context, string) error
	UpsertFleetMember(context.Context, FleetMemberRecord) (FleetMemberRecord, error)
	UpsertFleetMemberForActiveFleet(context.Context, FleetMemberRecord) (FleetMemberRecord, error)
	ReconcileFleetInviteAcceptance(context.Context, string, entmoot.NodeID, string, int64, string) (FleetMemberRecord, FleetActivityRecord, bool, error)
	ListFleetMembers(context.Context, string) ([]FleetMemberRecord, error)
	DeleteFleetMember(context.Context, string, entmoot.NodeID) error
	CreateFleetInvite(context.Context, FleetInviteRecord) (FleetInviteRecord, error)
	CreateFleetInviteForActiveFleet(context.Context, FleetInviteRecord) (FleetInviteRecord, error)
	ListFleetInvites(context.Context, string) ([]FleetInviteRecord, error)
	DeleteFleetInvite(context.Context, string) error
	AppendFleetActivity(context.Context, FleetActivityRecord) (FleetActivityRecord, error)
	ListFleetActivity(context.Context, string, int, int64) ([]FleetActivityRecord, error)
	DeleteFleetActivity(context.Context, string, string) error
	UpsertFleetTask(context.Context, FleetTaskRecord) (FleetTaskRecord, error)
	UpdateFleetTaskIfCurrent(context.Context, FleetTaskRecord, int64) (FleetTaskRecord, bool, error)
	ClaimFleetTask(context.Context, FleetTaskRecord) (FleetTaskRecord, bool, error)
	SubmitFleetTask(context.Context, FleetTaskRecord, int64, FleetTaskSubmissionRecord) (FleetTaskRecord, FleetTaskSubmissionRecord, bool, error)
	GetFleetTask(context.Context, string, string) (FleetTaskRecord, bool, error)
	ListFleetTasks(context.Context, string, string) ([]FleetTaskRecord, error)
	DeleteFleetTask(context.Context, string, string) error
	UpsertFleetTaskSubmission(context.Context, FleetTaskSubmissionRecord) (FleetTaskSubmissionRecord, error)
	GetFleetTaskSubmission(context.Context, string, string, string) (FleetTaskSubmissionRecord, bool, error)
	ListFleetTaskSubmissions(context.Context, string, string) ([]FleetTaskSubmissionRecord, error)
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
	ErrFleetNotActive             = errors.New("esphttp: fleet is not active")
)

// MemoryStateStore is useful for tests and dev-mode ESP handlers.
type MemoryStateStore struct {
	mu            sync.Mutex
	requests      map[string]SignRequest
	devices       map[string]DeviceState
	idem          map[string]IdempotencyRecord
	groups        map[entmoot.GroupID]json.RawMessage
	invites       map[string]OpenInviteRecord
	redeems       map[string]map[string]OpenInviteRedemption
	chals         map[string]OpenInviteChallenge
	fleets        map[string]FleetRecord
	fleetMembers  map[string]map[entmoot.NodeID]FleetMemberRecord
	fleetInvites  map[string][]FleetInviteRecord
	fleetActivity map[string][]FleetActivityRecord
	fleetTasks    map[string]map[string]FleetTaskRecord
	fleetTaskSubs map[string]map[string][]FleetTaskSubmissionRecord
	clock         func() time.Time
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		requests:      make(map[string]SignRequest),
		devices:       make(map[string]DeviceState),
		idem:          make(map[string]IdempotencyRecord),
		groups:        make(map[entmoot.GroupID]json.RawMessage),
		invites:       make(map[string]OpenInviteRecord),
		redeems:       make(map[string]map[string]OpenInviteRedemption),
		chals:         make(map[string]OpenInviteChallenge),
		fleets:        make(map[string]FleetRecord),
		fleetMembers:  make(map[string]map[entmoot.NodeID]FleetMemberRecord),
		fleetInvites:  make(map[string][]FleetInviteRecord),
		fleetActivity: make(map[string][]FleetActivityRecord),
		fleetTasks:    make(map[string]map[string]FleetTaskRecord),
		fleetTaskSubs: make(map[string]map[string][]FleetTaskSubmissionRecord),
		clock:         time.Now,
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

func (s *MemoryStateStore) CreateFleet(_ context.Context, rec FleetRecord) (FleetRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	if rec.FleetID == "" {
		var err error
		rec.FleetID, err = NewFleetID()
		if err != nil {
			return FleetRecord{}, err
		}
	}
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.Status = NormalizeFleetStatus(rec.Status)
	rec.UpdatedAtMS = now
	s.fleets[rec.FleetID] = cloneFleetRecord(rec)
	return cloneFleetRecord(rec), nil
}

func (s *MemoryStateStore) ListFleets(_ context.Context) ([]FleetRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]FleetRecord, 0, len(s.fleets))
	for _, rec := range s.fleets {
		out = append(out, cloneFleetRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAtMS == out[j].CreatedAtMS {
			return out[i].FleetID < out[j].FleetID
		}
		return out[i].CreatedAtMS > out[j].CreatedAtMS
	})
	return out, nil
}

func (s *MemoryStateStore) GetFleet(_ context.Context, fleetID string) (FleetRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.fleets[fleetID]
	return cloneFleetRecord(rec), ok, nil
}

func (s *MemoryStateStore) GetFleetByControlGroup(_ context.Context, groupID entmoot.GroupID) (FleetRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.fleets {
		if rec.ControlGroupID == groupID {
			return cloneFleetRecord(rec), true, nil
		}
	}
	return FleetRecord{}, false, nil
}

func (s *MemoryStateStore) ArchiveFleet(_ context.Context, fleetID string, archivedAtMS int64) (FleetRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.fleets[fleetID]
	if !ok {
		return FleetRecord{}, false, nil
	}
	if archivedAtMS == 0 {
		archivedAtMS = s.nowMS()
	}
	if rec.Status != FleetStatusArchived {
		rec.Status = FleetStatusArchived
		rec.ArchivedAtMS = archivedAtMS
		rec.UpdatedAtMS = archivedAtMS
		s.fleets[fleetID] = cloneFleetRecord(rec)
	}
	delete(s.fleetInvites, fleetID)
	return cloneFleetRecord(rec), true, nil
}

func (s *MemoryStateStore) RestoreFleet(_ context.Context, fleetID string, restoredAtMS int64) (FleetRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.fleets[fleetID]
	if !ok {
		return FleetRecord{}, false, nil
	}
	if restoredAtMS == 0 {
		restoredAtMS = s.nowMS()
	}
	if rec.Status != FleetStatusActive {
		rec.Status = FleetStatusActive
		rec.ArchivedAtMS = 0
		rec.DeletedAtMS = 0
		rec.UpdatedAtMS = restoredAtMS
		s.fleets[fleetID] = cloneFleetRecord(rec)
	}
	return cloneFleetRecord(rec), true, nil
}

func (s *MemoryStateStore) DeleteFleet(_ context.Context, fleetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.fleets, fleetID)
	delete(s.fleetMembers, fleetID)
	delete(s.fleetInvites, fleetID)
	delete(s.fleetActivity, fleetID)
	delete(s.fleetTasks, fleetID)
	delete(s.fleetTaskSubs, fleetID)
	return nil
}

func (s *MemoryStateStore) UpsertFleetMember(_ context.Context, rec FleetMemberRecord) (FleetMemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsertFleetMemberLocked(rec)
}

func (s *MemoryStateStore) UpsertFleetMemberForActiveFleet(_ context.Context, rec FleetMemberRecord) (FleetMemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fleetActiveLocked(rec.FleetID) {
		return FleetMemberRecord{}, ErrFleetNotActive
	}
	return s.upsertFleetMemberLocked(rec)
}

func (s *MemoryStateStore) ReconcileFleetInviteAcceptance(_ context.Context, fleetID string, nodeID entmoot.NodeID, entmootPubKey string, acceptedAtMS int64, hostname string) (FleetMemberRecord, FleetActivityRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entmootPubKey = strings.TrimSpace(entmootPubKey)
	if !s.fleetActiveLocked(fleetID) {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, nil
	}
	members := s.fleetMembers[fleetID]
	member, ok := members[nodeID]
	if !ok || member.Status != FleetMemberInvited || member.Role == FleetRoleCoordinator || member.EntmootPubKey != entmootPubKey {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, nil
	}
	if acceptedAtMS == 0 {
		acceptedAtMS = s.nowMS()
	}
	hasInvite := false
	invites := s.fleetInvites[fleetID]
	remainingInvites := invites[:0]
	for _, invite := range invites {
		if invite.NodeID == nodeID && invite.EntmootPubKey == entmootPubKey && invite.Status == FleetMemberInvited {
			hasInvite = true
			continue
		}
		remainingInvites = append(remainingInvites, invite)
	}
	if !hasInvite {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, nil
	}
	if strings.TrimSpace(member.Hostname) == "" {
		member.Hostname = strings.TrimSpace(hostname)
	}
	member.Status = FleetMemberActive
	member.AcceptedAtMS = acceptedAtMS
	member.RemovedAtMS = 0
	member.UpdatedAtMS = s.nowMS()
	activity, err := fleetAcceptanceActivityFromMember(member, acceptedAtMS)
	if err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, err
	}
	members[nodeID] = cloneFleetMemberRecord(member)
	if len(remainingInvites) == 0 {
		delete(s.fleetInvites, fleetID)
	} else {
		s.fleetInvites[fleetID] = append([]FleetInviteRecord(nil), remainingInvites...)
	}
	s.fleetActivity[fleetID] = append(s.fleetActivity[fleetID], cloneFleetActivityRecord(activity))
	return cloneFleetMemberRecord(member), cloneFleetActivityRecord(activity), true, nil
}

func (s *MemoryStateStore) fleetActiveLocked(fleetID string) bool {
	fleet, ok := s.fleets[fleetID]
	return ok && NormalizeFleetStatus(fleet.Status) == FleetStatusActive
}

func (s *MemoryStateStore) upsertFleetMemberLocked(rec FleetMemberRecord) (FleetMemberRecord, error) {
	if s.fleetMembers[rec.FleetID] == nil {
		s.fleetMembers[rec.FleetID] = make(map[entmoot.NodeID]FleetMemberRecord)
	}
	rec.Role = NormalizeFleetMemberRole(rec.Role)
	rec.Status = NormalizeFleetMemberStatus(rec.Status)
	rec.UpdatedAtMS = s.nowMS()
	s.fleetMembers[rec.FleetID][rec.NodeID] = cloneFleetMemberRecord(rec)
	return cloneFleetMemberRecord(rec), nil
}

func (s *MemoryStateStore) ListFleetMembers(_ context.Context, fleetID string) ([]FleetMemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	members := s.fleetMembers[fleetID]
	out := make([]FleetMemberRecord, 0, len(members))
	for _, rec := range members {
		out = append(out, cloneFleetMemberRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Role != out[j].Role {
			return out[i].Role == FleetRoleCoordinator
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out, nil
}

func (s *MemoryStateStore) DeleteFleetMember(_ context.Context, fleetID string, nodeID entmoot.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if members := s.fleetMembers[fleetID]; members != nil {
		delete(members, nodeID)
		if len(members) == 0 {
			delete(s.fleetMembers, fleetID)
		}
	}
	return nil
}

func (s *MemoryStateStore) CreateFleetInvite(_ context.Context, rec FleetInviteRecord) (FleetInviteRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createFleetInviteLocked(rec)
}

func (s *MemoryStateStore) CreateFleetInviteForActiveFleet(_ context.Context, rec FleetInviteRecord) (FleetInviteRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fleetActiveLocked(rec.FleetID) {
		return FleetInviteRecord{}, ErrFleetNotActive
	}
	return s.createFleetInviteLocked(rec)
}

func (s *MemoryStateStore) createFleetInviteLocked(rec FleetInviteRecord) (FleetInviteRecord, error) {
	now := s.nowMS()
	if rec.InviteID == "" {
		var err error
		rec.InviteID, err = NewFleetInviteID()
		if err != nil {
			return FleetInviteRecord{}, err
		}
	}
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	rec.Status = strings.TrimSpace(rec.Status)
	if rec.Status == "" {
		rec.Status = FleetMemberInvited
	}
	s.fleetInvites[rec.FleetID] = append(s.fleetInvites[rec.FleetID], cloneFleetInviteRecord(rec))
	return cloneFleetInviteRecord(rec), nil
}

func (s *MemoryStateStore) ListFleetInvites(_ context.Context, fleetID string) ([]FleetInviteRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]FleetInviteRecord(nil), s.fleetInvites[fleetID]...)
	for i := range out {
		out[i] = cloneFleetInviteRecord(out[i])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAtMS == out[j].CreatedAtMS {
			return out[i].InviteID < out[j].InviteID
		}
		return out[i].CreatedAtMS > out[j].CreatedAtMS
	})
	return out, nil
}

func (s *MemoryStateStore) DeleteFleetInvite(_ context.Context, inviteID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for fleetID, invites := range s.fleetInvites {
		dst := invites[:0]
		for _, invite := range invites {
			if invite.InviteID == inviteID {
				continue
			}
			dst = append(dst, invite)
		}
		if len(dst) == 0 {
			delete(s.fleetInvites, fleetID)
			continue
		}
		s.fleetInvites[fleetID] = append([]FleetInviteRecord(nil), dst...)
	}
	return nil
}

func (s *MemoryStateStore) AppendFleetActivity(_ context.Context, rec FleetActivityRecord) (FleetActivityRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec.EventID == "" {
		var err error
		rec.EventID, err = NewFleetActivityID()
		if err != nil {
			return FleetActivityRecord{}, err
		}
	}
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = s.nowMS()
	}
	s.fleetActivity[rec.FleetID] = append(s.fleetActivity[rec.FleetID], cloneFleetActivityRecord(rec))
	return cloneFleetActivityRecord(rec), nil
}

func (s *MemoryStateStore) ListFleetActivity(_ context.Context, fleetID string, limit int, beforeMS int64) ([]FleetActivityRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var out []FleetActivityRecord
	events := s.fleetActivity[fleetID]
	for i := len(events) - 1; i >= 0 && len(out) < limit; i-- {
		ev := events[i]
		if beforeMS > 0 && ev.CreatedAtMS >= beforeMS {
			continue
		}
		out = append(out, cloneFleetActivityRecord(ev))
	}
	return out, nil
}

func (s *MemoryStateStore) DeleteFleetActivity(_ context.Context, fleetID string, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if events := s.fleetActivity[fleetID]; events != nil {
		dst := events[:0]
		for _, event := range events {
			if event.EventID == eventID {
				continue
			}
			dst = append(dst, event)
		}
		if len(dst) == 0 {
			delete(s.fleetActivity, fleetID)
			return nil
		}
		s.fleetActivity[fleetID] = append([]FleetActivityRecord(nil), dst...)
	}
	return nil
}

func (s *MemoryStateStore) UpsertFleetTask(_ context.Context, rec FleetTaskRecord) (FleetTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	var err error
	rec, err = normalizeFleetTaskRecord(rec, now)
	if err != nil {
		return FleetTaskRecord{}, err
	}
	if s.fleetTasks[rec.FleetID] == nil {
		s.fleetTasks[rec.FleetID] = make(map[string]FleetTaskRecord)
	}
	s.fleetTasks[rec.FleetID][rec.TaskID] = cloneFleetTaskRecord(rec)
	return cloneFleetTaskRecord(rec), nil
}

func (s *MemoryStateStore) ClaimFleetTask(_ context.Context, rec FleetTaskRecord) (FleetTaskRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	var err error
	rec, err = normalizeFleetTaskRecord(rec, now)
	if err != nil {
		return FleetTaskRecord{}, false, err
	}
	current, ok := s.fleetTasks[rec.FleetID][rec.TaskID]
	if !ok || !fleetTaskCanBeClaimed(current) {
		return FleetTaskRecord{}, false, nil
	}
	if s.fleetTasks[rec.FleetID] == nil {
		s.fleetTasks[rec.FleetID] = make(map[string]FleetTaskRecord)
	}
	s.fleetTasks[rec.FleetID][rec.TaskID] = cloneFleetTaskRecord(rec)
	return cloneFleetTaskRecord(rec), true, nil
}

func (s *MemoryStateStore) UpdateFleetTaskIfCurrent(_ context.Context, rec FleetTaskRecord, expectedUpdatedAtMS int64) (FleetTaskRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	var err error
	rec, err = normalizeFleetTaskRecord(rec, now)
	if err != nil {
		return FleetTaskRecord{}, false, err
	}
	current, ok := s.fleetTasks[rec.FleetID][rec.TaskID]
	if !ok || current.UpdatedAtMS != expectedUpdatedAtMS {
		return FleetTaskRecord{}, false, nil
	}
	if s.fleetTasks[rec.FleetID] == nil {
		s.fleetTasks[rec.FleetID] = make(map[string]FleetTaskRecord)
	}
	s.fleetTasks[rec.FleetID][rec.TaskID] = cloneFleetTaskRecord(rec)
	return cloneFleetTaskRecord(rec), true, nil
}

func (s *MemoryStateStore) SubmitFleetTask(_ context.Context, rec FleetTaskRecord, expectedUpdatedAtMS int64, submission FleetTaskSubmissionRecord) (FleetTaskRecord, FleetTaskSubmissionRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	var err error
	rec, err = normalizeFleetTaskRecord(rec, now)
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
	}
	submission, err = normalizeFleetTaskSubmissionRecord(submission, now)
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
	}
	current, ok := s.fleetTasks[rec.FleetID][rec.TaskID]
	if rec.Mode == FleetTaskModeOpenSubmission {
		if !ok || current.Mode != FleetTaskModeOpenSubmission || !fleetTaskCanAcceptSubmission(current) {
			return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, nil
		}
		stored, err := s.upsertFleetTaskSubmissionLocked(submission)
		if err != nil {
			return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
		}
		return cloneFleetTaskRecord(current), stored, true, nil
	}
	if !ok || current.UpdatedAtMS != expectedUpdatedAtMS {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, nil
	}
	if s.fleetTasks[rec.FleetID] == nil {
		s.fleetTasks[rec.FleetID] = make(map[string]FleetTaskRecord)
	}
	s.fleetTasks[rec.FleetID][rec.TaskID] = cloneFleetTaskRecord(rec)
	stored, err := s.upsertFleetTaskSubmissionLocked(submission)
	if err != nil {
		return FleetTaskRecord{}, FleetTaskSubmissionRecord{}, false, err
	}
	return cloneFleetTaskRecord(rec), stored, true, nil
}

func (s *MemoryStateStore) GetFleetTask(_ context.Context, fleetID, taskID string) (FleetTaskRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.fleetTasks[fleetID][taskID]
	return cloneFleetTaskRecord(rec), ok, nil
}

func (s *MemoryStateStore) ListFleetTasks(_ context.Context, fleetID, status string) ([]FleetTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]FleetTaskRecord, 0, len(s.fleetTasks[fleetID]))
	status = strings.TrimSpace(status)
	for _, rec := range s.fleetTasks[fleetID] {
		if status != "" && rec.Status != status {
			continue
		}
		out = append(out, cloneFleetTaskRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAtMS == out[j].UpdatedAtMS {
			return out[i].TaskID < out[j].TaskID
		}
		return out[i].UpdatedAtMS > out[j].UpdatedAtMS
	})
	return out, nil
}

func (s *MemoryStateStore) DeleteFleetTask(_ context.Context, fleetID, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tasks := s.fleetTasks[fleetID]; tasks != nil {
		delete(tasks, taskID)
		if len(tasks) == 0 {
			delete(s.fleetTasks, fleetID)
		}
	}
	if subs := s.fleetTaskSubs[fleetID]; subs != nil {
		delete(subs, taskID)
		if len(subs) == 0 {
			delete(s.fleetTaskSubs, fleetID)
		}
	}
	return nil
}

func (s *MemoryStateStore) UpsertFleetTaskSubmission(_ context.Context, rec FleetTaskSubmissionRecord) (FleetTaskSubmissionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowMS()
	var err error
	rec, err = normalizeFleetTaskSubmissionRecord(rec, now)
	if err != nil {
		return FleetTaskSubmissionRecord{}, err
	}
	return s.upsertFleetTaskSubmissionLocked(rec)
}

func (s *MemoryStateStore) upsertFleetTaskSubmissionLocked(rec FleetTaskSubmissionRecord) (FleetTaskSubmissionRecord, error) {
	if s.fleetTaskSubs[rec.FleetID] == nil {
		s.fleetTaskSubs[rec.FleetID] = make(map[string][]FleetTaskSubmissionRecord)
	}
	submissions := s.fleetTaskSubs[rec.FleetID][rec.TaskID]
	replaced := false
	for i := range submissions {
		if submissions[i].SubmissionID == rec.SubmissionID {
			submissions[i] = cloneFleetTaskSubmissionRecord(rec)
			replaced = true
			break
		}
	}
	if !replaced {
		submissions = append(submissions, cloneFleetTaskSubmissionRecord(rec))
	}
	s.fleetTaskSubs[rec.FleetID][rec.TaskID] = submissions
	return cloneFleetTaskSubmissionRecord(rec), nil
}

func (s *MemoryStateStore) GetFleetTaskSubmission(_ context.Context, fleetID, taskID, submissionID string) (FleetTaskSubmissionRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.fleetTaskSubs[fleetID][taskID] {
		if rec.SubmissionID == submissionID {
			return cloneFleetTaskSubmissionRecord(rec), true, nil
		}
	}
	return FleetTaskSubmissionRecord{}, false, nil
}

func (s *MemoryStateStore) ListFleetTaskSubmissions(_ context.Context, fleetID, taskID string) ([]FleetTaskSubmissionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]FleetTaskSubmissionRecord(nil), s.fleetTaskSubs[fleetID][taskID]...)
	for i := range out {
		out[i] = cloneFleetTaskSubmissionRecord(out[i])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAtMS == out[j].CreatedAtMS {
			return out[i].SubmissionID < out[j].SubmissionID
		}
		return out[i].CreatedAtMS > out[j].CreatedAtMS
	})
	return out, nil
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

CREATE TABLE IF NOT EXISTS esp_fleets (
  fleet_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  control_group_id BLOB,
  coordinator_node_id INTEGER NOT NULL,
  coordinator_pubkey TEXT NOT NULL,
  coordinator_device_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  archived_at_ms INTEGER NOT NULL DEFAULT 0,
  deleted_at_ms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS esp_fleet_members (
  fleet_id TEXT NOT NULL,
  node_id INTEGER NOT NULL,
  entmoot_pubkey TEXT NOT NULL,
  hostname TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL,
  status TEXT NOT NULL,
  invited_at_ms INTEGER NOT NULL DEFAULT 0,
  accepted_at_ms INTEGER NOT NULL DEFAULT 0,
  removed_at_ms INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY(fleet_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_fleet_members_status
  ON esp_fleet_members(fleet_id, status, role, node_id);

CREATE TABLE IF NOT EXISTS esp_fleet_invites (
  invite_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  node_id INTEGER NOT NULL,
  entmoot_pubkey TEXT NOT NULL,
  hostname TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  invite BLOB,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_fleet_invites_fleet
  ON esp_fleet_invites(fleet_id, created_at_ms DESC);

CREATE TABLE IF NOT EXISTS esp_fleet_activity (
  event_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  type TEXT NOT NULL,
  actor_node_id INTEGER NOT NULL,
  actor_pubkey TEXT NOT NULL,
  subject_node_id INTEGER NOT NULL DEFAULT 0,
  subject_pubkey TEXT NOT NULL DEFAULT '',
  summary TEXT NOT NULL DEFAULT '',
  metadata BLOB,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fleet_activity_fleet_time
  ON esp_fleet_activity(fleet_id, created_at_ms DESC, event_id);

CREATE TABLE IF NOT EXISTS esp_fleet_tasks (
  task_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  creator_node_id INTEGER NOT NULL,
  creator_pubkey TEXT NOT NULL,
  assignee_node_id INTEGER NOT NULL DEFAULT 0,
  assignee_pubkey TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  completed_at_ms INTEGER NOT NULL DEFAULT 0,
  rejected_at_ms INTEGER NOT NULL DEFAULT 0,
  canceled_at_ms INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_fleet_tasks_fleet_status
  ON esp_fleet_tasks(fleet_id, status, updated_at_ms DESC);

CREATE TABLE IF NOT EXISTS esp_fleet_task_submissions (
  submission_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  author_node_id INTEGER NOT NULL,
  author_pubkey TEXT NOT NULL,
  content TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fleet_task_submissions_task
  ON esp_fleet_task_submissions(fleet_id, task_id, created_at_ms DESC);

CREATE TABLE IF NOT EXISTS esp_agent_commands (
  command_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  control_group_id BLOB NOT NULL,
  issuer_node_id INTEGER NOT NULL,
  agent_node_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  target BLOB NOT NULL,
  instruction TEXT NOT NULL,
  context BLOB,
  args BLOB,
  command BLOB NOT NULL,
  payload BLOB NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT NOT NULL DEFAULT '',
  lease_until_ms INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0,
  received_at_ms INTEGER NOT NULL,
  started_at_ms INTEGER NOT NULL DEFAULT 0,
  completed_at_ms INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL,
  result BLOB,
  last_error TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_agent_commands_status
  ON esp_agent_commands(status, received_at_ms);

CREATE INDEX IF NOT EXISTS idx_agent_commands_lease
  ON esp_agent_commands(status, lease_until_ms);
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

func (s *SQLiteStateStore) ListOpenInvitesByGroup(ctx context.Context, groupID entmoot.GroupID) ([]OpenInviteRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT token_hash, group_id, device_id, max_uses, use_count, revoked, bootstrap_peers, created_at_ms, updated_at_ms, expires_at_ms FROM esp_open_invites WHERE group_id = ? ORDER BY created_at_ms DESC`, groupID[:])
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

func (s *SQLiteStateStore) CreateFleet(ctx context.Context, rec FleetRecord) (FleetRecord, error) {
	now := time.Now().UnixMilli()
	if rec.FleetID == "" {
		var err error
		rec.FleetID, err = NewFleetID()
		if err != nil {
			return FleetRecord{}, err
		}
	}
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	var controlGroup []byte
	if rec.ControlGroupID != (entmoot.GroupID{}) {
		controlGroup = rec.ControlGroupID[:]
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO esp_fleets
  (fleet_id, name, control_group_id, coordinator_node_id, coordinator_pubkey, coordinator_device_id, status, created_at_ms, updated_at_ms, archived_at_ms, deleted_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.FleetID, rec.Name, controlGroup, rec.Coordinator.PilotNodeID, base64.StdEncoding.EncodeToString(rec.Coordinator.EntmootPubKey),
		rec.CoordinatorDeviceID, NormalizeFleetStatus(rec.Status), rec.CreatedAtMS, rec.UpdatedAtMS, rec.ArchivedAtMS, rec.DeletedAtMS); err != nil {
		return FleetRecord{}, fmt.Errorf("esphttp: create fleet: %w", err)
	}
	rec.Status = NormalizeFleetStatus(rec.Status)
	return cloneFleetRecord(rec), nil
}

func (s *SQLiteStateStore) ListFleets(ctx context.Context) ([]FleetRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT fleet_id, name, control_group_id, coordinator_node_id, coordinator_pubkey, coordinator_device_id, status, created_at_ms, updated_at_ms, archived_at_ms, deleted_at_ms FROM esp_fleets ORDER BY created_at_ms DESC`)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list fleets: %w", err)
	}
	defer rows.Close()
	var out []FleetRecord
	for rows.Next() {
		rec, err := scanFleetRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) GetFleet(ctx context.Context, fleetID string) (FleetRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT fleet_id, name, control_group_id, coordinator_node_id, coordinator_pubkey, coordinator_device_id, status, created_at_ms, updated_at_ms, archived_at_ms, deleted_at_ms FROM esp_fleets WHERE fleet_id = ?`, fleetID)
	rec, err := scanFleetRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetRecord{}, false, nil
	}
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: get fleet: %w", err)
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) GetFleetByControlGroup(ctx context.Context, groupID entmoot.GroupID) (FleetRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT fleet_id, name, control_group_id, coordinator_node_id, coordinator_pubkey, coordinator_device_id, status, created_at_ms, updated_at_ms, archived_at_ms, deleted_at_ms FROM esp_fleets WHERE control_group_id = ?`, groupID[:])
	rec, err := scanFleetRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetRecord{}, false, nil
	}
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: get fleet by control group: %w", err)
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) ArchiveFleet(ctx context.Context, fleetID string, archivedAtMS int64) (FleetRecord, bool, error) {
	if archivedAtMS == 0 {
		archivedAtMS = time.Now().UnixMilli()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: archive fleet begin: %w", err)
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE esp_fleets SET status = ?, archived_at_ms = CASE WHEN archived_at_ms = 0 THEN ? ELSE archived_at_ms END, updated_at_ms = ? WHERE fleet_id = ? AND status != ?`,
		FleetStatusArchived, archivedAtMS, archivedAtMS, fleetID, FleetStatusArchived)
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: archive fleet: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: archive fleet affected rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM esp_fleet_invites WHERE fleet_id = ?`, fleetID); err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: archive fleet invites: %w", err)
	}
	row := tx.QueryRowContext(ctx, `SELECT fleet_id, name, control_group_id, coordinator_node_id, coordinator_pubkey, coordinator_device_id, status, created_at_ms, updated_at_ms, archived_at_ms, deleted_at_ms FROM esp_fleets WHERE fleet_id = ?`, fleetID)
	rec, err := scanFleetRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetRecord{}, false, nil
	}
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: get archived fleet: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: archive fleet commit: %w", err)
	}
	if affected == 0 && rec.Status != FleetStatusArchived {
		return FleetRecord{}, false, nil
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) RestoreFleet(ctx context.Context, fleetID string, restoredAtMS int64) (FleetRecord, bool, error) {
	if restoredAtMS == 0 {
		restoredAtMS = time.Now().UnixMilli()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: restore fleet begin: %w", err)
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE esp_fleets SET status = ?, archived_at_ms = 0, deleted_at_ms = 0, updated_at_ms = ? WHERE fleet_id = ? AND status != ?`,
		FleetStatusActive, restoredAtMS, fleetID, FleetStatusActive)
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: restore fleet: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: restore fleet affected rows: %w", err)
	}
	row := tx.QueryRowContext(ctx, `SELECT fleet_id, name, control_group_id, coordinator_node_id, coordinator_pubkey, coordinator_device_id, status, created_at_ms, updated_at_ms, archived_at_ms, deleted_at_ms FROM esp_fleets WHERE fleet_id = ?`, fleetID)
	rec, err := scanFleetRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetRecord{}, false, nil
	}
	if err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: get restored fleet: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return FleetRecord{}, false, fmt.Errorf("esphttp: restore fleet commit: %w", err)
	}
	if affected == 0 && rec.Status != FleetStatusActive {
		return FleetRecord{}, false, nil
	}
	return rec, true, nil
}

func (s *SQLiteStateStore) DeleteFleet(ctx context.Context, fleetID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_task_submissions WHERE fleet_id = ?`, fleetID); err != nil {
		return fmt.Errorf("esphttp: delete fleet task submissions: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_tasks WHERE fleet_id = ?`, fleetID); err != nil {
		return fmt.Errorf("esphttp: delete fleet tasks: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_activity WHERE fleet_id = ?`, fleetID); err != nil {
		return fmt.Errorf("esphttp: delete fleet activity: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_invites WHERE fleet_id = ?`, fleetID); err != nil {
		return fmt.Errorf("esphttp: delete fleet invites: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_members WHERE fleet_id = ?`, fleetID); err != nil {
		return fmt.Errorf("esphttp: delete fleet members: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleets WHERE fleet_id = ?`, fleetID); err != nil {
		return fmt.Errorf("esphttp: delete fleet: %w", err)
	}
	return nil
}

func (s *SQLiteStateStore) UpsertFleetMember(ctx context.Context, rec FleetMemberRecord) (FleetMemberRecord, error) {
	return s.upsertFleetMember(ctx, rec, false)
}

func (s *SQLiteStateStore) UpsertFleetMemberForActiveFleet(ctx context.Context, rec FleetMemberRecord) (FleetMemberRecord, error) {
	return s.upsertFleetMember(ctx, rec, true)
}

func (s *SQLiteStateStore) ReconcileFleetInviteAcceptance(ctx context.Context, fleetID string, nodeID entmoot.NodeID, entmootPubKey string, acceptedAtMS int64, hostname string) (FleetMemberRecord, FleetActivityRecord, bool, error) {
	entmootPubKey = strings.TrimSpace(entmootPubKey)
	if acceptedAtMS == 0 {
		acceptedAtMS = time.Now().UnixMilli()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, fmt.Errorf("esphttp: reconcile fleet invite acceptance begin: %w", err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `
SELECT m.fleet_id, m.node_id, m.entmoot_pubkey, m.hostname, m.role, m.status, m.invited_at_ms, m.accepted_at_ms, m.removed_at_ms, m.updated_at_ms
FROM esp_fleet_members m
JOIN esp_fleets f ON f.fleet_id = m.fleet_id
WHERE m.fleet_id = ? AND m.node_id = ? AND f.status = ?`, fleetID, nodeID, FleetStatusActive)
	member, err := scanFleetMemberRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, nil
	}
	if err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, fmt.Errorf("esphttp: reconcile fleet invite acceptance member: %w", err)
	}
	if member.Status != FleetMemberInvited || member.Role == FleetRoleCoordinator || member.EntmootPubKey != entmootPubKey {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, nil
	}
	var inviteCount int
	if err := tx.QueryRowContext(ctx, `
	SELECT COUNT(1)
	FROM esp_fleet_invites
	WHERE fleet_id = ? AND node_id = ? AND entmoot_pubkey = ? AND status = ?`,
		fleetID, nodeID, entmootPubKey, FleetMemberInvited).Scan(&inviteCount); err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, fmt.Errorf("esphttp: reconcile fleet invite acceptance invites: %w", err)
	}
	if inviteCount == 0 {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, nil
	}
	if strings.TrimSpace(member.Hostname) == "" {
		member.Hostname = strings.TrimSpace(hostname)
	}
	member.Status = FleetMemberActive
	member.AcceptedAtMS = acceptedAtMS
	member.RemovedAtMS = 0
	member.UpdatedAtMS = time.Now().UnixMilli()
	activity, err := fleetAcceptanceActivityFromMember(member, acceptedAtMS)
	if err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, err
	}
	res, err := tx.ExecContext(ctx, `
UPDATE esp_fleet_members
SET hostname = ?, status = ?, accepted_at_ms = ?, removed_at_ms = 0, updated_at_ms = ?
WHERE fleet_id = ? AND node_id = ? AND entmoot_pubkey = ? AND status = ? AND role != ?
  AND EXISTS (SELECT 1 FROM esp_fleets WHERE fleet_id = ? AND status = ?)
	  AND EXISTS (
	    SELECT 1 FROM esp_fleet_invites
	    WHERE fleet_id = ? AND node_id = ? AND entmoot_pubkey = ? AND status = ?
	  )`,
		member.Hostname, member.Status, member.AcceptedAtMS, member.UpdatedAtMS,
		fleetID, nodeID, entmootPubKey, FleetMemberInvited, FleetRoleCoordinator,
		fleetID, FleetStatusActive,
		fleetID, nodeID, entmootPubKey, FleetMemberInvited)
	if err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, fmt.Errorf("esphttp: reconcile fleet invite acceptance update member: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, fmt.Errorf("esphttp: reconcile fleet invite acceptance affected rows: %w", err)
	}
	if affected == 0 {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, nil
	}
	if _, err := tx.ExecContext(ctx, `
	DELETE FROM esp_fleet_invites
	WHERE fleet_id = ? AND node_id = ? AND entmoot_pubkey = ? AND status = ?`,
		fleetID, nodeID, entmootPubKey, FleetMemberInvited); err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, fmt.Errorf("esphttp: reconcile fleet invite acceptance delete invites: %w", err)
	}
	if err := insertFleetActivity(ctx, tx, activity); err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return FleetMemberRecord{}, FleetActivityRecord{}, false, fmt.Errorf("esphttp: reconcile fleet invite acceptance commit: %w", err)
	}
	return cloneFleetMemberRecord(member), cloneFleetActivityRecord(activity), true, nil
}

func (s *SQLiteStateStore) upsertFleetMember(ctx context.Context, rec FleetMemberRecord, requireActive bool) (FleetMemberRecord, error) {
	rec.Role = NormalizeFleetMemberRole(rec.Role)
	rec.Status = NormalizeFleetMemberStatus(rec.Status)
	rec.UpdatedAtMS = time.Now().UnixMilli()
	query := `
INSERT INTO esp_fleet_members
  (fleet_id, node_id, entmoot_pubkey, hostname, role, status, invited_at_ms, accepted_at_ms, removed_at_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fleet_id, node_id) DO UPDATE SET
  entmoot_pubkey = excluded.entmoot_pubkey,
  hostname = excluded.hostname,
  role = excluded.role,
  status = excluded.status,
  invited_at_ms = excluded.invited_at_ms,
  accepted_at_ms = excluded.accepted_at_ms,
  removed_at_ms = excluded.removed_at_ms,
  updated_at_ms = excluded.updated_at_ms`
	args := []any{rec.FleetID, rec.NodeID, rec.EntmootPubKey, rec.Hostname, rec.Role, rec.Status, rec.InvitedAtMS, rec.AcceptedAtMS, rec.RemovedAtMS, rec.UpdatedAtMS}
	if requireActive {
		query = `
INSERT INTO esp_fleet_members
  (fleet_id, node_id, entmoot_pubkey, hostname, role, status, invited_at_ms, accepted_at_ms, removed_at_ms, updated_at_ms)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
WHERE EXISTS (SELECT 1 FROM esp_fleets WHERE fleet_id = ? AND status = ?)
ON CONFLICT(fleet_id, node_id) DO UPDATE SET
  entmoot_pubkey = excluded.entmoot_pubkey,
  hostname = excluded.hostname,
  role = excluded.role,
  status = excluded.status,
  invited_at_ms = excluded.invited_at_ms,
  accepted_at_ms = excluded.accepted_at_ms,
  removed_at_ms = excluded.removed_at_ms,
  updated_at_ms = excluded.updated_at_ms`
		args = append(args, rec.FleetID, FleetStatusActive)
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return FleetMemberRecord{}, fmt.Errorf("esphttp: upsert fleet member: %w", err)
	}
	if requireActive {
		affected, err := res.RowsAffected()
		if err != nil {
			return FleetMemberRecord{}, fmt.Errorf("esphttp: upsert fleet member affected rows: %w", err)
		}
		if affected == 0 {
			return FleetMemberRecord{}, ErrFleetNotActive
		}
	}
	return cloneFleetMemberRecord(rec), nil
}

func (s *SQLiteStateStore) ListFleetMembers(ctx context.Context, fleetID string) ([]FleetMemberRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT fleet_id, node_id, entmoot_pubkey, hostname, role, status, invited_at_ms, accepted_at_ms, removed_at_ms, updated_at_ms FROM esp_fleet_members WHERE fleet_id = ? ORDER BY role = 'coordinator' DESC, node_id ASC`, fleetID)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list fleet members: %w", err)
	}
	defer rows.Close()
	var out []FleetMemberRecord
	for rows.Next() {
		rec, err := scanFleetMemberRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) DeleteFleetMember(ctx context.Context, fleetID string, nodeID entmoot.NodeID) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_members WHERE fleet_id = ? AND node_id = ?`, fleetID, nodeID); err != nil {
		return fmt.Errorf("esphttp: delete fleet member: %w", err)
	}
	return nil
}

func (s *SQLiteStateStore) CreateFleetInvite(ctx context.Context, rec FleetInviteRecord) (FleetInviteRecord, error) {
	return s.createFleetInvite(ctx, rec, false)
}

func (s *SQLiteStateStore) CreateFleetInviteForActiveFleet(ctx context.Context, rec FleetInviteRecord) (FleetInviteRecord, error) {
	return s.createFleetInvite(ctx, rec, true)
}

func (s *SQLiteStateStore) createFleetInvite(ctx context.Context, rec FleetInviteRecord, requireActive bool) (FleetInviteRecord, error) {
	now := time.Now().UnixMilli()
	if rec.InviteID == "" {
		var err error
		rec.InviteID, err = NewFleetInviteID()
		if err != nil {
			return FleetInviteRecord{}, err
		}
	}
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = now
	}
	rec.UpdatedAtMS = now
	if strings.TrimSpace(rec.Status) == "" {
		rec.Status = FleetMemberInvited
	}
	query := `
INSERT INTO esp_fleet_invites
  (invite_id, fleet_id, node_id, entmoot_pubkey, hostname, status, invite, created_at_ms, updated_at_ms, expires_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	args := []any{rec.InviteID, rec.FleetID, rec.NodeID, rec.EntmootPubKey, rec.Hostname, rec.Status, []byte(rec.Invite), rec.CreatedAtMS, rec.UpdatedAtMS, rec.ExpiresAtMS}
	if requireActive {
		query = `
INSERT INTO esp_fleet_invites
  (invite_id, fleet_id, node_id, entmoot_pubkey, hostname, status, invite, created_at_ms, updated_at_ms, expires_at_ms)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
WHERE EXISTS (SELECT 1 FROM esp_fleets WHERE fleet_id = ? AND status = ?)`
		args = append(args, rec.FleetID, FleetStatusActive)
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return FleetInviteRecord{}, fmt.Errorf("esphttp: create fleet invite: %w", err)
	}
	if requireActive {
		affected, err := res.RowsAffected()
		if err != nil {
			return FleetInviteRecord{}, fmt.Errorf("esphttp: create fleet invite affected rows: %w", err)
		}
		if affected == 0 {
			return FleetInviteRecord{}, ErrFleetNotActive
		}
	}
	return cloneFleetInviteRecord(rec), nil
}

func (s *SQLiteStateStore) ListFleetInvites(ctx context.Context, fleetID string) ([]FleetInviteRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT invite_id, fleet_id, node_id, entmoot_pubkey, hostname, status, invite, created_at_ms, updated_at_ms, expires_at_ms FROM esp_fleet_invites WHERE fleet_id = ? ORDER BY created_at_ms DESC`, fleetID)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list fleet invites: %w", err)
	}
	defer rows.Close()
	var out []FleetInviteRecord
	for rows.Next() {
		rec, err := scanFleetInviteRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) DeleteFleetInvite(ctx context.Context, inviteID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_invites WHERE invite_id = ?`, inviteID); err != nil {
		return fmt.Errorf("esphttp: delete fleet invite: %w", err)
	}
	return nil
}

func (s *SQLiteStateStore) AppendFleetActivity(ctx context.Context, rec FleetActivityRecord) (FleetActivityRecord, error) {
	if rec.EventID == "" {
		var err error
		rec.EventID, err = NewFleetActivityID()
		if err != nil {
			return FleetActivityRecord{}, err
		}
	}
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = time.Now().UnixMilli()
	}
	if err := insertFleetActivity(ctx, s.db, rec); err != nil {
		return FleetActivityRecord{}, err
	}
	return cloneFleetActivityRecord(rec), nil
}

type fleetActivityExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertFleetActivity(ctx context.Context, execer fleetActivityExecer, rec FleetActivityRecord) error {
	var subjectNode entmoot.NodeID
	var subjectPub string
	if rec.Subject != nil {
		subjectNode = rec.Subject.PilotNodeID
		subjectPub = base64.StdEncoding.EncodeToString(rec.Subject.EntmootPubKey)
	}
	_, err := execer.ExecContext(ctx, `
INSERT OR IGNORE INTO esp_fleet_activity
  (event_id, fleet_id, type, actor_node_id, actor_pubkey, subject_node_id, subject_pubkey, summary, metadata, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.EventID, rec.FleetID, rec.Type, rec.Actor.PilotNodeID, base64.StdEncoding.EncodeToString(rec.Actor.EntmootPubKey),
		subjectNode, subjectPub, rec.Summary, []byte(rec.Metadata), rec.CreatedAtMS)
	if err != nil {
		return fmt.Errorf("esphttp: append fleet activity: %w", err)
	}
	return nil
}

func (s *SQLiteStateStore) ListFleetActivity(ctx context.Context, fleetID string, limit int, beforeMS int64) ([]FleetActivityRecord, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := `SELECT event_id, fleet_id, type, actor_node_id, actor_pubkey, subject_node_id, subject_pubkey, summary, metadata, created_at_ms FROM esp_fleet_activity WHERE fleet_id = ?`
	args := []any{fleetID}
	if beforeMS > 0 {
		query += ` AND created_at_ms < ?`
		args = append(args, beforeMS)
	}
	query += ` ORDER BY created_at_ms DESC, event_id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("esphttp: list fleet activity: %w", err)
	}
	defer rows.Close()
	var out []FleetActivityRecord
	for rows.Next() {
		rec, err := scanFleetActivityRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *SQLiteStateStore) DeleteFleetActivity(ctx context.Context, fleetID string, eventID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM esp_fleet_activity WHERE fleet_id = ? AND event_id = ?`, fleetID, eventID); err != nil {
		return fmt.Errorf("esphttp: delete fleet activity: %w", err)
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
	fleetCols, err := tableColumns(db, "esp_fleets")
	if err != nil {
		return err
	}
	for _, stmt := range []struct {
		name string
		sql  string
	}{
		{"status", `ALTER TABLE esp_fleets ADD COLUMN status TEXT NOT NULL DEFAULT 'active'`},
		{"archived_at_ms", `ALTER TABLE esp_fleets ADD COLUMN archived_at_ms INTEGER NOT NULL DEFAULT 0`},
		{"deleted_at_ms", `ALTER TABLE esp_fleets ADD COLUMN deleted_at_ms INTEGER NOT NULL DEFAULT 0`},
	} {
		if fleetCols[stmt.name] {
			continue
		}
		if _, err := db.Exec(stmt.sql); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add fleet %s: %w", stmt.name, err)
		}
	}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS esp_agent_commands (
		  command_id TEXT PRIMARY KEY,
		  fleet_id TEXT NOT NULL,
		  control_group_id BLOB NOT NULL,
		  issuer_node_id INTEGER NOT NULL,
		  agent_node_id INTEGER NOT NULL,
		  action TEXT NOT NULL,
		  target BLOB NOT NULL,
		  instruction TEXT NOT NULL,
		  context BLOB,
		  args BLOB,
		  command BLOB NOT NULL,
		  payload BLOB NOT NULL,
		  status TEXT NOT NULL,
		  attempts INTEGER NOT NULL DEFAULT 0,
		  lease_owner TEXT NOT NULL DEFAULT '',
		  lease_until_ms INTEGER NOT NULL DEFAULT 0,
		  created_at_ms INTEGER NOT NULL,
		  expires_at_ms INTEGER NOT NULL DEFAULT 0,
		  received_at_ms INTEGER NOT NULL,
		  started_at_ms INTEGER NOT NULL DEFAULT 0,
		  completed_at_ms INTEGER NOT NULL DEFAULT 0,
		  updated_at_ms INTEGER NOT NULL,
		  result BLOB,
		  last_error TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_commands_status ON esp_agent_commands(status, received_at_ms)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_commands_lease ON esp_agent_commands(status, lease_until_ms)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("esphttp: migrate state schema add agent commands: %w", err)
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

type fleetScanner interface {
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

func scanFleetRecord(row fleetScanner) (FleetRecord, error) {
	var rec FleetRecord
	var controlGroup []byte
	var pub string
	if err := row.Scan(&rec.FleetID, &rec.Name, &controlGroup, &rec.Coordinator.PilotNodeID, &pub, &rec.CoordinatorDeviceID, &rec.Status, &rec.CreatedAtMS, &rec.UpdatedAtMS, &rec.ArchivedAtMS, &rec.DeletedAtMS); err != nil {
		return FleetRecord{}, err
	}
	rec.Status = NormalizeFleetStatus(rec.Status)
	if len(controlGroup) == len(rec.ControlGroupID) {
		copy(rec.ControlGroupID[:], controlGroup)
	}
	if pub != "" {
		raw, err := base64.StdEncoding.DecodeString(pub)
		if err != nil {
			return FleetRecord{}, fmt.Errorf("esphttp: decode fleet coordinator pubkey: %w", err)
		}
		rec.Coordinator.EntmootPubKey = raw
	}
	return rec, nil
}

func scanFleetMemberRecord(row fleetScanner) (FleetMemberRecord, error) {
	var rec FleetMemberRecord
	if err := row.Scan(&rec.FleetID, &rec.NodeID, &rec.EntmootPubKey, &rec.Hostname, &rec.Role, &rec.Status, &rec.InvitedAtMS, &rec.AcceptedAtMS, &rec.RemovedAtMS, &rec.UpdatedAtMS); err != nil {
		return FleetMemberRecord{}, err
	}
	return rec, nil
}

func scanFleetInviteRecord(row fleetScanner) (FleetInviteRecord, error) {
	var rec FleetInviteRecord
	var invite []byte
	if err := row.Scan(&rec.InviteID, &rec.FleetID, &rec.NodeID, &rec.EntmootPubKey, &rec.Hostname, &rec.Status, &invite, &rec.CreatedAtMS, &rec.UpdatedAtMS, &rec.ExpiresAtMS); err != nil {
		return FleetInviteRecord{}, err
	}
	rec.Invite = append(json.RawMessage(nil), invite...)
	return rec, nil
}

func scanFleetActivityRecord(row fleetScanner) (FleetActivityRecord, error) {
	var rec FleetActivityRecord
	var actorPub string
	var subjectNode entmoot.NodeID
	var subjectPub string
	var metadata []byte
	if err := row.Scan(&rec.EventID, &rec.FleetID, &rec.Type, &rec.Actor.PilotNodeID, &actorPub, &subjectNode, &subjectPub, &rec.Summary, &metadata, &rec.CreatedAtMS); err != nil {
		return FleetActivityRecord{}, err
	}
	if actorPub != "" {
		raw, err := base64.StdEncoding.DecodeString(actorPub)
		if err != nil {
			return FleetActivityRecord{}, fmt.Errorf("esphttp: decode fleet actor pubkey: %w", err)
		}
		rec.Actor.EntmootPubKey = raw
	}
	if subjectNode != 0 || subjectPub != "" {
		subj := entmoot.NodeInfo{PilotNodeID: subjectNode}
		if subjectPub != "" {
			raw, err := base64.StdEncoding.DecodeString(subjectPub)
			if err != nil {
				return FleetActivityRecord{}, fmt.Errorf("esphttp: decode fleet subject pubkey: %w", err)
			}
			subj.EntmootPubKey = raw
		}
		rec.Subject = &subj
	}
	rec.Metadata = append(json.RawMessage(nil), metadata...)
	return rec, nil
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

func sortOpenInviteRecords(records []OpenInviteRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAtMS == records[j].CreatedAtMS {
			return records[i].TokenHash < records[j].TokenHash
		}
		return records[i].CreatedAtMS > records[j].CreatedAtMS
	})
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
