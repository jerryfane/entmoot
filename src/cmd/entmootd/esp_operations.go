package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

type espOperationExecutor struct {
	dataDir       string
	identity      *keystore.Identity
	socketPath    string
	timeout       time.Duration
	metadataStore esphttp.GroupMetadataStore
	stateStore    esphttp.StateStore
	deviceGroups  deviceGroupAuthorizer
}

var espInviteRosterLocks sync.Map
var espOpenInviteRedeemLocks keyedMutexMap

type keyedMutexMap struct {
	mu    sync.Mutex
	locks map[string]*keyedMutexEntry
}

type keyedMutexEntry struct {
	mu   sync.Mutex
	refs int
}

func (m *keyedMutexMap) Lock(key string) func() {
	m.mu.Lock()
	if m.locks == nil {
		m.locks = make(map[string]*keyedMutexEntry)
	}
	entry := m.locks[key]
	if entry == nil {
		entry = &keyedMutexEntry{}
		m.locks[key] = entry
	}
	entry.refs++
	m.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		m.mu.Lock()
		entry.refs--
		if entry.refs == 0 && m.locks[key] == entry {
			delete(m.locks, key)
		}
		m.mu.Unlock()
	}
}

func (m *keyedMutexMap) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.locks)
}

type groupCreatePayload struct {
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	Tags        []string        `json:"tags,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}

type inviteTargetPayload struct {
	PilotNodeID   entmoot.NodeID `json:"pilot_node_id"`
	PilotPubKey   []byte         `json:"pilot_pubkey,omitempty"`
	EntmootPubKey []byte         `json:"entmoot_pubkey"`
}

type inviteCreatePayload struct {
	ValidFor       string               `json:"valid_for,omitempty"`
	ValidUntilMS   int64                `json:"valid_until_ms,omitempty"`
	Peers          string               `json:"peers,omitempty"`
	BootstrapPeers []entmoot.NodeID     `json:"bootstrap_peers,omitempty"`
	Target         *inviteTargetPayload `json:"target,omitempty"`
}

type openInviteCreatePayload struct {
	ValidFor       string           `json:"valid_for,omitempty"`
	ValidUntilMS   int64            `json:"valid_until_ms,omitempty"`
	MaxUses        int              `json:"max_uses"`
	BootstrapPeers []entmoot.NodeID `json:"bootstrap_peers,omitempty"`
}

type inviteAcceptPayload struct {
	Invite *entmoot.Invite `json:"invite,omitempty"`
}

type openInviteRedeemPayload struct {
	PilotNodeID    entmoot.NodeID `json:"pilot_node_id"`
	PilotPubKey    []byte         `json:"pilot_pubkey"`
	EntmootPubKey  []byte         `json:"entmoot_pubkey"`
	ChallengeID    string         `json:"challenge_id"`
	PilotSignature []byte         `json:"pilot_signature"`
}

type openInvitePilotProofEnvelope struct {
	Type          string          `json:"type"`
	TokenHash     string          `json:"token_hash"`
	GroupID       entmoot.GroupID `json:"group_id"`
	ChallengeID   string          `json:"challenge_id"`
	Nonce         string          `json:"nonce"`
	IssuedAtMS    int64           `json:"issued_at_ms"`
	ExpiresAtMS   int64           `json:"expires_at_ms"`
	PilotNodeID   entmoot.NodeID  `json:"pilot_node_id"`
	PilotPubKey   []byte          `json:"pilot_pubkey"`
	EntmootPubKey []byte          `json:"entmoot_pubkey"`
}

const openInviteChallengeTTL = 5 * time.Minute
const pilotSignChallengeDomain = "pilot.ipc.sign_challenge.v1\x00"
const maxOpenInviteActiveChallenges = 128
const minOpenInviteActiveChallenges = 8

func (e espOperationExecutor) ExecuteSignRequest(ctx context.Context, req esphttp.SignRequest, _ []byte) (json.RawMessage, error) {
	switch req.Kind {
	case "invite_accept":
		return e.acceptInvite(ctx, req)
	case "invite_create":
		return e.createInvite(ctx, req)
	case "open_invite_create":
		return e.createOpenInvite(ctx, req)
	case "member_remove":
		return e.removeMember(ctx, req)
	case "group_create":
		return e.createGroup(ctx, req)
	case "group_update":
		return e.updateGroup(ctx, req)
	default:
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "unsupported_operation", Message: "unsupported sign request kind"}
	}
}

func (e espOperationExecutor) CreateOpenInviteChallenge(ctx context.Context, token string, raw json.RawMessage) (json.RawMessage, error) {
	if e.stateStore == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "open_invite_unavailable", Message: "open invite store is not configured"}
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "open invite token is required"}
	}
	var payload openInviteRedeemPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid open invite challenge payload"}
	}
	claimTarget := &inviteTargetPayload{PilotNodeID: payload.PilotNodeID, PilotPubKey: payload.PilotPubKey, EntmootPubKey: payload.EntmootPubKey}
	if _, err := validateInviteTarget(claimTarget); err != nil {
		return nil, err
	}
	tokenHash := esphttp.HashOpenInviteToken(token)
	rec, ok, err := e.stateStore.GetOpenInviteByTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "open_invite_not_found", Message: "open invite not found"}
	}
	now := time.Now().UnixMilli()
	if err := ensureOpenInviteCanIssueChallenge(rec, now); err != nil {
		return nil, openInviteStoreError(err)
	}
	if _, err := e.checkInviteAuthorityOverIPC(ctx, &ipc.InviteAuthorityCheckReq{GroupID: rec.GroupID}); err != nil {
		return nil, err
	}
	nonce, err := esphttp.NewOpenInviteChallengeNonce()
	if err != nil {
		return nil, err
	}
	challengeID := newOpenInviteChallengeID()
	expires := time.UnixMilli(now).Add(openInviteChallengeTTL).UnixMilli()
	proof, err := canonical.Encode(openInvitePilotProofEnvelope{
		Type:          "entmoot.open_invite.redeem.v1",
		TokenHash:     tokenHash,
		GroupID:       rec.GroupID,
		ChallengeID:   challengeID,
		Nonce:         nonce,
		IssuedAtMS:    now,
		ExpiresAtMS:   expires,
		PilotNodeID:   payload.PilotNodeID,
		PilotPubKey:   append([]byte(nil), payload.PilotPubKey...),
		EntmootPubKey: append([]byte(nil), payload.EntmootPubKey...),
	})
	if err != nil {
		return nil, err
	}
	challenge, err := e.stateStore.CreateOrReuseOpenInviteChallenge(ctx, esphttp.OpenInviteChallenge{
		ChallengeID:    challengeID,
		TokenHash:      tokenHash,
		GroupID:        rec.GroupID,
		PilotNodeID:    payload.PilotNodeID,
		PilotPubKey:    base64.StdEncoding.EncodeToString(payload.PilotPubKey),
		EntmootPubKey:  base64.StdEncoding.EncodeToString(payload.EntmootPubKey),
		Nonce:          nonce,
		SigningPayload: base64.StdEncoding.EncodeToString(proof),
		CreatedAtMS:    now,
		ExpiresAtMS:    expires,
	}, openInviteChallengeCap(rec), now)
	if err != nil {
		return nil, openInviteStoreError(err)
	}
	signingPayload, err := base64.StdEncoding.DecodeString(challenge.SigningPayload)
	if err != nil {
		return nil, fmt.Errorf("decode stored open invite challenge payload: %w", err)
	}
	return json.Marshal(map[string]any{
		"status":                 "challenge",
		"challenge_id":           challenge.ChallengeID,
		"canonical_type":         "entmoot.open_invite.redeem.v1",
		"signature_algorithm":    "ed25519",
		"signing_payload":        challenge.SigningPayload,
		"signing_payload_sha256": sha256Base64(signingPayload),
		"expires_at_ms":          challenge.ExpiresAtMS,
	})
}

func (e espOperationExecutor) RedeemOpenInvite(ctx context.Context, token string, raw json.RawMessage) (json.RawMessage, error) {
	if e.stateStore == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "open_invite_unavailable", Message: "open invite store is not configured"}
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "open invite token is required"}
	}
	var payload openInviteRedeemPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid open invite redemption payload"}
	}
	claimTarget := &inviteTargetPayload{PilotNodeID: payload.PilotNodeID, PilotPubKey: payload.PilotPubKey, EntmootPubKey: payload.EntmootPubKey}
	target, err := validateInviteTarget(claimTarget)
	if err != nil {
		return nil, err
	}
	tokenHash := esphttp.HashOpenInviteToken(token)
	rec, ok, err := e.stateStore.GetOpenInviteByTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "open_invite_not_found", Message: "open invite not found"}
	}
	redeemerKey := fmt.Sprintf("%d:%s", payload.PilotNodeID, base64.StdEncoding.EncodeToString(payload.EntmootPubKey))
	unlock := lockESPOpenInviteRedemption(tokenHash, redeemerKey)
	defer unlock()

	now := time.Now().UnixMilli()
	if rec.Revoked {
		return nil, openInviteStoreError(esphttp.ErrOpenInviteRevoked)
	}
	if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= now {
		return nil, openInviteStoreError(esphttp.ErrOpenInviteExpired)
	}
	existing, ok, err := e.stateStore.GetOpenInviteRedemption(ctx, tokenHash, redeemerKey)
	if err != nil {
		return nil, err
	}
	if !ok && rec.MaxUses > 0 && rec.UseCount >= rec.MaxUses {
		return nil, openInviteStoreError(esphttp.ErrOpenInviteExhausted)
	}
	proof, err := e.verifyOpenInvitePilotProof(ctx, tokenHash, rec, payload, ok)
	if err != nil {
		return nil, openInviteStoreError(err)
	}
	if ok && len(existing.Result) > 0 {
		return append(json.RawMessage(nil), existing.Result...), nil
	}

	rec, redemption, alreadyRedeemed, err := e.stateStore.RedeemOpenInvite(ctx, tokenHash, esphttp.OpenInviteRedemption{
		RedeemerKey:   redeemerKey,
		PilotNodeID:   payload.PilotNodeID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(payload.EntmootPubKey),
	}, now)
	if err != nil {
		return nil, openInviteStoreError(err)
	}
	if alreadyRedeemed && len(redemption.Result) > 0 {
		return append(json.RawMessage(nil), redemption.Result...), nil
	}
	resp, err := e.createInviteOverIPC(ctx, &ipc.InviteCreateReq{
		GroupID:              rec.GroupID,
		Target:               target,
		TargetPilotPubKey:    append([]byte(nil), payload.PilotPubKey...),
		RequirePilotIdentity: true,
		RequirePilotProof:    true,
		TargetPilotProof:     proof,
		TargetPilotSignature: append([]byte(nil), payload.PilotSignature...),
		BootstrapPeers:       append([]entmoot.NodeID(nil), rec.BootstrapPeers...),
	})
	if err != nil {
		if (!resp.sent || resp.rejected) && !alreadyRedeemed {
			_ = e.stateStore.ReleaseOpenInviteRedemption(ctx, tokenHash, redeemerKey, time.Now().UnixMilli())
		}
		return nil, err
	}
	result, err := json.Marshal(map[string]any{
		"status":          "redeemed",
		"group_id":        resp.GroupID,
		"invite":          resp.Invite,
		"max_uses":        rec.MaxUses,
		"use_count":       rec.UseCount,
		"bootstrap_peers": rec.BootstrapPeers,
		"expires_at_ms":   rec.ExpiresAtMS,
	})
	if err != nil {
		return nil, err
	}
	if err := e.stateStore.CompleteOpenInviteRedemption(ctx, tokenHash, redeemerKey, result, time.Now().UnixMilli()); err != nil {
		return nil, err
	}
	return result, nil
}

func openInviteStoreError(err error) error {
	switch {
	case errors.Is(err, esphttp.ErrOpenInviteExpired):
		return &esphttp.OperationError{HTTPStatus: http.StatusConflict, Code: "open_invite_expired", Message: "open invite has expired"}
	case errors.Is(err, esphttp.ErrOpenInviteRevoked):
		return &esphttp.OperationError{HTTPStatus: http.StatusConflict, Code: "open_invite_revoked", Message: "open invite has been revoked"}
	case errors.Is(err, esphttp.ErrOpenInviteExhausted):
		return &esphttp.OperationError{HTTPStatus: http.StatusConflict, Code: "open_invite_exhausted", Message: "open invite has no remaining uses"}
	case errors.Is(err, esphttp.ErrOpenInviteChallengeExpired):
		return &esphttp.OperationError{HTTPStatus: http.StatusConflict, Code: "open_invite_challenge_expired", Message: "open invite challenge has expired"}
	case errors.Is(err, esphttp.ErrOpenInviteChallengeUsed):
		return &esphttp.OperationError{HTTPStatus: http.StatusConflict, Code: "open_invite_challenge_used", Message: "open invite challenge has already been used"}
	case errors.Is(err, esphttp.ErrOpenInviteChallengeLimit):
		return &esphttp.OperationError{HTTPStatus: http.StatusTooManyRequests, Code: "open_invite_challenge_limit", Message: "open invite has too many active challenges; retry after an existing challenge expires"}
	case errors.Is(err, sql.ErrNoRows):
		return &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "open_invite_not_found", Message: "open invite not found"}
	default:
		return err
	}
}

func (e espOperationExecutor) verifyOpenInvitePilotProof(ctx context.Context, tokenHash string, rec esphttp.OpenInviteRecord, payload openInviteRedeemPayload, allowConsumed bool) ([]byte, error) {
	if strings.TrimSpace(payload.ChallengeID) == "" {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "challenge_id is required"}
	}
	if len(payload.PilotSignature) != ed25519.SignatureSize {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "pilot_signature must be a base64 Ed25519 signature"}
	}
	challenge, ok, err := e.stateStore.GetOpenInviteChallenge(ctx, payload.ChallengeID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "open_invite_challenge_not_found", Message: "open invite challenge not found"}
	}
	now := time.Now().UnixMilli()
	if challenge.TokenHash != tokenHash || challenge.GroupID != rec.GroupID ||
		challenge.PilotNodeID != payload.PilotNodeID ||
		challenge.PilotPubKey != base64.StdEncoding.EncodeToString(payload.PilotPubKey) ||
		challenge.EntmootPubKey != base64.StdEncoding.EncodeToString(payload.EntmootPubKey) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "invalid_challenge", Message: "open invite challenge does not match redemption identity"}
	}
	proof, err := base64.StdEncoding.DecodeString(challenge.SigningPayload)
	if err != nil || len(proof) == 0 {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "invalid_challenge", Message: "open invite challenge payload is invalid"}
	}
	if !ed25519.Verify(ed25519.PublicKey(payload.PilotPubKey), pilotChallengeSigningBytes(proof), payload.PilotSignature) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "invalid_signature", Message: "pilot_signature does not verify"}
	}
	if challenge.UsedAtMS != 0 {
		if allowConsumed {
			return proof, nil
		}
		return nil, openInviteStoreError(esphttp.ErrOpenInviteChallengeUsed)
	}
	if challenge.ExpiresAtMS > 0 && challenge.ExpiresAtMS <= now {
		return nil, openInviteStoreError(esphttp.ErrOpenInviteChallengeExpired)
	}
	if _, err := e.stateStore.ConsumeOpenInviteChallenge(ctx, challenge.ChallengeID, now); err != nil {
		return nil, openInviteStoreError(err)
	}
	return proof, nil
}

func ensureOpenInviteActive(rec esphttp.OpenInviteRecord, nowMS int64) error {
	if rec.Revoked {
		return esphttp.ErrOpenInviteRevoked
	}
	if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS {
		return esphttp.ErrOpenInviteExpired
	}
	if rec.MaxUses > 0 && rec.UseCount >= rec.MaxUses {
		return esphttp.ErrOpenInviteExhausted
	}
	return nil
}

func openInviteChallengeCap(rec esphttp.OpenInviteRecord) int {
	remaining := rec.MaxUses - rec.UseCount
	if remaining < 0 {
		remaining = 0
	}
	cap := remaining * 2
	if cap < minOpenInviteActiveChallenges {
		cap = minOpenInviteActiveChallenges
	}
	if cap > maxOpenInviteActiveChallenges {
		cap = maxOpenInviteActiveChallenges
	}
	return cap
}

func ensureOpenInviteCanIssueChallenge(rec esphttp.OpenInviteRecord, nowMS int64) error {
	if rec.Revoked {
		return esphttp.ErrOpenInviteRevoked
	}
	if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS {
		return esphttp.ErrOpenInviteExpired
	}
	if rec.MaxUses > 0 && rec.UseCount >= rec.MaxUses {
		return esphttp.ErrOpenInviteExhausted
	}
	return nil
}

func (e espOperationExecutor) acceptInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	invite, err := parseInviteAccept(req.Payload)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	resp, err := e.joinGroup(ctx, invite)
	if err != nil {
		return nil, err
	}
	groupID := resp.GroupID
	if groupID == (entmoot.GroupID{}) {
		groupID = invite.GroupID
	}
	if err := e.grantDeviceGroupIfNeeded(ctx, req.DeviceID, groupID); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":   resp.Status,
		"group_id": groupID,
		"members":  resp.Members,
	})
}

func (e espOperationExecutor) createInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	var payload inviteCreatePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid invite_create payload"}
	}
	ipcReq, err := buildInviteCreateIPCRequest(req.GroupID, payload)
	if err != nil {
		return nil, err
	}
	resp, err := e.createInviteOverIPC(ctx, ipcReq)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":   "created",
		"group_id": resp.GroupID,
		"invite":   resp.Invite,
	})
}

func (e espOperationExecutor) createOpenInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if req.GroupID == (entmoot.GroupID{}) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "open_invite_create requires group_id"}
	}
	if e.stateStore == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "open_invite_unavailable", Message: "open invite store is not configured"}
	}
	var payload openInviteCreatePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid open_invite_create payload"}
	}
	if payload.MaxUses <= 0 || payload.MaxUses > 100 {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "max_uses must be between 1 and 100"}
	}
	ttl := 24 * time.Hour
	if payload.ValidFor != "" {
		parsed, err := parseDurationDays(payload.ValidFor)
		if err != nil || parsed <= 0 {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid valid_for"}
		}
		if parsed < time.Millisecond {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "valid_for must be at least 1ms"}
		}
		ttl = parsed
	}
	now := time.Now().UnixMilli()
	expires := now + ttl.Milliseconds()
	if payload.ValidUntilMS > 0 {
		expires = payload.ValidUntilMS
	}
	if expires <= now {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "open invite expiry must be in the future"}
	}
	if _, err := e.checkInviteAuthorityOverIPC(ctx, &ipc.InviteAuthorityCheckReq{GroupID: req.GroupID}); err != nil {
		return nil, err
	}
	token, tokenHash, err := esphttp.NewOpenInviteToken()
	if err != nil {
		return nil, err
	}
	rec, err := e.stateStore.CreateOpenInvite(ctx, esphttp.OpenInviteRecord{
		TokenHash:      tokenHash,
		GroupID:        req.GroupID,
		DeviceID:       req.DeviceID,
		MaxUses:        payload.MaxUses,
		BootstrapPeers: append([]entmoot.NodeID(nil), payload.BootstrapPeers...),
		CreatedAtMS:    now,
		ExpiresAtMS:    expires,
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":          "created",
		"group_id":        rec.GroupID,
		"token":           token,
		"max_uses":        rec.MaxUses,
		"use_count":       rec.UseCount,
		"bootstrap_peers": rec.BootstrapPeers,
		"expires_at_ms":   rec.ExpiresAtMS,
	})
}

func (e espOperationExecutor) createGroup(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	var payload groupCreatePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid group_create payload"}
	}
	metadata, err := normalizeGroupMetadata(payload)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	info, err := e.daemonInfo()
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	var gid entmoot.GroupID
	gid, err = groupIDForCreateRequest(req)
	if err != nil {
		return nil, err
	}
	groupPath := groupDirPath(e.dataDir, gid)
	groupPreexisted := pathExists(groupPath)
	committed := false
	metadataWritten := false
	var previousMetadata json.RawMessage
	metadataHadPrevious := false
	deviceGroupGranted := false
	deviceAdminGroupGranted := false
	var st *store.SQLite
	var rlog *roster.RosterLog
	defer func() {
		if rlog != nil {
			_ = rlog.Close()
		}
		if st != nil {
			_ = st.Close()
		}
		if committed {
			return
		}
		if metadataWritten && e.metadataStore != nil {
			var err error
			if metadataHadPrevious {
				err = e.metadataStore.SetGroupMetadata(context.Background(), gid, previousMetadata)
			} else {
				err = e.metadataStore.DeleteGroupMetadata(context.Background(), gid)
			}
			if err != nil {
				slog.Warn("esp group_create rollback: restore metadata failed", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
			}
		}
		if deviceGroupGranted && e.deviceGroups != nil {
			if err := e.deviceGroups.RevokeDeviceGroup(context.Background(), req.DeviceID, gid); err != nil {
				slog.Warn("esp group_create rollback: revoke device group failed", slog.String("group_id", gid.String()), slog.String("device_id", req.DeviceID), slog.String("err", err.Error()))
			}
		}
		if deviceAdminGroupGranted && e.deviceGroups != nil {
			if err := e.deviceGroups.RevokeDeviceAdminGroup(context.Background(), req.DeviceID, gid); err != nil {
				slog.Warn("esp group_create rollback: revoke device admin group failed", slog.String("group_id", gid.String()), slog.String("device_id", req.DeviceID), slog.String("err", err.Error()))
			}
		}
		if !groupPreexisted {
			if err := os.RemoveAll(groupPath); err != nil {
				slog.Warn("esp group_create rollback: remove group dir failed", slog.String("group_id", gid.String()), slog.String("path", groupPath), slog.String("err", err.Error()))
			}
		}
	}()
	if req.DeviceID != "" {
		changed, err := e.grantDeviceGroup(ctx, req.DeviceID, gid)
		if err != nil {
			return nil, err
		}
		deviceGroupGranted = changed
		changed, err = e.grantDeviceAdminGroup(ctx, req.DeviceID, gid)
		if err != nil {
			return nil, err
		}
		deviceAdminGroupGranted = changed
	}
	st, err = store.OpenSQLite(e.dataDir)
	if err != nil {
		return nil, err
	}
	rlog, err = roster.OpenJSONL(e.dataDir, gid)
	if err != nil {
		return nil, err
	}
	founder := entmoot.NodeInfo{
		PilotNodeID:   info.PilotNodeID,
		EntmootPubKey: append([]byte(nil), e.identity.PublicKey...),
	}
	now := req.CreatedAtMS
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	if existing, ok := rlog.Founder(); ok {
		if existing.PilotNodeID != founder.PilotNodeID || !bytes.Equal(existing.EntmootPubKey, founder.EntmootPubKey) {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusConflict, Code: "group_create_conflict", Message: "deterministic group id already belongs to another founder"}
		}
	} else {
		if err := rlog.Genesis(e.identity, founder, now); err != nil {
			return nil, err
		}
	}
	if e.metadataStore != nil {
		previousMetadata, metadataHadPrevious, err = e.metadataStore.GetGroupMetadata(ctx, gid)
		if err != nil {
			return nil, err
		}
		if err := e.metadataStore.SetGroupMetadata(ctx, gid, metadata); err != nil {
			return nil, err
		}
		metadataWritten = true
	}
	root, err := st.MerkleRoot(ctx, gid)
	if err != nil {
		return nil, err
	}
	invite := entmoot.Invite{
		GroupID:    gid,
		Founder:    founder,
		RosterHead: rlog.Head(),
		MerkleRoot: root,
		IssuedAt:   now,
		ValidUntil: now + int64((24*time.Hour)/time.Millisecond),
		Issuer:     founder,
	}
	if err := signInvite(e.identity, &invite); err != nil {
		return nil, err
	}
	if err := rlog.Close(); err != nil {
		return nil, err
	}
	rlog = nil
	if err := st.Close(); err != nil {
		return nil, err
	}
	st = nil
	resp, err := e.joinGroup(ctx, invite)
	if err != nil {
		return nil, err
	}
	result, err := json.Marshal(map[string]any{
		"status":   resp.Status,
		"group_id": gid,
		"members":  resp.Members,
		"founder":  founder,
		"metadata": json.RawMessage(metadata),
	})
	if err != nil {
		return nil, err
	}
	committed = true
	return result, nil
}

func (e espOperationExecutor) updateGroup(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if req.GroupID == (entmoot.GroupID{}) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "group_update requires group_id"}
	}
	if e.metadataStore == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "metadata_unavailable", Message: "group metadata store is not configured"}
	}
	metadata := req.Payload
	metadata, err := esphttp.NormalizeGroupMetadata(metadata)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	if err := e.metadataStore.SetGroupMetadata(ctx, req.GroupID, metadata); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":   "updated",
		"group_id": req.GroupID,
		"metadata": json.RawMessage(metadata),
	})
}

type memberRemovePayload struct {
	Target *inviteTargetPayload `json:"target,omitempty"`
}

func (e espOperationExecutor) removeMember(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if req.GroupID == (entmoot.GroupID{}) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "member_remove requires group_id"}
	}
	var payload memberRemovePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid member_remove payload"}
	}
	target, err := validateInviteTargetForRemove(payload.Target)
	if err != nil {
		return nil, err
	}
	resp, err := e.removeMemberOverIPC(ctx, &ipc.MemberRemoveReq{GroupID: req.GroupID, Target: target})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":      resp.Status,
		"group_id":    resp.GroupID,
		"roster_head": resp.RosterHead,
		"members":     resp.Members,
	})
}

func buildInviteCreateIPCRequest(gid entmoot.GroupID, payload inviteCreatePayload) (*ipc.InviteCreateReq, error) {
	if gid == (entmoot.GroupID{}) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invite_create requires group_id"}
	}
	target, err := validateInviteTarget(payload.Target)
	if err != nil {
		return nil, err
	}
	ttl := 24 * time.Hour
	if payload.ValidFor != "" {
		parsed, err := parseDurationDays(payload.ValidFor)
		if err != nil {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid valid_for"}
		}
		if parsed <= 0 {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "valid_for must be positive"}
		}
		if parsed < time.Millisecond {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "valid_for must be at least 1ms"}
		}
		ttl = parsed
	}
	peers := append([]entmoot.NodeID(nil), payload.BootstrapPeers...)
	if len(peers) == 0 && payload.Peers != "" {
		parsed, err := parsePeerList(payload.Peers)
		if err != nil {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
		}
		peers = parsed
	}
	return &ipc.InviteCreateReq{
		GroupID:              gid,
		Target:               target,
		TargetPilotPubKey:    append([]byte(nil), payload.Target.PilotPubKey...),
		RequirePilotIdentity: true,
		ValidForMS:           ttl.Milliseconds(),
		ValidUntilMS:         payload.ValidUntilMS,
		BootstrapPeers:       peers,
	}, nil
}

func validateInviteTarget(target *inviteTargetPayload) (entmoot.NodeInfo, error) {
	if target == nil {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "target_required", Message: "invite_create requires target agent identity"}
	}
	if target.PilotNodeID == 0 {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "target pilot_node_id is required"}
	}
	if len(target.EntmootPubKey) != ed25519.PublicKeySize {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "target entmoot_pubkey must be 32 bytes"}
	}
	if len(target.PilotPubKey) != ed25519.PublicKeySize {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "target pilot_pubkey must be 32 bytes"}
	}
	return entmoot.NodeInfo{
		PilotNodeID:   target.PilotNodeID,
		EntmootPubKey: append([]byte(nil), target.EntmootPubKey...),
	}, nil
}

func validateInviteTargetForRemove(target *inviteTargetPayload) (entmoot.NodeInfo, error) {
	if target == nil {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "target_required", Message: "member_remove requires target agent identity"}
	}
	if target.PilotNodeID == 0 {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "target pilot_node_id is required"}
	}
	if len(target.EntmootPubKey) != ed25519.PublicKeySize {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "target entmoot_pubkey must be 32 bytes"}
	}
	return entmoot.NodeInfo{
		PilotNodeID:   target.PilotNodeID,
		EntmootPubKey: append([]byte(nil), target.EntmootPubKey...),
	}, nil
}

func lockESPInviteRoster(gid entmoot.GroupID) func() {
	lockAny, _ := espInviteRosterLocks.LoadOrStore(gid.String(), &sync.Mutex{})
	mu := lockAny.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func lockESPOpenInviteRedemption(tokenHash string, redeemerKey string) func() {
	key := tokenHash + "\x00" + redeemerKey
	return espOpenInviteRedeemLocks.Lock(key)
}

func newOpenInviteChallengeID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

func sha256Base64(data []byte) string {
	sum := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func pilotChallengeSigningBytes(payload []byte) []byte {
	out := make([]byte, 0, len(pilotSignChallengeDomain)+len(payload))
	out = append(out, pilotSignChallengeDomain...)
	out = append(out, payload...)
	return out
}

func applyFounderRosterAdd(identity *keystore.Identity, rlog *roster.RosterLog, founder entmoot.NodeInfo, target entmoot.NodeInfo) error {
	now := time.Now().UnixMilli()
	entries := rlog.Entries()
	if len(entries) > 0 && now <= entries[len(entries)-1].Timestamp {
		now = entries[len(entries)-1].Timestamp + 1
	}
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   target,
		Actor:     founder.PilotNodeID,
		Timestamp: now,
		Parents:   []entmoot.RosterEntryID{rlog.Head()},
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		return err
	}
	entry.Signature = identity.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)
	if err := rlog.Apply(entry); err != nil {
		if errors.Is(err, entmoot.ErrRosterReject) {
			return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "roster_rejected", Message: err.Error()}
		}
		return err
	}
	return nil
}

func applyFounderRosterRemove(identity *keystore.Identity, rlog *roster.RosterLog, founder entmoot.NodeInfo, target entmoot.NodeInfo) error {
	now := time.Now().UnixMilli()
	entries := rlog.Entries()
	if len(entries) > 0 && now <= entries[len(entries)-1].Timestamp {
		now = entries[len(entries)-1].Timestamp + 1
	}
	entry := entmoot.RosterEntry{
		Op:        "remove",
		Subject:   target,
		Actor:     founder.PilotNodeID,
		Timestamp: now,
		Parents:   []entmoot.RosterEntryID{rlog.Head()},
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		return err
	}
	entry.Signature = identity.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)
	if err := rlog.Apply(entry); err != nil {
		if errors.Is(err, entmoot.ErrRosterReject) {
			return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "roster_rejected", Message: err.Error()}
		}
		return err
	}
	return nil
}

func (e espOperationExecutor) daemonInfo() (*ipc.InfoResp, error) {
	return infoOverIPC(e.socketPath)
}

type inviteCreateIPCResult struct {
	*ipc.InviteCreateResp
	sent     bool
	rejected bool
}

func (e espOperationExecutor) createInviteOverIPC(ctx context.Context, req *ipc.InviteCreateReq) (*inviteCreateIPCResult, error) {
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "unix", e.socketPath)
	if err != nil {
		return &inviteCreateIPCResult{}, joinUnavailableError(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return &inviteCreateIPCResult{}, err
	}
	t, body, err := ipc.Encode(req)
	if err != nil {
		return &inviteCreateIPCResult{}, err
	}
	result := &inviteCreateIPCResult{}
	if err := ipc.WriteFrame(conn, t, body); err != nil {
		return result, err
	}
	result.sent = true
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return result, err
	}
	switch v := payload.(type) {
	case *ipc.InviteCreateResp:
		result.InviteCreateResp = v
		return result, nil
	case *ipc.ErrorFrame:
		result.rejected = true
		return result, operationIPCError(v)
	default:
		return result, fmt.Errorf("unexpected invite create response %T", payload)
	}
}

func (e espOperationExecutor) checkInviteAuthorityOverIPC(ctx context.Context, req *ipc.InviteAuthorityCheckReq) (*ipc.InviteAuthorityCheckResp, error) {
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "unix", e.socketPath)
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := ipc.EncodeAndWrite(conn, req); err != nil {
		return nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, err
	}
	switch v := payload.(type) {
	case *ipc.InviteAuthorityCheckResp:
		return v, nil
	case *ipc.ErrorFrame:
		return nil, operationIPCError(v)
	default:
		return nil, fmt.Errorf("unexpected invite authority response %T", payload)
	}
}

func (e espOperationExecutor) removeMemberOverIPC(ctx context.Context, req *ipc.MemberRemoveReq) (*ipc.MemberRemoveResp, error) {
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "unix", e.socketPath)
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := ipc.EncodeAndWrite(conn, req); err != nil {
		return nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, err
	}
	switch v := payload.(type) {
	case *ipc.MemberRemoveResp:
		return v, nil
	case *ipc.ErrorFrame:
		return nil, operationIPCError(v)
	default:
		return nil, fmt.Errorf("unexpected member remove response %T", payload)
	}
}

func (e espOperationExecutor) joinGroup(ctx context.Context, invite entmoot.Invite) (*ipc.JoinGroupResp, error) {
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "unix", e.socketPath)
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := ipc.EncodeAndWrite(conn, &ipc.JoinGroupReq{Invite: invite}); err != nil {
		return nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, err
	}
	switch v := payload.(type) {
	case *ipc.JoinGroupResp:
		return v, nil
	case *ipc.ErrorFrame:
		return nil, operationIPCError(v)
	default:
		return nil, fmt.Errorf("unexpected join group response %T", payload)
	}
}

func parseInviteAccept(payload json.RawMessage) (entmoot.Invite, error) {
	var wrapped inviteAcceptPayload
	if err := json.Unmarshal(payload, &wrapped); err == nil && wrapped.Invite != nil {
		return *wrapped.Invite, nil
	}
	var invite entmoot.Invite
	if err := json.Unmarshal(payload, &invite); err != nil {
		return entmoot.Invite{}, fmt.Errorf("invalid invite_accept payload")
	}
	return invite, nil
}

func signInvite(identity *keystore.Identity, invite *entmoot.Invite) error {
	if identity == nil || invite == nil {
		return fmt.Errorf("invite signer is not configured")
	}
	signing := *invite
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return err
	}
	invite.Signature = identity.Sign(sigInput)
	return nil
}

func normalizeGroupMetadata(payload groupCreatePayload) (json.RawMessage, error) {
	meta := make(map[string]any)
	if len(payload.Metadata) > 0 && json.Valid(payload.Metadata) {
		var err error
		meta, err = decodeGroupMetadataObject(payload.Metadata)
		if err != nil {
			return nil, fmt.Errorf("esphttp: group metadata must be a JSON object")
		}
	} else if len(payload.Metadata) > 0 {
		return nil, fmt.Errorf("esphttp: group metadata must be a JSON object")
	}
	if payload.Name != "" {
		meta["name"] = payload.Name
	}
	if payload.Description != "" {
		meta["description"] = payload.Description
	}
	if len(payload.Tags) > 0 {
		meta["tags"] = normalizeGroupTags(payload.Tags)
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	return esphttp.NormalizeGroupMetadata(data)
}

func decodeGroupMetadataObject(metadata json.RawMessage) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(metadata))
	dec.UseNumber()
	var meta map[string]any
	if err := dec.Decode(&meta); err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, fmt.Errorf("non-object metadata")
	}
	return meta, nil
}

func normalizeGroupTags(tags []string) []string {
	out := make([]string, 0, len(tags))
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	return out
}

func groupIDForCreateRequest(req esphttp.SignRequest) (entmoot.GroupID, error) {
	var gid entmoot.GroupID
	if req.ID == "" {
		return gid, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "group_create requires sign request id"}
	}
	sum := sha256.Sum256([]byte("entmoot.esp.group_create.v1\x00" + req.ID + "\x00" + req.SigningPayloadSHA256))
	copy(gid[:], sum[:])
	return gid, nil
}

func groupDirPath(dataDir string, gid entmoot.GroupID) string {
	return filepath.Join(groupsDir(dataDir), base64.RawURLEncoding.EncodeToString(gid[:]))
}

func (e espOperationExecutor) grantDeviceGroupIfNeeded(ctx context.Context, deviceID string, gid entmoot.GroupID) error {
	if deviceID == "" {
		return nil
	}
	_, err := e.grantDeviceGroup(ctx, deviceID, gid)
	return err
}

func (e espOperationExecutor) grantDeviceAdminGroup(ctx context.Context, deviceID string, gid entmoot.GroupID) (bool, error) {
	if deviceID == "" {
		return false, nil
	}
	if e.deviceGroups == nil {
		return false, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "device_registry_unavailable", Message: "device registry is not configured"}
	}
	changed, err := e.deviceGroups.GrantDeviceAdminGroup(ctx, deviceID, gid)
	if err != nil {
		return false, err
	}
	return changed, nil
}

func (e espOperationExecutor) grantDeviceGroup(ctx context.Context, deviceID string, gid entmoot.GroupID) (bool, error) {
	if e.deviceGroups == nil {
		return false, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "device_registry_unavailable", Message: "device group authorizer is not configured"}
	}
	return e.deviceGroups.GrantDeviceGroup(ctx, deviceID, gid)
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func joinUnavailableError(err error) error {
	return &esphttp.OperationError{
		HTTPStatus: http.StatusServiceUnavailable,
		Code:       "join_unavailable",
		Message:    noJoinHelp + ": " + err.Error(),
	}
}

func operationIPCError(frame *ipc.ErrorFrame) error {
	if frame == nil {
		return &esphttp.OperationError{HTTPStatus: http.StatusInternalServerError, Code: "internal_error", Message: "operation failed"}
	}
	status := http.StatusInternalServerError
	code := "internal_error"
	switch frame.Code {
	case ipc.CodeInvalidArgument:
		status = http.StatusBadRequest
		code = "bad_request"
	case ipc.CodeConflict:
		status = http.StatusConflict
		code = "member_identity_conflict"
	case ipc.CodeNotMember:
		status = http.StatusForbidden
		code = "not_member"
	case ipc.CodeGroupNotFound:
		status = http.StatusNotFound
		code = "group_not_found"
	case ipc.CodeUnavailable:
		status = http.StatusServiceUnavailable
		code = "join_unavailable"
	case ipc.CodeInternal:
		status = http.StatusInternalServerError
	}
	return &esphttp.OperationError{HTTPStatus: status, Code: code, Message: frame.Message}
}
