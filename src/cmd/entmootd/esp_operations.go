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
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
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
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

type espOperationExecutor struct {
	dataDir            string
	identity           *keystore.Identity
	socketPath         string
	pilotSocketPath    string
	timeout            time.Duration
	metadataStore      esphttp.GroupMetadataStore
	stateStore         esphttp.StateStore
	deviceGroups       deviceGroupAuthorizer
	pilotIdentity      func(context.Context) (entmoot.NodeID, string, error)
	pilotSignChallenge func(context.Context, []byte) (string, error)
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

type openInviteAcceptPayload struct {
	IssuerURL string `json:"issuer_url"`
	Token     string `json:"token"`
}

type fleetCreatePayload struct {
	Name string `json:"name"`
}

type fleetScopedPayload struct {
	FleetID string `json:"fleet_id"`
}

type fleetInviteCreatePayload struct {
	FleetID      string               `json:"fleet_id"`
	Target       *inviteTargetPayload `json:"target,omitempty"`
	Hostname     string               `json:"hostname,omitempty"`
	ValidFor     string               `json:"valid_for,omitempty"`
	ValidUntilMS int64                `json:"valid_until_ms,omitempty"`
}

type fleetMemberRemovePayload struct {
	FleetID string               `json:"fleet_id"`
	Target  *inviteTargetPayload `json:"target,omitempty"`
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
	case "open_invite_accept":
		return e.acceptOpenInvite(ctx, req)
	case "member_remove":
		return e.removeMember(ctx, req)
	case "fleet_create":
		return e.createFleet(ctx, req)
	case "fleet_invite_create":
		return e.createFleetInvite(ctx, req)
	case "fleet_member_remove":
		return e.removeFleetMember(ctx, req)
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
	redeemerKey := openInviteRedeemerKey(payload.PilotNodeID, payload.EntmootPubKey)
	if err := e.ensureOpenInviteCanIssueChallenge(ctx, rec, tokenHash, redeemerKey, now); err != nil {
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
	redeemerKey := openInviteRedeemerKey(payload.PilotNodeID, payload.EntmootPubKey)
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

func openInviteRedeemerKey(nodeID entmoot.NodeID, entmootPub []byte) string {
	return fmt.Sprintf("%d:%s", nodeID, base64.StdEncoding.EncodeToString(entmootPub))
}

func (e espOperationExecutor) ensureOpenInviteCanIssueChallenge(ctx context.Context, rec esphttp.OpenInviteRecord, tokenHash, redeemerKey string, nowMS int64) error {
	if rec.Revoked {
		return esphttp.ErrOpenInviteRevoked
	}
	if rec.ExpiresAtMS > 0 && rec.ExpiresAtMS <= nowMS {
		return esphttp.ErrOpenInviteExpired
	}
	if rec.MaxUses > 0 && rec.UseCount >= rec.MaxUses {
		if e.stateStore != nil && redeemerKey != "" {
			if _, ok, err := e.stateStore.GetOpenInviteRedemption(ctx, tokenHash, redeemerKey); err != nil {
				return err
			} else if ok {
				return nil
			}
		}
		return esphttp.ErrOpenInviteExhausted
	}
	return nil
}

type openInviteChallengeResponse struct {
	ChallengeID          string `json:"challenge_id"`
	SigningPayload       string `json:"signing_payload"`
	SigningPayloadSHA256 string `json:"signing_payload_sha256"`
	ExpiresAtMS          int64  `json:"expires_at_ms"`
}

type openInviteRedeemResponse struct {
	Status         string           `json:"status"`
	GroupID        entmoot.GroupID  `json:"group_id"`
	Invite         entmoot.Invite   `json:"invite"`
	MaxUses        int              `json:"max_uses"`
	UseCount       int              `json:"use_count"`
	BootstrapPeers []entmoot.NodeID `json:"bootstrap_peers,omitempty"`
	ExpiresAtMS    int64            `json:"expires_at_ms"`
}

func parseOpenInviteAcceptPayload(payload openInviteAcceptPayload) (*url.URL, string, error) {
	token := strings.TrimSpace(payload.Token)
	if token == "" {
		return nil, "", &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "open invite token is required"}
	}
	issuerRaw := strings.TrimSpace(payload.IssuerURL)
	if issuerRaw == "" {
		return nil, "", &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "issuer_url is required"}
	}
	issuer, err := url.Parse(issuerRaw)
	if err != nil || issuer.Scheme == "" || issuer.Host == "" {
		return nil, "", &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "issuer_url must be an absolute http(s) URL"}
	}
	if issuer.User != nil {
		return nil, "", &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "issuer_url must not contain credentials"}
	}
	if issuer.Scheme != "https" && !(issuer.Scheme == "http" && issuerHostAllowsCleartext(issuer.Hostname())) {
		return nil, "", &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "issuer_url must use https except for localhost or .local development hosts"}
	}
	issuer.RawQuery = ""
	issuer.Fragment = ""
	return issuer, token, nil
}

func issuerHostAllowsCleartext(host string) bool {
	host = strings.ToLower(strings.Trim(host, "[]"))
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasSuffix(host, ".local")
}

func openInviteIssuerEndpoint(base *url.URL, token, suffix string) string {
	u := *base
	prefix := strings.TrimRight(u.EscapedPath(), "/")
	u.Path = prefix + "/v1/open-invites/" + url.PathEscape(token) + "/" + suffix
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func (e espOperationExecutor) redeemOpenInviteFromIssuer(ctx context.Context, issuer *url.URL, token string) (entmoot.Invite, openInviteRedeemResponse, error) {
	nodeID, pilotPub, err := e.localPilotIdentity(ctx)
	if err != nil {
		return entmoot.Invite{}, openInviteRedeemResponse{}, err
	}
	if e.identity == nil || len(e.identity.PublicKey) != ed25519.PublicKeySize {
		return entmoot.Invite{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "identity_unavailable", Message: "local Entmoot identity is not configured"}
	}
	pilotPubBytes, err := base64.StdEncoding.DecodeString(pilotPub)
	if err != nil || len(pilotPubBytes) != ed25519.PublicKeySize {
		return entmoot.Invite{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot identity public key is invalid"}
	}
	entmootPub := base64.StdEncoding.EncodeToString(e.identity.PublicKey)
	claim := map[string]any{
		"pilot_node_id":  nodeID,
		"pilot_pubkey":   pilotPub,
		"entmoot_pubkey": entmootPub,
	}
	var challenge openInviteChallengeResponse
	if err := e.postIssuerJSON(ctx, openInviteIssuerEndpoint(issuer, token, "challenge"), claim, &challenge); err != nil {
		return entmoot.Invite{}, openInviteRedeemResponse{}, err
	}
	proof, err := base64.StdEncoding.DecodeString(challenge.SigningPayload)
	if err != nil || len(proof) == 0 {
		return entmoot.Invite{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_bad_response", Message: "issuer returned an invalid challenge"}
	}
	if challenge.SigningPayloadSHA256 != "" && sha256Base64(proof) != challenge.SigningPayloadSHA256 {
		return entmoot.Invite{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_bad_response", Message: "issuer challenge digest does not match payload"}
	}
	challengeGroupID, err := validateOpenInviteIssuerChallenge(proof, challenge.ChallengeID, token, nodeID, pilotPubBytes, e.identity.PublicKey, time.Now().UnixMilli())
	if err != nil {
		return entmoot.Invite{}, openInviteRedeemResponse{}, err
	}
	signature, err := e.signPilotChallenge(ctx, proof)
	if err != nil {
		return entmoot.Invite{}, openInviteRedeemResponse{}, err
	}
	redeemReq := map[string]any{
		"pilot_node_id":   nodeID,
		"pilot_pubkey":    pilotPub,
		"entmoot_pubkey":  entmootPub,
		"challenge_id":    challenge.ChallengeID,
		"pilot_signature": signature,
	}
	var redeemed openInviteRedeemResponse
	if err := e.postIssuerJSON(ctx, openInviteIssuerEndpoint(issuer, token, "redeem"), redeemReq, &redeemed); err != nil {
		return entmoot.Invite{}, openInviteRedeemResponse{}, err
	}
	if redeemed.Invite.GroupID == (entmoot.GroupID{}) {
		return entmoot.Invite{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_bad_response", Message: "issuer redemption response did not include an invite"}
	}
	if redeemed.Invite.GroupID != challengeGroupID {
		return entmoot.Invite{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_bad_response", Message: "issuer redemption invite group does not match challenge"}
	}
	if redeemed.GroupID != (entmoot.GroupID{}) && redeemed.GroupID != challengeGroupID {
		return entmoot.Invite{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_bad_response", Message: "issuer redemption group does not match challenge"}
	}
	return redeemed.Invite, redeemed, nil
}

func validateOpenInviteIssuerChallenge(proof []byte, challengeID, token string, pilotNodeID entmoot.NodeID, pilotPub, entmootPub []byte, nowMS int64) (entmoot.GroupID, error) {
	var env openInvitePilotProofEnvelope
	if err := json.Unmarshal(proof, &env); err != nil {
		return entmoot.GroupID{}, issuerChallengeError("issuer returned a malformed challenge payload")
	}
	canonicalProof, err := canonical.Encode(env)
	if err != nil || !bytes.Equal(canonicalProof, proof) {
		return entmoot.GroupID{}, issuerChallengeError("issuer returned a non-canonical challenge payload")
	}
	if env.Type != "entmoot.open_invite.redeem.v1" {
		return entmoot.GroupID{}, issuerChallengeError("issuer challenge type is invalid")
	}
	if env.TokenHash != esphttp.HashOpenInviteToken(token) {
		return entmoot.GroupID{}, issuerChallengeError("issuer challenge token does not match")
	}
	if strings.TrimSpace(challengeID) == "" || env.ChallengeID != challengeID {
		return entmoot.GroupID{}, issuerChallengeError("issuer challenge id does not match")
	}
	if env.GroupID == (entmoot.GroupID{}) {
		return entmoot.GroupID{}, issuerChallengeError("issuer challenge group is invalid")
	}
	if env.ExpiresAtMS <= nowMS {
		return entmoot.GroupID{}, issuerChallengeError("issuer challenge is expired")
	}
	if env.PilotNodeID != pilotNodeID || !bytes.Equal(env.PilotPubKey, pilotPub) || !bytes.Equal(env.EntmootPubKey, entmootPub) {
		return entmoot.GroupID{}, issuerChallengeError("issuer challenge identity does not match")
	}
	return env.GroupID, nil
}

func issuerChallengeError(message string) *esphttp.OperationError {
	return &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_bad_response", Message: message}
}

func (e espOperationExecutor) postIssuerJSON(ctx context.Context, endpoint string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid issuer_url"}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "issuer_unavailable", Message: "open invite issuer is not reachable: " + err.Error()}
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "issuer_unavailable", Message: "open invite issuer response could not be read"}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			return &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_redirect_disallowed", Message: "open invite issuer redirects are not allowed"}
		}
		var env struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		code, msg := "issuer_error", strings.TrimSpace(string(respBody))
		if err := json.Unmarshal(respBody, &env); err == nil && env.Error.Code != "" {
			code, msg = env.Error.Code, env.Error.Message
		}
		if msg == "" {
			msg = "open invite issuer rejected the request"
		}
		return &esphttp.OperationError{HTTPStatus: resp.StatusCode, Code: code, Message: msg}
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_bad_response", Message: "open invite issuer returned invalid JSON"}
	}
	return nil
}

func (e espOperationExecutor) acceptInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	invite, err := parseInviteAccept(req.Payload)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	fleetPlan, fleetAccept, err := e.preflightFleetInviteAcceptance(ctx, invite.GroupID)
	if err != nil {
		return nil, err
	}
	var (
		fleet       esphttp.FleetRecord
		fleetMember esphttp.FleetMemberRecord
		rollback    func()
	)
	if fleetAccept {
		fleet, fleetMember, rollback, err = e.prepareFleetInviteAcceptance(ctx, fleetPlan)
		if err != nil {
			return nil, err
		}
		defer func() {
			if rollback != nil {
				rollback()
			}
		}()
	}
	deviceGroupGranted := false
	if req.DeviceID != "" {
		if fleetAccept {
			if e.deviceGroups == nil {
				return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "device_registry_unavailable", Message: "device group authorizer is not configured"}
			}
			changed, err := e.grantDeviceGroup(ctx, req.DeviceID, invite.GroupID)
			if err != nil {
				return nil, err
			}
			deviceGroupGranted = changed
			defer func() {
				if deviceGroupGranted && e.deviceGroups != nil {
					if err := e.deviceGroups.RevokeDeviceGroup(context.Background(), req.DeviceID, invite.GroupID); err != nil {
						slog.Warn("esp invite_accept rollback: revoke device group failed", slog.String("group_id", invite.GroupID.String()), slog.String("device_id", req.DeviceID), slog.String("err", err.Error()))
					}
				}
			}()
		}
	}
	resp, err := e.joinGroup(ctx, invite)
	if err != nil {
		return nil, err
	}
	groupID := resp.GroupID
	if groupID == (entmoot.GroupID{}) {
		groupID = invite.GroupID
	}
	if !fleetAccept {
		if err := e.grantDeviceGroupIfNeeded(ctx, req.DeviceID, groupID); err != nil {
			return nil, err
		}
	}
	rollback = nil
	deviceGroupGranted = false
	result := map[string]any{
		"status":   resp.Status,
		"group_id": groupID,
		"members":  resp.Members,
	}
	if fleetAccept {
		result["fleet"] = fleet
		result["fleet_member"] = fleetMember
	}
	return json.Marshal(result)
}

func (e espOperationExecutor) acceptOpenInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	var payload openInviteAcceptPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid open_invite_accept payload"}
	}
	issuer, token, err := parseOpenInviteAcceptPayload(payload)
	if err != nil {
		return nil, err
	}
	invite, issuerResult, err := e.redeemOpenInviteFromIssuer(ctx, issuer, token)
	if err != nil {
		return nil, err
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
		"status":          resp.Status,
		"group_id":        groupID,
		"members":         resp.Members,
		"issuer_url":      issuer.String(),
		"max_uses":        issuerResult.MaxUses,
		"use_count":       issuerResult.UseCount,
		"expires_at_ms":   issuerResult.ExpiresAtMS,
		"bootstrap_peers": issuerResult.BootstrapPeers,
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

func (e espOperationExecutor) createFleetControlGroup(ctx context.Context, controlGID entmoot.GroupID, fleetID, name string, founder entmoot.NodeInfo, createdAtMS int64) (entmoot.GroupID, error) {
	groupPath := groupDirPath(e.dataDir, controlGID)
	groupPreexisted := pathExists(groupPath)
	committed := false
	metadataWritten := false
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
			if err := e.metadataStore.DeleteGroupMetadata(context.Background(), controlGID); err != nil {
				slog.Warn("esp fleet_create rollback: delete control group metadata failed", slog.String("group_id", controlGID.String()), slog.String("fleet_id", fleetID), slog.String("err", err.Error()))
			}
		}
		if !groupPreexisted {
			if err := os.RemoveAll(groupPath); err != nil {
				slog.Warn("esp fleet_create rollback: remove control group dir failed", slog.String("group_id", controlGID.String()), slog.String("fleet_id", fleetID), slog.String("path", groupPath), slog.String("err", err.Error()))
			}
		}
	}()
	now := createdAtMS
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	var err error
	st, err = store.OpenSQLite(e.dataDir)
	if err != nil {
		return entmoot.GroupID{}, err
	}
	rlog, err = roster.OpenJSONL(e.dataDir, controlGID)
	if err != nil {
		return entmoot.GroupID{}, err
	}
	if err := rlog.Genesis(e.identity, founder, now); err != nil {
		return entmoot.GroupID{}, err
	}
	root, err := st.MerkleRoot(ctx, controlGID)
	if err != nil {
		return entmoot.GroupID{}, err
	}
	if e.metadataStore != nil {
		meta, err := json.Marshal(map[string]any{
			"name":          "Fleet Control: " + name,
			"description":   "Hidden Fleet control channel",
			"hidden":        true,
			"fleet_control": true,
			"fleet_id":      fleetID,
		})
		if err != nil {
			return entmoot.GroupID{}, err
		}
		if err := e.metadataStore.SetGroupMetadata(ctx, controlGID, meta); err != nil {
			return entmoot.GroupID{}, err
		}
		metadataWritten = true
	}
	invite := entmoot.Invite{
		GroupID:    controlGID,
		Founder:    founder,
		RosterHead: rlog.Head(),
		MerkleRoot: root,
		IssuedAt:   now,
		ValidUntil: now + int64((24*time.Hour)/time.Millisecond),
		Issuer:     founder,
	}
	if err := signInvite(e.identity, &invite); err != nil {
		return entmoot.GroupID{}, err
	}
	if err := rlog.Close(); err != nil {
		return entmoot.GroupID{}, err
	}
	rlog = nil
	if err := st.Close(); err != nil {
		return entmoot.GroupID{}, err
	}
	st = nil
	if _, err := e.joinGroup(ctx, invite); err != nil {
		return entmoot.GroupID{}, err
	}
	committed = true
	return controlGID, nil
}

func (e espOperationExecutor) appendFleetActivity(ctx context.Context, fleetID, typ string, actor entmoot.NodeInfo, subject *entmoot.NodeInfo, summary string, metadata map[string]any) (esphttp.FleetActivityRecord, error) {
	if e.stateStore == nil {
		return esphttp.FleetActivityRecord{}, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "fleet_unavailable", Message: "fleet store is not configured"}
	}
	var raw json.RawMessage
	if metadata != nil {
		data, err := json.Marshal(metadata)
		if err != nil {
			return esphttp.FleetActivityRecord{}, err
		}
		raw = data
	}
	return e.stateStore.AppendFleetActivity(ctx, esphttp.FleetActivityRecord{
		FleetID:     fleetID,
		Type:        typ,
		Actor:       actor,
		Subject:     subject,
		Summary:     summary,
		Metadata:    raw,
		CreatedAtMS: time.Now().UnixMilli(),
	})
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

type fleetInviteAcceptancePlan struct {
	fleet         esphttp.FleetRecord
	target        entmoot.NodeInfo
	member        esphttp.FleetMemberRecord
	previous      esphttp.FleetMemberRecord
	previousFound bool
	invites       []esphttp.FleetInviteRecord
	noop          bool
}

func (e espOperationExecutor) preflightFleetInviteAcceptance(ctx context.Context, groupID entmoot.GroupID) (*fleetInviteAcceptancePlan, bool, error) {
	if e.stateStore == nil || groupID == (entmoot.GroupID{}) || e.identity == nil {
		return nil, false, nil
	}
	fleets, err := e.stateStore.ListFleets(ctx)
	if err != nil {
		return nil, false, err
	}
	var fleet esphttp.FleetRecord
	found := false
	for _, candidate := range fleets {
		if candidate.ControlGroupID == groupID {
			fleet = candidate
			found = true
			break
		}
	}
	if !found {
		return nil, false, nil
	}
	info, err := e.daemonInfo()
	if err != nil {
		return nil, false, joinUnavailableError(err)
	}
	target := entmoot.NodeInfo{PilotNodeID: info.PilotNodeID, EntmootPubKey: append([]byte(nil), e.identity.PublicKey...)}
	targetPubKey := encodeBase64(target.EntmootPubKey)
	members, err := e.stateStore.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		return nil, false, err
	}
	prev, exists := fleetMemberForNode(members, target.PilotNodeID)
	if exists {
		storedPub, err := base64.StdEncoding.DecodeString(prev.EntmootPubKey)
		if err != nil || !bytes.Equal(storedPub, target.EntmootPubKey) {
			return nil, false, &esphttp.OperationError{
				HTTPStatus: http.StatusConflict,
				Code:       "member_identity_conflict",
				Message:    "accepted invite identity does not match fleet member",
			}
		}
		if prev.Role == esphttp.FleetRoleCoordinator {
			return &fleetInviteAcceptancePlan{fleet: fleet, target: target, member: prev, previous: prev, previousFound: true, noop: true}, true, nil
		}
		if prev.Status == esphttp.FleetMemberActive {
			return &fleetInviteAcceptancePlan{fleet: fleet, target: target, member: prev, previous: prev, previousFound: true, noop: true}, true, nil
		}
	}
	now := time.Now().UnixMilli()
	invites, err := e.stateStore.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		return nil, false, err
	}
	matchingInvite := false
	identityConflict := false
	targetInvites := make([]esphttp.FleetInviteRecord, 0, 1)
	for _, invite := range invites {
		if invite.NodeID != target.PilotNodeID {
			continue
		}
		targetInvites = append(targetInvites, invite)
		if invite.EntmootPubKey != targetPubKey {
			identityConflict = true
			continue
		}
		if invite.Status != esphttp.FleetMemberInvited {
			continue
		}
		if invite.ExpiresAtMS > 0 && invite.ExpiresAtMS <= now {
			continue
		}
		matchingInvite = true
	}
	if !exists || prev.Status != esphttp.FleetMemberInvited || !matchingInvite {
		if identityConflict {
			return nil, false, &esphttp.OperationError{
				HTTPStatus: http.StatusConflict,
				Code:       "member_identity_conflict",
				Message:    "accepted invite identity does not match fleet invite",
			}
		}
		return nil, false, &esphttp.OperationError{
			HTTPStatus: http.StatusConflict,
			Code:       "fleet_invite_required",
			Message:    "fleet invite acceptance requires a current pending fleet invite",
		}
	}
	member := esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        target.PilotNodeID,
		EntmootPubKey: targetPubKey,
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  now,
	}
	if exists {
		member.Hostname = prev.Hostname
		member.InvitedAtMS = prev.InvitedAtMS
	}
	return &fleetInviteAcceptancePlan{
		fleet:         fleet,
		target:        target,
		member:        member,
		previous:      prev,
		previousFound: exists,
		invites:       targetInvites,
	}, true, nil
}

func (e espOperationExecutor) prepareFleetInviteAcceptance(ctx context.Context, plan *fleetInviteAcceptancePlan) (esphttp.FleetRecord, esphttp.FleetMemberRecord, func(), error) {
	if plan == nil {
		return esphttp.FleetRecord{}, esphttp.FleetMemberRecord{}, nil, nil
	}
	if plan.noop {
		return plan.fleet, plan.member, nil, nil
	}
	fleet := plan.fleet
	target := plan.target
	member := plan.member
	memberApplied := false
	var removedInvites []esphttp.FleetInviteRecord
	activityApplied := false
	activityID := ""
	rollback := func() {
		if e.stateStore == nil {
			return
		}
		if activityApplied {
			if err := e.stateStore.DeleteFleetActivity(context.Background(), fleet.FleetID, activityID); err != nil {
				slog.Warn("esp invite_accept rollback: delete fleet activity failed", slog.String("fleet_id", fleet.FleetID), slog.String("event_id", activityID), slog.String("err", err.Error()))
			}
		}
		if memberApplied {
			if plan.previousFound {
				if _, err := e.stateStore.UpsertFleetMember(context.Background(), plan.previous); err != nil {
					slog.Warn("esp invite_accept rollback: restore fleet member failed", slog.String("fleet_id", fleet.FleetID), slog.Uint64("node_id", uint64(target.PilotNodeID)), slog.String("err", err.Error()))
				}
			} else if err := e.stateStore.DeleteFleetMember(context.Background(), fleet.FleetID, target.PilotNodeID); err != nil {
				slog.Warn("esp invite_accept rollback: restore fleet member failed", slog.String("fleet_id", fleet.FleetID), slog.Uint64("node_id", uint64(target.PilotNodeID)), slog.String("err", err.Error()))
			}
		}
		for _, invite := range removedInvites {
			if _, err := e.stateStore.CreateFleetInvite(context.Background(), invite); err != nil {
				slog.Warn("esp invite_accept rollback: restore fleet invite failed", slog.String("fleet_id", fleet.FleetID), slog.String("invite_id", invite.InviteID), slog.String("err", err.Error()))
			}
		}
	}
	member, err := e.stateStore.UpsertFleetMember(ctx, member)
	if err != nil {
		return esphttp.FleetRecord{}, esphttp.FleetMemberRecord{}, nil, err
	}
	memberApplied = true
	for _, invite := range plan.invites {
		if err := e.stateStore.DeleteFleetInvite(ctx, invite.InviteID); err != nil {
			rollback()
			return esphttp.FleetRecord{}, esphttp.FleetMemberRecord{}, nil, err
		}
		removedInvites = append(removedInvites, invite)
	}
	activity, err := e.appendFleetActivity(ctx, fleet.FleetID, "member.accepted", target, &target, "Agent joined Fleet", map[string]any{"hostname": member.Hostname})
	if err != nil {
		rollback()
		return esphttp.FleetRecord{}, esphttp.FleetMemberRecord{}, nil, err
	}
	activityApplied = true
	activityID = activity.EventID
	return fleet, member, rollback, nil
}

func (e espOperationExecutor) createFleet(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if e.stateStore == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "fleet_unavailable", Message: "fleet store is not configured"}
	}
	var payload fleetCreatePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid fleet_create payload"}
	}
	name, err := esphttp.NormalizeFleetName(payload.Name)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	fleetID, err := esphttp.NewFleetID()
	if err != nil {
		return nil, err
	}
	var controlGID entmoot.GroupID
	if _, err := rand.Read(controlGID[:]); err != nil {
		return nil, err
	}
	info, err := e.daemonInfo()
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	coordinator := entmoot.NodeInfo{PilotNodeID: info.PilotNodeID, EntmootPubKey: append([]byte(nil), e.identity.PublicKey...)}
	now := req.CreatedAtMS
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	committed := false
	fleetCreated := false
	defer func() {
		if committed || !fleetCreated || e.stateStore == nil {
			return
		}
		if err := e.stateStore.DeleteFleet(context.Background(), fleetID); err != nil {
			slog.Warn("esp fleet_create rollback: delete fleet state failed", slog.String("fleet_id", fleetID), slog.String("err", err.Error()))
		}
	}()
	fleet, err := e.stateStore.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:             fleetID,
		Name:                name,
		ControlGroupID:      controlGID,
		Coordinator:         coordinator,
		CoordinatorDeviceID: req.DeviceID,
		CreatedAtMS:         now,
	})
	if err != nil {
		return nil, err
	}
	fleetCreated = true
	_, err = e.stateStore.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        coordinator.PilotNodeID,
		EntmootPubKey: encodeBase64(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  now,
	})
	if err != nil {
		return nil, err
	}
	activity, err := e.appendFleetActivity(ctx, fleet.FleetID, "fleet.created", coordinator, nil, "Fleet created", map[string]any{"name": name})
	if err != nil {
		return nil, err
	}
	if _, err := e.createFleetControlGroup(ctx, controlGID, fleetID, name, coordinator, req.CreatedAtMS); err != nil {
		return nil, err
	}
	committed = true
	return json.Marshal(map[string]any{"status": "created", "fleet": fleet, "activity": activity})
}

func (e espOperationExecutor) createFleetInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if e.stateStore == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "fleet_unavailable", Message: "fleet store is not configured"}
	}
	var payload fleetInviteCreatePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid fleet_invite_create payload"}
	}
	payload.FleetID = strings.TrimSpace(payload.FleetID)
	if payload.FleetID == "" {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "fleet_invite_create requires fleet_id"}
	}
	fleet, ok, err := e.stateStore.GetFleet(ctx, payload.FleetID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "fleet_not_found", Message: "fleet not found"}
	}
	if req.DeviceID != "" && fleet.CoordinatorDeviceID != "" && req.DeviceID != fleet.CoordinatorDeviceID {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusForbidden, Code: "forbidden", Message: "device is not authorized to manage fleet"}
	}
	target, err := validateInviteTarget(payload.Target)
	if err != nil {
		return nil, err
	}
	if target.PilotNodeID == fleet.Coordinator.PilotNodeID && bytes.Equal(target.EntmootPubKey, fleet.Coordinator.EntmootPubKey) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "fleet invite target is already the coordinator"}
	}
	members, err := e.stateStore.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		return nil, err
	}
	if existing, ok := fleetMemberForNode(members, target.PilotNodeID); ok && existing.Status == esphttp.FleetMemberActive {
		return nil, &esphttp.OperationError{
			HTTPStatus: http.StatusConflict,
			Code:       "fleet_member_exists",
			Message:    "target is already a fleet member",
		}
	}
	prevMember, prevMemberExists := fleetMemberForNode(members, target.PilotNodeID)
	invites, err := e.stateStore.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		return nil, err
	}
	var staleInvites []esphttp.FleetInviteRecord
	for _, invite := range invites {
		if invite.NodeID == target.PilotNodeID {
			staleInvites = append(staleInvites, invite)
		}
	}
	var removedStaleInvites []esphttp.FleetInviteRecord
	memberApplied := false
	controlInviteApplied := false
	inviteCreated := false
	inviteID := ""
	activityApplied := false
	activityID := ""
	committed := false
	now := time.Now().UnixMilli()
	defer func() {
		if committed || e.stateStore == nil {
			return
		}
		if controlInviteApplied {
			if _, err := e.revokeFleetControlMember(context.Background(), fleet.ControlGroupID, target); err != nil {
				slog.Warn("esp fleet_invite_create rollback: revoke control invite failed", slog.String("fleet_id", fleet.FleetID), slog.Uint64("node_id", uint64(target.PilotNodeID)), slog.String("err", err.Error()))
			}
		}
		if activityApplied {
			if err := e.stateStore.DeleteFleetActivity(context.Background(), fleet.FleetID, activityID); err != nil {
				slog.Warn("esp fleet_invite_create rollback: delete activity failed", slog.String("fleet_id", fleet.FleetID), slog.String("event_id", activityID), slog.String("err", err.Error()))
			}
		}
		if inviteCreated {
			if err := e.stateStore.DeleteFleetInvite(context.Background(), inviteID); err != nil {
				slog.Warn("esp fleet_invite_create rollback: delete invite failed", slog.String("fleet_id", fleet.FleetID), slog.String("invite_id", inviteID), slog.String("err", err.Error()))
			}
		}
		if memberApplied {
			if prevMemberExists {
				if _, err := e.stateStore.UpsertFleetMember(context.Background(), prevMember); err != nil {
					slog.Warn("esp fleet_invite_create rollback: restore member failed", slog.String("fleet_id", fleet.FleetID), slog.Uint64("node_id", uint64(target.PilotNodeID)), slog.String("err", err.Error()))
				}
			} else {
				if err := e.stateStore.DeleteFleetMember(context.Background(), fleet.FleetID, target.PilotNodeID); err != nil {
					slog.Warn("esp fleet_invite_create rollback: delete member failed", slog.String("fleet_id", fleet.FleetID), slog.Uint64("node_id", uint64(target.PilotNodeID)), slog.String("err", err.Error()))
				}
			}
		}
		for _, invite := range removedStaleInvites {
			if _, err := e.stateStore.CreateFleetInvite(context.Background(), invite); err != nil {
				slog.Warn("esp fleet_invite_create rollback: restore stale invite failed", slog.String("fleet_id", fleet.FleetID), slog.String("invite_id", invite.InviteID), slog.String("err", err.Error()))
			}
		}
	}()
	member, err := e.stateStore.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        target.PilotNodeID,
		EntmootPubKey: encodeBase64(target.EntmootPubKey),
		Hostname:      strings.TrimSpace(payload.Hostname),
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   time.Now().UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	memberApplied = true
	for _, invite := range staleInvites {
		if err := e.stateStore.DeleteFleetInvite(ctx, invite.InviteID); err != nil {
			return nil, err
		}
		removedStaleInvites = append(removedStaleInvites, invite)
	}
	ipcReq, err := buildInviteCreateIPCRequest(fleet.ControlGroupID, inviteCreatePayload{
		ValidFor:     payload.ValidFor,
		ValidUntilMS: payload.ValidUntilMS,
		Target:       payload.Target,
	})
	if err != nil {
		return nil, err
	}
	resp, err := e.createInviteOverIPC(ctx, ipcReq)
	if err != nil {
		if resp != nil && resp.sent && !resp.rejected {
			controlInviteApplied = true
		}
		return nil, err
	}
	controlInviteApplied = true
	inviteRaw, err := json.Marshal(resp.Invite)
	if err != nil {
		return nil, err
	}
	invite, err := e.stateStore.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		FleetID:       fleet.FleetID,
		NodeID:        target.PilotNodeID,
		EntmootPubKey: encodeBase64(target.EntmootPubKey),
		Hostname:      strings.TrimSpace(payload.Hostname),
		Status:        esphttp.FleetMemberInvited,
		Invite:        inviteRaw,
		CreatedAtMS:   now,
		ExpiresAtMS:   resp.Invite.ValidUntil,
	})
	if err != nil {
		return nil, err
	}
	inviteCreated = true
	inviteID = invite.InviteID
	activity, err := e.appendFleetActivity(ctx, fleet.FleetID, "member.invited", fleet.Coordinator, &target, "Agent invited to Fleet", map[string]any{"hostname": payload.Hostname})
	if err != nil {
		return nil, err
	}
	activityApplied = true
	activityID = activity.EventID
	committed = true
	return json.Marshal(map[string]any{"status": "invited", "fleet": fleet, "member": member, "invite": invite, "activity": activity})
}

func (e espOperationExecutor) removeFleetMember(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if e.stateStore == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "fleet_unavailable", Message: "fleet store is not configured"}
	}
	var payload fleetMemberRemovePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid fleet_member_remove payload"}
	}
	payload.FleetID = strings.TrimSpace(payload.FleetID)
	if payload.FleetID == "" {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "fleet_member_remove requires fleet_id"}
	}
	fleet, ok, err := e.stateStore.GetFleet(ctx, payload.FleetID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "fleet_not_found", Message: "fleet not found"}
	}
	if req.DeviceID != "" && fleet.CoordinatorDeviceID != "" && req.DeviceID != fleet.CoordinatorDeviceID {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusForbidden, Code: "forbidden", Message: "device is not authorized to manage fleet"}
	}
	target, err := validateInviteTargetForRemove(payload.Target)
	if err != nil {
		return nil, err
	}
	members, err := e.stateStore.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		return nil, err
	}
	prevMember, prevMemberExists := fleetMemberForNode(members, target.PilotNodeID)
	if !prevMemberExists {
		return nil, &esphttp.OperationError{
			HTTPStatus: http.StatusNotFound,
			Code:       "not_member",
			Message:    "target is not a fleet member",
		}
	}
	if prevMember.Role == esphttp.FleetRoleCoordinator || target.PilotNodeID == fleet.Coordinator.PilotNodeID {
		return nil, &esphttp.OperationError{
			HTTPStatus: http.StatusBadRequest,
			Code:       "bad_request",
			Message:    "cannot remove fleet coordinator",
		}
	}
	storedPub, err := base64.StdEncoding.DecodeString(prevMember.EntmootPubKey)
	if err != nil || !bytes.Equal(storedPub, target.EntmootPubKey) {
		return nil, &esphttp.OperationError{
			HTTPStatus: http.StatusConflict,
			Code:       "member_identity_conflict",
			Message:    "target identity does not match fleet member",
		}
	}
	invites, err := e.stateStore.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		return nil, err
	}
	var deletedInvites []esphttp.FleetInviteRecord
	for _, invite := range invites {
		if invite.NodeID == target.PilotNodeID {
			deletedInvites = append(deletedInvites, invite)
		}
	}
	var removedInvites []esphttp.FleetInviteRecord
	if prevMember.Status == esphttp.FleetMemberRemoved {
		for _, invite := range deletedInvites {
			if err := e.stateStore.DeleteFleetInvite(ctx, invite.InviteID); err != nil {
				for _, removed := range removedInvites {
					if _, restoreErr := e.stateStore.CreateFleetInvite(context.Background(), removed); restoreErr != nil {
						slog.Warn("esp fleet_member_remove already-removed rollback: restore invite failed", slog.String("fleet_id", fleet.FleetID), slog.String("invite_id", removed.InviteID), slog.String("err", restoreErr.Error()))
					}
				}
				return nil, err
			}
			removedInvites = append(removedInvites, invite)
		}
		return json.Marshal(map[string]any{
			"status":   "removed",
			"fleet":    fleet,
			"member":   prevMember,
			"activity": nil,
		})
	}
	memberApplied := false
	activityApplied := false
	activityID := ""
	committed := false
	defer func() {
		if committed || e.stateStore == nil || !memberApplied {
			return
		}
		if activityApplied {
			if err := e.stateStore.DeleteFleetActivity(context.Background(), fleet.FleetID, activityID); err != nil {
				slog.Warn("esp fleet_member_remove rollback: delete activity failed", slog.String("fleet_id", fleet.FleetID), slog.String("event_id", activityID), slog.String("err", err.Error()))
			}
		}
		if prevMemberExists {
			if _, err := e.stateStore.UpsertFleetMember(context.Background(), prevMember); err != nil {
				slog.Warn("esp fleet_member_remove rollback: restore member failed", slog.String("fleet_id", fleet.FleetID), slog.Uint64("node_id", uint64(target.PilotNodeID)), slog.String("err", err.Error()))
			}
		} else {
			if err := e.stateStore.DeleteFleetMember(context.Background(), fleet.FleetID, target.PilotNodeID); err != nil {
				slog.Warn("esp fleet_member_remove rollback: delete member failed", slog.String("fleet_id", fleet.FleetID), slog.Uint64("node_id", uint64(target.PilotNodeID)), slog.String("err", err.Error()))
			}
		}
		for _, invite := range removedInvites {
			if _, err := e.stateStore.CreateFleetInvite(context.Background(), invite); err != nil {
				slog.Warn("esp fleet_member_remove rollback: restore invite failed", slog.String("fleet_id", fleet.FleetID), slog.String("invite_id", invite.InviteID), slog.String("err", err.Error()))
			}
		}
	}()
	member, err := e.stateStore.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        target.PilotNodeID,
		EntmootPubKey: encodeBase64(target.EntmootPubKey),
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberRemoved,
		RemovedAtMS:   time.Now().UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	memberApplied = true
	for _, invite := range deletedInvites {
		if err := e.stateStore.DeleteFleetInvite(ctx, invite.InviteID); err != nil {
			return nil, err
		}
		removedInvites = append(removedInvites, invite)
	}
	activity, err := e.appendFleetActivity(ctx, fleet.FleetID, "member.removed", fleet.Coordinator, &target, "Agent removed from Fleet", nil)
	if err != nil {
		return nil, err
	}
	activityApplied = true
	activityID = activity.EventID
	removeResp, err := e.revokeFleetControlMember(ctx, fleet.ControlGroupID, target)
	if err != nil {
		if removeResp != nil && removeResp.sent && !removeResp.rejected {
			committed = true
		}
		return nil, err
	}
	committed = true
	return json.Marshal(map[string]any{"status": "removed", "fleet": fleet, "member": member, "activity": activity})
}

func (e espOperationExecutor) revokeFleetControlMember(ctx context.Context, groupID entmoot.GroupID, target entmoot.NodeInfo) (*memberRemoveIPCResult, error) {
	resp, err := e.removeMemberOverIPC(ctx, &ipc.MemberRemoveReq{GroupID: groupID, Target: target})
	if err != nil && operationErrorCode(err) == "not_member" {
		return resp, nil
	}
	return resp, err
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

func fleetMemberForNode(members []esphttp.FleetMemberRecord, nodeID entmoot.NodeID) (esphttp.FleetMemberRecord, bool) {
	for _, member := range members {
		if member.NodeID == nodeID {
			return member, true
		}
	}
	return esphttp.FleetMemberRecord{}, false
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

func (e espOperationExecutor) localPilotIdentity(ctx context.Context) (entmoot.NodeID, string, error) {
	if e.pilotIdentity != nil {
		return e.pilotIdentity(ctx)
	}
	socketPath := e.pilotSocketPath
	if strings.TrimSpace(socketPath) == "" {
		socketPath = "/tmp/pilot.sock"
	}
	drv, err := ipcclient.Connect(socketPath)
	if err != nil {
		return 0, "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot daemon is not reachable: " + err.Error()}
	}
	defer drv.Close()
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	infoCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	info, err := drv.InfoStruct(infoCtx)
	if err != nil {
		return 0, "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot identity is unavailable: " + err.Error()}
	}
	if info.NodeID == 0 || strings.TrimSpace(info.PublicKey) == "" {
		return 0, "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot identity is incomplete"}
	}
	return entmoot.NodeID(info.NodeID), strings.TrimSpace(info.PublicKey), nil
}

func (e espOperationExecutor) signPilotChallenge(ctx context.Context, payload []byte) (string, error) {
	if e.pilotSignChallenge != nil {
		return e.pilotSignChallenge(ctx, payload)
	}
	socketPath := e.pilotSocketPath
	if strings.TrimSpace(socketPath) == "" {
		socketPath = "/tmp/pilot.sock"
	}
	drv, err := ipcclient.Connect(socketPath)
	if err != nil {
		return "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot daemon is not reachable: " + err.Error()}
	}
	defer drv.Close()
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	signCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	sig, err := drv.SignChallenge(signCtx, payload)
	if err != nil {
		return "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot challenge signing failed: " + err.Error()}
	}
	if strings.TrimSpace(sig.Signature) == "" {
		return "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot returned no challenge signature"}
	}
	return strings.TrimSpace(sig.Signature), nil
}

type inviteCreateIPCResult struct {
	*ipc.InviteCreateResp
	sent     bool
	rejected bool
}

type memberRemoveIPCResult struct {
	*ipc.MemberRemoveResp
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

func (e espOperationExecutor) removeMemberOverIPC(ctx context.Context, req *ipc.MemberRemoveReq) (*memberRemoveIPCResult, error) {
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "unix", e.socketPath)
	if err != nil {
		return &memberRemoveIPCResult{}, joinUnavailableError(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return &memberRemoveIPCResult{}, err
	}
	t, body, err := ipc.Encode(req)
	if err != nil {
		return &memberRemoveIPCResult{}, err
	}
	result := &memberRemoveIPCResult{}
	if err := ipc.WriteFrame(conn, t, body); err != nil {
		return result, err
	}
	result.sent = true
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return result, err
	}
	switch v := payload.(type) {
	case *ipc.MemberRemoveResp:
		result.MemberRemoveResp = v
		return result, nil
	case *ipc.ErrorFrame:
		result.rejected = true
		return result, operationIPCError(v)
	default:
		return result, fmt.Errorf("unexpected member remove response %T", payload)
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

func operationErrorCode(err error) string {
	var opErr *esphttp.OperationError
	if errors.As(err, &opErr) && opErr != nil {
		return opErr.Code
	}
	return ""
}
