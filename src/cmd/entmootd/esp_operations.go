package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
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
	"slices"
	"strings"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/publicmoot"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
	libpeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type espOperationExecutor struct {
	identity      *keystore.Identity
	dataDir       string
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

type groupCreatePayload struct {
	Name              string            `json:"name,omitempty"`
	Description       string            `json:"description,omitempty"`
	Tags              []string          `json:"tags,omitempty"`
	Visibility        string            `json:"visibility,omitempty"`
	JoinMode          string            `json:"join_mode,omitempty"`
	PolicySource      string            `json:"policy_source,omitempty"`
	Policy            *entpolicy.Policy `json:"policy,omitempty"`
	IssuerURL         string            `json:"issuer_url,omitempty"`
	OpenInviteMaxUses *int              `json:"open_invite_max_uses,omitempty"`
	Metadata          json.RawMessage   `json:"metadata,omitempty"`
}

type inviteTargetPayload struct {
	MemberID      entmoot.MemberID `json:"member_id"`
	PeerID        string           `json:"peer_id"`
	EntmootPubKey []byte           `json:"entmoot_pubkey"`
}

type inviteCreatePayload struct {
	ValidFor            string               `json:"valid_for,omitempty"`
	ValidUntilMS        int64                `json:"valid_until_ms,omitempty"`
	BootstrapMultiaddrs []string             `json:"bootstrap_multiaddrs,omitempty"`
	Target              *inviteTargetPayload `json:"target,omitempty"`
	// NoFallbackPeers declines the other-member addresses the daemon would
	// attach so the invite outlives this node's uptime.
	NoFallbackPeers bool `json:"no_fallback_peers,omitempty"`
}

type openInviteCreatePayload struct {
	ValidFor            string   `json:"valid_for,omitempty"`
	ValidUntilMS        int64    `json:"valid_until_ms,omitempty"`
	MaxUses             *int     `json:"max_uses"`
	BootstrapMultiaddrs []string `json:"bootstrap_multiaddrs,omitempty"`
	// NoFallbackPeers is carried to redemption, when the capability is
	// actually minted, so a widely shared link can decline to disclose other
	// members' addresses.
	NoFallbackPeers bool `json:"no_fallback_peers,omitempty"`
}

type openInviteAcceptPayload struct {
	IssuerURL string `json:"issuer_url"`
	Token     string `json:"token"`
}

type groupPolicyPayload struct {
	Preset       string            `json:"preset,omitempty"`
	PolicySource string            `json:"policy_source,omitempty"`
	Policy       *entpolicy.Policy `json:"policy,omitempty"`
}

type groupPublicPublishPayload struct {
	ESPURL string `json:"esp_url"`
}

type inviteAcceptPayload struct {
	Capability *entmoot.BootstrapCapability `json:"capability,omitempty"`
}

type openInviteRedeemPayload struct {
	MemberID      entmoot.MemberID `json:"member_id"`
	PeerID        string           `json:"peer_id"`
	EntmootPubKey []byte           `json:"entmoot_pubkey"`
}

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
	case "group_create":
		return e.createGroup(ctx, req)
	case "group_update":
		return e.updateGroup(ctx, req)
	case "group_policy_update":
		return e.updateGroupPolicy(ctx, req)
	case "group_policy_clear":
		return e.clearGroupPolicy(ctx, req)
	case "group_public_publish":
		return e.publishPublicMoot(ctx, req)
	default:
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "unsupported_operation", Message: "unsupported sign request kind"}
	}
}

func (e espOperationExecutor) GroupPolicyReport(ctx context.Context, groupID entmoot.GroupID) (json.RawMessage, error) {
	report, err := loadGroupPolicyReportFromStore(ctx, e.dataDir, groupID)
	if err != nil {
		return nil, err
	}
	return json.Marshal(report)
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
	binding, err := libp2ptransport.BindingFromPublicKey(payload.EntmootPubKey)
	if err != nil || payload.MemberID != binding.MemberID || payload.PeerID != binding.PeerID.String() {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "member_id, peer_id, and entmoot_pubkey must use the same key"}
	}
	tokenHash := esphttp.HashOpenInviteToken(token)
	rec, ok, err := e.stateStore.GetOpenInviteByTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "open_invite_not_found", Message: "open invite not found"}
	}
	redeemerKey := binding.MemberID.String()
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
	if ok && len(existing.Result) > 0 {
		return append(json.RawMessage(nil), existing.Result...), nil
	}
	if !ok && esphttp.OpenInviteUseLimitReached(rec) {
		return nil, openInviteStoreError(esphttp.ErrOpenInviteExhausted)
	}
	rec, redemption, alreadyRedeemed, err := e.stateStore.RedeemOpenInvite(ctx, tokenHash, esphttp.OpenInviteRedemption{
		RedeemerKey:   redeemerKey,
		MemberID:      binding.MemberID,
		PeerID:        binding.PeerID.String(),
		EntmootPubKey: base64.StdEncoding.EncodeToString(payload.EntmootPubKey),
	}, now)
	if err != nil {
		return nil, openInviteStoreError(err)
	}
	if alreadyRedeemed && len(redemption.Result) > 0 {
		return append(json.RawMessage(nil), redemption.Result...), nil
	}
	resp, err := e.createInviteOverIPC(ctx, &ipc.InviteCreateReq{
		GroupID:             rec.GroupID,
		TargetPublicKey:     append([]byte(nil), payload.EntmootPubKey...),
		BootstrapMultiaddrs: append([]string(nil), rec.BootstrapMultiaddrs...),
		NoFallbackPeers:     rec.NoFallbackPeers,
	})
	if err != nil {
		if (!resp.sent || resp.rejected) && !alreadyRedeemed {
			_ = e.stateStore.ReleaseOpenInviteRedemption(ctx, tokenHash, redeemerKey, time.Now().UnixMilli())
		}
		return nil, err
	}
	result, err := json.Marshal(map[string]any{
		"status":               "redeemed",
		"group_id":             resp.GroupID,
		"capability":           resp.Capability,
		"max_uses":             rec.MaxUses,
		"use_count":            rec.UseCount,
		"bootstrap_multiaddrs": rec.BootstrapMultiaddrs,
		"expires_at_ms":        rec.ExpiresAtMS,
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
	case errors.Is(err, sql.ErrNoRows):
		return &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "open_invite_not_found", Message: "open invite not found"}
	default:
		return err
	}
}

type openInviteRedeemResponse struct {
	Status              string                      `json:"status"`
	GroupID             entmoot.GroupID             `json:"group_id"`
	Capability          entmoot.BootstrapCapability `json:"capability"`
	MaxUses             int                         `json:"max_uses"`
	UseCount            int                         `json:"use_count"`
	BootstrapMultiaddrs []string                    `json:"bootstrap_multiaddrs,omitempty"`
	ExpiresAtMS         int64                       `json:"expires_at_ms"`
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

func (e espOperationExecutor) redeemOpenInviteFromIssuer(ctx context.Context, issuer *url.URL, token string) (entmoot.BootstrapCapability, openInviteRedeemResponse, error) {
	if e.identity == nil || len(e.identity.PublicKey) != ed25519.PublicKeySize {
		return entmoot.BootstrapCapability{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "identity_unavailable", Message: "local Entmoot identity is not configured"}
	}
	binding, err := libp2ptransport.BindingFromPublicKey(e.identity.PublicKey)
	if err != nil {
		return entmoot.BootstrapCapability{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "identity_unavailable", Message: "local identity binding is invalid"}
	}
	request := map[string]any{
		"member_id":      binding.MemberID,
		"peer_id":        binding.PeerID.String(),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(e.identity.PublicKey),
	}
	var redeemed openInviteRedeemResponse
	if err := e.postIssuerJSON(ctx, openInviteIssuerEndpoint(issuer, token, "redeem"), request, &redeemed); err != nil {
		return entmoot.BootstrapCapability{}, openInviteRedeemResponse{}, err
	}
	if redeemed.Capability.GroupID == (entmoot.GroupID{}) || redeemed.Capability.GroupID != redeemed.GroupID {
		return entmoot.BootstrapCapability{}, openInviteRedeemResponse{}, &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "issuer_bad_response", Message: "issuer returned an invalid capability group"}
	}
	return redeemed.Capability, redeemed, nil
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
	capability, err := parseInviteAccept(req.Payload)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	resp, err := e.joinGroup(ctx, capability)
	if err != nil {
		return nil, err
	}
	groupID := resp.GroupID
	if err := e.grantDeviceGroupIfNeeded(ctx, req.DeviceID, groupID); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"status": resp.Status, "group_id": groupID, "members": resp.Members})
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
	capability, issuerResult, err := e.redeemOpenInviteFromIssuer(ctx, issuer, token)
	if err != nil {
		return nil, err
	}
	resp, err := e.joinGroup(ctx, capability)
	if err != nil {
		return nil, err
	}
	groupID := resp.GroupID
	if err := e.grantDeviceGroupIfNeeded(ctx, req.DeviceID, groupID); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":               resp.Status,
		"group_id":             groupID,
		"members":              resp.Members,
		"issuer_url":           issuer.String(),
		"max_uses":             issuerResult.MaxUses,
		"use_count":            issuerResult.UseCount,
		"expires_at_ms":        issuerResult.ExpiresAtMS,
		"bootstrap_multiaddrs": issuerResult.BootstrapMultiaddrs,
	})
}

func (e espOperationExecutor) createInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	var payload inviteCreatePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid invite_create payload"}
	}
	target, err := validateInviteTarget(payload.Target)
	if err != nil {
		return nil, err
	}
	ipcReq := &ipc.InviteCreateReq{
		GroupID:             req.GroupID,
		TargetPublicKey:     target.EntmootPubKey,
		BootstrapMultiaddrs: append([]string(nil), payload.BootstrapMultiaddrs...),
		ValidUntilMS:        payload.ValidUntilMS,
		NoFallbackPeers:     payload.NoFallbackPeers,
	}
	if payload.ValidFor != "" {
		ttl, err := parseDurationDays(payload.ValidFor)
		if err != nil || ttl <= 0 {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid valid_for"}
		}
		ipcReq.ValidForMS = ttl.Milliseconds()
	}
	resp, err := e.createInviteOverIPC(ctx, ipcReq)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"status": "created", "group_id": resp.GroupID, "capability": resp.Capability})
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
	if payload.MaxUses == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "max_uses is required"}
	}
	maxUses := *payload.MaxUses
	if err := esphttp.ValidateOpenInviteMaxUses(maxUses); err != nil || maxUses > 100 {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "max_uses must be between 0 and 100 (0 means unlimited)"}
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
	authority, err := e.checkInviteAuthorityOverIPC(ctx, &ipc.InviteAuthorityCheckReq{GroupID: req.GroupID})
	if err != nil {
		return nil, err
	}
	// Validate the addresses here, not at redemption. The token this call
	// returns is the thing that gets shared, and no capability exists yet, so
	// a malformed, non-member or over-long list would otherwise produce a link
	// that fails for every joiner with an error the joiner cannot act on.
	if err := validateOpenInviteBootstrap(payload.BootstrapMultiaddrs, authority.MemberPeerIDs, authority.LocalPeerID, payload.NoFallbackPeers); err != nil {
		return nil, err
	}
	token, tokenHash, err := esphttp.NewOpenInviteToken()
	if err != nil {
		return nil, err
	}
	rec, err := e.stateStore.CreateOpenInvite(ctx, esphttp.OpenInviteRecord{
		TokenHash:           tokenHash,
		GroupID:             req.GroupID,
		DeviceID:            req.DeviceID,
		MaxUses:             maxUses,
		BootstrapMultiaddrs: append([]string(nil), payload.BootstrapMultiaddrs...),
		NoFallbackPeers:     payload.NoFallbackPeers,
		CreatedAtMS:         now,
		ExpiresAtMS:         expires,
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":               "created",
		"group_id":             rec.GroupID,
		"token":                token,
		"max_uses":             rec.MaxUses,
		"use_count":            rec.UseCount,
		"bootstrap_multiaddrs": rec.BootstrapMultiaddrs,
		"expires_at_ms":        rec.ExpiresAtMS,
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
	policyResolution, err := resolveESPGroupCreatePolicy(payload)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	if err := validateESPGroupCreateOpenInvite(payload, metadata); err != nil {
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
	var previousPolicy entpolicy.Policy
	policyHadPrevious := false
	policyWritten := false
	var openInvite *groupCreateOpenInviteOutput
	deviceGroupGranted := false
	deviceAdminGroupGranted := false
	var st *store.SQLite
	var rlog *membership.Group
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
		if policyWritten {
			policyStore, err := entpolicy.OpenFileStore(e.dataDir)
			if err == nil {
				if policyHadPrevious {
					err = policyStore.Put(context.Background(), gid, previousPolicy)
				} else {
					err = policyStore.Delete(context.Background(), gid)
				}
			}
			if err != nil {
				slog.Warn("esp group_create rollback: restore policy failed", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
			}
		}
		if openInvite != nil && e.stateStore != nil {
			if _, _, err := e.stateStore.RevokeOpenInvite(context.Background(), openInvite.TokenHash, time.Now().UnixMilli()); err != nil {
				slog.Warn("esp group_create rollback: revoke open invite failed", slog.String("group_id", gid.String()), slog.String("token_hash", openInvite.TokenHash), slog.String("err", err.Error()))
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
	founder := entmoot.NodeInfo{
		MemberID:      &info.MemberID,
		PeerID:        info.PeerID,
		EntmootPubKey: append([]byte(nil), e.identity.PublicKey...),
	}
	now := req.CreatedAtMS
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	// A deterministic group id may already exist. Reuse it only when it is
	// this founder's group; otherwise two founders would share an id.
	if membership.Exists(e.dataDir, gid) {
		rlog, err = membership.Open(e.dataDir, gid)
		if err != nil {
			return nil, err
		}
		existing := rlog.Founder()
		if existing.MemberID == nil || *existing.MemberID != *founder.MemberID || !bytes.Equal(existing.EntmootPubKey, founder.EntmootPubKey) {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusConflict, Code: "group_create_conflict", Message: "deterministic group id already belongs to another founder"}
		}
	} else {
		rlog, err = membership.Create(e.dataDir, e.identity, founder, gid, membership.DefaultPolicy(), now)
		if err != nil {
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
	policyStore, err := entpolicy.OpenFileStore(e.dataDir)
	if err != nil {
		return nil, err
	}
	previousPolicy, policyHadPrevious, err = policyStore.Get(ctx, gid)
	if err != nil {
		return nil, err
	}
	if policyResolution.Policy != nil {
		if err := policyStore.Put(ctx, gid, *policyResolution.Policy); err != nil {
			return nil, err
		}
	} else if err := policyStore.Delete(ctx, gid); err != nil {
		return nil, err
	}
	policyWritten = true
	if err := rlog.Close(); err != nil {
		return nil, err
	}
	rlog = nil
	if err := st.Close(); err != nil {
		return nil, err
	}
	st = nil
	resp, err := e.activateLocalGroup(ctx, gid)
	if err != nil {
		return nil, err
	}
	openInvite, metadata, err = e.maybeCreateGroupOpenInvite(ctx, req, payload, gid, metadata)
	if err != nil {
		return nil, err
	}
	result, err := json.Marshal(map[string]any{
		"status":            resp.Status,
		"group_id":          gid,
		"members":           resp.Members,
		"founder":           founder,
		"metadata":          json.RawMessage(metadata),
		"policy_configured": policyResolution.Policy != nil,
		"policy_source":     policyResolution.Source,
		"policy_summary":    policySummary(policyResolution.Policy),
		"open_invite":       openInvite,
	})
	if err != nil {
		return nil, err
	}
	committed = true
	return result, nil
}

func resolveESPGroupCreatePolicy(payload groupCreatePayload) (entpolicy.SourceResolution, error) {
	if payload.Policy != nil {
		if strings.TrimSpace(payload.PolicySource) != "" {
			return entpolicy.SourceResolution{}, errors.New("choose either policy_source or policy, not both")
		}
		if err := payload.Policy.Validate(); err != nil {
			return entpolicy.SourceResolution{}, fmt.Errorf("policy: %w", err)
		}
		p := *payload.Policy
		return entpolicy.SourceResolution{Policy: &p, Source: "custom"}, nil
	}
	source := strings.TrimSpace(payload.PolicySource)
	if source == "" {
		source = "preset:" + entpolicy.PresetStandard
	}
	return resolveESPPolicySource(source)
}

func validateESPGroupCreateOpenInvite(payload groupCreatePayload, metadata json.RawMessage) error {
	meta, err := decodeGroupMetadataObject(metadata)
	if err != nil {
		return err
	}
	joinMode, _ := meta["join_mode"].(string)
	if joinMode != groupJoinModeOpenInvite {
		return nil
	}
	if payload.OpenInviteMaxUses != nil {
		if err := esphttp.ValidateOpenInviteMaxUses(*payload.OpenInviteMaxUses); err != nil || *payload.OpenInviteMaxUses > 100 {
			return errors.New("open_invite_max_uses must be between 0 and 100 (0 means unlimited)")
		}
	}
	if strings.TrimSpace(payload.IssuerURL) == "" && strings.TrimSpace(os.Getenv("ENTMOOT_ESP_URL")) == "" {
		return errors.New("open_invite join mode requires issuer_url or ENTMOOT_ESP_URL")
	}
	return nil
}

func policySummary(p *entpolicy.Policy) string {
	if p == nil {
		return ""
	}
	return entpolicy.Summary(*p)
}

func (e espOperationExecutor) maybeCreateGroupOpenInvite(ctx context.Context, req esphttp.SignRequest, payload groupCreatePayload, gid entmoot.GroupID, metadata json.RawMessage) (*groupCreateOpenInviteOutput, json.RawMessage, error) {
	var meta map[string]any
	if err := json.Unmarshal(metadata, &meta); err != nil {
		return nil, metadata, err
	}
	joinMode, _ := meta["join_mode"].(string)
	if joinMode != groupJoinModeOpenInvite {
		return nil, metadata, nil
	}
	if e.stateStore == nil {
		return nil, metadata, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "open_invite_unavailable", Message: "open invite store is not configured"}
	}
	issuerURL, err := groupCreateOpenInviteIssuerURLFor(strings.TrimSpace(payload.IssuerURL))
	if err != nil {
		return nil, metadata, err
	}
	if _, err := e.checkInviteAuthorityOverIPC(ctx, &ipc.InviteAuthorityCheckReq{GroupID: gid}); err != nil {
		return nil, metadata, fmt.Errorf("open invite authority unavailable: %w", err)
	}
	maxUses := esphttp.OpenInviteUnlimitedMaxUses
	if payload.OpenInviteMaxUses != nil {
		maxUses = *payload.OpenInviteMaxUses
	}
	token, tokenHash, err := esphttp.NewOpenInviteToken()
	if err != nil {
		return nil, metadata, err
	}
	now := time.Now().UnixMilli()
	rec, err := e.stateStore.CreateOpenInvite(ctx, esphttp.OpenInviteRecord{
		TokenHash:   tokenHash,
		GroupID:     gid,
		DeviceID:    req.DeviceID,
		MaxUses:     maxUses,
		CreatedAtMS: now,
	})
	if err != nil {
		return nil, metadata, err
	}
	out := &groupCreateOpenInviteOutput{
		Token:       token,
		TokenHash:   rec.TokenHash,
		Link:        fmt.Sprintf("entmoot://open-invite?issuer=%s&token=%s", url.QueryEscape(issuerURL), url.QueryEscape(token)),
		IssuerURL:   issuerURL,
		MaxUses:     rec.MaxUses,
		ExpiresAtMS: rec.ExpiresAtMS,
	}
	if visibility, _ := meta["visibility"].(string); visibility == groupVisibilityPublic {
		updated, err := persistGroupCreatePublicOpenInviteMetadata(ctx, e.metadataStore, gid, metadata, out)
		if err != nil {
			_, _, _ = e.stateStore.RevokeOpenInvite(context.Background(), rec.TokenHash, time.Now().UnixMilli())
			return nil, metadata, fmt.Errorf("persist public open invite descriptor metadata: %w", err)
		}
		metadata = updated
	}
	return out, metadata, nil
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

func (e espOperationExecutor) updateGroupPolicy(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if req.GroupID == (entmoot.GroupID{}) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "group_policy_update requires group_id"}
	}
	var payload groupPolicyPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid group_policy_update payload"}
	}
	resolved, err := resolveESPGroupPolicyPayload(payload)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	report, err := e.applyGroupPolicy(ctx, req.GroupID, resolved.Policy)
	if err != nil {
		return nil, err
	}
	report.Source = resolved.Source
	return json.Marshal(report)
}

func (e espOperationExecutor) clearGroupPolicy(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if req.GroupID == (entmoot.GroupID{}) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "group_policy_clear requires group_id"}
	}
	report, err := e.applyGroupPolicy(ctx, req.GroupID, nil)
	if err != nil {
		return nil, err
	}
	report.Source = "clear"
	return json.Marshal(report)
}

func resolveESPGroupPolicyPayload(payload groupPolicyPayload) (entpolicy.SourceResolution, error) {
	if payload.Policy != nil {
		if strings.TrimSpace(payload.PolicySource) != "" || strings.TrimSpace(payload.Preset) != "" {
			return entpolicy.SourceResolution{}, errors.New("choose either preset, policy_source, or policy")
		}
		if err := payload.Policy.Validate(); err != nil {
			return entpolicy.SourceResolution{}, fmt.Errorf("policy: %w", err)
		}
		p := *payload.Policy
		return entpolicy.SourceResolution{Policy: &p, Source: "custom"}, nil
	}
	source := strings.TrimSpace(payload.PolicySource)
	preset := strings.TrimSpace(payload.Preset)
	if source != "" && preset != "" {
		return entpolicy.SourceResolution{}, errors.New("choose either preset, policy_source, or policy")
	}
	if source == "" && preset != "" {
		source = "preset:" + preset
	}
	if source == "" {
		return entpolicy.SourceResolution{}, errors.New("choose preset, policy_source, or policy")
	}
	return resolveESPPolicySource(source)
}

func resolveESPPolicySource(raw string) (entpolicy.SourceResolution, error) {
	raw = strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(raw, "preset:"):
		return entpolicy.ResolveSource(strings.TrimSpace(strings.TrimPrefix(raw, "preset:")), "")
	case raw == entpolicy.PresetNone:
		return entpolicy.ResolveSource(entpolicy.PresetNone, "")
	case strings.HasPrefix(raw, "file:"):
		return entpolicy.SourceResolution{}, errors.New("file policy sources are not supported over ESP")
	default:
		return entpolicy.SourceResolution{}, fmt.Errorf("unsupported policy source %q (want: preset:standard, preset:relaxed, none, or inline policy)", raw)
	}
}

func (e espOperationExecutor) applyGroupPolicy(ctx context.Context, gid entmoot.GroupID, p *entpolicy.Policy) (groupPolicyReport, error) {
	store, err := entpolicy.OpenFileStore(e.dataDir)
	if err != nil {
		return groupPolicyReport{}, err
	}
	socketPath := e.socketPath
	if socketPath == "" {
		socketPath = controlSocketPath(e.dataDir)
	}
	if controlSocketAlive(socketPath, 200*time.Millisecond) {
		report, err := publishGroupPolicyUpdateToSocket(ctx, socketPath, store, gid, p)
		if err == nil {
			return report, nil
		}
		if publishIPCErrorCode(err) != ipc.CodeGroupNotFound {
			return groupPolicyReport{}, err
		}
	}
	return applyGroupPolicyUpdateLocal(ctx, store, gid, p)
}

func (e espOperationExecutor) publishPublicMoot(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if req.GroupID == (entmoot.GroupID{}) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "group_public_publish requires group_id"}
	}
	var payload groupPublicPublishPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid group_public_publish payload"}
	}
	espURL := strings.TrimSpace(payload.ESPURL)
	if espURL == "" {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "esp_url is required"}
	}
	desc, err := buildPublicMootDescriptorWithIdentity(ctx, e.dataDir, e.identity, req.GroupID, time.Now().UnixMilli())
	if err != nil {
		return nil, groupPublicOperationError(err)
	}
	resp, err := publishPublicMootDescriptor(ctx, espURL, desc)
	if err != nil {
		return nil, groupPublicOperationError(err)
	}
	return json.Marshal(groupPublicPublishResult{
		Status:     "published",
		GroupID:    desc.GroupID,
		ESPURL:     espURL,
		Descriptor: desc,
		Response:   resp,
	})
}

func groupPublicOperationError(err error) error {
	switch {
	case errors.Is(err, errGroupPublicNotFound):
		return &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "group_not_found", Message: err.Error()}
	case errors.Is(err, errGroupPublicForbidden):
		return &esphttp.OperationError{HTTPStatus: http.StatusForbidden, Code: "forbidden", Message: err.Error()}
	case errors.Is(err, errGroupPublicInvalid), errors.Is(err, publicmoot.ErrInvalidDescriptor), errors.Is(err, publicmoot.ErrDescriptorSignature):
		return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "invalid_public_moot", Message: err.Error()}
	case strings.Contains(err.Error(), "esp-url") || strings.Contains(err.Error(), "absolute http"):
		return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	case strings.HasPrefix(err.Error(), "ESP "):
		return &esphttp.OperationError{HTTPStatus: http.StatusBadGateway, Code: "directory_publish_failed", Message: err.Error()}
	default:
		return err
	}
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
	// A removed member's own invites stop working by rule, but bearer invites
	// from other admins do not, so the caller is told what remains —
	// including when a store could not be read, since a zero there would read
	// as "nothing outstanding".
	out := map[string]any{
		"status":                       resp.Status,
		"group_id":                     resp.GroupID,
		"roster_head":                  resp.RosterHead,
		"members":                      resp.Members,
		"outstanding_open_invites":     resp.OutstandingOpenInvites,
		"outstanding_esp_open_invites": resp.OutstandingESPOpenInvites,
	}
	if resp.InviteLedgerError != "" {
		out["invite_ledger_error"] = resp.InviteLedgerError
	}
	if resp.ESPOpenInvitesError != "" {
		out["esp_open_invites_error"] = resp.ESPOpenInvitesError
	}
	return json.Marshal(out)
}

func validateInviteTarget(target *inviteTargetPayload) (entmoot.NodeInfo, error) {
	if target == nil {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "target_required", Message: "invite_create requires target agent identity"}
	}
	binding, err := libp2ptransport.BindingFromPublicKey(target.EntmootPubKey)
	if err != nil || target.MemberID != binding.MemberID || target.PeerID != binding.PeerID.String() {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "target member_id, peer_id, and entmoot_pubkey must use the same key"}
	}
	memberID := binding.MemberID
	return entmoot.NodeInfo{EntmootPubKey: append([]byte(nil), target.EntmootPubKey...), MemberID: &memberID, PeerID: binding.PeerID.String()}, nil
}

func validateInviteTargetForRemove(target *inviteTargetPayload) (entmoot.NodeInfo, error) {
	if target == nil {
		return entmoot.NodeInfo{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "target_required", Message: "member_remove requires target agent identity"}
	}
	return validateInviteTarget(target)
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

// applyRosterRemove records a removal. There is no matching add: a member
// signs its own join, so an ESP invite hands out a capability rather than
// writing somebody into the group.
func applyRosterRemove(identity *keystore.Identity, group *membership.Group, target entmoot.NodeInfo) error {
	if _, err := group.SignRecord(identity, membership.Record{Kind: membership.KindRemove, Subject: target}); err != nil {
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

func (e espOperationExecutor) deactivateGroupOverIPC(ctx context.Context, req *ipc.GroupDeactivateReq) (*ipc.GroupDeactivateResp, error) {
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
	case *ipc.GroupDeactivateResp:
		return v, nil
	case *ipc.ErrorFrame:
		return nil, operationIPCError(v)
	default:
		return nil, fmt.Errorf("unexpected group deactivate response %T", payload)
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

func parseInviteAccept(payload json.RawMessage) (entmoot.BootstrapCapability, error) {
	var wrapped inviteAcceptPayload
	if err := json.Unmarshal(payload, &wrapped); err == nil && wrapped.Capability != nil {
		return *wrapped.Capability, nil
	}
	var capability entmoot.BootstrapCapability
	if err := json.Unmarshal(payload, &capability); err != nil || capability.GroupID == (entmoot.GroupID{}) {
		return entmoot.BootstrapCapability{}, fmt.Errorf("invalid invite_accept capability")
	}
	return capability, nil
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
	if payload.Visibility != "" {
		visibility := normalizeGroupVisibility(payload.Visibility)
		if visibility == "" {
			return nil, fmt.Errorf("invalid visibility %q", payload.Visibility)
		}
		meta["visibility"] = visibility
	}
	if payload.JoinMode != "" {
		joinMode := normalizeGroupJoinMode(payload.JoinMode)
		if joinMode == "" {
			return nil, fmt.Errorf("invalid join_mode %q", payload.JoinMode)
		}
		meta["join_mode"] = joinMode
	}
	if _, ok := meta["visibility"]; !ok {
		meta["visibility"] = groupVisibilityPrivate
	}
	if _, ok := meta["join_mode"]; !ok {
		meta["join_mode"] = groupJoinModeInviteOnly
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

// groupDirPath is the per-group directory every store keys by. The encoding
// lives on GroupID.DirName so it is stated once, not per store.
func groupDirPath(dataDir string, gid entmoot.GroupID) string {
	return filepath.Join(groupsDir(dataDir), gid.DirName())
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

func (e espOperationExecutor) joinGroup(ctx context.Context, capability entmoot.BootstrapCapability) (*ipc.JoinGroupResp, error) {
	return e.joinGroupRequest(ctx, &ipc.JoinGroupReq{Capability: &capability})
}

func (e espOperationExecutor) activateLocalGroup(ctx context.Context, groupID entmoot.GroupID) (*ipc.JoinGroupResp, error) {
	return e.joinGroupRequest(ctx, &ipc.JoinGroupReq{LocalGroupID: &groupID})
}

func (e espOperationExecutor) joinGroupRequest(ctx context.Context, request *ipc.JoinGroupReq) (*ipc.JoinGroupResp, error) {
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
	request.TimeoutMS = timeout.Milliseconds()
	if err := ipc.EncodeAndWrite(conn, request); err != nil {
		return nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, err
	}
	switch response := payload.(type) {
	case *ipc.JoinGroupResp:
		return response, nil
	case *ipc.ErrorFrame:
		return nil, operationIPCError(response)
	default:
		return nil, fmt.Errorf("unexpected join group response %T", payload)
	}
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
	status := http.StatusBadGateway
	code := "ipc_error"
	switch frame.Code {
	case ipc.CodeInvalidArgument:
		status, code = http.StatusBadRequest, "bad_request"
	case ipc.CodeConflict:
		status, code = http.StatusConflict, "conflict"
	case ipc.CodeNotMember:
		status, code = http.StatusForbidden, "not_member"
	case ipc.CodeGroupNotFound:
		status, code = http.StatusNotFound, "group_not_found"
	case ipc.CodeUnavailable:
		status, code = http.StatusServiceUnavailable, "join_unavailable"
	case ipc.CodeInternal:
		status, code = http.StatusInternalServerError, "internal"
	}
	return &esphttp.OperationError{HTTPStatus: status, Code: code, Message: frame.Message}
}

// validateOpenInviteBootstrap rejects a bootstrap list that cannot produce a
// redeemable capability: a malformed multiaddr, an address naming no current
// member, or so many addresses that the minted capability would not fit the
// request a joiner sends.
//
// The size check must model what the MINT will sign, not just the list: a
// capability also carries founder and issuer identities, the checkpoint id, a
// nonce, a signature, target fields and relay hints, plus the fallback member
// addresses the daemon attaches when the opt-out is off. Measuring the bare
// list accepted 26 addresses that the mint then refused, which is the failure
// this check exists to prevent, moved one step earlier.
func validateOpenInviteBootstrap(addresses, memberPeerIDs []string, localPeerID string, noFallback bool) error {
	peerIDs := make([]string, 0, len(addresses))
	for _, raw := range addresses {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request",
				Message: fmt.Sprintf("invalid bootstrap multiaddr %q", raw)}
		}
		info, err := libpeer.AddrInfoFromP2pAddr(address)
		if err != nil {
			return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request",
				Message: fmt.Sprintf("bootstrap multiaddr %q must end in /p2p/<peer-id>", raw)}
		}
		// Membership can change before redemption, but an address naming
		// nobody today is knowable now.
		// The mint accepts the issuing node's own address even when it is not a
		// member — a founder may issue after standing down — so creation must
		// not be stricter than the thing it pre-empts.
		if len(memberPeerIDs) > 0 && info.ID.String() != localPeerID && !slices.Contains(memberPeerIDs, info.ID.String()) {
			return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request",
				Message: fmt.Sprintf("bootstrap multiaddr %q does not name a member of this group", raw)}
		}
		peerIDs = append(peerIDs, info.ID.String())
	}
	if size, tooLarge := openInviteCapabilityTooLarge(addresses, peerIDs, noFallback); tooLarge {
		// The figure is an upper bound, not a prediction: everything the
		// caller did not name is charged at its ceiling, so say so rather
		// than report it as the size the invite will have. Name the remedy
		// too — declining the fallback peers buys most of the allowance back.
		return &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request",
			Message: fmt.Sprintf("this list could mint an invite of up to %d bytes, over the %d a joiner can send; name fewer addresses, or set no_fallback_peers to stop the daemon attaching others", size, libp2ptransport.MaxCapabilityBytes)}
	}
	return nil
}

// openInviteCapabilityTooLarge reports whether a bootstrap list would mint a
// capability too large to redeem.
//
// It does NOT model the JSON. Three attempts at that undercounted in turn: the
// fallback slots, then the relay bytes, then the nonce, the timestamps and the
// per-element array overhead. The size of a document is a fact about the
// encoder, not something to reason about in prose, so the arithmetic here is
// deliberately coarse and one-directional: everything the caller did not name
// is charged at its enforced ceiling, and the fixed fields are charged at
// maxInviteCapabilityOverhead, a constant TestCapabilityOverheadIsBounded
// measures against a capability built the way the mint builds one.
func openInviteCapabilityTooLarge(addresses, peerIDs []string, noFallback bool) (int, bool) {
	// Each array element costs its own quotes and comma on the wire.
	const perElement = 4
	size := maxInviteCapabilityOverhead
	for _, addr := range addresses {
		size += len(addr) + perElement
	}
	for _, id := range peerIDs {
		size += len(id) + perElement
	}
	// Relay hints come from this node's configuration, not the caller.
	size += maxInviteFallbackBytes + libp2ptransport.MaxCapabilityRelays*perElement
	if len(addresses) == 0 {
		// A request naming nothing is filled with this node's own addresses,
		// and that happens whether or not the caller declined the fallback
		// members: no_fallback_peers gates addKnownMemberPeers only.
		size += maxInviteFallbackBytes + maxInviteFallbackAddrs*perElement
		size += maxPeerIDBytes + perElement
	}
	if !noFallback {
		size += maxInviteFallbackBytes + maxInviteFallbackAddrs*perElement
		size += maxInviteFallbackPeers * (maxPeerIDBytes + perElement)
	}
	return size, size > libp2ptransport.MaxCapabilityBytes
}
