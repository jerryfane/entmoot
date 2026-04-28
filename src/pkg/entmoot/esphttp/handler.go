// Package esphttp exposes a small HTTP mailbox bridge for Entmoot Service
// Provider deployments.
package esphttp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/espnotify"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// GroupExistsFunc reports whether groupID is locally joined/served.
type GroupExistsFunc func(context.Context, entmoot.GroupID) (bool, error)

// Config wires the HTTP handler to an existing mailbox service.
type Config struct {
	Token       string
	AuthMode    AuthMode
	Devices     *DeviceRegistry
	Clock       func() time.Time
	Service     *mailbox.Service
	Publisher   Publisher
	Operations  OperationExecutor
	Notifier    espnotify.Notifier
	State       StateStore
	Groups      GroupCatalog
	GroupExists GroupExistsFunc
	Logger      *slog.Logger
}

// AuthMode selects how ESP v1 HTTP requests authenticate.
type AuthMode string

const (
	AuthModeBearer AuthMode = "bearer"
	AuthModeDevice AuthMode = "device"
	AuthModeDual   AuthMode = "dual"
)

const (
	deviceIDHeader    = "X-Entmoot-Device-ID"
	idempotencyHeader = "Idempotency-Key"
	timestampHeader   = "X-Entmoot-Timestamp-Ms"
	nonceHeader       = "X-Entmoot-Nonce"
	signatureHeader   = "X-Entmoot-Signature"
	deviceAuthVersion = "ENTMOOT-ESP-AUTH-V1"
	deviceAuthSkew    = 5 * time.Minute
	maxAuthBodyBytes  = 16 << 20
	maxListLimit      = 200
)

// Device describes one ESP client device authorized to use this service.
type Device struct {
	ID        string
	PublicKey ed25519.PublicKey
	Groups    []entmoot.GroupID
	ClientIDs []string
	Disabled  bool
}

// DeviceRegistry is the in-memory authorization projection loaded by ESP
// deployments from local configuration.
type DeviceRegistry struct {
	mu      sync.RWMutex
	Devices []Device
	byID    map[string]Device
}

// NewDeviceRegistry validates and indexes devices.
func NewDeviceRegistry(devices []Device) (*DeviceRegistry, error) {
	reg := &DeviceRegistry{
		Devices: append([]Device(nil), devices...),
		byID:    make(map[string]Device, len(devices)),
	}
	for i, d := range reg.Devices {
		d.ID = strings.TrimSpace(d.ID)
		if d.ID == "" {
			return nil, errors.New("esphttp: device id is required")
		}
		for j := range d.ClientIDs {
			d.ClientIDs[j] = strings.TrimSpace(d.ClientIDs[j])
			if d.ClientIDs[j] == "" {
				return nil, fmt.Errorf("esphttp: device %q client id is required", d.ID)
			}
		}
		if len(d.PublicKey) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("esphttp: device %q public key length %d", d.ID, len(d.PublicKey))
		}
		if _, exists := reg.byID[d.ID]; exists {
			return nil, fmt.Errorf("esphttp: duplicate device id %q", d.ID)
		}
		d.PublicKey = append(ed25519.PublicKey(nil), d.PublicKey...)
		d.Groups = append([]entmoot.GroupID(nil), d.Groups...)
		d.ClientIDs = append([]string(nil), d.ClientIDs...)
		reg.Devices[i] = d
		reg.byID[d.ID] = d
	}
	return reg, nil
}

func (r *DeviceRegistry) lookup(id string) (Device, bool) {
	if r == nil {
		return Device{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.byID[strings.TrimSpace(id)]
	return cloneDevice(d), ok
}

// Snapshot returns a deep copy of the registry suitable for serialization or
// copy-on-write updates.
func (r *DeviceRegistry) Snapshot() []Device {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneDevices(r.Devices)
}

// Replace atomically swaps the registry contents with a validated replacement.
func (r *DeviceRegistry) Replace(next *DeviceRegistry) {
	if r == nil || next == nil {
		return
	}
	nextDevices := next.Snapshot()
	nextByID := make(map[string]Device, len(nextDevices))
	for _, d := range nextDevices {
		nextByID[d.ID] = cloneDevice(d)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Devices = nextDevices
	r.byID = nextByID
}

// WithGroupGranted returns a validated registry copy with gid granted to
// deviceID. The returned bool is false when the grant was already present.
func (r *DeviceRegistry) WithGroupGranted(deviceID string, gid entmoot.GroupID) (*DeviceRegistry, bool, error) {
	return r.withDeviceGroup(deviceID, gid, true)
}

// WithGroupRevoked returns a validated registry copy with gid removed from
// deviceID. The returned bool is false when there was nothing to remove.
func (r *DeviceRegistry) WithGroupRevoked(deviceID string, gid entmoot.GroupID) (*DeviceRegistry, bool, error) {
	return r.withDeviceGroup(deviceID, gid, false)
}

func (r *DeviceRegistry) withDeviceGroup(deviceID string, gid entmoot.GroupID, grant bool) (*DeviceRegistry, bool, error) {
	if r == nil {
		return nil, false, errors.New("esphttp: device registry is not configured")
	}
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return nil, false, errors.New("esphttp: device id is required")
	}
	devices := r.Snapshot()
	changed := false
	found := false
	for i := range devices {
		if devices[i].ID != deviceID {
			continue
		}
		found = true
		idx := -1
		for j, existing := range devices[i].Groups {
			if existing == gid {
				idx = j
				break
			}
		}
		if grant {
			if idx >= 0 {
				break
			}
			devices[i].Groups = append(devices[i].Groups, gid)
			changed = true
			break
		}
		if idx < 0 {
			break
		}
		devices[i].Groups = append(devices[i].Groups[:idx], devices[i].Groups[idx+1:]...)
		changed = true
		break
	}
	if !found {
		return nil, false, fmt.Errorf("esphttp: device %q not found", deviceID)
	}
	next, err := NewDeviceRegistry(devices)
	if err != nil {
		return nil, false, err
	}
	return next, changed, nil
}

func cloneDevices(devices []Device) []Device {
	out := make([]Device, len(devices))
	for i, d := range devices {
		out[i] = cloneDevice(d)
	}
	return out
}

func cloneDevice(d Device) Device {
	d.PublicKey = append(ed25519.PublicKey(nil), d.PublicKey...)
	d.Groups = append([]entmoot.GroupID(nil), d.Groups...)
	d.ClientIDs = append([]string(nil), d.ClientIDs...)
	return d
}

// Publisher submits already-signed messages to the running Entmoot daemon.
type Publisher interface {
	PublishSigned(context.Context, entmoot.Message) (PublishResult, error)
}

// PublishResult is the HTTP response for an accepted phone-signed message.
type PublishResult struct {
	Status      string            `json:"status"`
	MessageID   entmoot.MessageID `json:"message_id"`
	GroupID     entmoot.GroupID   `json:"group_id"`
	Author      entmoot.NodeID    `json:"author"`
	TimestampMS int64             `json:"timestamp_ms"`
}

// PublishError lets publisher implementations request stable HTTP error
// mapping without coupling esphttp to a concrete IPC client.
type PublishError struct {
	HTTPStatus int
	Code       string
	Message    string
}

func (e *PublishError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// Handler serves the ESP mailbox API.
type Handler struct {
	token       string
	authMode    AuthMode
	devices     *DeviceRegistry
	nonceCache  *nonceCache
	clock       func() time.Time
	service     *mailbox.Service
	publisher   Publisher
	operations  OperationExecutor
	notifier    espnotify.Notifier
	state       StateStore
	groups      GroupCatalog
	groupExists GroupExistsFunc
	logger      *slog.Logger
}

// NewHandler returns an HTTP handler for the ESP mailbox API.
func NewHandler(cfg Config) (*Handler, error) {
	authMode := cfg.AuthMode
	if authMode == "" {
		authMode = AuthModeBearer
	}
	switch authMode {
	case AuthModeBearer, AuthModeDevice, AuthModeDual:
	default:
		return nil, fmt.Errorf("esphttp: unknown auth mode %q", authMode)
	}
	if (authMode == AuthModeBearer || authMode == AuthModeDual) && cfg.Token == "" {
		return nil, errors.New("esphttp: token is required")
	}
	if (authMode == AuthModeDevice || authMode == AuthModeDual) && cfg.Devices == nil {
		return nil, errors.New("esphttp: device registry is required")
	}
	if cfg.Service == nil {
		return nil, errors.New("esphttp: mailbox service is required")
	}
	state := cfg.State
	if state == nil {
		state = NewMemoryStateStore()
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	groupExists := cfg.GroupExists
	if groupExists == nil {
		groupExists = func(context.Context, entmoot.GroupID) (bool, error) { return true, nil }
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		token:       cfg.Token,
		authMode:    authMode,
		devices:     cfg.Devices,
		nonceCache:  newNonceCache(clock),
		clock:       clock,
		service:     cfg.Service,
		publisher:   cfg.Publisher,
		operations:  cfg.Operations,
		notifier:    cfg.Notifier,
		state:       state,
		groups:      cfg.Groups,
		groupExists: groupExists,
		logger:      logger,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		writeError(w, http.StatusNotFound, "not_found", "not found")
		return
	}
	auth, ok := h.authorize(w, r)
	if !ok {
		return
	}
	r = r.WithContext(context.WithValue(r.Context(), authContextKey{}, auth))
	switch r.URL.Path {
	case "/v1/mailbox/pull":
		h.handlePull(w, r)
	case "/v1/mailbox/ack":
		h.handleAck(w, r)
	case "/v1/mailbox/cursor":
		h.handleCursor(w, r)
	case "/v1/messages":
		h.handleMessagePublish(w, r)
	case "/v1/session":
		h.handleSession(w, r)
	case "/v1/status":
		h.handleStatus(w, r)
	case "/v1/groups":
		h.handleGroups(w, r)
	case "/v1/invites/accept":
		h.handleInviteAccept(w, r)
	case "/v1/sign-requests":
		h.handleSignRequests(w, r)
	case "/v1/devices/current":
		h.handleCurrentDevice(w, r)
	case "/v1/devices/current/push-token":
		h.handlePushToken(w, r)
	case "/v1/notifications/preferences":
		h.handleNotificationPreferences(w, r)
	case "/v1/notifications/test":
		h.handleNotificationTest(w, r)
	default:
		if h.handleGroupSubroute(w, r) {
			return
		}
		if h.handleSignRequestSubroute(w, r) {
			return
		}
		writeError(w, http.StatusNotFound, "not_found", "not found")
	}
}

func (h *Handler) handleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	resp := map[string]any{
		"authenticated": true,
		"auth_mode":     h.authMode,
	}
	if auth.device != nil {
		resp["device"] = deviceView(*auth.device)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	groups := 0
	if h.groups != nil {
		if list, err := h.groups.ListGroups(r.Context()); err == nil {
			groups = len(list)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":          "ok",
		"auth_mode":       h.authMode,
		"groups":          groups,
		"mailbox_enabled": h.service != nil,
		"publisher":       h.publisher != nil,
	})
}

func (h *Handler) handleGroups(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleListGroups(w, r)
	case http.MethodPost:
		h.withIdempotency(w, r, "group_create", func(w http.ResponseWriter, r *http.Request) {
			h.createSignRequestFromHTTP(w, r, "group_create", entmoot.GroupID{})
		})
	default:
		methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
	}
}

func (h *Handler) handleListGroups(w http.ResponseWriter, r *http.Request) {
	if h.groups == nil {
		writeJSON(w, http.StatusOK, map[string]any{"groups": []GroupSummary{}})
		return
	}
	groups, err := h.groups.ListGroups(r.Context())
	if err != nil {
		h.logger.Error("esphttp: list groups", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "group listing failed")
		return
	}
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device != nil {
		filtered := groups[:0]
		for _, g := range groups {
			if deviceAllowsGroup(*auth.device, g.GroupID) {
				filtered = append(filtered, g)
			}
		}
		groups = filtered
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups})
}

func (h *Handler) handleGroupSubroute(w http.ResponseWriter, r *http.Request) bool {
	const prefix = "/v1/groups/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return false
	}
	rest := strings.TrimPrefix(r.URL.Path, prefix)
	rawGroup, suffix, _ := strings.Cut(rest, "/")
	groupID, err := decodeGroupID(rawGroup)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return true
	}
	if ok := h.checkGroup(w, r, groupID); !ok {
		return true
	}
	switch suffix {
	case "":
		switch r.Method {
		case http.MethodGet:
			h.handleGetGroup(w, r, groupID)
		case http.MethodPatch:
			h.withIdempotency(w, r, "group_update:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) {
				h.createSignRequestFromHTTP(w, r, "group_update", groupID)
			})
		default:
			methodNotAllowed(w, http.MethodGet+", "+http.MethodPatch)
		}
	case "members":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleListMembers(w, r, groupID)
	case "invites":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return true
		}
		h.withIdempotency(w, r, "invite_create:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) {
			h.createSignRequestFromHTTP(w, r, "invite_create", groupID)
		})
	case "messages":
		switch r.Method {
		case http.MethodGet:
			h.handleGroupMessages(w, r, groupID)
		case http.MethodPost:
			h.withIdempotency(w, r, "message_publish:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) {
				h.handleGroupMessagePublish(w, r, groupID)
			})
		default:
			methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
		}
	case "mailbox":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleGroupMessages(w, r, groupID)
	default:
		writeError(w, http.StatusNotFound, "not_found", "not found")
	}
	return true
}

func (h *Handler) handleGetGroup(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if h.groups == nil {
		writeError(w, http.StatusNotFound, "group_not_found", "group not joined")
		return
	}
	group, ok, err := h.groups.GetGroup(r.Context(), groupID)
	if err != nil {
		h.logger.Error("esphttp: get group", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "group lookup failed")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "group_not_found", "group not joined")
		return
	}
	writeJSON(w, http.StatusOK, group)
}

func (h *Handler) handleListMembers(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if h.groups == nil {
		writeJSON(w, http.StatusOK, map[string]any{"members": []MemberSummary{}})
		return
	}
	members, err := h.groups.ListMembers(r.Context(), groupID)
	if err != nil {
		h.logger.Error("esphttp: list members", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "member listing failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members})
}

func (h *Handler) handleGroupMessages(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	clientID := strings.TrimSpace(r.URL.Query().Get("client_id"))
	if clientID == "" {
		if auth, _ := r.Context().Value(authContextKey{}).(authContext); auth.device != nil {
			clientID = auth.device.ID
		}
	}
	if clientID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "client_id is required")
		return
	}
	if !h.checkDeviceClient(w, r, groupID, clientID) {
		return
	}
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("limit must be between 0 and %d", maxListLimit))
			return
		}
		limit = n
	}
	result, err := h.service.Pull(r.Context(), groupID, clientID, limit)
	h.writeMailboxResult(w, "group messages", result, err)
}

func (h *Handler) handleGroupMessagePublish(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	var raw map[string]json.RawMessage
	body, ok := decodeRawBody(w, r, 16<<20, &raw)
	if !ok {
		return
	}
	if msgRaw, hasMessage := raw["message"]; hasMessage {
		if h.publisher == nil {
			writeError(w, http.StatusServiceUnavailable, "join_unavailable", "no running join publisher configured")
			return
		}
		var msg entmoot.Message
		if err := json.Unmarshal(msgRaw, &msg); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid message: %v", err))
			return
		}
		if msg.GroupID != groupID {
			writeError(w, http.StatusBadRequest, "bad_request", "message.group_id does not match URL group_id")
			return
		}
		result, err := h.publisher.PublishSigned(r.Context(), msg)
		h.writePublishResult(w, result, err)
		return
	}
	var draft messagePublishDraft
	if err := json.Unmarshal(body, &draft); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid message draft: %v", err))
		return
	}
	h.createMessagePublishSignRequest(w, r, groupID, draft)
}

func (h *Handler) handleInviteAccept(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	h.withIdempotency(w, r, "invite_accept", func(w http.ResponseWriter, r *http.Request) {
		h.createSignRequestFromHTTP(w, r, "invite_accept", entmoot.GroupID{})
	})
}

func (h *Handler) handleSignRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	requests, err := h.state.ListSignRequests(r.Context(), deviceIDForRequest(auth))
	if err != nil {
		h.logger.Error("esphttp: list sign requests", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "sign request listing failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sign_requests": requests})
}

func (h *Handler) handleSignRequestSubroute(w http.ResponseWriter, r *http.Request) bool {
	const prefix = "/v1/sign-requests/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return false
	}
	rest := strings.TrimPrefix(r.URL.Path, prefix)
	id, suffix, _ := strings.Cut(rest, "/")
	id = strings.TrimSpace(id)
	if id == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "sign request id is required")
		return true
	}
	switch suffix {
	case "":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		req, ok, err := h.state.GetSignRequest(r.Context(), id)
		h.writeSignRequestLookup(w, r, req, ok, err)
	case "complete":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return true
		}
		h.withIdempotency(w, r, "sign_request_complete:"+id, func(w http.ResponseWriter, r *http.Request) {
			h.handleCompleteSignRequest(w, r, id)
		})
	case "reject":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return true
		}
		h.handleRejectSignRequest(w, r, id)
	default:
		writeError(w, http.StatusNotFound, "not_found", "not found")
	}
	return true
}

func (h *Handler) handleCompleteSignRequest(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		Signature            string `json:"signature"`
		SigningPayloadSHA256 string `json:"signing_payload_sha256"`
	}
	if _, ok := decodeRawBody(w, r, 1<<20, &body); !ok {
		return
	}
	signatureText := strings.TrimSpace(body.Signature)
	if signatureText == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "signature is required")
		return
	}
	req, ok, err := h.state.GetSignRequest(r.Context(), id)
	if err != nil {
		h.writeSignRequestMutation(w, SignRequest{}, err)
		return
	}
	if !ok {
		h.writeSignRequestMutation(w, SignRequest{}, sql.ErrNoRows)
		return
	}
	if !h.signRequestVisible(w, r, req) {
		return
	}
	if !h.checkSignRequestPending(w, req) {
		return
	}
	gotDigest := strings.TrimSpace(body.SigningPayloadSHA256)
	executeOperation := executableOperationKind(req.Kind)
	if signRequestRequiresPayloadDigest(req.Kind) && gotDigest == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "signing_payload_sha256 is required")
		return
	}
	if gotDigest != "" && gotDigest != req.SigningPayloadSHA256 {
		writeError(w, http.StatusBadRequest, "signing_payload_mismatch", "signing payload digest does not match sign request")
		return
	}
	sig, err := base64.StdEncoding.DecodeString(signatureText)
	if err != nil || len(sig) != ed25519.SignatureSize {
		writeError(w, http.StatusBadRequest, "bad_request", "signature must be a base64 Ed25519 signature")
		return
	}
	var publishResult *PublishResult
	var operationResult json.RawMessage
	if req.Kind == signRequestKindMessagePublish {
		result, ok := h.completeMessagePublishSignRequest(w, r, req, sig)
		if !ok {
			return
		}
		publishResult = &result
		operationResult = marshalOperationResult(w, result)
		if operationResult == nil {
			return
		}
	} else if executeOperation {
		result, ok := h.completeExecutableSignRequest(w, r, req, sig)
		if !ok {
			return
		}
		operationResult = result
	}
	req, err = h.state.CompleteSignRequest(r.Context(), id, signatureText, publishResult, operationResult)
	h.writeSignRequestMutation(w, req, err)
}

func signRequestRequiresPayloadDigest(kind string) bool {
	return kind == signRequestKindMessagePublish || executableOperationKind(kind)
}

func (h *Handler) handleRejectSignRequest(w http.ResponseWriter, r *http.Request, id string) {
	req, ok, err := h.state.GetSignRequest(r.Context(), id)
	if err != nil {
		h.writeSignRequestMutation(w, SignRequest{}, err)
		return
	}
	if !ok {
		h.writeSignRequestMutation(w, SignRequest{}, sql.ErrNoRows)
		return
	}
	if !h.signRequestVisible(w, r, req) {
		return
	}
	if !h.checkSignRequestPending(w, req) {
		return
	}
	req, err = h.state.RejectSignRequest(r.Context(), id)
	h.writeSignRequestMutation(w, req, err)
}

func (h *Handler) handleCurrentDevice(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device == nil {
		writeJSON(w, http.StatusOK, map[string]any{"device": nil, "auth_mode": h.authMode})
		return
	}
	state, err := h.state.GetDeviceState(r.Context(), auth.device.ID)
	if err != nil {
		h.logger.Error("esphttp: get device state", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "device lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"device": deviceView(*auth.device), "state": state})
}

func (h *Handler) handlePushToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		methodNotAllowed(w, http.MethodPut)
		return
	}
	h.withIdempotency(w, r, "push_token:"+deviceIDForRequest(authFromContext(r)), h.handlePushTokenMutation)
}

func (h *Handler) handlePushTokenMutation(w http.ResponseWriter, r *http.Request) {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device == nil {
		writeError(w, http.StatusForbidden, "forbidden", "device auth is required")
		return
	}
	var body struct {
		Platform string `json:"platform"`
		Token    string `json:"token"`
	}
	if _, ok := decodeRawBody(w, r, 1<<20, &body); !ok {
		return
	}
	if strings.TrimSpace(body.Token) == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "token is required")
		return
	}
	platform := strings.TrimSpace(body.Platform)
	if platform == "" {
		platform = "apns"
	}
	state, err := h.state.UpsertPushToken(r.Context(), auth.device.ID, platform, strings.TrimSpace(body.Token))
	if err != nil {
		h.logger.Error("esphttp: update push token", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "push token update failed")
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (h *Handler) handleNotificationPreferences(w http.ResponseWriter, r *http.Request) {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device == nil {
		writeError(w, http.StatusForbidden, "forbidden", "device auth is required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		state, err := h.state.GetDeviceState(r.Context(), auth.device.ID)
		if err != nil {
			h.logger.Error("esphttp: get notification preferences", slog.String("err", err.Error()))
			writeError(w, http.StatusInternalServerError, "internal_error", "notification preference lookup failed")
			return
		}
		writeJSON(w, http.StatusOK, state.NotificationPreferences)
	case http.MethodPatch:
		var prefs NotificationPreferences
		if _, ok := decodeRawBody(w, r, 1<<20, &prefs); !ok {
			return
		}
		state, err := h.state.PatchNotificationPreferences(r.Context(), auth.device.ID, prefs)
		if err != nil {
			h.logger.Error("esphttp: patch notification preferences", slog.String("err", err.Error()))
			writeError(w, http.StatusInternalServerError, "internal_error", "notification preference update failed")
			return
		}
		writeJSON(w, http.StatusOK, state.NotificationPreferences)
	default:
		methodNotAllowed(w, http.MethodGet+", "+http.MethodPatch)
	}
}

func (h *Handler) handleNotificationTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device == nil {
		writeError(w, http.StatusForbidden, "forbidden", "device auth is required")
		return
	}
	state, err := h.state.GetDeviceState(r.Context(), auth.device.ID)
	if err != nil {
		h.logger.Error("esphttp: notification test state", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "device lookup failed")
		return
	}
	if state.PushToken == "" {
		writeError(w, http.StatusBadRequest, "push_token_missing", "device has no registered push token")
		return
	}
	notifier := h.notifier
	if notifier == nil {
		notifier = espnotify.NoopNotifier{}
	}
	result, err := notifier.SendWakeup(r.Context(), espnotify.DeviceTarget{
		DeviceID: state.DeviceID,
		Platform: state.PushPlatform,
		Token:    state.PushToken,
	}, espnotify.WakeupEvent{Type: espnotify.EventNotificationTest, Reason: "operator_test"})
	if err != nil {
		if espnotify.IsInvalidToken(err) {
			_, _ = h.state.ClearPushToken(r.Context(), auth.device.ID)
		}
		status := http.StatusBadGateway
		code := "provider_unavailable"
		if espnotify.IsRetryable(err) {
			status = http.StatusServiceUnavailable
			code = "retry_later"
		}
		writeError(w, status, code, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":      result.Status,
		"device_id":   auth.device.ID,
		"provider_id": result.ProviderID,
	})
}

func (h *Handler) handlePull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	groupID, ok := h.requireGroup(w, r)
	if !ok {
		return
	}
	clientID := strings.TrimSpace(r.URL.Query().Get("client_id"))
	if clientID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "client_id is required")
		return
	}
	if !h.checkDeviceClient(w, r, groupID, clientID) {
		return
	}
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("limit must be between 0 and %d", maxListLimit))
			return
		}
		limit = n
	}
	result, err := h.service.Pull(r.Context(), groupID, clientID, limit)
	h.writeMailboxResult(w, "mailbox pull", result, err)
}

func (h *Handler) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		ClientID  string            `json:"client_id"`
		GroupID   entmoot.GroupID   `json:"group_id"`
		MessageID entmoot.MessageID `json:"message_id"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	if strings.TrimSpace(req.ClientID) == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "client_id is required")
		return
	}
	if req.GroupID == (entmoot.GroupID{}) {
		writeError(w, http.StatusBadRequest, "bad_request", "group_id is required")
		return
	}
	if req.MessageID == (entmoot.MessageID{}) {
		writeError(w, http.StatusBadRequest, "bad_request", "message_id is required")
		return
	}
	if ok := h.checkGroup(w, r, req.GroupID); !ok {
		return
	}
	if !h.checkDeviceClient(w, r, req.GroupID, strings.TrimSpace(req.ClientID)) {
		return
	}
	result, err := h.service.AckMessage(r.Context(), req.GroupID, strings.TrimSpace(req.ClientID), req.MessageID)
	h.writeMailboxResult(w, "mailbox ack", result, err)
}

func (h *Handler) handleCursor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	groupID, ok := h.requireGroup(w, r)
	if !ok {
		return
	}
	clientID := strings.TrimSpace(r.URL.Query().Get("client_id"))
	if clientID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "client_id is required")
		return
	}
	if !h.checkDeviceClient(w, r, groupID, clientID) {
		return
	}
	result, err := h.service.CursorStatus(r.Context(), groupID, clientID)
	h.writeMailboxResult(w, "mailbox cursor", result, err)
}

func (h *Handler) handleMessagePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if h.publisher == nil {
		writeError(w, http.StatusServiceUnavailable, "join_unavailable", "no running join publisher configured")
		return
	}
	var req struct {
		Message entmoot.Message `json:"message"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	if req.Message.ID == (entmoot.MessageID{}) &&
		req.Message.GroupID == (entmoot.GroupID{}) &&
		req.Message.Author.PilotNodeID == 0 &&
		len(req.Message.Author.EntmootPubKey) == 0 &&
		req.Message.Timestamp == 0 &&
		len(req.Message.Topics) == 0 &&
		len(req.Message.Content) == 0 &&
		len(req.Message.Parents) == 0 &&
		len(req.Message.References) == 0 &&
		len(req.Message.Signature) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "message is required")
		return
	}
	if req.Message.GroupID == (entmoot.GroupID{}) {
		writeError(w, http.StatusBadRequest, "bad_request", "message.group_id is required")
		return
	}
	if ok := h.checkGroup(w, r, req.Message.GroupID); !ok {
		return
	}
	result, err := h.publisher.PublishSigned(r.Context(), req.Message)
	h.writePublishResult(w, result, err)
}

func (h *Handler) requireGroup(w http.ResponseWriter, r *http.Request) (entmoot.GroupID, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get("group_id"))
	if raw == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "group_id is required")
		return entmoot.GroupID{}, false
	}
	groupID, err := decodeGroupID(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return entmoot.GroupID{}, false
	}
	return groupID, h.checkGroup(w, r, groupID)
}

func (h *Handler) checkGroup(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) bool {
	exists, err := h.groupExists(r.Context(), groupID)
	if err != nil {
		h.logger.Error("esphttp: group lookup failed", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "group lookup failed")
		return false
	}
	if !exists {
		writeError(w, http.StatusNotFound, "group_not_found", "group not joined")
		return false
	}
	if !h.checkDeviceGroup(w, r, groupID) {
		return false
	}
	return true
}

func (h *Handler) writePublishResult(w http.ResponseWriter, result PublishResult, err error) {
	if err == nil {
		writeJSON(w, http.StatusAccepted, result)
		return
	}
	var pubErr *PublishError
	if errors.As(err, &pubErr) && pubErr != nil {
		status := pubErr.HTTPStatus
		if status == 0 {
			status = http.StatusInternalServerError
		}
		writeError(w, status, pubErr.Code, pubErr.Message)
		return
	}
	h.logger.Error("esphttp: signed publish", slog.String("err", err.Error()))
	writeError(w, http.StatusInternalServerError, "internal_error", "signed publish failed")
}

func (h *Handler) writeMailboxResult(w http.ResponseWriter, op string, result any, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, result)
		return
	}
	switch {
	case errors.Is(err, mailbox.ErrInvalidClient):
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusBadRequest, "message_not_found", "message not found")
	default:
		h.logger.Error("esphttp: "+op, slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "mailbox operation failed")
	}
}

type authContextKey struct{}

type authContext struct {
	device *Device
}

func (h *Handler) authorize(w http.ResponseWriter, r *http.Request) (authContext, bool) {
	body, ok := h.bufferBodyForAuth(w, r)
	if !ok {
		return authContext{}, false
	}
	switch h.authMode {
	case AuthModeBearer:
		if h.authorizedBearer(r) {
			return authContext{}, true
		}
	case AuthModeDevice:
		return h.authorizedDevice(w, r, body)
	case AuthModeDual:
		if h.authorizedBearer(r) {
			return authContext{}, true
		}
		return h.authorizedDevice(w, r, body)
	}
	if h.authMode == AuthModeBearer || h.authMode == AuthModeDual {
		w.Header().Set("WWW-Authenticate", `Bearer realm="entmoot-esp"`)
	}
	writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid credentials")
	return authContext{}, false
}

func (h *Handler) bufferBodyForAuth(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.Body == nil {
		return nil, true
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAuthBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
		return nil, false
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, true
}

func (h *Handler) authorizedBearer(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	got := strings.TrimSpace(token)
	if got == "" || len(got) != len(h.token) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(h.token)) == 1
}

func (h *Handler) authorizedDevice(w http.ResponseWriter, r *http.Request, body []byte) (authContext, bool) {
	deviceID := strings.TrimSpace(r.Header.Get(deviceIDHeader))
	if deviceID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing device id")
		return authContext{}, false
	}
	device, ok := h.devices.lookup(deviceID)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "unknown device")
		return authContext{}, false
	}
	if device.Disabled {
		writeError(w, http.StatusForbidden, "device_disabled", "device is disabled")
		return authContext{}, false
	}
	tsRaw := strings.TrimSpace(r.Header.Get(timestampHeader))
	tsMillis, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid request timestamp")
		return authContext{}, false
	}
	now := h.clock()
	ts := time.UnixMilli(tsMillis)
	if ts.Before(now.Add(-deviceAuthSkew)) || ts.After(now.Add(deviceAuthSkew)) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "request timestamp outside allowed window")
		return authContext{}, false
	}
	nonce := strings.TrimSpace(r.Header.Get(nonceHeader))
	if nonce == "" || len(nonce) > 256 {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid nonce")
		return authContext{}, false
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(r.Header.Get(signatureHeader)))
	if err != nil || len(sig) != ed25519.SignatureSize {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid signature")
		return authContext{}, false
	}
	input := DeviceSigningInput(r.Method, r.URL.RequestURI(), tsMillis, nonce, body)
	if !ed25519.Verify(device.PublicKey, []byte(input), sig) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid signature")
		return authContext{}, false
	}
	if !h.nonceCache.use(device.ID, nonce, now.Add(deviceAuthSkew)) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "replayed nonce")
		return authContext{}, false
	}
	return authContext{device: &device}, true
}

// DeviceSigningInput returns the canonical bytes a device signs for one ESP
// HTTP request.
func DeviceSigningInput(method, pathWithRawQuery string, timestampMillis int64, nonce string, body []byte) string {
	sum := sha256.Sum256(body)
	return strings.Join([]string{
		deviceAuthVersion,
		strings.ToUpper(method),
		pathWithRawQuery,
		strconv.FormatInt(timestampMillis, 10),
		nonce,
		base64.StdEncoding.EncodeToString(sum[:]),
	}, "\n")
}

func (h *Handler) checkDeviceGroup(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) bool {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device == nil {
		return true
	}
	for _, allowed := range auth.device.Groups {
		if allowed == groupID {
			return true
		}
	}
	writeError(w, http.StatusForbidden, "forbidden", "device is not authorized for group")
	return false
}

func (h *Handler) checkDeviceClient(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, clientID string) bool {
	if !h.checkDeviceGroup(w, r, groupID) {
		return false
	}
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device == nil {
		return true
	}
	for _, allowed := range auth.device.ClientIDs {
		if allowed == clientID {
			return true
		}
	}
	writeError(w, http.StatusForbidden, "forbidden", "device is not authorized for client_id")
	return false
}

func (h *Handler) createSignRequestFromHTTP(w http.ResponseWriter, r *http.Request, kind string, groupID entmoot.GroupID) {
	var payload json.RawMessage
	body, ok := decodeRawBody(w, r, 16<<20, &payload)
	if !ok {
		return
	}
	if len(payload) == 0 || string(payload) == "null" {
		body = []byte("{}")
	}
	h.createSignRequest(w, r, kind, groupID, body)
}

func (h *Handler) createSignRequest(w http.ResponseWriter, r *http.Request, kind string, groupID entmoot.GroupID, payload []byte) {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if executableOperationKind(kind) && auth.device == nil {
		writeError(w, http.StatusForbidden, "device_signature_required", "operation requires a registered device signature")
		return
	}
	if groupID != (entmoot.GroupID{}) && !h.checkDeviceGroup(w, r, groupID) {
		return
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	req, err := h.state.CreateSignRequest(r.Context(), SignRequest{
		DeviceID: deviceIDForRequest(auth),
		Kind:     kind,
		GroupID:  groupID,
		Payload:  append(json.RawMessage(nil), payload...),
	})
	if err != nil {
		h.logger.Error("esphttp: create sign request", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "sign request creation failed")
		return
	}
	h.notifyDeviceSignRequest(r.Context(), req)
	writeJSON(w, http.StatusAccepted, map[string]any{"sign_request": req})
}

func (h *Handler) createMessagePublishSignRequest(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, draft messagePublishDraft) {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if !h.checkDeviceGroup(w, r, groupID) {
		return
	}
	req, err := buildMessagePublishSignRequest(deviceIDForRequest(auth), groupID, draft, h.clock().UnixMilli())
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	req, err = h.state.CreateSignRequest(r.Context(), req)
	if err != nil {
		h.logger.Error("esphttp: create message publish sign request", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "sign request creation failed")
		return
	}
	h.notifyDeviceSignRequest(r.Context(), req)
	writeJSON(w, http.StatusAccepted, map[string]any{"sign_request": req})
}

func (h *Handler) notifyDeviceSignRequest(ctx context.Context, req SignRequest) {
	if h.notifier == nil || req.DeviceID == "" {
		return
	}
	state, err := h.state.GetDeviceState(ctx, req.DeviceID)
	if err != nil || state.PushToken == "" || !state.NotificationPreferences.Enabled {
		return
	}
	result, err := h.notifier.SendWakeup(ctx, espnotify.DeviceTarget{
		DeviceID: state.DeviceID,
		Platform: state.PushPlatform,
		Token:    state.PushToken,
	}, espnotify.WakeupEvent{
		Type:    espnotify.EventSignRequest,
		GroupID: req.GroupID.String(),
		Reason:  req.Kind,
	})
	if err != nil {
		if espnotify.IsInvalidToken(err) {
			_, _ = h.state.ClearPushToken(ctx, req.DeviceID)
		}
		h.logger.Warn("esphttp: sign request wakeup failed", slog.String("device_id", req.DeviceID), slog.String("err", err.Error()))
		return
	}
	h.logger.Debug("esphttp: sign request wakeup sent", slog.String("device_id", req.DeviceID), slog.String("status", result.Status))
}

func (h *Handler) completeMessagePublishSignRequest(w http.ResponseWriter, r *http.Request, req SignRequest, signature []byte) (PublishResult, bool) {
	if h.publisher == nil {
		writeError(w, http.StatusServiceUnavailable, "join_unavailable", "no running join publisher configured")
		return PublishResult{}, false
	}
	var payload messagePublishPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		h.logger.Error("esphttp: parse message publish sign request", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "sign request payload is invalid")
		return PublishResult{}, false
	}
	msg := payload.Message
	if msg.GroupID != req.GroupID {
		writeError(w, http.StatusBadRequest, "bad_request", "sign request message group does not match request group")
		return PublishResult{}, false
	}
	msg.ID = canonical.MessageID(msg)
	msg.Signature = append([]byte(nil), signature...)
	if err := signing.VerifyMessage(msg, msg.Author); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_signature", err.Error())
		return PublishResult{}, false
	}
	result, err := h.publisher.PublishSigned(r.Context(), msg)
	if err != nil {
		h.writePublishResult(w, result, err)
		return PublishResult{}, false
	}
	return result, true
}

func (h *Handler) completeExecutableSignRequest(w http.ResponseWriter, r *http.Request, req SignRequest, signature []byte) (json.RawMessage, bool) {
	if h.operations == nil {
		writeError(w, http.StatusServiceUnavailable, "operation_unavailable", "no operation executor configured")
		return nil, false
	}
	if !h.verifyOperationSignature(w, req, signature) {
		return nil, false
	}
	result, err := h.operations.ExecuteSignRequest(r.Context(), req, signature)
	if err != nil {
		var opErr *OperationError
		if errors.As(err, &opErr) {
			writeError(w, opErr.HTTPStatus, opErr.Code, opErr.Message)
			return nil, false
		}
		h.logger.Error("esphttp: execute sign request", slog.String("kind", req.Kind), slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "operation execution failed")
		return nil, false
	}
	if len(result) == 0 {
		result = json.RawMessage(`{}`)
	}
	return append(json.RawMessage(nil), result...), true
}

func (h *Handler) verifyOperationSignature(w http.ResponseWriter, req SignRequest, signature []byte) bool {
	if req.DeviceID == "" {
		writeError(w, http.StatusForbidden, "device_signature_required", "operation requires a registered device signature")
		return false
	}
	if h.devices == nil {
		writeError(w, http.StatusForbidden, "forbidden", "device signature cannot be verified")
		return false
	}
	device, ok := h.devices.lookup(req.DeviceID)
	if !ok {
		writeError(w, http.StatusForbidden, "forbidden", "sign request device is not registered")
		return false
	}
	signingPayload, err := base64.StdEncoding.DecodeString(req.SigningPayload)
	if err != nil {
		h.logger.Error("esphttp: decode operation signing payload", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "sign request signing payload is invalid")
		return false
	}
	if !ed25519.Verify(device.PublicKey, signingPayload, signature) {
		writeError(w, http.StatusBadRequest, "invalid_signature", "operation signature does not verify")
		return false
	}
	return true
}

func marshalOperationResult(w http.ResponseWriter, result any) json.RawMessage {
	data, err := json.Marshal(result)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "operation result encoding failed")
		return nil
	}
	return append(json.RawMessage(nil), data...)
}

func (h *Handler) writeSignRequestLookup(w http.ResponseWriter, r *http.Request, req SignRequest, ok bool, err error) {
	if err != nil {
		h.logger.Error("esphttp: get sign request", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "sign request lookup failed")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "sign_request_not_found", "sign request not found")
		return
	}
	if !h.signRequestVisible(w, r, req) {
		return
	}
	writeJSON(w, http.StatusOK, req)
}

func (h *Handler) writeSignRequestMutation(w http.ResponseWriter, req SignRequest, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, req)
		return
	}
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "sign_request_not_found", "sign request not found")
		return
	}
	h.logger.Error("esphttp: mutate sign request", slog.String("err", err.Error()))
	writeError(w, http.StatusInternalServerError, "internal_error", "sign request update failed")
}

func (h *Handler) checkSignRequestPending(w http.ResponseWriter, req SignRequest) bool {
	if req.Status != signRequestPending {
		writeError(w, http.StatusConflict, "sign_request_not_pending", "sign request is not pending")
		return false
	}
	if req.ExpiresAtMS > 0 && !time.UnixMilli(req.ExpiresAtMS).After(h.clock()) {
		writeError(w, http.StatusConflict, "sign_request_expired", "sign request has expired")
		return false
	}
	return true
}

func (h *Handler) signRequestVisible(w http.ResponseWriter, r *http.Request, req SignRequest) bool {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device == nil {
		return true
	}
	if req.DeviceID != "" && req.DeviceID != auth.device.ID {
		writeError(w, http.StatusForbidden, "forbidden", "device is not authorized for sign request")
		return false
	}
	if req.GroupID != (entmoot.GroupID{}) && !deviceAllowsGroup(*auth.device, req.GroupID) {
		writeError(w, http.StatusForbidden, "forbidden", "device is not authorized for group")
		return false
	}
	return true
}

func (h *Handler) withIdempotency(w http.ResponseWriter, r *http.Request, scope string, next func(http.ResponseWriter, *http.Request)) {
	key := strings.TrimSpace(r.Header.Get(idempotencyHeader))
	if key == "" {
		next(w, r)
		return
	}
	if len(key) > 256 {
		writeError(w, http.StatusBadRequest, "bad_request", "Idempotency-Key is too long")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAuthBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	sum := sha256.Sum256(body)
	bodyHash := base64.StdEncoding.EncodeToString(sum[:])
	rec, ok, err := h.state.GetIdempotencyRecord(r.Context(), scope, key)
	if err != nil {
		h.logger.Error("esphttp: idempotency lookup", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "idempotency lookup failed")
		return
	}
	if ok {
		if rec.RequestHash != bodyHash {
			writeError(w, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was already used with a different request body")
			return
		}
		writeStoredJSON(w, rec.StatusCode, rec.Response)
		return
	}
	recorder := newCaptureResponseWriter()
	next(recorder, r)
	response := recorder.body.Bytes()
	if len(response) == 0 {
		response = []byte("{}")
	}
	if err := h.state.SaveIdempotencyRecord(r.Context(), IdempotencyRecord{
		Scope:       scope,
		Key:         key,
		RequestHash: bodyHash,
		StatusCode:  recorder.statusCode(),
		Response:    append(json.RawMessage(nil), response...),
	}); err != nil {
		h.logger.Warn("esphttp: idempotency save failed", slog.String("err", err.Error()))
	}
	copyCapturedResponse(w, recorder)
}

func decodeRawBody(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) ([]byte, bool) {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
		return nil, false
	}
	if len(bytes.TrimSpace(data)) == 0 {
		data = []byte("{}")
	}
	if dst != nil {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(dst); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid JSON body: %v", err))
			return nil, false
		}
	}
	return data, true
}

func deviceAllowsGroup(device Device, groupID entmoot.GroupID) bool {
	for _, allowed := range device.Groups {
		if allowed == groupID {
			return true
		}
	}
	return false
}

func authFromContext(r *http.Request) authContext {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	return auth
}

func deviceView(device Device) map[string]any {
	groups := make([]entmoot.GroupID, 0, len(device.Groups))
	groups = append(groups, device.Groups...)
	clients := append([]string(nil), device.ClientIDs...)
	return map[string]any{
		"id":         device.ID,
		"groups":     groups,
		"client_ids": clients,
		"disabled":   device.Disabled,
	}
}

type captureResponseWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newCaptureResponseWriter() *captureResponseWriter {
	return &captureResponseWriter{header: make(http.Header)}
}

func (w *captureResponseWriter) Header() http.Header {
	return w.header
}

func (w *captureResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *captureResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(data)
}

func (w *captureResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func copyCapturedResponse(dst http.ResponseWriter, src *captureResponseWriter) {
	for k, values := range src.header {
		for _, v := range values {
			dst.Header().Add(k, v)
		}
	}
	dst.WriteHeader(src.statusCode())
	_, _ = dst.Write(src.body.Bytes())
}

func writeStoredJSON(w http.ResponseWriter, status int, body []byte) {
	if status == 0 {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

type nonceCache struct {
	mu    sync.Mutex
	clock func() time.Time
	seen  map[string]time.Time
}

func newNonceCache(clock func() time.Time) *nonceCache {
	return &nonceCache{
		clock: clock,
		seen:  make(map[string]time.Time),
	}
}

func (c *nonceCache) use(deviceID, nonce string, expires time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	for key, exp := range c.seen {
		if !exp.After(now) {
			delete(c.seen, key)
		}
	}
	key := deviceID + "\x00" + nonce
	if exp, ok := c.seen[key]; ok && exp.After(now) {
		return false
	}
	c.seen[key] = expires
	return true
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Message: message}})
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeGroupID(s string) (entmoot.GroupID, error) {
	var gid entmoot.GroupID
	raw, err := decodeBase64Array32("group_id", s)
	if err != nil {
		return gid, err
	}
	copy(gid[:], raw)
	return gid, nil
}

func decodeBase64Array32(name, s string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("%s: expected 32 bytes, got %d", name, len(raw))
	}
	return raw, nil
}
