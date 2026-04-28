// Package esphttp exposes a small HTTP mailbox bridge for Entmoot Service
// Provider deployments.
package esphttp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
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
	"entmoot/pkg/entmoot/mailbox"
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
	timestampHeader   = "X-Entmoot-Timestamp-Ms"
	nonceHeader       = "X-Entmoot-Nonce"
	signatureHeader   = "X-Entmoot-Signature"
	deviceAuthVersion = "ENTMOOT-ESP-AUTH-V1"
	deviceAuthSkew    = 5 * time.Minute
	maxAuthBodyBytes  = 16 << 20
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
	d, ok := r.byID[strings.TrimSpace(id)]
	return d, ok
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
	default:
		writeError(w, http.StatusNotFound, "not_found", "not found")
	}
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
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "bad_request", "limit must be a non-negative integer")
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
	if !ok || device.Disabled {
		writeError(w, http.StatusUnauthorized, "unauthorized", "unknown or disabled device")
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
