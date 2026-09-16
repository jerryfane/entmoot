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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/espnotify"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// GroupExistsFunc reports whether groupID is locally joined/served.
type GroupExistsFunc func(context.Context, entmoot.GroupID) (bool, error)

// DiagnosticsProvider produces a group-scoped health report for ESP clients.
type DiagnosticsProvider interface {
	GroupDiagnostics(context.Context, entmoot.GroupID, bool, time.Duration) (any, error)
}

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
	Diagnostics DiagnosticsProvider
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
	deviceIDHeader        = "X-Entmoot-Device-ID"
	memberIDHeader        = "X-Entmoot-Member-ID"
	memberPeerHeader      = "X-Entmoot-Peer-ID"
	memberPubKeyHeader    = "X-Entmoot-Member-Pubkey"
	memberSignatureHeader = "X-Entmoot-Member-Signature"
	idempotencyHeader     = "Idempotency-Key"
	timestampHeader       = "X-Entmoot-Timestamp-Ms"
	nonceHeader           = "X-Entmoot-Nonce"
	signatureHeader       = "X-Entmoot-Signature"
	deviceAuthVersion     = "ENTMOOT-ESP-AUTH-V1"
	memberAuthVersion     = "ENTMOOT-ESP-MEMBER-AUTH-V2"
	deviceAuthSkew        = 5 * time.Minute
	maxAuthBodyBytes      = 16 << 20
	maxListLimit          = 200
)

// Device describes one ESP client device authorized to use this service.
type Device struct {
	ID            string
	PublicKey     ed25519.PublicKey
	Groups        []entmoot.GroupID
	AdminGroups   []entmoot.GroupID
	ClientIDs     []string
	MemberID      entmoot.MemberID
	PeerID        string
	EntmootPubKey []byte
	Disabled      bool
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
		if d.MemberID != (entmoot.MemberID{}) && (d.PeerID == "" || len(d.EntmootPubKey) != ed25519.PublicKeySize) {
			return nil, fmt.Errorf("esphttp: device %q has incomplete member identity", d.ID)
		}
		if _, exists := reg.byID[d.ID]; exists {
			return nil, fmt.Errorf("esphttp: duplicate device id %q", d.ID)
		}
		d.PublicKey = append(ed25519.PublicKey(nil), d.PublicKey...)
		d.EntmootPubKey = append([]byte(nil), d.EntmootPubKey...)
		d.Groups = append([]entmoot.GroupID(nil), d.Groups...)
		d.AdminGroups = append([]entmoot.GroupID(nil), d.AdminGroups...)
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

// WithAdminGroupGranted returns a validated registry copy with gid granted as
// an admin-managed group to deviceID. Admin implies management privileges, not
// message access by itself.
func (r *DeviceRegistry) WithAdminGroupGranted(deviceID string, gid entmoot.GroupID) (*DeviceRegistry, bool, error) {
	return r.withDeviceAdminGroup(deviceID, gid, true)
}

// WithAdminGroupRevoked returns a validated registry copy with gid removed
// from the admin-managed group set for deviceID.
func (r *DeviceRegistry) WithAdminGroupRevoked(deviceID string, gid entmoot.GroupID) (*DeviceRegistry, bool, error) {
	return r.withDeviceAdminGroup(deviceID, gid, false)
}

// WithDeviceIdentity returns a validated registry copy with deviceID bound to
// the Entmoot member identity used by member-scoped operations.
func (r *DeviceRegistry) WithDeviceIdentity(deviceID string, memberID entmoot.MemberID, peerID string, entmootPubKey []byte) (*DeviceRegistry, bool, error) {
	if r == nil {
		return nil, false, errors.New("esphttp: device registry is not configured")
	}
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return nil, false, errors.New("esphttp: device id is required")
	}
	if len(entmootPubKey) != ed25519.PublicKeySize {
		return nil, false, fmt.Errorf("esphttp: entmoot pubkey length %d", len(entmootPubKey))
	}
	derivedMemberID, err := entmoot.MemberIDFromPublicKey(entmootPubKey)
	if err != nil || memberID != derivedMemberID {
		return nil, false, errors.New("esphttp: member id does not match entmoot pubkey")
	}
	derivedPeerID := peerIDForPublicKey(entmootPubKey)
	if derivedPeerID == "" || peerID != derivedPeerID {
		return nil, false, errors.New("esphttp: peer id does not match entmoot pubkey")
	}
	devices := r.Snapshot()
	changed := false
	found := false
	for i := range devices {
		if devices[i].ID != deviceID {
			continue
		}
		found = true
		if devices[i].MemberID == memberID && devices[i].PeerID == peerID && bytes.Equal(devices[i].EntmootPubKey, entmootPubKey) {
			break
		}
		devices[i].MemberID = memberID
		devices[i].PeerID = peerID
		devices[i].EntmootPubKey = append([]byte(nil), entmootPubKey...)
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

func (r *DeviceRegistry) withDeviceAdminGroup(deviceID string, gid entmoot.GroupID, grant bool) (*DeviceRegistry, bool, error) {
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
		for j, existing := range devices[i].AdminGroups {
			if existing == gid {
				idx = j
				break
			}
		}
		if grant {
			if idx >= 0 {
				break
			}
			devices[i].AdminGroups = append(devices[i].AdminGroups, gid)
			changed = true
			break
		}
		if idx < 0 {
			break
		}
		devices[i].AdminGroups = append(devices[i].AdminGroups[:idx], devices[i].AdminGroups[idx+1:]...)
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
	d.EntmootPubKey = append([]byte(nil), d.EntmootPubKey...)
	d.Groups = append([]entmoot.GroupID(nil), d.Groups...)
	d.AdminGroups = append([]entmoot.GroupID(nil), d.AdminGroups...)
	d.ClientIDs = append([]string(nil), d.ClientIDs...)
	return d
}

// Publisher submits already-signed messages to the running Entmoot daemon.
type Publisher interface {
	PublishSigned(context.Context, entmoot.Message) (PublishResult, error)
}

// PublishResult is the HTTP response for an accepted phone-signed message.
type PublishResult struct {
	Status         string            `json:"status"`
	MessageID      entmoot.MessageID `json:"message_id"`
	GroupID        entmoot.GroupID   `json:"group_id"`
	AuthorMemberID entmoot.MemberID  `json:"author_member_id"`
	TimestampMS    int64             `json:"timestamp_ms"`
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
	token                 string
	authMode              AuthMode
	devices               *DeviceRegistry
	nonceCache            *nonceCache
	clock                 func() time.Time
	service               *mailbox.Service
	publisher             Publisher
	operations            OperationExecutor
	notifier              espnotify.Notifier
	state                 StateStore
	groups                GroupCatalog
	diagnostics           DiagnosticsProvider
	groupExists           GroupExistsFunc
	groupExistsConfigured bool
	logger                *slog.Logger
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
	groupExistsConfigured := cfg.GroupExists != nil
	groupExists := cfg.GroupExists
	if groupExists == nil {
		groupExists = func(context.Context, entmoot.GroupID) (bool, error) { return true, nil }
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		token:                 cfg.Token,
		authMode:              authMode,
		devices:               cfg.Devices,
		nonceCache:            newNonceCache(clock),
		clock:                 clock,
		service:               cfg.Service,
		publisher:             cfg.Publisher,
		operations:            cfg.Operations,
		notifier:              cfg.Notifier,
		state:                 state,
		groups:                cfg.Groups,
		diagnostics:           cfg.Diagnostics,
		groupExists:           groupExists,
		groupExistsConfigured: groupExistsConfigured,
		logger:                logger,
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
	if h.handleOpenInviteRedeem(w, r) {
		return
	}
	if h.handlePublicMootRoute(w, r) {
		return
	}
	if h.handleCapabilities(w, r) {
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
	case "/v1/open-invites/accept":
		h.handleOpenInviteAccept(w, r)
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

func (h *Handler) handleCapabilities(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/v1/capabilities" {
		return false
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return true
	}
	writeJSON(w, http.StatusOK, map[string]any{})
	return true
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
	if auth.member != nil {
		resp["member"] = memberAuthView(*auth.member)
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
		h.withIdempotency(w, r, "group_create", func(w http.ResponseWriter, r *http.Request) bool {
			return h.authorizeSignRequestCreation(w, r, "group_create", entmoot.GroupID{})
		}, func(w http.ResponseWriter, r *http.Request) {
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
	groups, err := h.listGroupsForRequest(r)
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

func (h *Handler) listGroupsForRequest(r *http.Request) ([]GroupSummary, error) {
	if parseBoolQuery(r.URL.Query().Get("include_hidden")) {
		if catalog, ok := h.groups.(GroupCatalogWithOptions); ok {
			return catalog.ListGroupsWithOptions(r.Context(), GroupListOptions{IncludeHidden: true})
		}
	}
	return h.groups.ListGroups(r.Context())
}

func (h *Handler) handleGroupSubroute(w http.ResponseWriter, r *http.Request) bool {
	const prefix = "/v1/groups/"
	escapedPath := r.URL.EscapedPath()
	if !strings.HasPrefix(escapedPath, prefix) {
		return false
	}
	rest := strings.TrimPrefix(escapedPath, prefix)
	escapedGroup, suffix, _ := strings.Cut(rest, "/")
	rawGroup, err := url.PathUnescape(escapedGroup)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return true
	}
	groupID, err := decodeGroupID(rawGroup)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return true
	}
	if ok := h.checkGroupExists(w, r, groupID); !ok {
		return true
	}
	if strings.HasPrefix(suffix, "members/") {
		if r.Method != http.MethodDelete {
			methodNotAllowed(w, http.MethodDelete)
			return true
		}
		escapedMember := strings.TrimPrefix(suffix, "members/")
		h.withIdempotency(w, r, "member_remove:"+groupID.String()+":"+escapedMember, func(w http.ResponseWriter, r *http.Request) bool {
			return h.authorizeSignRequestCreation(w, r, "member_remove", groupID)
		}, func(w http.ResponseWriter, r *http.Request) {
			h.createMemberRemoveSignRequest(w, r, groupID, escapedMember)
		})
		return true
	}
	if strings.HasPrefix(suffix, "live-agents/") {
		escapedNode := strings.TrimPrefix(suffix, "live-agents/")
		nodeID, ok := parseLiveAgentNodePath(w, escapedNode)
		if !ok {
			return true
		}
		switch r.Method {
		case http.MethodPut:
			h.withIdempotency(w, r, "live_agent_config:"+groupID.String()+":"+nodeID.String(), func(w http.ResponseWriter, r *http.Request) bool {
				return h.checkLiveAgentConfigWrite(w, r, groupID, nodeID)
			}, func(w http.ResponseWriter, r *http.Request) {
				h.handleUpsertLiveAgentConfig(w, r, groupID, nodeID)
			})
		case http.MethodDelete:
			h.withIdempotency(w, r, "live_agent_config_delete:"+groupID.String()+":"+nodeID.String(), func(w http.ResponseWriter, r *http.Request) bool {
				return h.checkLiveAgentConfigWrite(w, r, groupID, nodeID)
			}, func(w http.ResponseWriter, r *http.Request) {
				h.handleDeleteLiveAgentConfig(w, r, groupID, nodeID)
			})
		default:
			methodNotAllowed(w, http.MethodPut+", "+http.MethodDelete)
		}
		return true
	}
	if strings.HasPrefix(suffix, "open-invites/") {
		rest := strings.TrimPrefix(suffix, "open-invites/")
		escapedInvite, action, ok := strings.Cut(rest, "/")
		if !ok || action != "revoke" {
			writeError(w, http.StatusNotFound, "not_found", "not found")
			return true
		}
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return true
		}
		h.withIdempotency(w, r, "open_invite_revoke:"+groupID.String()+":"+escapedInvite, func(w http.ResponseWriter, r *http.Request) bool {
			return h.checkDeviceGroupAdmin(w, r, groupID)
		}, func(w http.ResponseWriter, r *http.Request) {
			h.handleRevokeOpenInvite(w, r, groupID, escapedInvite)
		})
		return true
	}
	switch suffix {
	case "":
		switch r.Method {
		case http.MethodGet:
			h.handleGetGroup(w, r, groupID)
		case http.MethodPatch:
			h.withIdempotency(w, r, "group_update:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) bool {
				return h.authorizeSignRequestCreation(w, r, "group_update", groupID)
			}, func(w http.ResponseWriter, r *http.Request) {
				h.createSignRequestFromHTTP(w, r, "group_update", groupID)
			})
		default:
			methodNotAllowed(w, http.MethodGet+", "+http.MethodPatch)
		}
	case "policy":
		switch r.Method {
		case http.MethodGet:
			h.handleGetGroupPolicy(w, r, groupID)
		case http.MethodPut:
			h.withIdempotency(w, r, "group_policy_update:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) bool {
				return h.authorizeSignRequestCreation(w, r, signRequestKindGroupPolicyUpdate, groupID)
			}, func(w http.ResponseWriter, r *http.Request) {
				h.createSignRequestFromHTTP(w, r, signRequestKindGroupPolicyUpdate, groupID)
			})
		case http.MethodDelete:
			h.withIdempotency(w, r, "group_policy_clear:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) bool {
				return h.authorizeSignRequestCreation(w, r, signRequestKindGroupPolicyClear, groupID)
			}, func(w http.ResponseWriter, r *http.Request) {
				h.createSignRequestFromHTTP(w, r, signRequestKindGroupPolicyClear, groupID)
			})
		default:
			methodNotAllowed(w, http.MethodGet+", "+http.MethodPut+", "+http.MethodDelete)
		}
	case "public-moot/publish":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return true
		}
		h.withIdempotency(w, r, "group_public_publish:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) bool {
			return h.authorizeSignRequestCreation(w, r, signRequestKindGroupPublicPublish, groupID)
		}, func(w http.ResponseWriter, r *http.Request) {
			h.createSignRequestFromHTTP(w, r, signRequestKindGroupPublicPublish, groupID)
		})
	case "members":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleListMembers(w, r, groupID)
	case "live-agents":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleListLiveAgentConfigs(w, r, groupID)
	case "invites":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return true
		}
		h.withIdempotency(w, r, "invite_create:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) bool {
			return h.authorizeSignRequestCreation(w, r, "invite_create", groupID)
		}, func(w http.ResponseWriter, r *http.Request) {
			h.createSignRequestFromHTTP(w, r, "invite_create", groupID)
		})
	case "open-invites":
		switch r.Method {
		case http.MethodGet:
			h.handleListOpenInvites(w, r, groupID)
		case http.MethodPost:
			h.withIdempotency(w, r, "open_invite_create:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) bool {
				return h.authorizeSignRequestCreation(w, r, "open_invite_create", groupID)
			}, func(w http.ResponseWriter, r *http.Request) {
				h.createSignRequestFromHTTP(w, r, "open_invite_create", groupID)
			})
		default:
			methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
		}
	case "diagnostics":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleGroupDiagnostics(w, r, groupID)
	case "messages":
		switch r.Method {
		case http.MethodGet:
			h.handleGroupMessages(w, r, groupID)
		case http.MethodPost:
			h.withIdempotency(w, r, "message_publish:"+groupID.String(), func(w http.ResponseWriter, r *http.Request) bool {
				return h.checkDeviceGroup(w, r, groupID)
			}, func(w http.ResponseWriter, r *http.Request) {
				h.handleGroupMessagePublish(w, r, groupID)
			})
		default:
			methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
		}
	case "history":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleGroupHistory(w, r, groupID)
	case "message-context":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleGroupMessageContext(w, r, groupID)
	case "search":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleGroupSearch(w, r, groupID)
	case "topics":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleGroupTopics(w, r, groupID)
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
	if !h.checkDeviceGroup(w, r, groupID) {
		return
	}
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

func (h *Handler) handleGetGroupPolicy(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if !h.checkDeviceGroup(w, r, groupID) {
		return
	}
	reporter, ok := h.operations.(GroupPolicyReporter)
	if !ok || reporter == nil {
		writeError(w, http.StatusServiceUnavailable, "policy_unavailable", "group policy reporter is not configured")
		return
	}
	raw, err := reporter.GroupPolicyReport(r.Context(), groupID)
	if err != nil {
		h.writeOperationError(w, "group policy status", "group policy status failed", err)
		return
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	writeJSON(w, http.StatusOK, raw)
}

func (h *Handler) handleListMembers(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if !h.checkDeviceGroup(w, r, groupID) {
		return
	}
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
	members, err = EnrichMemberDisplayNames(r.Context(), h.state, groupID, members)
	if err != nil {
		h.logger.Error("esphttp: enrich members", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "member listing failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members})
}

type liveAgentConfigHTTPPayload struct {
	Enabled           *bool    `json:"enabled,omitempty"`
	Mode              string   `json:"mode,omitempty"`
	TopicFilters      []string `json:"topic_filters,omitempty"`
	AllowedActions    []string `json:"allowed_actions,omitempty"`
	MaxActionsPerScan int      `json:"max_actions_per_scan,omitempty"`
	MaxActionBytes    int      `json:"max_action_bytes,omitempty"`
}

func (h *Handler) handleListLiveAgentConfigs(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if !h.checkLiveAgentConfigRead(w, r, groupID) {
		return
	}
	configs, err := h.state.ListLiveAgentConfigs(r.Context(), groupID)
	if err != nil {
		h.logger.Error("esphttp: list live agent configs", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "live agent config listing failed")
		return
	}
	presence, err := h.state.ListLiveAgentPresence(r.Context(), groupID)
	if err != nil {
		h.logger.Error("esphttp: list live agent presence", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "live agent presence listing failed")
		return
	}
	states := LiveAgentStatesByMember(configs, presence, h.clock().UnixMilli())
	writeJSON(w, http.StatusOK, map[string]any{"configs": configs, "presence": presence, "members": states})
}

func (h *Handler) handleUpsertLiveAgentConfig(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, nodeID entmoot.MemberID) {
	if !h.checkLiveAgentConfigWrite(w, r, groupID, nodeID) {
		return
	}
	var payload liveAgentConfigHTTPPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body")
		return
	}
	enabled := true
	if payload.Enabled != nil {
		enabled = *payload.Enabled
	}
	mode := NormalizeLiveMode(payload.Mode)
	if mode == "" {
		if strings.TrimSpace(payload.Mode) != "" {
			writeError(w, http.StatusBadRequest, "bad_request", "invalid live mode")
			return
		}
		mode = LiveModeReplyOnMention
	}
	topicFilters := NormalizeLiveTopicFilters(payload.TopicFilters)
	if len(topicFilters) == 0 {
		topicFilters = []string{"#"}
	}
	if unknown := UnknownLiveActions(payload.AllowedActions); len(unknown) > 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "unknown live actions: "+strings.Join(unknown, ", "))
		return
	}
	actions, ok := liveActionsForHTTPPayload(w, mode, payload.AllowedActions)
	if !ok {
		return
	}
	if payload.MaxActionsPerScan < 0 || payload.MaxActionBytes < 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "live spam controls must be non-negative")
		return
	}
	cfg, err := h.state.UpsertLiveAgentConfig(r.Context(), LiveAgentConfig{
		GroupID:           groupID,
		MemberID:          nodeID,
		Enabled:           enabled,
		Mode:              mode,
		TopicFilters:      topicFilters,
		AllowedActions:    actions,
		MaxActionsPerScan: payload.MaxActionsPerScan,
		MaxActionBytes:    payload.MaxActionBytes,
		UpdatedAtMS:       h.clock().UnixMilli(),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"config": cfg})
}

func liveActionsForHTTPPayload(w http.ResponseWriter, mode string, raw []string) ([]string, bool) {
	actionsExplicit := raw != nil
	actions := NormalizeLiveActions(raw)
	if actionsExplicit && len(actions) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "live action list cannot be empty")
		return nil, false
	}
	if mode == LiveModeOperator && len(actions) == 0 && !actionsExplicit {
		actions = DefaultLiveActions()
	}
	return actions, true
}

func (h *Handler) handleDeleteLiveAgentConfig(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, nodeID entmoot.MemberID) {
	if !h.checkLiveAgentConfigWrite(w, r, groupID, nodeID) {
		return
	}
	if err := h.state.DeleteLiveAgentConfig(r.Context(), groupID, nodeID, h.clock().UnixMilli()); err != nil {
		h.logger.Error("esphttp: delete live agent config", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "live agent config delete failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"group_id": groupID, "node_id": nodeID, "enabled": false})
}

func (h *Handler) handleListOpenInvites(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if !h.checkDeviceGroupAdmin(w, r, groupID) {
		return
	}
	records, err := h.state.ListOpenInvitesByGroup(r.Context(), groupID)
	if err != nil {
		h.logger.Error("esphttp: list open invites", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "open invite listing failed")
		return
	}
	now := h.clock().UnixMilli()
	out := make([]OpenInviteSummary, 0, len(records))
	for _, rec := range records {
		out = append(out, OpenInviteSummaryFromRecord(rec, now))
	}
	writeJSON(w, http.StatusOK, map[string]any{"open_invites": out})
}

func (h *Handler) handleRevokeOpenInvite(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, escapedInviteID string) {
	if !h.checkDeviceGroupAdmin(w, r, groupID) {
		return
	}
	inviteID, err := url.PathUnescape(escapedInviteID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	inviteID = strings.TrimSpace(inviteID)
	if inviteID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "open invite id is required")
		return
	}
	existing, ok, err := h.state.GetOpenInviteByTokenHash(r.Context(), inviteID)
	if err != nil {
		h.logger.Error("esphttp: get open invite for revoke", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "open invite lookup failed")
		return
	}
	if !ok || existing.GroupID != groupID {
		writeError(w, http.StatusNotFound, "open_invite_not_found", "open invite not found")
		return
	}
	rec, ok, err := h.state.RevokeOpenInvite(r.Context(), inviteID, h.clock().UnixMilli())
	if err != nil {
		h.logger.Error("esphttp: revoke open invite", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "open invite revoke failed")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "open_invite_not_found", "open invite not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"open_invite": OpenInviteSummaryFromRecord(rec, h.clock().UnixMilli())})
}

func (h *Handler) handleGroupDiagnostics(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if !h.checkDeviceGroup(w, r, groupID) {
		return
	}
	if h.diagnostics == nil {
		writeError(w, http.StatusServiceUnavailable, "diagnostics_unavailable", "diagnostics are not configured")
		return
	}
	probe := parseBoolQuery(r.URL.Query().Get("probe"))
	timeout := 3 * time.Second
	if raw := strings.TrimSpace(r.URL.Query().Get("timeout")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "timeout must be a Go duration such as 3s")
			return
		}
		timeout = parsed
	}
	report, err := h.diagnostics.GroupDiagnostics(r.Context(), groupID, probe, timeout)
	if err != nil {
		var opErr *OperationError
		if errors.As(err, &opErr) {
			writeError(w, opErr.HTTPStatus, opErr.Code, opErr.Message)
			return
		}
		h.logger.Error("esphttp: group diagnostics", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "diagnostics failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"group": report})
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
	if !h.checkDeviceClientRead(w, r, groupID, clientID) {
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

func (h *Handler) handleGroupHistory(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
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
	if !h.checkDeviceClientRead(w, r, groupID, clientID) {
		return
	}
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("limit must be between 1 and %d", maxListLimit))
			return
		}
		limit = n
	}
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	boundary, err := parseHistoryCursor(strings.TrimSpace(r.URL.Query().Get("cursor")), groupID, topic)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if topic != "" {
		result, err := h.service.TopicHistoryBefore(r.Context(), groupID, topic, limit, boundary)
		if err == nil {
			result.NextCursor = encodeHistoryCursor(groupID, topic, result.NextCursorBoundary)
		}
		h.writeMailboxResult(w, "group topic history", result, err)
		return
	}
	result, err := h.service.HistoryBefore(r.Context(), groupID, limit, boundary)
	if err == nil {
		result.NextCursor = encodeHistoryCursor(groupID, topic, result.NextCursorBoundary)
	}
	h.writeMailboxResult(w, "group history", result, err)
}

func (h *Handler) handleGroupSearch(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
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
	if !h.checkDeviceClientRead(w, r, groupID, clientID) {
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	normalized, err := store.NormalizeSearchQuery(query)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("limit must be between 1 and %d", maxListLimit))
			return
		}
		limit = n
	}
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	queryHash := searchQueryHash(normalized.Query)
	boundary, err := parseSearchCursor(strings.TrimSpace(r.URL.Query().Get("cursor")), groupID, topic, queryHash)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	result, err := h.service.Search(r.Context(), groupID, normalized.Query, limit, boundary, topic)
	if err == nil {
		result.NextCursor = encodeSearchCursor(groupID, topic, queryHash, result.NextCursorBoundary)
	}
	h.writeMailboxResult(w, "group search", result, err)
}

func (h *Handler) handleGroupMessageContext(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
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
	if !h.checkDeviceClientRead(w, r, groupID, clientID) {
		return
	}
	messageID, ok := parseMessageIDQuery(w, r.URL.Query().Get("message_id"))
	if !ok {
		return
	}
	before, ok := parseContextSideQuery(w, r, "before", store.DefaultMessageContextBefore)
	if !ok {
		return
	}
	after, ok := parseContextSideQuery(w, r, "after", store.DefaultMessageContextAfter)
	if !ok {
		return
	}
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	result, err := h.service.MessageContext(r.Context(), groupID, messageID, before, after, topic)
	if err == nil {
		result.OlderCursor = encodeHistoryCursor(groupID, topic, result.OlderCursorBoundary)
	}
	h.writeMessageContextResult(w, result, err)
}

func parseMessageIDQuery(w http.ResponseWriter, raw string) (entmoot.MessageID, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "message_id is required")
		return entmoot.MessageID{}, false
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(decoded) != 32 {
		writeError(w, http.StatusBadRequest, "bad_request", "message_id must be a base64-encoded 32-byte id")
		return entmoot.MessageID{}, false
	}
	var id entmoot.MessageID
	copy(id[:], decoded)
	return id, true
}

func parseContextSideQuery(w http.ResponseWriter, r *http.Request, name string, fallback int) (int, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return fallback, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > store.MaxMessageContextSide {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("%s must be between 0 and %d", name, store.MaxMessageContextSide))
		return 0, false
	}
	return n, true
}

func (h *Handler) writeMessageContextResult(w http.ResponseWriter, result mailbox.MessageContextResult, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, result)
		return
	}
	switch {
	case errors.Is(err, mailbox.ErrInvalidClient):
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "message_not_found", "message not found")
	default:
		h.logger.Error("esphttp: group message context", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "mailbox operation failed")
	}
}

type historyCursorPayload struct {
	Version        int               `json:"v"`
	GroupID        entmoot.GroupID   `json:"group_id"`
	Topic          string            `json:"topic,omitempty"`
	TimestampMS    int64             `json:"timestamp_ms"`
	AuthorMemberID entmoot.MemberID  `json:"author_member_id"`
	MessageID      entmoot.MessageID `json:"message_id"`
}

func parseHistoryCursor(raw string, groupID entmoot.GroupID, topic string) (*store.PageBoundary, error) {
	if raw == "" {
		return nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid history cursor")
	}
	var payload historyCursorPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("invalid history cursor")
	}
	if payload.Version != 2 {
		return nil, fmt.Errorf("unsupported history cursor")
	}
	if payload.GroupID != groupID || payload.Topic != topic {
		return nil, fmt.Errorf("history cursor does not match requested group or topic")
	}
	if payload.MessageID == (entmoot.MessageID{}) {
		return nil, fmt.Errorf("invalid history cursor")
	}
	return &store.PageBoundary{
		TimestampMS:    payload.TimestampMS,
		AuthorMemberID: payload.AuthorMemberID,
		MessageID:      payload.MessageID,
	}, nil
}

func encodeHistoryCursor(groupID entmoot.GroupID, topic string, boundary *store.PageBoundary) string {
	if boundary == nil {
		return ""
	}
	data, err := json.Marshal(historyCursorPayload{
		Version:        2,
		GroupID:        groupID,
		Topic:          topic,
		TimestampMS:    boundary.TimestampMS,
		AuthorMemberID: boundary.AuthorMemberID,
		MessageID:      boundary.MessageID,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

type searchCursorPayload struct {
	Version        int               `json:"v"`
	GroupID        entmoot.GroupID   `json:"group_id"`
	Topic          string            `json:"topic,omitempty"`
	QueryHash      string            `json:"query_hash"`
	TimestampMS    int64             `json:"timestamp_ms"`
	AuthorMemberID entmoot.MemberID  `json:"author_member_id"`
	MessageID      entmoot.MessageID `json:"message_id"`
}

func parseSearchCursor(raw string, groupID entmoot.GroupID, topic, queryHash string) (*store.SearchBoundary, error) {
	if raw == "" {
		return nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid search cursor")
	}
	var payload searchCursorPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("invalid search cursor")
	}
	if payload.Version != 2 {
		return nil, fmt.Errorf("unsupported search cursor")
	}
	if payload.GroupID != groupID || payload.Topic != topic || payload.QueryHash != queryHash {
		return nil, fmt.Errorf("search cursor does not match requested group, topic, or query")
	}
	if payload.MessageID == (entmoot.MessageID{}) {
		return nil, fmt.Errorf("invalid search cursor")
	}
	return &store.SearchBoundary{
		TimestampMS:    payload.TimestampMS,
		AuthorMemberID: payload.AuthorMemberID,
		MessageID:      payload.MessageID,
	}, nil
}

func encodeSearchCursor(groupID entmoot.GroupID, topic, queryHash string, boundary *store.SearchBoundary) string {
	if boundary == nil {
		return ""
	}
	data, err := json.Marshal(searchCursorPayload{
		Version:        2,
		GroupID:        groupID,
		Topic:          topic,
		QueryHash:      queryHash,
		TimestampMS:    boundary.TimestampMS,
		AuthorMemberID: boundary.AuthorMemberID,
		MessageID:      boundary.MessageID,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func searchQueryHash(query string) string {
	sum := sha256.Sum256([]byte(query))
	return hex.EncodeToString(sum[:])
}

func (h *Handler) handleGroupTopics(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
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
	if !h.checkDeviceClientRead(w, r, groupID, clientID) {
		return
	}
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("limit must be between 1 and %d", maxListLimit))
			return
		}
		limit = n
	}
	result, err := h.service.Topics(r.Context(), groupID, limit)
	h.writeMailboxResult(w, "group topics", result, err)
}

func (h *Handler) handleGroupMessagePublish(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if !h.checkDeviceGroup(w, r, groupID) {
		return
	}
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
	h.withIdempotency(w, r, "invite_accept", func(w http.ResponseWriter, r *http.Request) bool {
		return h.authorizeSignRequestCreation(w, r, "invite_accept", entmoot.GroupID{})
	}, func(w http.ResponseWriter, r *http.Request) {
		h.createSignRequestFromHTTP(w, r, "invite_accept", entmoot.GroupID{})
	})
}

func (h *Handler) handleOpenInviteAccept(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	h.withIdempotency(w, r, "open_invite_accept", func(w http.ResponseWriter, r *http.Request) bool {
		return h.authorizeSignRequestCreation(w, r, "open_invite_accept", entmoot.GroupID{})
	}, func(w http.ResponseWriter, r *http.Request) {
		h.createSignRequestFromHTTP(w, r, "open_invite_accept", entmoot.GroupID{})
	})
}

func (h *Handler) handleOpenInviteRedeem(w http.ResponseWriter, r *http.Request) bool {
	const prefix = "/v1/open-invites/"
	if !strings.HasPrefix(r.URL.EscapedPath(), prefix) {
		return false
	}
	if r.URL.EscapedPath() == prefix+"accept" {
		return false
	}
	rest := strings.TrimPrefix(r.URL.EscapedPath(), prefix)
	escapedToken, suffix, ok := strings.Cut(rest, "/")
	if !ok || suffix != "redeem" {
		writeError(w, http.StatusNotFound, "not_found", "not found")
		return true
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return true
	}
	token, err := url.PathUnescape(escapedToken)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return true
	}
	var payload json.RawMessage
	body, decoded := decodeRawBody(w, r, 1<<20, &payload)
	if !decoded {
		return true
	}
	if len(payload) == 0 || string(payload) == "null" {
		body = []byte("{}")
	}
	redeemer, ok := h.operations.(OpenInviteRedeemer)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "open_invite_unavailable", "open invite redemption is not configured")
		return true
	}
	result, err := redeemer.RedeemOpenInvite(r.Context(), token, body)
	if err != nil {
		var opErr *OperationError
		if errors.As(err, &opErr) {
			writeError(w, opErr.HTTPStatus, opErr.Code, opErr.Message)
			return true
		}
		h.logger.Error("esphttp: redeem open invite", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "open invite redemption failed")
		return true
	}
	if len(result) == 0 {
		result = json.RawMessage(`{}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(result)
	return true
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
		h.withIdempotency(w, r, "sign_request_complete:"+id, func(w http.ResponseWriter, r *http.Request) bool {
			return h.authorizeSignRequestCompletion(w, r, id)
		}, func(w http.ResponseWriter, r *http.Request) {
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

func (h *Handler) authorizeSignRequestCompletion(w http.ResponseWriter, r *http.Request, id string) bool {
	req, found, err := h.state.GetSignRequest(r.Context(), id)
	if err != nil {
		h.writeSignRequestMutation(w, SignRequest{}, err)
		return false
	}
	if !found {
		h.writeSignRequestMutation(w, SignRequest{}, sql.ErrNoRows)
		return false
	}
	if !h.signRequestVisible(w, r, req) {
		return false
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
	h.withIdempotency(w, r, "push_token", h.authorizePushTokenMutation, h.handlePushTokenMutation)
}

func (h *Handler) authorizePushTokenMutation(w http.ResponseWriter, r *http.Request) bool {
	if authFromContext(r).device == nil {
		writeError(w, http.StatusForbidden, "forbidden", "device auth is required")
		return false
	}
	return true
}

func (h *Handler) handlePushTokenMutation(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r)
	if !h.authorizePushTokenMutation(w, r) {
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
		req.Message.Author.MemberID == nil &&
		req.Message.Author.PeerID == "" &&
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
	if !h.checkGroupExists(w, r, groupID) {
		return false
	}
	if !h.checkDeviceGroup(w, r, groupID) {
		return false
	}
	return true
}

func (h *Handler) checkGroupExists(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) bool {
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
	case errors.Is(err, store.ErrInvalidSearchQuery):
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
	bearer bool
	device *Device
	member *MemberAuth
}

type MemberAuth struct {
	MemberID      entmoot.MemberID `json:"member_id"`
	PeerID        string           `json:"peer_id"`
	EntmootPubKey []byte           `json:"entmoot_pubkey"`
	Method        string           `json:"-"`
	Path          string           `json:"-"`
	TimestampMS   int64            `json:"-"`
	Nonce         string           `json:"-"`
	Signature     string           `json:"-"`
}

func (h *Handler) authorize(w http.ResponseWriter, r *http.Request) (authContext, bool) {
	body, ok := h.bufferBodyForAuth(w, r)
	if !ok {
		return authContext{}, false
	}
	switch h.authMode {
	case AuthModeBearer:
		bearerOK := h.authorizedBearer(r)
		if requestHasMemberAuth(r) && memberAuthAllowedForRequest(r) {
			auth, msg, ok := h.memberAuthFromRequest(r, body)
			if ok {
				auth.bearer = bearerOK
				return auth, true
			}
			if !bearerOK {
				writeError(w, http.StatusUnauthorized, "unauthorized", msg)
				return authContext{}, false
			}
		}
		if bearerOK {
			return authContext{bearer: true}, true
		}
	case AuthModeDevice:
		if r.Header.Get(deviceIDHeader) != "" {
			return h.authorizedDevice(w, r, body)
		}
		if requestHasMemberAuth(r) && memberAuthAllowedForRequest(r) {
			return h.authorizedMember(w, r, body)
		}
	case AuthModeDual:
		bearerOK := h.authorizedBearer(r)
		memberAttempted := false
		memberMsg := ""
		if requestHasMemberAuth(r) && memberAuthAllowedForRequest(r) {
			memberAttempted = true
			auth, msg, ok := h.memberAuthFromRequest(r, body)
			if ok {
				auth.bearer = bearerOK
				return auth, true
			}
			memberMsg = msg
		}
		if bearerOK {
			return authContext{bearer: true}, true
		}
		if r.Header.Get(deviceIDHeader) != "" {
			return h.authorizedDevice(w, r, body)
		}
		if memberAttempted {
			writeError(w, http.StatusUnauthorized, "unauthorized", memberMsg)
			return authContext{}, false
		}
	}
	if h.authMode == AuthModeBearer || h.authMode == AuthModeDual {
		w.Header().Set("WWW-Authenticate", `Bearer realm="entmoot-esp"`)
	}
	writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid credentials")
	return authContext{}, false
}

func requestHasMemberAuth(r *http.Request) bool {
	return r.Header.Get(memberIDHeader) != "" ||
		r.Header.Get(memberPeerHeader) != "" ||
		r.Header.Get(memberPubKeyHeader) != "" ||
		r.Header.Get(memberSignatureHeader) != ""
}

func memberAuthAllowedForRequest(r *http.Request) bool {
	if r.Method == http.MethodGet && r.URL.Path == "/v1/session" {
		return true
	}
	return memberAuthAllowedForGroupRequest(r)
}

func memberAuthAllowedForGroupRequest(r *http.Request) bool {
	const prefix = "/v1/groups/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return false
	}
	rest := strings.TrimPrefix(r.URL.Path, prefix)
	_, suffix, ok := strings.Cut(rest, "/")
	if !ok {
		return false
	}
	return suffix == "live-agents" || strings.HasPrefix(suffix, "live-agents/")
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

func (h *Handler) authorizedMember(w http.ResponseWriter, r *http.Request, body []byte) (authContext, bool) {
	auth, msg, ok := h.memberAuthFromRequest(r, body)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", msg)
		return authContext{}, false
	}
	return auth, true
}

func (h *Handler) memberAuthFromRequest(r *http.Request, body []byte) (authContext, string, bool) {
	var memberID entmoot.MemberID
	if err := memberID.UnmarshalJSON([]byte(strconv.Quote(strings.TrimSpace(r.Header.Get(memberIDHeader))))); err != nil || memberID == (entmoot.MemberID{}) {
		return authContext{}, "invalid member id", false
	}
	peerIDRaw := strings.TrimSpace(r.Header.Get(memberPeerHeader))
	peerID, err := peer.Decode(peerIDRaw)
	if err != nil {
		return authContext{}, "invalid peer id", false
	}
	pub, err := base64.StdEncoding.DecodeString(strings.TrimSpace(r.Header.Get(memberPubKeyHeader)))
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return authContext{}, "invalid member public key", false
	}
	derivedMemberID, err := entmoot.MemberIDFromPublicKey(pub)
	if err != nil || derivedMemberID != memberID {
		return authContext{}, "member id does not match public key", false
	}
	publicKey, err := libp2pcrypto.UnmarshalEd25519PublicKey(pub)
	if err != nil {
		return authContext{}, "invalid member public key", false
	}
	derivedPeerID, err := peer.IDFromPublicKey(publicKey)
	if err != nil || derivedPeerID != peerID {
		return authContext{}, "peer id does not match public key", false
	}
	tsRaw := strings.TrimSpace(r.Header.Get(timestampHeader))
	tsMillis, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return authContext{}, "invalid request timestamp", false
	}
	now := h.clock()
	ts := time.UnixMilli(tsMillis)
	if ts.Before(now.Add(-deviceAuthSkew)) || ts.After(now.Add(deviceAuthSkew)) {
		return authContext{}, "request timestamp outside allowed window", false
	}
	nonce := strings.TrimSpace(r.Header.Get(nonceHeader))
	if nonce == "" || len(nonce) > 256 {
		return authContext{}, "invalid nonce", false
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(r.Header.Get(memberSignatureHeader)))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return authContext{}, "invalid signature", false
	}
	input := MemberSigningInput(r.Method, r.URL.RequestURI(), memberID, peerIDRaw, pub, tsMillis, nonce, body)
	if !ed25519.Verify(pub, []byte(input), sig) {
		return authContext{}, "invalid signature", false
	}
	nonceKey := "member:" + memberID.String() + ":" + peerIDRaw
	if !h.nonceCache.use(nonceKey, nonce, now.Add(deviceAuthSkew)) {
		return authContext{}, "replayed nonce", false
	}
	return authContext{member: &MemberAuth{MemberID: memberID, PeerID: peerIDRaw, EntmootPubKey: append([]byte(nil), pub...), Method: r.Method, Path: r.URL.RequestURI(), TimestampMS: tsMillis, Nonce: nonce, Signature: strings.TrimSpace(r.Header.Get(memberSignatureHeader))}}, "", true
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

func MemberSigningInput(method, pathWithRawQuery string, memberID entmoot.MemberID, peerID string, entmootPubKey []byte, timestampMillis int64, nonce string, body []byte) string {
	sum := sha256.Sum256(body)
	return strings.Join([]string{
		memberAuthVersion,
		strings.ToUpper(method),
		pathWithRawQuery,
		memberID.String(),
		peerID,
		base64.StdEncoding.EncodeToString(entmootPubKey),
		strconv.FormatInt(timestampMillis, 10),
		nonce,
		base64.StdEncoding.EncodeToString(sum[:]),
	}, "\n")
}

func (h *Handler) checkDeviceGroup(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) bool {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.bearer {
		return true
	}
	if auth.device != nil && deviceAllowsGroup(*auth.device, groupID) {
		return true
	}
	writeError(w, http.StatusForbidden, "forbidden", "device is not authorized for group")
	return false
}

func (h *Handler) checkDeviceGroupAdmin(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) bool {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.bearer {
		return true
	}
	if auth.device != nil && deviceCanAdminGroup(*auth.device, groupID) {
		return true
	}
	writeError(w, http.StatusForbidden, "forbidden", "device is not authorized to manage group")
	return false
}

func (h *Handler) checkLiveAgentConfigRead(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) bool {
	auth := authFromContext(r)
	if auth.bearer {
		return true
	}
	if auth.member != nil {
		if h.memberAuthMatchesGroupMember(r.Context(), groupID, auth.member.MemberID, auth.member.EntmootPubKey) {
			return true
		}
		writeError(w, http.StatusForbidden, "forbidden", "member is not authorized for group")
		return false
	}
	return h.checkDeviceGroup(w, r, groupID)
}

func (h *Handler) checkLiveAgentConfigWrite(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, nodeID entmoot.MemberID) bool {
	auth := authFromContext(r)
	if auth.bearer {
		return true
	}
	if auth.device != nil {
		if deviceCanAdminGroup(*auth.device, groupID) {
			return true
		}
		writeError(w, http.StatusForbidden, "forbidden", "device is not authorized to manage group")
		return false
	}
	if auth.member != nil {
		if auth.member.MemberID == (entmoot.MemberID{}) {
			writeError(w, http.StatusForbidden, "forbidden", "member can only manage its own live agent config")
			return false
		}
		if h.memberAuthMatchesGroupMember(r.Context(), groupID, auth.member.MemberID, auth.member.EntmootPubKey) {
			return true
		}
		writeError(w, http.StatusForbidden, "forbidden", "member is not authorized for group")
		return false
	}
	writeError(w, http.StatusForbidden, "device_signature_required", "live agent config requires a registered device or member signature")
	return false
}

func (h *Handler) memberAuthMatchesGroupMember(ctx context.Context, groupID entmoot.GroupID, memberID entmoot.MemberID, pub []byte) bool {
	if h.groups == nil {
		return false
	}
	members, err := h.groups.ListMembers(ctx, groupID)
	if err != nil {
		h.logger.Error("esphttp: live agent member lookup failed", slog.String("err", err.Error()))
		return false
	}
	encodedPub := base64.StdEncoding.EncodeToString(pub)
	for _, member := range members {
		if member.MemberID == memberID && member.EntmootPubKey == encodedPub {
			return true
		}
	}
	return false
}

func parseLiveAgentNodePath(w http.ResponseWriter, escapedNode string) (entmoot.MemberID, bool) {
	rawMember, err := url.PathUnescape(escapedNode)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "member id is invalid")
		return entmoot.MemberID{}, false
	}
	var memberID entmoot.MemberID
	if err := memberID.UnmarshalJSON([]byte(strconv.Quote(strings.TrimSpace(rawMember)))); err != nil || memberID == (entmoot.MemberID{}) {
		writeError(w, http.StatusBadRequest, "bad_request", "member id is required")
		return entmoot.MemberID{}, false
	}
	return memberID, true
}

func (h *Handler) checkDeviceClient(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, clientID string) bool {
	if !h.checkDeviceGroup(w, r, groupID) {
		return false
	}
	return h.checkDeviceClientID(w, r, clientID)
}

func (h *Handler) checkDeviceClientRead(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, clientID string) bool {
	if !h.checkDeviceGroup(w, r, groupID) {
		return false
	}
	return h.checkDeviceClientID(w, r, clientID)
}

func (h *Handler) checkDeviceClientID(w http.ResponseWriter, r *http.Request, clientID string) bool {
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.bearer {
		return true
	}
	if auth.device == nil {
		writeError(w, http.StatusForbidden, "forbidden", "device is not authorized for client_id")
		return false
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

func (h *Handler) createMemberRemoveSignRequest(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID, escapedMember string) {
	memberID, ok := parseLiveAgentNodePath(w, escapedMember)
	if !ok {
		return
	}
	var body struct {
		PeerID        string `json:"peer_id"`
		EntmootPubKey string `json:"entmoot_pubkey"`
	}
	if _, ok := decodeRawBody(w, r, 1<<20, &body); !ok {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"target": map[string]any{
			"member_id":      memberID,
			"peer_id":        strings.TrimSpace(body.PeerID),
			"entmoot_pubkey": strings.TrimSpace(body.EntmootPubKey),
		},
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "member remove payload encoding failed")
		return
	}
	h.createSignRequest(w, r, "member_remove", groupID, payload)
}

func (h *Handler) authorizeSignRequestCreation(w http.ResponseWriter, r *http.Request, kind string, groupID entmoot.GroupID) bool {
	auth := authFromContext(r)
	if executableOperationKind(kind) && auth.device == nil {
		writeError(w, http.StatusForbidden, "device_signature_required", "operation requires a registered device signature")
		return false
	}
	if groupID == (entmoot.GroupID{}) {
		return true
	}
	if !h.checkDeviceGroup(w, r, groupID) {
		return false
	}
	if requiresGroupAdmin(kind) && !h.checkDeviceGroupAdmin(w, r, groupID) {
		return false
	}
	return true
}

func (h *Handler) createSignRequest(w http.ResponseWriter, r *http.Request, kind string, groupID entmoot.GroupID, payload []byte) {
	auth := authFromContext(r)
	if !h.authorizeSignRequestCreation(w, r, kind, groupID) {
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
	if !h.checkSignRequestDeviceRights(w, req) {
		return nil, false
	}
	if !h.verifyOperationSignature(w, req, signature) {
		return nil, false
	}
	result, err := h.operations.ExecuteSignRequest(r.Context(), req, signature)
	if err != nil {
		h.writeOperationError(w, "execute sign request", "operation execution failed", err, slog.String("kind", req.Kind))
		return nil, false
	}
	if len(result) == 0 {
		result = json.RawMessage(`{}`)
	}
	return append(json.RawMessage(nil), result...), true
}

func (h *Handler) writeOperationError(w http.ResponseWriter, op, fallback string, err error, attrs ...slog.Attr) {
	var opErr *OperationError
	if errors.As(err, &opErr) {
		writeError(w, opErr.HTTPStatus, opErr.Code, opErr.Message)
		return
	}
	args := make([]any, 0, 2+len(attrs))
	for _, attr := range attrs {
		args = append(args, attr)
	}
	args = append(args, slog.String("err", err.Error()))
	h.logger.Error("esphttp: "+op, args...)
	writeError(w, http.StatusInternalServerError, "internal_error", fallback)
}

func (h *Handler) checkSignRequestDeviceRights(w http.ResponseWriter, req SignRequest) bool {
	if req.GroupID == (entmoot.GroupID{}) {
		return true
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusForbidden, "device_signature_required", "operation requires a registered device signature")
		return false
	}
	if h.devices == nil {
		writeError(w, http.StatusForbidden, "forbidden", "device rights cannot be verified")
		return false
	}
	device, ok := h.devices.lookup(req.DeviceID)
	if !ok {
		writeError(w, http.StatusForbidden, "forbidden", "sign request device is not registered")
		return false
	}
	if !deviceAllowsGroup(device, req.GroupID) {
		writeError(w, http.StatusForbidden, "forbidden", "device is not authorized for group")
		return false
	}
	if requiresGroupAdmin(req.Kind) && !deviceCanAdminGroup(device, req.GroupID) {
		writeError(w, http.StatusForbidden, "forbidden", "device is not authorized to manage group")
		return false
	}
	return true
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
	if req.GroupID != (entmoot.GroupID{}) {
		if !deviceAllowsGroup(*auth.device, req.GroupID) {
			writeError(w, http.StatusForbidden, "forbidden", "device is not authorized for group")
			return false
		}
		if requiresGroupAdmin(req.Kind) && !deviceCanAdminGroup(*auth.device, req.GroupID) {
			writeError(w, http.StatusForbidden, "forbidden", "device is not authorized to manage group")
			return false
		}
	}
	return true
}

type idempotencyAuthorizer func(http.ResponseWriter, *http.Request) bool

func (h *Handler) withIdempotency(w http.ResponseWriter, r *http.Request, routeScope string, authorize idempotencyAuthorizer, next func(http.ResponseWriter, *http.Request)) {
	key := strings.TrimSpace(r.Header.Get(idempotencyHeader))
	if key == "" {
		next(w, r)
		return
	}
	if authorize == nil {
		h.logger.Error("esphttp: idempotency route missing authorizer", slog.String("path", r.URL.Path))
		writeError(w, http.StatusInternalServerError, "internal_error", "idempotency authorization is not configured")
		return
	}
	if !authorize(w, r) {
		return
	}
	if len(key) > 256 {
		writeError(w, http.StatusBadRequest, "bad_request", "Idempotency-Key is too long")
		return
	}
	scope, ok := h.principalIdempotencyScope(r, routeScope)
	if !ok {
		writeError(w, http.StatusForbidden, "forbidden", "authenticated principal is required")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAuthBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	requestHash := idempotencyRequestHash(r, body)
	rec, found, err := h.state.GetIdempotencyRecord(r.Context(), scope, key)
	if err != nil {
		h.logger.Error("esphttp: idempotency lookup", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "idempotency lookup failed")
		return
	}
	if found {
		if rec.RequestHash != requestHash {
			writeError(w, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was already used with a different request")
			return
		}
		writeStoredJSON(w, rec.StatusCode, rec.Response)
		return
	}
	recorder := newCaptureResponseWriter()
	next(recorder, r)
	status := recorder.statusCode()
	response := recorder.body.Bytes()
	if len(response) == 0 {
		response = []byte("{}")
	}
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		if err := h.state.SaveIdempotencyRecord(r.Context(), IdempotencyRecord{
			Scope:       scope,
			Key:         key,
			RequestHash: requestHash,
			StatusCode:  status,
			Response:    append(json.RawMessage(nil), response...),
		}); err != nil {
			h.logger.Warn("esphttp: idempotency save failed", slog.String("err", err.Error()))
		}
	}
	copyCapturedResponse(w, recorder)
}

func (h *Handler) principalIdempotencyScope(r *http.Request, routeScope string) (string, bool) {
	auth := authFromContext(r)
	var principal string
	switch {
	case auth.member != nil:
		principal = "member\x00" +
			auth.member.MemberID.String() + "\x00" +
			auth.member.PeerID + "\x00" + base64.StdEncoding.EncodeToString(auth.member.EntmootPubKey)
	case auth.device != nil:
		principal = "device\x00" + strings.TrimSpace(auth.device.ID)
	case auth.bearer:
		principal = "bearer\x00" + h.token
	default:
		return "", false
	}
	principalHash := sha256.Sum256([]byte(principal))
	resourceHash := sha256.Sum256([]byte(strings.Join([]string{
		strings.ToUpper(r.Method),
		r.URL.EscapedPath(),
		routeScope,
	}, "\n")))
	return "v2:" +
		base64.RawURLEncoding.EncodeToString(principalHash[:]) + ":" +
		base64.RawURLEncoding.EncodeToString(resourceHash[:]), true
}

func idempotencyRequestHash(r *http.Request, body []byte) string {
	sum := sha256.New()
	_, _ = io.WriteString(sum, strings.ToUpper(r.Method))
	_, _ = io.WriteString(sum, "\n")
	_, _ = io.WriteString(sum, r.URL.RequestURI())
	_, _ = io.WriteString(sum, "\n")
	_, _ = sum.Write(body)
	return base64.StdEncoding.EncodeToString(sum.Sum(nil))
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

func parseBoolQuery(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	default:
		return false
	}
}

func deviceCanAdminGroup(device Device, groupID entmoot.GroupID) bool {
	for _, allowed := range device.AdminGroups {
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
	adminGroups := make([]entmoot.GroupID, 0, len(device.AdminGroups))
	adminGroups = append(adminGroups, device.AdminGroups...)
	clients := append([]string(nil), device.ClientIDs...)
	out := map[string]any{
		"id":           device.ID,
		"groups":       groups,
		"admin_groups": adminGroups,
		"client_ids":   clients,
		"member_id":    device.MemberID,
		"peer_id":      device.PeerID,
		"disabled":     device.Disabled,
	}
	if len(device.EntmootPubKey) > 0 {
		out["entmoot_pubkey"] = base64.StdEncoding.EncodeToString(device.EntmootPubKey)
	}
	return out
}

func memberAuthView(member MemberAuth) map[string]any {
	return map[string]any{
		"member_id":      member.MemberID,
		"peer_id":        member.PeerID,
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(member.EntmootPubKey),
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
	// The status line is already sent, so an encoding failure cannot become an
	// error response — but it must not be silent either. Discarding it is how
	// the live-agent listing served 200 with an empty body for months: its
	// response is keyed by MemberID, which had no TextMarshaler.
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h := slog.Default()
		h.Error("esphttp: response encoding failed after status was sent",
			slog.Int("status", status),
			slog.String("err", err.Error()))
	}
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

func peerIDForPublicKey(publicKey []byte) string {
	peerID, err := entmoot.PeerIDFromPublicKey(publicKey)
	if err != nil {
		return ""
	}
	return peerID
}
