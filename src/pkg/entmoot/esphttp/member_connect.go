package esphttp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/signing"
)

// MemberConnectPath is the unauthenticated enrollment route. The request
// proves group membership with the member's Entmoot identity signature
// instead of an existing device credential.
const MemberConnectPath = "/v1/devices/connect"

const (
	// memberConnectVersion is the signature domain of a connect request. It
	// is used for nothing else, so a connect signature can never be replayed
	// as a message, record, sign-request approval, or HTTP request signature.
	memberConnectVersion = "ENTMOOT-ESP-MEMBER-CONNECT-V1"
	// selfEnrolledDeviceIDDomain separates the device-id hash from every
	// other hash of a device public key.
	selfEnrolledDeviceIDDomain = "entmoot.esp.member-device-id.v1\x00"
	selfEnrolledDeviceIDPrefix = "member-"

	maxMemberConnectBodyBytes = 64 << 10
	maxConnectNonceBytes      = 128
	maxConnectClientIDBytes   = 64

	// DefaultMaxDevicesPerMember caps self-enrolled devices one member may
	// hold on an ESP. Reaching it rejects new keys; nothing is evicted.
	DefaultMaxDevicesPerMember = 4
	// DefaultMaxConnectGroups caps the groups one connect request may name.
	DefaultMaxConnectGroups = 16
	// DefaultMaxSelfEnrolledDevices bounds the registry file regardless of
	// how many members the served groups hold.
	DefaultMaxSelfEnrolledDevices = 4096
)

// MemberRoster answers from the authoritative roster whether a member is in
// a group right now. Removed and banned members are not active.
type MemberRoster interface {
	ActiveMember(ctx context.Context, groupID entmoot.GroupID, memberID entmoot.MemberID) (entmoot.NodeInfo, bool, error)
}

// MemberConnectConfig enables member self-enrollment.
type MemberConnectConfig struct {
	// Enabled turns on POST /v1/devices/connect. When false the route
	// answers 404 exactly like any unknown path.
	Enabled bool
	// RegistryPath is the device registry file every successful connect is
	// persisted to before it takes effect. Required when Enabled.
	RegistryPath string
	// MaxDevicesPerMember, MaxGroupsPerRequest, and MaxDevices default to
	// the package Default* values when zero.
	MaxDevicesPerMember int
	MaxGroupsPerRequest int
	MaxDevices          int
}

// MemberConnectRequest is the JSON body of POST /v1/devices/connect.
type MemberConnectRequest struct {
	DevicePublicKey string            `json:"device_public_key"`
	GroupIDs        []entmoot.GroupID `json:"group_ids"`
	ClientID        string            `json:"client_id,omitempty"`
	MemberID        entmoot.MemberID  `json:"member_id"`
	EntmootPubKey   string            `json:"entmoot_pubkey"`
	TimestampMS     int64             `json:"timestamp_ms"`
	Nonce           string            `json:"nonce"`
	Signature       string            `json:"signature"`
}

// MemberConnectResponse is the success body of POST /v1/devices/connect.
type MemberConnectResponse struct {
	DeviceID     string            `json:"device_id"`
	ClientID     string            `json:"client_id"`
	Groups       []entmoot.GroupID `json:"groups"`
	MemberID     entmoot.MemberID  `json:"member_id"`
	SelfEnrolled bool              `json:"self_enrolled"`
	Created      bool              `json:"created"`
	Changed      bool              `json:"changed"`
}

// MemberConnectSigningInput returns the bytes the member's Entmoot identity
// signs for a connect request. Every request field except the signature is
// bound, one per line, under a domain no other Entmoot signature uses.
func MemberConnectSigningInput(req MemberConnectRequest) string {
	groups := make([]string, 0, len(req.GroupIDs))
	for _, gid := range req.GroupIDs {
		groups = append(groups, gid.String())
	}
	return strings.Join([]string{
		memberConnectVersion,
		strings.TrimSpace(req.DevicePublicKey),
		req.MemberID.String(),
		strings.TrimSpace(req.EntmootPubKey),
		req.ClientID,
		strconv.FormatInt(req.TimestampMS, 10),
		req.Nonce,
		strconv.Itoa(len(groups)),
		strings.Join(groups, ","),
	}, "\n")
}

// SelfEnrolledDeviceID derives the registry id of the self-enrolled device
// holding devicePublicKey.
func SelfEnrolledDeviceID(devicePublicKey ed25519.PublicKey) string {
	sum := sha256.Sum256(append([]byte(selfEnrolledDeviceIDDomain), devicePublicKey...))
	return selfEnrolledDeviceIDPrefix + hex.EncodeToString(sum[:8])
}

// SelfEnrolledClientID is the mailbox client id granted to a self-enrolled
// device. Client ids are namespaced by the device id so a member can never
// name, and so read or advance, another device's mailbox cursor.
func SelfEnrolledClientID(deviceID, requested string) string {
	if requested == "" {
		return deviceID
	}
	return deviceID + ":" + requested
}

// MembershipRoster reads the membership store under Root, the same store the
// daemon writes and the ESP group catalog reads. Each call opens the store, so
// a removal or ban the daemon applies is visible on the next request.
//
// Opens are serialized: membership.Open probes the SQLite store without a
// busy timeout and reports a contended store as absent, so concurrent opens
// from this process would turn a real member into a spurious refusal. Any
// failure to read the roster is an error (the caller answers 503), never a
// "not a member" answer.
type MembershipRoster struct {
	Root string
	mu   sync.Mutex
}

// ActiveMember implements MemberRoster.
func (m *MembershipRoster) ActiveMember(_ context.Context, groupID entmoot.GroupID, memberID entmoot.MemberID) (entmoot.NodeInfo, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	group, err := membership.Open(m.Root, groupID)
	if err != nil {
		return entmoot.NodeInfo{}, false, err
	}
	defer group.Close()
	if group.IsBanned(memberID) {
		return entmoot.NodeInfo{}, false, nil
	}
	info, ok := group.MemberInfoByID(memberID)
	return info, ok, nil
}

func (h *Handler) handleMemberConnect(w http.ResponseWriter, r *http.Request) {
	if !h.memberConnect.Enabled {
		writeError(w, http.StatusNotFound, "not_found", "not found")
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxMemberConnectBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
		return
	}
	var req MemberConnectRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	devicePub, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.DevicePublicKey))
	if err != nil || len(devicePub) != ed25519.PublicKeySize {
		writeError(w, http.StatusBadRequest, "bad_request", "device_public_key must be a base64 Ed25519 public key")
		return
	}
	if !validConnectToken(req.ClientID, maxConnectClientIDBytes, true) {
		writeError(w, http.StatusBadRequest, "bad_request", "client_id must be up to 64 characters of [A-Za-z0-9._-]")
		return
	}
	if !validConnectToken(req.Nonce, maxConnectNonceBytes, false) {
		writeError(w, http.StatusBadRequest, "bad_request", "nonce must be 1-128 characters of [A-Za-z0-9._-]")
		return
	}
	if len(req.GroupIDs) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "group_ids is required")
		return
	}
	if len(req.GroupIDs) > h.memberConnect.MaxGroupsPerRequest {
		writeError(w, http.StatusBadRequest, "too_many_groups", fmt.Sprintf("at most %d groups per connect request", h.memberConnect.MaxGroupsPerRequest))
		return
	}
	seen := make(map[entmoot.GroupID]struct{}, len(req.GroupIDs))
	for _, gid := range req.GroupIDs {
		if gid == (entmoot.GroupID{}) {
			writeError(w, http.StatusBadRequest, "bad_request", "group_ids must not contain an empty group id")
			return
		}
		if _, dup := seen[gid]; dup {
			writeError(w, http.StatusBadRequest, "bad_request", "group_ids must not repeat a group")
			return
		}
		seen[gid] = struct{}{}
	}
	now := h.clock()
	ts := time.UnixMilli(req.TimestampMS)
	if ts.Before(now.Add(-deviceAuthSkew)) || ts.After(now.Add(deviceAuthSkew)) {
		writeError(w, http.StatusUnauthorized, "stale", "timestamp_ms outside allowed window")
		return
	}
	entmootPub, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.EntmootPubKey))
	if err != nil || len(entmootPub) != ed25519.PublicKeySize {
		writeError(w, http.StatusBadRequest, "bad_request", "entmoot_pubkey must be a base64 Ed25519 public key")
		return
	}
	memberID, err := entmoot.MemberIDFromPublicKey(entmootPub)
	if err != nil || memberID != req.MemberID {
		writeError(w, http.StatusUnauthorized, "member_mismatch", "member_id does not match entmoot_pubkey")
		return
	}
	peerID := peerIDForPublicKey(entmootPub)
	if peerID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "entmoot_pubkey is not a usable Ed25519 key")
		return
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.Signature))
	if err != nil || len(sig) != ed25519.SignatureSize || !ed25519.Verify(entmootPub, []byte(MemberConnectSigningInput(req)), sig) {
		writeError(w, http.StatusUnauthorized, "bad_signature", "signature does not verify against entmoot_pubkey")
		return
	}
	// The nonce is spent only after the signature verifies, so unsigned
	// traffic cannot grow the cache or burn a member's nonces.
	if !h.nonceCache.use("connect:"+memberID.String(), req.Nonce, now.Add(deviceAuthSkew)) {
		writeError(w, http.StatusUnauthorized, "replay", "nonce already used")
		return
	}
	for _, gid := range req.GroupIDs {
		exists, err := h.groupExists(r.Context(), gid)
		if err != nil {
			h.logger.Error("esphttp: member connect group lookup", slog.String("err", err.Error()))
			writeError(w, http.StatusInternalServerError, "internal_error", "group lookup failed")
			return
		}
		if !exists {
			writeError(w, http.StatusNotFound, "unknown_group", "group "+gid.String()+" is not served by this ESP")
			return
		}
		info, active, err := h.memberRoster.ActiveMember(r.Context(), gid, memberID)
		if err != nil {
			h.logger.Error("esphttp: member connect roster lookup", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
			writeError(w, http.StatusServiceUnavailable, "roster_unavailable", "group roster lookup failed")
			return
		}
		if !active || !bytes.Equal(info.EntmootPubKey, entmootPub) {
			writeError(w, http.StatusForbidden, "not_member", "member is not an active member of group "+gid.String())
			return
		}
	}
	deviceID := SelfEnrolledDeviceID(devicePub)
	clientID := SelfEnrolledClientID(deviceID, req.ClientID)
	created := false
	changed, err := h.devices.Update(h.memberConnect.RegistryPath, func(current *DeviceRegistry) (*DeviceRegistry, bool, error) {
		devices, isNew, changed, err := h.enrollMemberDevice(current.Snapshot(), deviceID, devicePub, memberID, peerID, entmootPub, req.GroupIDs, clientID)
		if err != nil || !changed {
			return nil, false, err
		}
		next, err := NewDeviceRegistry(devices)
		if err != nil {
			return nil, false, err
		}
		created = isNew
		return next, true, nil
	})
	if err != nil {
		var opErr *OperationError
		if errors.As(err, &opErr) {
			writeError(w, opErr.HTTPStatus, opErr.Code, opErr.Message)
			return
		}
		h.logger.Error("esphttp: member connect persist", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "device registry update failed")
		return
	}
	h.logger.Info("esphttp: member device connected",
		slog.String("device_id", deviceID),
		slog.String("member_id", memberID.String()),
		slog.Int("groups", len(req.GroupIDs)),
		slog.Bool("created", created),
		slog.Bool("changed", changed))
	h.writeJSON(w, r, http.StatusOK, MemberConnectResponse{
		DeviceID:     deviceID,
		ClientID:     clientID,
		Groups:       append([]entmoot.GroupID(nil), req.GroupIDs...),
		MemberID:     memberID,
		SelfEnrolled: true,
		Created:      created,
		Changed:      changed,
	})
}

// enrollMemberDevice applies one connect to a registry snapshot. It never
// touches an operator device: a colliding id or reused key is refused, and
// the per-member cap refuses a new key rather than evicting anything.
func (h *Handler) enrollMemberDevice(devices []Device, deviceID string, devicePub ed25519.PublicKey, memberID entmoot.MemberID, peerID string, entmootPub []byte, groups []entmoot.GroupID, clientID string) ([]Device, bool, bool, error) {
	idx := -1
	memberDevices := 0
	selfEnrolled := 0
	for i, d := range devices {
		if d.ID == deviceID {
			idx = i
			continue
		}
		if bytes.Equal(d.PublicKey, devicePub) {
			return nil, false, false, &OperationError{HTTPStatus: http.StatusConflict, Code: "device_key_conflict", Message: "device key is already registered to another device"}
		}
		if d.SelfEnrolled {
			selfEnrolled++
			if d.MemberID == memberID {
				memberDevices++
			}
		}
	}
	if idx >= 0 {
		existing := devices[idx]
		if !existing.SelfEnrolled || !bytes.Equal(existing.PublicKey, devicePub) {
			return nil, false, false, &OperationError{HTTPStatus: http.StatusConflict, Code: "device_id_conflict", Message: "device id is held by an operator device"}
		}
		if existing.MemberID != memberID {
			return nil, false, false, &OperationError{HTTPStatus: http.StatusForbidden, Code: "device_bound_to_other_member", Message: "device key is bound to another member"}
		}
		if existing.Disabled {
			return nil, false, false, &OperationError{HTTPStatus: http.StatusForbidden, Code: "device_disabled", Message: "device is disabled"}
		}
		if sameGroups(existing.Groups, groups) && len(existing.ClientIDs) == 1 && existing.ClientIDs[0] == clientID {
			return nil, false, false, nil
		}
		existing.Groups = append([]entmoot.GroupID(nil), groups...)
		existing.ClientIDs = []string{clientID}
		devices[idx] = existing
		return devices, false, true, nil
	}
	if memberDevices >= h.memberConnect.MaxDevicesPerMember {
		return nil, false, false, &OperationError{HTTPStatus: http.StatusForbidden, Code: "device_limit", Message: fmt.Sprintf("member already has %d self-enrolled devices", memberDevices)}
	}
	if selfEnrolled >= h.memberConnect.MaxDevices {
		return nil, false, false, &OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "registry_full", Message: "ESP self-enrolled device limit reached"}
	}
	devices = append(devices, Device{
		ID:            deviceID,
		PublicKey:     append(ed25519.PublicKey(nil), devicePub...),
		Groups:        append([]entmoot.GroupID(nil), groups...),
		ClientIDs:     []string{clientID},
		MemberID:      memberID,
		PeerID:        peerID,
		EntmootPubKey: append([]byte(nil), entmootPub...),
		SelfEnrolled:  true,
	})
	return devices, true, true, nil
}

// checkSelfEnrolledMember re-reads the roster for a self-enrolled device on
// every group-scoped request, so removal or a ban revokes access at once.
// Operator devices never reach this check.
func (h *Handler) checkSelfEnrolledMember(w http.ResponseWriter, r *http.Request, device Device, groupID entmoot.GroupID) bool {
	active, err := h.selfEnrolledMemberActive(r.Context(), device, groupID)
	if err != nil {
		h.logger.Error("esphttp: self-enrolled roster check", slog.String("device_id", device.ID), slog.String("err", err.Error()))
		writeError(w, http.StatusServiceUnavailable, "roster_unavailable", "group roster lookup failed")
		return false
	}
	if !active {
		writeError(w, http.StatusForbidden, "not_member", "device member is no longer an active member of group")
		return false
	}
	return true
}

func (h *Handler) selfEnrolledMemberActive(ctx context.Context, device Device, groupID entmoot.GroupID) (bool, error) {
	if h.memberRoster == nil {
		return false, errors.New("member roster is not configured")
	}
	info, active, err := h.memberRoster.ActiveMember(ctx, groupID, device.MemberID)
	if err != nil {
		return false, err
	}
	return active && bytes.Equal(info.EntmootPubKey, device.EntmootPubKey), nil
}

// selfEnrolledRouteAllowed is the whole surface a self-enrolled device may
// call: reading its granted groups and mailboxes, and publishing messages it
// signed itself. Sign requests, invites, admin routes, push registration, and
// diagnostics stay operator-device only.
func selfEnrolledRouteAllowed(r *http.Request) bool {
	switch r.URL.Path {
	case "/v1/session", "/v1/status", "/v1/devices/current", "/v1/groups":
		return r.Method == http.MethodGet
	case "/v1/mailbox/pull", "/v1/mailbox/ack", "/v1/mailbox/cursor":
		return true
	}
	const prefix = "/v1/groups/"
	escaped := r.URL.EscapedPath()
	if !strings.HasPrefix(escaped, prefix) {
		return false
	}
	_, suffix, _ := strings.Cut(strings.TrimPrefix(escaped, prefix), "/")
	switch suffix {
	case "", "members", "history", "message-context", "search", "topics", "mailbox", "policy":
		return r.Method == http.MethodGet
	case "messages":
		return r.Method == http.MethodGet || r.Method == http.MethodPost
	}
	return false
}

// checkSelfEnrolledPublish limits a self-enrolled device to relaying messages
// its bound member already signed.
func checkSelfEnrolledPublish(w http.ResponseWriter, device Device, msg entmoot.Message) bool {
	if msg.Author.MemberID == nil || *msg.Author.MemberID != device.MemberID || !bytes.Equal(msg.Author.EntmootPubKey, device.EntmootPubKey) {
		writeError(w, http.StatusForbidden, "author_mismatch", "self-enrolled devices may only publish as their bound member")
		return false
	}
	if err := signing.VerifyMessage(msg, msg.Author); err != nil {
		writeError(w, http.StatusBadRequest, "bad_signature", "message signature does not verify")
		return false
	}
	return true
}

func validConnectToken(s string, maxLen int, allowEmpty bool) bool {
	if s == "" {
		return allowEmpty
	}
	if len(s) > maxLen {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

func sameGroups(a, b []entmoot.GroupID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
