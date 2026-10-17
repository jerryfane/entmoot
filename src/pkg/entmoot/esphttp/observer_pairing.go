package esphttp

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
)

const ObserverPairVersion = "entmoot.observer-pair.v1"
const ObserverPairLifetime = 10 * time.Minute

// ObserverPairRequest contains only public data. The phone keeps the new
// device private key and signs a short-lived request for this exact endpoint.
type ObserverPairRequest struct {
	Version         string `json:"version"`
	ESP             string `json:"esp"`
	DevicePublicKey string `json:"device_public_key"`
	Nonce           string `json:"nonce"`
	ExpiresAtMS     int64  `json:"expires_at_ms"`
	Signature       string `json:"signature"`
}

// ObserverPairConsent is signed by the agent and approved independently by
// the ESP owner. It binds a phone request to one agent, never operator rights.
type ObserverPairConsent struct {
	Request       ObserverPairRequest `json:"request"`
	MemberID      entmoot.MemberID    `json:"member_id"`
	EntmootPubKey string              `json:"entmoot_pubkey"`
	Signature     string              `json:"signature"`
}

type ObserverPairResult struct {
	Consent     ObserverPairConsent `json:"consent"`
	DeviceID    string              `json:"device_id"`
	FollowAgent bool                `json:"follow_agent"`
}

func ObserverPairRequestSigningInput(r ObserverPairRequest) string {
	return strings.Join([]string{ObserverPairVersion + "/phone", r.ESP, r.DevicePublicKey, r.Nonce, strconv.FormatInt(r.ExpiresAtMS, 10)}, "\n")
}
func ObserverPairConsentSigningInput(c ObserverPairConsent) string {
	return strings.Join([]string{ObserverPairVersion + "/agent", ObserverPairRequestSigningInput(c.Request), c.Request.Signature, c.MemberID.String(), c.EntmootPubKey, "all-current-and-future-moots-read-only"}, "\n")
}

func ValidateObserverPairRequest(r ObserverPairRequest, now time.Time) (ed25519.PublicKey, error) {
	if r.Version != ObserverPairVersion {
		return nil, errors.New("unsupported observer pairing version")
	}
	u, err := url.Parse(r.ESP)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.HasSuffix(r.ESP, "/") || strings.ContainsAny(r.ESP, "\r\n") {
		return nil, errors.New("pairing requires a canonical ESP base URL without credentials, query or trailing slash")
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if u.Scheme != "https" && !(u.Scheme == "http" && (host == "localhost" || ip != nil && ip.IsLoopback())) {
		return nil, errors.New("pairing requires HTTPS except for loopback")
	}
	if r.ExpiresAtMS <= now.UnixMilli() || r.ExpiresAtMS > now.Add(ObserverPairLifetime).UnixMilli() {
		return nil, errors.New("pairing request expired or lifetime exceeds ten minutes")
	}
	nonce, err := base64.RawURLEncoding.DecodeString(r.Nonce)
	if err != nil || len(nonce) != 24 {
		return nil, errors.New("pairing nonce must be 24 random bytes")
	}
	pub, err := base64.StdEncoding.DecodeString(r.DevicePublicKey)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return nil, errors.New("invalid phone public key")
	}
	sig, err := base64.StdEncoding.DecodeString(r.Signature)
	if err != nil || !ed25519.Verify(pub, []byte(ObserverPairRequestSigningInput(r)), sig) {
		return nil, errors.New("invalid phone pairing signature")
	}
	return ed25519.PublicKey(pub), nil
}

func ValidateObserverPairConsent(c ObserverPairConsent, now time.Time) (ed25519.PublicKey, ed25519.PublicKey, error) {
	phone, err := ValidateObserverPairRequest(c.Request, now)
	if err != nil {
		return nil, nil, err
	}
	pub, err := base64.StdEncoding.DecodeString(c.EntmootPubKey)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return nil, nil, errors.New("invalid agent public key")
	}
	member, err := entmoot.MemberIDFromPublicKey(pub)
	if err != nil || member != c.MemberID {
		return nil, nil, errors.New("agent identity mismatch")
	}
	sig, err := base64.StdEncoding.DecodeString(c.Signature)
	if err != nil || !ed25519.Verify(pub, []byte(ObserverPairConsentSigningInput(c)), sig) {
		return nil, nil, errors.New("invalid agent consent signature")
	}
	return phone, ed25519.PublicKey(pub), nil
}

func (h *Handler) handleObservers(w http.ResponseWriter, r *http.Request) {
	// Enrollment of an agent with no current rooms needs explicit server-owner
	// authority; possession of any agent or ordinary device key is insufficient.
	if !h.authorizedBearer(r) {
		writeError(w, http.StatusForbidden, "owner_required", "observer management requires ESP owner authorization")
		return
	}
	if h.memberDevices == nil || h.memberConnect.RegistryPath == "" {
		writeError(w, http.StatusServiceUnavailable, "observer_unavailable", "persistent member device registry is required")
		return
	}
	if _, ok := h.bufferBodyForAuth(w, r); !ok {
		return
	}
	switch r.Method {
	case http.MethodGet:
		devices := []map[string]any{}
		for _, d := range h.memberDevices.Snapshot() {
			if d.FollowAgent {
				devices = append(devices, deviceView(d))
			}
		}
		h.writeJSON(w, r, http.StatusOK, map[string]any{"observers": devices, "pairing_version": ObserverPairVersion})
	case http.MethodPost:
		var consent ObserverPairConsent
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&consent); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "invalid observer consent")
			return
		}
		phone, agent, err := ValidateObserverPairConsent(consent, h.clock())
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_pairing", err.Error())
			return
		}
		deviceID := SelfEnrolledDeviceID(phone)
		_, err = h.memberDevices.UpdateMemberDevices(h.memberConnect.RegistryPath, func(current *DeviceRegistry) (*DeviceRegistry, bool, error) {
			devices := current.Snapshot()
			limit := h.memberConnect.MaxDevices
			if limit <= 0 {
				limit = DefaultMaxSelfEnrolledDevices
			}
			if len(devices) >= limit {
				return nil, false, &OperationError{HTTPStatus: http.StatusTooManyRequests, Code: "too_many_devices", Message: "member device registry is full"}
			}
			for _, registered := range [][]Device{devices, h.devices.Snapshot()} {
				for _, d := range registered {
					if d.ID == deviceID || d.PublicKey.Equal(phone) {
						return nil, false, &OperationError{HTTPStatus: http.StatusConflict, Code: "pairing_used", Message: "this phone key was already registered; create a fresh pairing request"}
					}
				}
			}
			devices = append(devices, Device{ID: deviceID, PublicKey: phone, ClientIDs: []string{deviceID}, MemberID: consent.MemberID, PeerID: peerIDForPublicKey(agent), EntmootPubKey: agent, SelfEnrolled: true, FollowAgent: true})
			next, err := NewDeviceRegistry(devices)
			return next, err == nil, err
		})
		if err != nil {
			h.writeOperationError(w, "pair observer", "observer registration failed", err)
			return
		}
		h.writeJSON(w, r, http.StatusCreated, ObserverPairResult{Consent: consent, DeviceID: deviceID, FollowAgent: true})
	case http.MethodDelete:
		id := r.URL.Query().Get("device_id")
		if id == "" {
			writeError(w, http.StatusBadRequest, "bad_request", "device_id is required")
			return
		}
		if err := h.disableObserver(id); err != nil {
			h.writeOperationError(w, "disable observer", "observer revocation failed", err)
			return
		}
		h.writeJSON(w, r, http.StatusOK, map[string]any{"device_id": id, "disabled": true})
	default:
		methodNotAllowed(w, "GET, POST, DELETE")
	}
}

func (h *Handler) handleObserverSelfRevoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	auth, _ := r.Context().Value(authContextKey{}).(authContext)
	if auth.device == nil || !auth.device.FollowAgent {
		writeError(w, http.StatusForbidden, "observer_required", "only an observer may revoke its own grant")
		return
	}
	if err := h.disableObserver(auth.device.ID); err != nil {
		h.writeOperationError(w, "revoke current observer", "observer revocation failed", err)
		return
	}
	h.writeJSON(w, r, http.StatusOK, map[string]any{"device_id": auth.device.ID, "disabled": true})
}

func (h *Handler) disableObserver(id string) error {
	_, err := h.memberDevices.UpdateMemberDevices(h.memberConnect.RegistryPath, func(current *DeviceRegistry) (*DeviceRegistry, bool, error) {
		devices := current.Snapshot()
		for i := range devices {
			if devices[i].ID != id || !devices[i].FollowAgent {
				continue
			}
			if devices[i].Disabled {
				return current, false, nil
			}
			devices[i].Disabled = true
			next, err := NewDeviceRegistry(devices)
			return next, err == nil, err
		}
		return nil, false, &OperationError{HTTPStatus: http.StatusNotFound, Code: "not_found", Message: fmt.Sprintf("observer %q not found", id)}
	})
	return err
}
