package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

func observerPhoneRequest(t *testing.T, key ed25519.PrivateKey, endpoint string) map[string]any {
	t.Helper()
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"version": "entmoot.observer-pair.v1", "esp": endpoint, "device_public_key": base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey)), "nonce": base64.RawURLEncoding.EncodeToString(nonce), "expires_at_ms": time.Now().Add(9 * time.Minute).UnixMilli()}
	body["signature"] = base64.StdEncoding.EncodeToString(ed25519.Sign(key, []byte(observerPhoneInput(body))))
	return body
}
func observerPhoneInput(body map[string]any) string {
	return strings.Join([]string{"entmoot.observer-pair.v1/phone", body["esp"].(string), body["device_public_key"].(string), body["nonce"].(string), strconv.FormatInt(body["expires_at_ms"].(int64), 10)}, "\n")
}
func observerConsent(t *testing.T, phone map[string]any, identity *keystore.Identity) map[string]any {
	t.Helper()
	member, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pub := base64.StdEncoding.EncodeToString(identity.PublicKey)
	input := strings.Join([]string{"entmoot.observer-pair.v1/agent", observerPhoneInput(phone), phone["signature"].(string), member.String(), pub, "all-current-and-future-moots-read-only"}, "\n")
	return map[string]any{"request": phone, "member_id": member.String(), "entmoot_pubkey": pub, "signature": base64.StdEncoding.EncodeToString(identity.Sign([]byte(input)))}
}
func observerHTTP(t *testing.T, f *memberConnectFixture, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func TestESPObserverPairCLIHandlesZeroMootsAndRevocation(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	agent, _ := mustDaemonIdentity(t)
	phoneKey := newDeviceKey(t)
	server := httptest.NewServer(f.handler)
	defer server.Close()
	request := observerPhoneRequest(t, phoneKey, server.URL)
	raw, _ := json.Marshal(request)
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	t.Setenv("ENTMOOT_ESP_TOKEN", "operator-secret")
	flags := daemonFlags(t, t.TempDir(), agent)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdESPDevice(flags, []string{"pair-observer", "-esp", server.URL, "-request", encoded})
	})
	if code != exitOK {
		t.Fatalf("pair: %d %s %s", code, stdout, stderr)
	}
	var result struct {
		DeviceID   string `json:"device_id"`
		PairingURL string `json:"pairing_url"`
	}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatal(err)
	}
	link, err := url.Parse(result.PairingURL)
	if err != nil || link.Scheme != "entmoot" || link.Host != "observe" {
		t.Fatalf("bad confirmation link: %v", err)
	}
	if strings.Contains(stdout, "private_key") || strings.Contains(stdout, "operator-secret") {
		t.Fatal("pairing exposed a credential")
	}
	groups := f.signed(result.DeviceID, phoneKey, http.MethodGet, "/v1/groups", nil)
	var initial struct {
		Groups []esphttp.GroupSummary `json:"groups"`
	}
	if groups.Code != http.StatusOK || json.Unmarshal(groups.Body.Bytes(), &initial) != nil || len(initial.Groups) != 0 {
		t.Fatalf("zero-moot agent leaked rooms: %d %s", groups.Code, groups.Body.String())
	}
	future := testESPGroupID(22)
	mustCreateGroup(t, f.dataDir, future, agent, membership.DefaultPolicy())
	if rec := f.history(result.DeviceID, phoneKey, future); rec.Code != http.StatusOK {
		t.Fatalf("new room: %d %s", rec.Code, rec.Body.String())
	}
	code, _, stderr = captureCommandOutput(t, func() int {
		return cmdESPDevice(flags, []string{"revoke-observer", "-esp", server.URL, "-id", result.DeviceID})
	})
	if code != exitOK {
		t.Fatalf("revoke: %d %s", code, stderr)
	}
	requireESPError(t, f.history(result.DeviceID, phoneKey, future), http.StatusForbidden, "device_disabled")
	code, _, stderr = captureCommandOutput(t, func() int {
		return cmdESPDevice(flags, []string{"pair-observer", "-esp", server.URL, "-request", encoded})
	})
	if code == exitOK || !strings.Contains(stderr, "pairing_used") {
		t.Fatalf("revoked grant replay: %d %s", code, stderr)
	}
}

func TestESPObserverPairRequiresOwnerAndUntamperedConsent(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	key := newDeviceKey(t)
	consent := observerConsent(t, observerPhoneRequest(t, key, "https://esp.example.org"), f.member)
	requireESPError(t, observerHTTP(t, f, http.MethodPost, "/v1/observers", "", consent), http.StatusForbidden, "owner_required")
	requireESPError(t, observerHTTP(t, f, http.MethodPost, "/v1/observers", "wrong", consent), http.StatusForbidden, "owner_required")
	for _, kind := range []string{"phone", "agent", "endpoint", "expired"} {
		t.Run(kind, func(t *testing.T) {
			phone := observerPhoneRequest(t, newDeviceKey(t), "https://esp.example.org")
			c := observerConsent(t, phone, f.member)
			switch kind {
			case "phone":
				phone["signature"] = base64.StdEncoding.EncodeToString(make([]byte, 64))
				c = observerConsent(t, phone, f.member)
			case "agent":
				c["signature"] = base64.StdEncoding.EncodeToString(make([]byte, 64))
			case "endpoint":
				phone["esp"] = "https://attacker.example"
			case "expired":
				phone["expires_at_ms"] = time.Now().Add(-time.Minute).UnixMilli()
			}
			requireESPError(t, observerHTTP(t, f, http.MethodPost, "/v1/observers", "operator-secret", c), http.StatusBadRequest, "invalid_pairing")
		})
	}
	rec := observerHTTP(t, f, http.MethodPost, "/v1/observers", "operator-secret", consent)
	if rec.Code != http.StatusCreated {
		t.Fatalf("valid pair: %d %s", rec.Code, rec.Body.String())
	}
	requireESPError(t, observerHTTP(t, f, http.MethodPost, "/v1/observers", "operator-secret", consent), http.StatusConflict, "pairing_used")
	id := esphttp.SelfEnrolledDeviceID(key.Public().(ed25519.PublicKey))
	requireESPError(t, f.signed(id, key, http.MethodGet, "/v1/observers", nil), http.StatusForbidden, "owner_required")
	requireESPError(t, f.signed(id, key, http.MethodPost, "/v1/messages", []byte(`{}`)), http.StatusForbidden, "observer_read_only")
}

func TestESPObserverPairSameServerPreservesAgentIsolation(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	aKey, bKey := newDeviceKey(t), newDeviceKey(t)
	a, b := esphttp.SelfEnrolledDeviceID(aKey.Public().(ed25519.PublicKey)), esphttp.SelfEnrolledDeviceID(bKey.Public().(ed25519.PublicKey))
	for _, item := range []struct {
		key      ed25519.PrivateKey
		identity *keystore.Identity
	}{{aKey, f.member}, {bKey, f.founder}} {
		consent := observerConsent(t, observerPhoneRequest(t, item.key, "https://esp.example.org"), item.identity)
		rec := observerHTTP(t, f, http.MethodPost, "/v1/observers", "operator-secret", consent)
		if rec.Code != http.StatusCreated {
			t.Fatalf("pair: %d %s", rec.Code, rec.Body.String())
		}
	}
	x := testESPGroupID(23)
	mustCreateGroup(t, f.dataDir, x, f.member, membership.DefaultPolicy())
	for _, tc := range []struct {
		id    string
		key   ed25519.PrivateKey
		group entmoot.GroupID
		want  int
	}{{a, aKey, x, 200}, {b, bKey, x, 403}, {a, aKey, f.otherGID, 403}, {b, bKey, f.otherGID, 200}, {a, aKey, f.gid, 200}, {b, bKey, f.gid, 200}} {
		rec := f.history(tc.id, tc.key, tc.group)
		if rec.Code != tc.want {
			t.Fatalf("%s group%s: %d want%d %s", tc.id, tc.group, rec.Code, tc.want, rec.Body.String())
		}
	}
	// The two observer credentials remain independent on the same ESP.
	revoked := f.signed(a, aKey, http.MethodPost, "/v1/devices/current/revoke-observer", nil)
	if revoked.Code != http.StatusOK {
		t.Fatal(revoked.Body.String())
	}
	requireESPError(t, f.history(a, aKey, f.gid), http.StatusForbidden, "device_disabled")
	if rec := f.history(b, bKey, f.gid); rec.Code != http.StatusOK {
		t.Fatalf("revoking A broke B: %d", rec.Code)
	}
}
