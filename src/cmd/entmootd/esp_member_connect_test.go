package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox/mailboxtest"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store/storetest"
)

// memberConnectFixture is a real ESP handler over a real membership store, a
// real registry file, and a real mailbox store. gid holds founder and member;
// otherGID is served but holds only the founder; unservedGID has no store.
type memberConnectFixture struct {
	t            *testing.T
	dataDir      string
	registryPath string
	gid          entmoot.GroupID
	otherGID     entmoot.GroupID
	unservedGID  entmoot.GroupID
	founder      *keystore.Identity
	member       *keystore.Identity
	outsider     *keystore.Identity
	operatorPriv ed25519.PrivateKey
	registry     *esphttp.DeviceRegistry
	publisher    *recordingSignedPublisher
	handler      http.Handler
}

type memberConnectOptions struct {
	disabled            bool
	maxDevicesPerMember int
}

const legacyOperatorDeviceID = "ops-phone"

func newMemberConnectFixture(t *testing.T, opts memberConnectOptions) *memberConnectFixture {
	t.Helper()
	f := &memberConnectFixture{
		t:           t,
		dataDir:     t.TempDir(),
		gid:         testESPGroupID(1),
		otherGID:    testESPGroupID(2),
		unservedGID: testESPGroupID(3),
		publisher:   &recordingSignedPublisher{},
	}
	f.registryPath = filepath.Join(f.dataDir, "esp-devices.json")
	f.founder, _ = mustDaemonIdentity(t)
	f.member, _ = mustDaemonIdentity(t)
	f.outsider, _ = mustDaemonIdentity(t)
	mustCreateGroup(t, f.dataDir, f.gid, f.founder, membership.DefaultPolicy())
	mustCreateGroup(t, f.dataDir, f.otherGID, f.founder, membership.DefaultPolicy())
	f.withGroup(f.gid, func(group *membership.Group) {
		memberInfo := mustDaemonNodeInfo(t, f.member)
		mustJoinWithInvite(t, group, f.member, mustDaemonInvite(t, group, f.founder, memberInfo, 1))
	})

	// An operator registry written before self-enrollment existed.
	operatorPub, operatorPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.operatorPriv = operatorPriv
	legacy := fmt.Sprintf(`{"devices":[{"id":%q,"public_key":%q,"groups":[%q],"admin_groups":[%q],"client_ids":[%q],"disabled":false}]}`,
		legacyOperatorDeviceID, base64.StdEncoding.EncodeToString(operatorPub), f.gid.String(), f.gid.String(), legacyOperatorDeviceID)
	if err := os.WriteFile(f.registryPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	f.registry, err = esphttp.LoadDeviceRegistry(f.registryPath)
	if err != nil {
		t.Fatalf("load legacy registry: %v", err)
	}

	st := storetest.New(t)
	msg, err := buildESPSignedMessage(context.Background(), f.member, f.gid, f.rosterHead(f.gid), []string{"general"}, []byte("hello from the moot"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(context.Background(), f.gid, msg); err != nil {
		t.Fatal(err)
	}
	handler, err := esphttp.NewHandler(esphttp.Config{
		AuthMode:     esphttp.AuthModeDevice,
		Devices:      f.registry,
		Service:      mailboxtest.New(t, st, nil),
		Publisher:    f.publisher,
		Groups:       localGroupCatalog{dataDir: f.dataDir},
		GroupExists:  espGroupExists(f.dataDir),
		MemberRoster: &esphttp.MembershipRoster{Root: f.dataDir},
		MemberConnect: esphttp.MemberConnectConfig{
			Enabled:             !opts.disabled,
			RegistryPath:        f.registryPath,
			MaxDevicesPerMember: opts.maxDevicesPerMember,
		},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	f.handler = handler
	return f
}

func (f *memberConnectFixture) withGroup(gid entmoot.GroupID, fn func(*membership.Group)) {
	f.t.Helper()
	group, err := membership.Open(f.dataDir, gid)
	if err != nil {
		f.t.Fatalf("membership.Open: %v", err)
	}
	defer group.Close()
	fn(group)
}

func (f *memberConnectFixture) rosterHead(gid entmoot.GroupID) entmoot.RosterEntryID {
	var head entmoot.RosterEntryID
	f.withGroup(gid, func(group *membership.Group) { head = group.Canonical().ID })
	return head
}

func newDeviceKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}

func (f *memberConnectFixture) connectRequest(identity *keystore.Identity, devicePriv ed25519.PrivateKey, groups ...entmoot.GroupID) esphttp.MemberConnectRequest {
	f.t.Helper()
	req, err := buildESPConnectRequest(identity, devicePriv, groups, "", time.Now())
	if err != nil {
		f.t.Fatal(err)
	}
	return req
}

func (f *memberConnectFixture) postConnect(req esphttp.MemberConnectRequest) *httptest.ResponseRecorder {
	f.t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		f.t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, esphttp.MemberConnectPath, bytes.NewReader(body)))
	return rec
}

func (f *memberConnectFixture) mustConnect(identity *keystore.Identity, devicePriv ed25519.PrivateKey, groups ...entmoot.GroupID) esphttp.MemberConnectResponse {
	f.t.Helper()
	rec := f.postConnect(f.connectRequest(identity, devicePriv, groups...))
	if rec.Code != http.StatusOK {
		f.t.Fatalf("connect status = %d, want 200\nbody=%s", rec.Code, rec.Body.String())
	}
	var resp esphttp.MemberConnectResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		f.t.Fatal(err)
	}
	return resp
}

func (f *memberConnectFixture) signed(deviceID string, priv ed25519.PrivateKey, method, pathWithQuery string, body []byte) *httptest.ResponseRecorder {
	f.t.Helper()
	nonce, err := generateESPRequestNonce()
	if err != nil {
		f.t.Fatal(err)
	}
	ts := time.Now().UnixMilli()
	req := httptest.NewRequest(method, pathWithQuery, bytes.NewReader(body))
	sig := ed25519.Sign(priv, []byte(esphttp.DeviceSigningInput(method, req.URL.RequestURI(), ts, nonce, body)))
	req.Header.Set("X-Entmoot-Device-ID", deviceID)
	req.Header.Set("X-Entmoot-Timestamp-Ms", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Entmoot-Nonce", nonce)
	req.Header.Set("X-Entmoot-Signature", base64.StdEncoding.EncodeToString(sig))
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

// history reads with the ESP's default client id, the device id.
func (f *memberConnectFixture) history(deviceID string, priv ed25519.PrivateKey, gid entmoot.GroupID) *httptest.ResponseRecorder {
	return f.signed(deviceID, priv, http.MethodGet, espGroupPath(gid, "history"), nil)
}

func (f *memberConnectFixture) registryFile() []byte {
	f.t.Helper()
	data, err := os.ReadFile(f.registryPath)
	if err != nil {
		f.t.Fatal(err)
	}
	return data
}

func (f *memberConnectFixture) registryDevice(id string) (esphttp.Device, bool) {
	f.t.Helper()
	reg, err := esphttp.LoadDeviceRegistry(f.registryPath)
	if err != nil {
		f.t.Fatalf("reload registry file: %v", err)
	}
	for _, d := range reg.Snapshot() {
		if d.ID == id {
			return d, true
		}
	}
	return esphttp.Device{}, false
}

func espErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	return envelope.Error.Code
}

func requireESPError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status || espErrorCode(t, rec) != code {
		t.Fatalf("status/code = %d/%s, want %d/%s\nbody=%s", rec.Code, espErrorCode(t, rec), status, code, rec.Body.String())
	}
}

type recordingSignedPublisher struct {
	mu   sync.Mutex
	msgs []entmoot.Message
}

func (p *recordingSignedPublisher) PublishSigned(_ context.Context, msg entmoot.Message) (esphttp.PublishResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msg)
	return esphttp.PublishResult{Status: "accepted", MessageID: msg.ID, GroupID: msg.GroupID, AuthorMemberID: *msg.Author.MemberID, TimestampMS: msg.Timestamp}, nil
}

func (p *recordingSignedPublisher) published() []entmoot.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]entmoot.Message(nil), p.msgs...)
}

func TestESPMemberConnectGrantsImmediateHistoryRead(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	devicePriv := newDeviceKey(t)
	resp := f.mustConnect(f.member, devicePriv, f.gid)
	wantID := esphttp.SelfEnrolledDeviceID(devicePriv.Public().(ed25519.PublicKey))
	if resp.DeviceID != wantID || len(resp.Groups) != 1 || resp.Groups[0] != f.gid || !resp.Created {
		t.Fatalf("connect response = %+v, want new device %s for the group", resp, wantID)
	}

	// Same handler, same process: no restart between enrollment and use.
	rec := f.history(resp.DeviceID, devicePriv, f.gid)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "hello from the moot") {
		t.Fatalf("history status = %d, want 200 with the stored message\nbody=%s", rec.Code, rec.Body.String())
	}

	device, ok := f.registryDevice(resp.DeviceID)
	memberID, _ := entmoot.MemberIDFromPublicKey(f.member.PublicKey)
	if !ok || !device.SelfEnrolled || device.MemberID != memberID || len(device.AdminGroups) != 0 ||
		len(device.Groups) != 1 || device.Groups[0] != f.gid || len(device.ClientIDs) != 1 || device.ClientIDs[0] != resp.DeviceID {
		t.Fatalf("persisted device = %+v (found %v), want self-enrolled, member-bound, no admin", device, ok)
	}
	operator, ok := f.registryDevice(legacyOperatorDeviceID)
	if !ok || operator.SelfEnrolled || len(operator.AdminGroups) != 1 {
		t.Fatalf("operator device after connect = %+v (found %v), want unchanged", operator, ok)
	}

	// The device reaches nothing beyond reading and own-author publishing.
	requireESPError(t, f.signed(resp.DeviceID, devicePriv, http.MethodPost, espGroupPath(f.gid, "invites"), []byte(`{}`)), http.StatusForbidden, "self_enrolled_forbidden")
	requireESPError(t, f.history(resp.DeviceID, devicePriv, f.otherGID), http.StatusForbidden, "forbidden")
}

func TestESPMemberConnectRejectsInvalidRequests(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	before := f.registryFile()
	cases := []struct {
		name   string
		mutate func(*esphttp.MemberConnectRequest)
		status int
		code   string
	}{
		{"forged signature", func(r *esphttp.MemberConnectRequest) {
			r.Signature = base64.StdEncoding.EncodeToString(f.outsider.Sign([]byte(esphttp.MemberConnectSigningInput(*r))))
		}, http.StatusUnauthorized, "bad_signature"},
		{"group added after signing", func(r *esphttp.MemberConnectRequest) {
			r.GroupIDs = append(r.GroupIDs, f.otherGID)
		}, http.StatusUnauthorized, "bad_signature"},
		{"member id of another key", func(r *esphttp.MemberConnectRequest) {
			other, _ := entmoot.MemberIDFromPublicKey(f.founder.PublicKey)
			r.MemberID = other
		}, http.StatusUnauthorized, "member_mismatch"},
		{"stale timestamp", func(r *esphttp.MemberConnectRequest) {
			r.TimestampMS = time.Now().Add(-10 * time.Minute).UnixMilli()
			r.Signature = base64.StdEncoding.EncodeToString(f.member.Sign([]byte(esphttp.MemberConnectSigningInput(*r))))
		}, http.StatusUnauthorized, "stale"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := f.connectRequest(f.member, newDeviceKey(t), f.gid)
			tc.mutate(&req)
			requireESPError(t, f.postConnect(req), tc.status, tc.code)
		})
	}

	// A non-member signing honestly with its own key is refused per group.
	requireESPError(t, f.postConnect(f.connectRequest(f.outsider, newDeviceKey(t), f.gid)), http.StatusForbidden, "not_member")
	// One group the member is not in fails the whole request.
	requireESPError(t, f.postConnect(f.connectRequest(f.member, newDeviceKey(t), f.gid, f.otherGID)), http.StatusForbidden, "not_member")
	requireESPError(t, f.postConnect(f.connectRequest(f.member, newDeviceKey(t), f.gid, f.unservedGID)), http.StatusNotFound, "unknown_group")

	if !bytes.Equal(before, f.registryFile()) {
		t.Fatalf("rejected connects changed the registry file:\n%s", f.registryFile())
	}

	replayed := f.connectRequest(f.member, newDeviceKey(t), f.gid)
	if rec := f.postConnect(replayed); rec.Code != http.StatusOK {
		t.Fatalf("first connect status = %d\nbody=%s", rec.Code, rec.Body.String())
	}
	requireESPError(t, f.postConnect(replayed), http.StatusUnauthorized, "replay")
}

func TestESPMemberConnectRevokesOnRemovalAndBan(t *testing.T) {
	for _, banned := range []bool{false, true} {
		t.Run(fmt.Sprintf("banned=%v", banned), func(t *testing.T) {
			f := newMemberConnectFixture(t, memberConnectOptions{})
			devicePriv := newDeviceKey(t)
			resp := f.mustConnect(f.member, devicePriv, f.gid)
			if rec := f.history(resp.DeviceID, devicePriv, f.gid); rec.Code != http.StatusOK {
				t.Fatalf("history before removal = %d\nbody=%s", rec.Code, rec.Body.String())
			}
			f.withGroup(f.gid, func(group *membership.Group) {
				if _, err := group.SignRecord(f.founder, membership.Record{Kind: membership.KindRemove, Subject: mustDaemonNodeInfo(t, f.member), Banned: banned}); err != nil {
					t.Fatalf("remove member: %v", err)
				}
			})
			requireESPError(t, f.history(resp.DeviceID, devicePriv, f.gid), http.StatusForbidden, "not_member")
			groups := f.signed(resp.DeviceID, devicePriv, http.MethodGet, "/v1/groups", nil)
			if groups.Code != http.StatusOK || strings.Contains(groups.Body.String(), f.gid.String()) {
				t.Fatalf("group list after removal = %d %s, want the group hidden", groups.Code, groups.Body.String())
			}
			// Operator devices are not roster-bound and keep their access.
			if rec := f.history(legacyOperatorDeviceID, f.operatorPriv, f.gid); rec.Code != http.StatusOK {
				t.Fatalf("operator history after member removal = %d\nbody=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestESPMemberConnectKeepsDevicesBoundToTheirOwner(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	// The founder is a member of both groups, so only device binding can
	// stop it from taking over the member's device.
	devicePriv := newDeviceKey(t)
	resp := f.mustConnect(f.member, devicePriv, f.gid)
	requireESPError(t, f.postConnect(f.connectRequest(f.founder, devicePriv, f.gid)), http.StatusForbidden, "device_bound_to_other_member")

	// An operator device id equal to the derived id is never taken over.
	collidingPriv := newDeviceKey(t)
	collidingID := esphttp.SelfEnrolledDeviceID(collidingPriv.Public().(ed25519.PublicKey))
	operatorKey := newDeviceKey(t)
	// An operator key reused as a member device key is refused too.
	reusedOperatorKey := newDeviceKey(t)
	f.addOperatorDevice(collidingID, operatorKey.Public().(ed25519.PublicKey))
	f.addOperatorDevice("ops-laptop", reusedOperatorKey.Public().(ed25519.PublicKey))
	before := f.registryFile()
	requireESPError(t, f.postConnect(f.connectRequest(f.member, collidingPriv, f.gid)), http.StatusConflict, "device_id_conflict")
	requireESPError(t, f.postConnect(f.connectRequest(f.member, reusedOperatorKey, f.gid)), http.StatusConflict, "device_key_conflict")
	if !bytes.Equal(before, f.registryFile()) {
		t.Fatal("refused connects changed the registry file")
	}
	if device, ok := f.registryDevice(resp.DeviceID); !ok || device.MemberID != *mustDaemonNodeInfo(t, f.member).MemberID {
		t.Fatalf("member device after hijack attempts = %+v", device)
	}
}

// addOperatorDevice edits the registry the way `esp device add` does, through
// the same serialized writer the running ESP uses.
func (f *memberConnectFixture) addOperatorDevice(id string, pub ed25519.PublicKey) {
	f.t.Helper()
	_, err := f.registry.Update(f.registryPath, func(current *esphttp.DeviceRegistry) (*esphttp.DeviceRegistry, bool, error) {
		next, err := esphttp.NewDeviceRegistry(append(current.Snapshot(), esphttp.Device{ID: id, PublicKey: pub, Groups: []entmoot.GroupID{f.gid}, ClientIDs: []string{id}}))
		return next, err == nil, err
	})
	if err != nil {
		f.t.Fatalf("add operator device: %v", err)
	}
}

func TestESPMemberConnectReconnectReplacesGroups(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	f.withGroup(f.otherGID, func(group *membership.Group) {
		memberInfo := mustDaemonNodeInfo(t, f.member)
		mustJoinWithInvite(t, group, f.member, mustDaemonInvite(t, group, f.founder, memberInfo, 1))
	})
	devicePriv := newDeviceKey(t)
	first := f.mustConnect(f.member, devicePriv, f.gid, f.otherGID)
	if rec := f.history(first.DeviceID, devicePriv, f.otherGID); rec.Code != http.StatusOK {
		t.Fatalf("history in second group = %d\nbody=%s", rec.Code, rec.Body.String())
	}
	second := f.mustConnect(f.member, devicePriv, f.gid)
	if second.DeviceID != first.DeviceID || second.Created || !second.Changed {
		t.Fatalf("reconnect = %+v, want same device updated in place", second)
	}
	requireESPError(t, f.history(first.DeviceID, devicePriv, f.otherGID), http.StatusForbidden, "forbidden")
	same := f.mustConnect(f.member, devicePriv, f.gid)
	if same.Changed {
		t.Fatalf("identical reconnect reported a change: %+v", same)
	}
	if reg, _ := esphttp.LoadDeviceRegistry(f.registryPath); len(reg.Snapshot()) != 2 {
		t.Fatalf("registry holds %d devices, want operator + one member device", len(reg.Snapshot()))
	}
}

func TestESPMemberConnectCapsDevicesPerMember(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{maxDevicesPerMember: 2})
	first := newDeviceKey(t)
	f.mustConnect(f.member, first, f.gid)
	f.mustConnect(f.member, newDeviceKey(t), f.gid)
	requireESPError(t, f.postConnect(f.connectRequest(f.member, newDeviceKey(t), f.gid)), http.StatusForbidden, "device_limit")
	// Reconnecting an existing device is not a new device.
	f.mustConnect(f.member, first, f.gid)
	// Another member's allowance is separate.
	f.mustConnect(f.founder, newDeviceKey(t), f.gid)
	if _, ok := f.registryDevice(legacyOperatorDeviceID); !ok {
		t.Fatal("cap enforcement evicted the operator device")
	}
	if reg, _ := esphttp.LoadDeviceRegistry(f.registryPath); len(reg.Snapshot()) != 4 {
		t.Fatalf("registry holds %d devices, want operator + 2 member + 1 founder", len(reg.Snapshot()))
	}
}

func TestESPMemberConnectConcurrentWritersKeepEveryChange(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{maxDevicesPerMember: 32})
	operatorGrants := &fileBackedDeviceGroupAuthorizer{path: f.registryPath, registry: f.registry}
	const connects = 12
	var wg sync.WaitGroup
	errs := make(chan error, connects+1)
	for range connects {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := buildESPConnectRequest(f.member, newDeviceKey(t), []entmoot.GroupID{f.gid}, "", time.Now())
			if err != nil {
				errs <- err
				return
			}
			if rec := f.postConnect(req); rec.Code != http.StatusOK {
				errs <- fmt.Errorf("connect status %d: %s", rec.Code, rec.Body.String())
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := operatorGrants.GrantDeviceGroup(context.Background(), legacyOperatorDeviceID, f.otherGID); err != nil {
			errs <- err
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	reg, err := esphttp.LoadDeviceRegistry(f.registryPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(reg.Snapshot()); got != connects+1 {
		t.Fatalf("registry file holds %d devices, want %d", got, connects+1)
	}
	operator, _ := f.registryDevice(legacyOperatorDeviceID)
	if len(operator.Groups) != 2 {
		t.Fatalf("operator grant lost under concurrent connects: groups = %v", operator.Groups)
	}
}

func TestESPMemberConnectDisabledByDefault(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{disabled: true})
	before := f.registryFile()
	requireESPError(t, f.postConnect(f.connectRequest(f.member, newDeviceKey(t), f.gid)), http.StatusNotFound, "not_found")
	if !bytes.Equal(before, f.registryFile()) {
		t.Fatal("disabled connect changed the registry file")
	}

	cfg, _, ok := parseESPServeConfig([]string{"-auth-mode", "device"})
	if !ok || cfg.allowMemberConnect {
		t.Fatalf("default serve config allowMemberConnect = %v, want false", cfg.allowMemberConnect)
	}
	cfg, _, _ = parseESPServeConfig([]string{"-auth-mode", "bearer", "-token", "x", "-allow-member-connect"})
	if err := validateESPServeConfig(cfg); err == nil {
		t.Fatal("member connect with bearer-only auth validated")
	}
}

func TestESPLegacyRegistryAndOperatorDeviceUnchanged(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	operator, ok := f.registryDevice(legacyOperatorDeviceID)
	if !ok || operator.SelfEnrolled || len(operator.AdminGroups) != 1 {
		t.Fatalf("legacy operator device = %+v", operator)
	}
	// The operator device is not a group member; it is not roster-checked.
	if rec := f.history(legacyOperatorDeviceID, f.operatorPriv, f.gid); rec.Code != http.StatusOK {
		t.Fatalf("operator history = %d\nbody=%s", rec.Code, rec.Body.String())
	}
	// Operator devices keep routes self-enrolled devices are refused.
	if rec := f.signed(legacyOperatorDeviceID, f.operatorPriv, http.MethodGet, "/v1/sign-requests", nil); rec.Code != http.StatusOK {
		t.Fatalf("operator sign-requests = %d\nbody=%s", rec.Code, rec.Body.String())
	}
	// An operator adds a device with the CLI while the ESP runs; the next
	// connect must keep that edit rather than rewrite the file from memory.
	operatorEdit := newDeviceKey(t).Public().(ed25519.PublicKey)
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdESPDevice(&globalFlags{data: f.dataDir}, []string{"add", "-id", "ops-tablet", "-pubkey", base64.StdEncoding.EncodeToString(operatorEdit), "-device-keys", f.registryPath, "-group", f.gid.String(), "-client", "ops-tablet"})
	})
	if code != exitOK {
		t.Fatalf("esp device add exit = %d stderr=%s", code, stderr)
	}
	f.mustConnect(f.member, newDeviceKey(t), f.gid)
	if _, ok := f.registryDevice("ops-tablet"); !ok {
		t.Fatal("connect dropped a device the operator added to the file while the ESP ran")
	}
	var doc struct {
		Devices []map[string]any `json:"devices"`
	}
	if err := json.Unmarshal(f.registryFile(), &doc); err != nil {
		t.Fatal(err)
	}
	for _, d := range doc.Devices {
		_, marked := d["self_enrolled"]
		id, _ := d["id"].(string)
		if strings.HasPrefix(id, "member-") != marked {
			t.Fatalf("device %s self_enrolled key present=%v; only member devices may carry it", id, marked)
		}
	}
}

func TestESPMemberConnectPublishOnlyAsBoundMember(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	devicePriv := newDeviceKey(t)
	resp := f.mustConnect(f.member, devicePriv, f.gid)
	head := f.rosterHead(f.gid)
	publish := func(author *keystore.Identity, gid entmoot.GroupID) *httptest.ResponseRecorder {
		msg, err := buildESPSignedMessage(context.Background(), author, gid, head, []string{"general"}, []byte("relayed"), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(map[string]entmoot.Message{"message": msg})
		return f.signed(resp.DeviceID, devicePriv, http.MethodPost, espGroupPath(gid, "messages"), body)
	}
	if rec := publish(f.member, f.gid); rec.Code != http.StatusAccepted {
		t.Fatalf("own publish = %d\nbody=%s", rec.Code, rec.Body.String())
	}
	requireESPError(t, publish(f.founder, f.gid), http.StatusForbidden, "author_mismatch")
	requireESPError(t, publish(f.member, f.otherGID), http.StatusForbidden, "forbidden")
	// Drafts would ask the ESP to mint a sign request; self-enrolled devices
	// only relay messages they already signed.
	draft := []byte(`{"author":{},"topics":["general"],"content":"aGk="}`)
	requireESPError(t, f.signed(resp.DeviceID, devicePriv, http.MethodPost, espGroupPath(f.gid, "messages"), draft), http.StatusForbidden, "self_enrolled_forbidden")
	if got := f.publisher.published(); len(got) != 1 || *got[0].Author.MemberID != *mustDaemonNodeInfo(t, f.member).MemberID {
		t.Fatalf("publisher received %d messages, want only the member's own", len(got))
	}
}

func TestESPClientConnectHistoryPublishEndToEnd(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	server := httptest.NewServer(f.handler)
	defer server.Close()
	agent := daemonFlags(t, t.TempDir(), f.member)

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdESPConnect(agent, []string{"-esp", server.URL + "/", "-group", f.gid.String(), "-client", "cli"})
	})
	if code != exitOK {
		t.Fatalf("esp connect exit = %d stderr=%s", code, stderr)
	}
	var out espConnectOutput
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("connect output %q: %v", stdout, err)
	}
	if out.ESP != server.URL || !strings.HasPrefix(out.DeviceID, "member-") || len(out.Groups) != 1 || out.Groups[0] != f.gid {
		t.Fatalf("connect output = %+v", out)
	}
	info, err := os.Stat(filepath.Join(agent.data, espDeviceKeyFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("device key file mode = %v err=%v, want 0600", info, err)
	}

	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdESPHistory(agent, []string{"-group", f.gid.String(), "-limit", "10"})
	})
	if code != exitOK || !strings.Contains(stdout, "hello from the moot") {
		t.Fatalf("esp history exit = %d stdout=%s stderr=%s", code, stdout, stderr)
	}

	code, _, stderr = captureCommandOutput(t, func() int {
		return cmdESPPublish(agent, []string{"-group", f.gid.String(), "-topic", "general", "-content", "from the cli"})
	})
	if code != exitOK {
		t.Fatalf("esp publish exit = %d stderr=%s", code, stderr)
	}
	if got := f.publisher.published(); len(got) != 1 || string(got[0].Content) != "from the cli" {
		t.Fatalf("publisher received %+v", got)
	}

	// Reconnect reuses the stored device key.
	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdESPConnect(agent, []string{"-esp", server.URL, "-group", f.gid.String(), "-client", "cli"})
	})
	var again espConnectOutput
	if code != exitOK || json.Unmarshal([]byte(stdout), &again) != nil || again.DeviceID != out.DeviceID {
		t.Fatalf("reconnect exit = %d stdout=%s stderr=%s", code, stdout, stderr)
	}

	// A refused connect surfaces the ESP's error code.
	outsider := daemonFlags(t, t.TempDir(), f.outsider)
	code, _, stderr = captureCommandOutput(t, func() int {
		return cmdESPConnect(outsider, []string{"-esp", server.URL, "-group", f.gid.String()})
	})
	if code == exitOK || !strings.Contains(stderr, "not_member") {
		t.Fatalf("outsider connect exit = %d stderr=%s, want not_member", code, stderr)
	}
}
