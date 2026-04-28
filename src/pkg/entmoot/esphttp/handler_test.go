package esphttp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/store"
)

func TestHandlerPullAckCursor(t *testing.T) {
	gid, msgs, handler := testHandler(t)

	unauth := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/mailbox/pull?client_id=ios-1&group_id="+gid.String(), nil)
	handler.ServeHTTP(unauth, req)
	if unauth.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauth.Code, http.StatusUnauthorized)
	}
	if unauth.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("unauthorized response missing WWW-Authenticate")
	}

	lowerScheme := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/mailbox/cursor?client_id=ios-1&group_id="+gid.String(), nil)
	req.Header.Set("Authorization", "bearer secret")
	handler.ServeHTTP(lowerScheme, req)
	if lowerScheme.Code != http.StatusOK {
		t.Fatalf("lowercase bearer status = %d, want %d", lowerScheme.Code, http.StatusOK)
	}

	pull := doJSONRequest[mailbox.PullResult](t, handler, http.MethodGet, "/v1/mailbox/pull?client_id=ios-1&group_id="+gid.String()+"&limit=1", nil, http.StatusOK)
	if pull.ClientID != "ios-1" || pull.GroupID != gid {
		t.Fatalf("pull envelope = %+v, want client/group", pull)
	}
	if pull.Count != 1 || len(pull.Messages) != 1 || !pull.HasMore {
		t.Fatalf("pull count/messages/has_more = %d/%d/%v, want 1/1/true", pull.Count, len(pull.Messages), pull.HasMore)
	}
	if pull.Messages[0].MessageID != msgs[0].ID || pull.NextCursor.MessageID != msgs[0].ID {
		t.Fatalf("pull returned message/cursor %+v %+v, want first message", pull.Messages[0], pull.NextCursor)
	}

	ackBody := map[string]any{
		"client_id":  "ios-1",
		"group_id":   gid,
		"message_id": msgs[0].ID,
	}
	ack := doJSONRequest[mailbox.AckResult](t, handler, http.MethodPost, "/v1/mailbox/ack", ackBody, http.StatusOK)
	if ack.MessageID != msgs[0].ID || ack.Cursor.MessageID != msgs[0].ID {
		t.Fatalf("ack = %+v, want first message cursor", ack)
	}

	cursor := doJSONRequest[mailbox.CursorResult](t, handler, http.MethodGet, "/v1/mailbox/cursor?client_id=ios-1&group_id="+gid.String(), nil, http.StatusOK)
	if cursor.Cursor.MessageID != msgs[0].ID || cursor.Unread != 1 {
		t.Fatalf("cursor = %+v, want first message cursor and 1 unread", cursor)
	}
}

func TestHandlerRejectsUnknownGroupAndMessage(t *testing.T) {
	gid, _, handler := testHandler(t)
	var unknownGroup entmoot.GroupID
	unknownGroup[0] = 9

	resp := httptest.NewRecorder()
	req := authedRequest(http.MethodGet, "/v1/mailbox/pull?client_id=ios-1&group_id="+unknownGroup.String(), nil)
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("unknown group status = %d, want %d", resp.Code, http.StatusNotFound)
	}

	var missing entmoot.MessageID
	missing[0] = 0xFF
	body := map[string]any{
		"client_id":  "ios-1",
		"group_id":   gid,
		"message_id": missing,
	}
	errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/mailbox/ack", body, http.StatusBadRequest)
	if errResp.Error.Code != "message_not_found" {
		t.Fatalf("error code = %q, want message_not_found", errResp.Error.Code)
	}
}

func TestHandlerHealthzDoesNotRequireAuth(t *testing.T) {
	_, _, handler := testHandler(t)
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want %d", resp.Code, http.StatusOK)
	}
}

func TestHandlerSignedPublish(t *testing.T) {
	gid := testGroupID(1)
	msg := testMessage(gid, 3, "phone signed")
	publisher := &fakePublisher{
		result: PublishResult{
			Status:      "accepted",
			MessageID:   msg.ID,
			GroupID:     gid,
			Author:      msg.Author.PilotNodeID,
			TimestampMS: msg.Timestamp,
		},
	}
	handler := testHandlerWithPublisher(t, gid, publisher)

	result := doJSONRequest[PublishResult](t, handler, http.MethodPost, "/v1/messages", map[string]any{
		"message": msg,
	}, http.StatusAccepted)
	if result.MessageID != msg.ID || result.GroupID != gid || result.Author != msg.Author.PilotNodeID {
		t.Fatalf("publish result = %+v, want accepted message metadata", result)
	}
	if publisher.got.ID != msg.ID {
		t.Fatalf("publisher got message %s, want %s", publisher.got.ID, msg.ID)
	}
}

func TestHandlerSignedPublishErrors(t *testing.T) {
	gid := testGroupID(1)
	msg := testMessage(gid, 3, "phone signed")

	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{
			name: "bad_request",
			err: &PublishError{
				HTTPStatus: http.StatusBadRequest,
				Code:       "bad_request",
				Message:    "invalid signature",
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "bad_request",
		},
		{
			name: "not_member",
			err: &PublishError{
				HTTPStatus: http.StatusForbidden,
				Code:       "not_member",
				Message:    "author not a member",
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "not_member",
		},
		{
			name: "join_unavailable",
			err: &PublishError{
				HTTPStatus: http.StatusServiceUnavailable,
				Code:       "join_unavailable",
				Message:    "no join",
			},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "join_unavailable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := testHandlerWithPublisher(t, gid, &fakePublisher{err: tc.err})
			errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/messages", map[string]any{
				"message": msg,
			}, tc.wantStatus)
			if errResp.Error.Code != tc.wantCode {
				t.Fatalf("error code = %q, want %q", errResp.Error.Code, tc.wantCode)
			}
		})
	}
}

func TestHandlerSignedPublishWithoutPublisher(t *testing.T) {
	gid, _, handler := testHandler(t)
	msg := testMessage(gid, 3, "phone signed")
	errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/messages", map[string]any{
		"message": msg,
	}, http.StatusServiceUnavailable)
	if errResp.Error.Code != "join_unavailable" {
		t.Fatalf("error code = %q, want join_unavailable", errResp.Error.Code)
	}
}

func TestHandlerDeviceAuth(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:        "ios-1-device",
		PublicKey: priv.Public().(ed25519.PublicKey),
		Groups:    []entmoot.GroupID{gid},
		ClientIDs: []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	handler := testDeviceHandler(t, gid, reg, time.UnixMilli(1_000_000))

	req := signedDeviceRequest(t, priv, http.MethodGet, "/v1/mailbox/pull?client_id=ios-1&group_id="+gid.String(), nil, 1_000_000, "nonce-1")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("device signed pull status = %d, want 200 body=%s", resp.Code, resp.Body.String())
	}

	replay := signedDeviceRequest(t, priv, http.MethodGet, "/v1/mailbox/pull?client_id=ios-1&group_id="+gid.String(), nil, 1_000_000, "nonce-1")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, replay)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("replay status = %d, want 401", resp.Code)
	}

	badClient := signedDeviceRequest(t, priv, http.MethodGet, "/v1/mailbox/pull?client_id=other&group_id="+gid.String(), nil, 1_000_000, "nonce-2")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, badClient)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("bad client status = %d, want 403 body=%s", resp.Code, resp.Body.String())
	}
}

func TestHandlerMobileGroupsAndSignRequests(t *testing.T) {
	gid := testGroupID(1)
	catalog := fakeCatalog{
		groups: []GroupSummary{{GroupID: gid, Members: 2}},
		members: []MemberSummary{{
			NodeID:        45491,
			EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("pub")),
			Founder:       true,
		}},
	}
	handler := testMobileHandler(t, gid, nil, &catalog, nil)

	groups := doJSONRequest[struct {
		Groups []GroupSummary `json:"groups"`
	}](t, handler, http.MethodGet, "/v1/groups", nil, http.StatusOK)
	if len(groups.Groups) != 1 || groups.Groups[0].GroupID != gid {
		t.Fatalf("groups = %+v, want %s", groups, gid)
	}

	group := doJSONRequest[GroupSummary](t, handler, http.MethodGet, "/v1/groups/"+gid.String(), nil, http.StatusOK)
	if group.GroupID != gid || group.Members != 2 {
		t.Fatalf("group = %+v, want gid/members", group)
	}

	members := doJSONRequest[struct {
		Members []MemberSummary `json:"members"`
	}](t, handler, http.MethodGet, "/v1/groups/"+gid.String()+"/members", nil, http.StatusOK)
	if len(members.Members) != 1 || members.Members[0].NodeID != 45491 {
		t.Fatalf("members = %+v, want founder", members)
	}

	created := doJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, http.MethodPost, "/v1/groups", map[string]any{"name": "mobile group"}, http.StatusAccepted)
	if created.SignRequest.Kind != "group_create" || created.SignRequest.Status != signRequestPending {
		t.Fatalf("sign request = %+v, want group_create pending", created.SignRequest)
	}

	list := doJSONRequest[struct {
		SignRequests []SignRequest `json:"sign_requests"`
	}](t, handler, http.MethodGet, "/v1/sign-requests", nil, http.StatusOK)
	if len(list.SignRequests) != 1 || list.SignRequests[0].ID != created.SignRequest.ID {
		t.Fatalf("sign request list = %+v, want created request", list)
	}

	completed := doJSONRequest[SignRequest](t, handler, http.MethodPost, "/v1/sign-requests/"+created.SignRequest.ID+"/complete", map[string]any{
		"signature": base64.StdEncoding.EncodeToString([]byte("sig")),
	}, http.StatusOK)
	if completed.Status != signRequestCompleted {
		t.Fatalf("completed status = %q, want %q", completed.Status, signRequestCompleted)
	}
}

func TestHandlerDeviceStateAndNotifications(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:        "ios-1-device",
		PublicKey: priv.Public().(ed25519.PublicKey),
		Groups:    []entmoot.GroupID{gid},
		ClientIDs: []string{"ios-1-device"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	handler := testMobileHandler(t, gid, reg, nil, func() time.Time { return time.UnixMilli(1_000_000) })

	state := doSignedJSONRequest[DeviceState](t, handler, priv, http.MethodPut, "/v1/devices/current/push-token", map[string]any{
		"platform": "apns",
		"token":    "token-1",
	}, http.StatusOK, 1_000_000, "push-1")
	if state.PushToken != "token-1" || state.PushPlatform != "apns" {
		t.Fatalf("device state = %+v, want APNs token", state)
	}

	prefs := doSignedJSONRequest[NotificationPreferences](t, handler, priv, http.MethodPatch, "/v1/notifications/preferences", NotificationPreferences{
		Enabled: true,
		Topics:  []string{"ops/#"},
	}, http.StatusOK, 1_000_000, "prefs-1")
	if !prefs.Enabled || len(prefs.Topics) != 1 || prefs.Topics[0] != "ops/#" {
		t.Fatalf("prefs = %+v, want patched topics", prefs)
	}

	current := doSignedJSONRequest[struct {
		Device map[string]any `json:"device"`
		State  DeviceState    `json:"state"`
	}](t, handler, priv, http.MethodGet, "/v1/devices/current", nil, http.StatusOK, 1_000_000, "current-1")
	if current.State.PushToken != "token-1" {
		t.Fatalf("current state = %+v, want persisted push token", current.State)
	}

	queued := doSignedJSONRequest[map[string]any](t, handler, priv, http.MethodPost, "/v1/notifications/test", nil, http.StatusAccepted, 1_000_000, "test-1")
	if queued["status"] != "queued" {
		t.Fatalf("notification test = %+v, want queued", queued)
	}
}

func TestHandlerDualAuthAcceptsBearerAndDevice(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:        "ios-1-device",
		PublicKey: priv.Public().(ed25519.PublicKey),
		Groups:    []entmoot.GroupID{gid},
		ClientIDs: []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	st := store.NewMemory()
	if err := st.Put(context.Background(), testMessage(gid, 1, "first")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	handler, err := NewHandler(Config{
		Token:    "secret",
		AuthMode: AuthModeDual,
		Devices:  reg,
		Service:  svc,
		Clock:    func() time.Time { return time.UnixMilli(1_000_000) },
		GroupExists: func(_ context.Context, got entmoot.GroupID) (bool, error) {
			return got == gid, nil
		},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	doJSONRequest[mailbox.PullResult](t, handler, http.MethodGet, "/v1/mailbox/pull?client_id=any-bearer-client&group_id="+gid.String(), nil, http.StatusOK)

	req := signedDeviceRequest(t, priv, http.MethodGet, "/v1/mailbox/pull?client_id=ios-1&group_id="+gid.String(), nil, 1_000_000, "nonce-1")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("device signed pull status = %d, want 200 body=%s", resp.Code, resp.Body.String())
	}
}

func TestLoadDeviceRegistry(t *testing.T) {
	gid := testGroupID(1)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	path := t.TempDir() + "/devices.json"
	body := map[string]any{
		"devices": []map[string]any{{
			"id":         "ios-1-device",
			"public_key": base64.StdEncoding.EncodeToString(pub),
			"groups":     []string{gid.String()},
			"client_ids": []string{"ios-1"},
		}},
	}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	reg, err := LoadDeviceRegistry(path)
	if err != nil {
		t.Fatalf("LoadDeviceRegistry: %v", err)
	}
	d, ok := reg.lookup("ios-1-device")
	if !ok || len(d.Groups) != 1 || d.Groups[0] != gid || len(d.ClientIDs) != 1 || d.ClientIDs[0] != "ios-1" {
		t.Fatalf("loaded device = %+v ok=%v", d, ok)
	}
}

func testHandler(t *testing.T) (entmoot.GroupID, []entmoot.Message, http.Handler) {
	t.Helper()
	gid := testGroupID(1)
	msgs := []entmoot.Message{
		testMessage(gid, 1, "first"),
		testMessage(gid, 2, "second"),
	}
	st := store.NewMemory()
	for _, msg := range msgs {
		if err := st.Put(context.Background(), msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	handler, err := NewHandler(Config{
		Token:   "secret",
		Service: svc,
		GroupExists: func(_ context.Context, got entmoot.GroupID) (bool, error) {
			return got == gid, nil
		},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return gid, msgs, handler
}

func testHandlerWithPublisher(t *testing.T, gid entmoot.GroupID, publisher Publisher) http.Handler {
	t.Helper()
	st := store.NewMemory()
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	handler, err := NewHandler(Config{
		Token:     "secret",
		Service:   svc,
		Publisher: publisher,
		GroupExists: func(_ context.Context, got entmoot.GroupID) (bool, error) {
			return got == gid, nil
		},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func testMobileHandler(t *testing.T, gid entmoot.GroupID, reg *DeviceRegistry, catalog GroupCatalog, clock func() time.Time) http.Handler {
	t.Helper()
	st := store.NewMemory()
	for _, msg := range []entmoot.Message{testMessage(gid, 1, "first")} {
		if err := st.Put(context.Background(), msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	mode := AuthModeBearer
	if reg != nil {
		mode = AuthModeDevice
	}
	if clock == nil {
		clock = time.Now
	}
	handler, err := NewHandler(Config{
		Token:    "secret",
		AuthMode: mode,
		Devices:  reg,
		Service:  svc,
		State:    NewMemoryStateStore(),
		Groups:   catalog,
		Clock:    clock,
		GroupExists: func(_ context.Context, got entmoot.GroupID) (bool, error) {
			return got == gid, nil
		},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func testDeviceHandler(t *testing.T, gid entmoot.GroupID, reg *DeviceRegistry, now time.Time) http.Handler {
	t.Helper()
	st := store.NewMemory()
	for _, msg := range []entmoot.Message{testMessage(gid, 1, "first")} {
		if err := st.Put(context.Background(), msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	handler, err := NewHandler(Config{
		AuthMode: AuthModeDevice,
		Devices:  reg,
		Service:  svc,
		Clock:    func() time.Time { return now },
		GroupExists: func(_ context.Context, got entmoot.GroupID) (bool, error) {
			return got == gid, nil
		},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

type fakePublisher struct {
	result PublishResult
	err    error
	got    entmoot.Message
}

func (p *fakePublisher) PublishSigned(_ context.Context, msg entmoot.Message) (PublishResult, error) {
	p.got = msg
	if p.err != nil {
		return PublishResult{}, p.err
	}
	return p.result, nil
}

func doJSONRequest[T any](t *testing.T, handler http.Handler, method, path string, body any, wantStatus int) T {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("Marshal request body: %v", err)
		}
		reader = bytes.NewReader(data)
	}
	resp := httptest.NewRecorder()
	req := authedRequest(method, path, reader)
	handler.ServeHTTP(resp, req)
	if resp.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d\nbody=%s", method, path, resp.Code, wantStatus, resp.Body.String())
	}
	var out T
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("Unmarshal response: %v\n%s", err, resp.Body.String())
	}
	return out
}

func authedRequest(method, path string, body *bytes.Reader) *http.Request {
	if body == nil {
		body = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Authorization", "Bearer secret")
	return req
}

func signedDeviceRequest(t *testing.T, priv ed25519.PrivateKey, method, path string, body any, timestampMillis int64, nonce string) *http.Request {
	t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("Marshal request body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	input := DeviceSigningInput(method, req.URL.RequestURI(), timestampMillis, nonce, data)
	sig := ed25519.Sign(priv, []byte(input))
	req.Header.Set(deviceIDHeader, "ios-1-device")
	req.Header.Set(timestampHeader, strconv.FormatInt(timestampMillis, 10))
	req.Header.Set(nonceHeader, nonce)
	req.Header.Set(signatureHeader, base64.StdEncoding.EncodeToString(sig))
	return req
}

func doSignedJSONRequest[T any](t *testing.T, handler http.Handler, priv ed25519.PrivateKey, method, path string, body any, wantStatus int, timestampMillis int64, nonce string) T {
	t.Helper()
	req := signedDeviceRequest(t, priv, method, path, body, timestampMillis, nonce)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d\nbody=%s", method, path, resp.Code, wantStatus, resp.Body.String())
	}
	var out T
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("Unmarshal response: %v\n%s", err, resp.Body.String())
	}
	return out
}

func testMessage(gid entmoot.GroupID, ts int64, content string) entmoot.Message {
	msg := entmoot.Message{
		GroupID:   gid,
		Author:    entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("pub")},
		Timestamp: ts,
		Topics:    []string{"test/mailbox"},
		Content:   []byte(content),
	}
	msg.ID = canonical.MessageID(msg)
	return msg
}

func testGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}

type fakeCatalog struct {
	groups  []GroupSummary
	members []MemberSummary
}

func (c *fakeCatalog) ListGroups(context.Context) ([]GroupSummary, error) {
	return append([]GroupSummary(nil), c.groups...), nil
}

func (c *fakeCatalog) GetGroup(_ context.Context, gid entmoot.GroupID) (GroupSummary, bool, error) {
	for _, group := range c.groups {
		if group.GroupID == gid {
			return group, true, nil
		}
	}
	return GroupSummary{}, false, nil
}

func (c *fakeCatalog) ListMembers(context.Context, entmoot.GroupID) ([]MemberSummary, error) {
	return append([]MemberSummary(nil), c.members...), nil
}
