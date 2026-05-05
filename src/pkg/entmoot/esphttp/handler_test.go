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
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/espnotify"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/signing"
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
		ID:          "ios-1-device",
		PublicKey:   priv.Public().(ed25519.PublicKey),
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
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

	history := signedDeviceRequest(t, priv, http.MethodGet, "/v1/groups/"+gid.String()+"/history?client_id=ios-1&limit=1", nil, 1_000_000, "nonce-3")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, history)
	if resp.Code != http.StatusOK {
		t.Fatalf("history status = %d, want 200 body=%s", resp.Code, resp.Body.String())
	}

	badHistoryClient := signedDeviceRequest(t, priv, http.MethodGet, "/v1/groups/"+gid.String()+"/history?client_id=other&limit=1", nil, 1_000_000, "nonce-4")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, badHistoryClient)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("bad history client status = %d, want 403 body=%s", resp.Code, resp.Body.String())
	}
}

func TestHandlerDisabledDeviceAuth(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   priv.Public().(ed25519.PublicKey),
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
		Disabled:    true,
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	handler := testDeviceHandler(t, gid, reg, time.UnixMilli(1_000_000))
	req := signedDeviceRequest(t, priv, http.MethodGet, "/v1/mailbox/pull?client_id=ios-1&group_id="+gid.String(), nil, 1_000_000, "nonce-1")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("disabled device status = %d, want 403 body=%s", resp.Code, resp.Body.String())
	}
	var errResp errorEnvelope
	if err := json.Unmarshal(resp.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if errResp.Error.Code != "device_disabled" {
		t.Fatalf("error code = %q, want device_disabled", errResp.Error.Code)
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

	createErr := doJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/groups", map[string]any{"name": "mobile group"}, http.StatusForbidden)
	if createErr.Error.Code != "device_signature_required" {
		t.Fatalf("error code = %q, want device_signature_required", createErr.Error.Code)
	}

	list := doJSONRequest[struct {
		SignRequests []SignRequest `json:"sign_requests"`
	}](t, handler, http.MethodGet, "/v1/sign-requests", nil, http.StatusOK)
	if len(list.SignRequests) != 0 {
		t.Fatalf("sign request list = %+v, want empty after rejected group_create", list)
	}
}

func TestHandlerOpenInviteListAndRevoke(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   priv.Public().(ed25519.PublicKey),
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	state := NewMemoryStateStore()
	if _, err := state.CreateOpenInvite(context.Background(), OpenInviteRecord{
		TokenHash:      "invite-id",
		GroupID:        gid,
		DeviceID:       "ios-1-device",
		MaxUses:        3,
		UseCount:       1,
		BootstrapPeers: []entmoot.NodeID{9},
		CreatedAtMS:    1_000,
		ExpiresAtMS:    2_000_000,
	}); err != nil {
		t.Fatalf("CreateOpenInvite: %v", err)
	}
	handler := testMobileHandlerFull(t, gid, reg, nil, func() time.Time { return time.UnixMilli(10_000) }, nil, state, nil)

	list := doSignedJSONRequest[struct {
		OpenInvites []OpenInviteSummary `json:"open_invites"`
	}](t, handler, priv, http.MethodGet, "/v1/groups/"+url.PathEscape(gid.String())+"/open-invites", nil, http.StatusOK, 10_000, "nonce-open-list")
	if len(list.OpenInvites) != 1 {
		t.Fatalf("open invite count = %d, want 1", len(list.OpenInvites))
	}
	if list.OpenInvites[0].ID != "invite-id" || list.OpenInvites[0].Status != "active" || list.OpenInvites[0].UseCount != 1 {
		t.Fatalf("open invite summary = %+v", list.OpenInvites[0])
	}

	revoked := doSignedJSONRequest[struct {
		OpenInvite OpenInviteSummary `json:"open_invite"`
	}](t, handler, priv, http.MethodPost, "/v1/groups/"+url.PathEscape(gid.String())+"/open-invites/invite-id/revoke", nil, http.StatusOK, 10_001, "nonce-open-revoke")
	if !revoked.OpenInvite.Revoked || revoked.OpenInvite.Status != "revoked" {
		t.Fatalf("revoked invite = %+v", revoked.OpenInvite)
	}
}

func TestHandlerFleetReadsRequireCoordinatorDevice(t *testing.T) {
	gid := testGroupID(1)
	_, coordinatorPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey coordinator: %v", err)
	}
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey other: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{
		{ID: "ios-1-device", PublicKey: coordinatorPriv.Public().(ed25519.PublicKey), ClientIDs: []string{"ios-1"}},
		{ID: "other-device", PublicKey: otherPriv.Public().(ed25519.PublicKey), ClientIDs: []string{"other"}},
	})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	state := NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("coordinator")}
	if _, err := state.CreateFleet(context.Background(), FleetRecord{
		FleetID:             "fleet-a",
		Name:                "Fleet A",
		Coordinator:         coordinator,
		CoordinatorDeviceID: "ios-1-device",
		CreatedAtMS:         1,
	}); err != nil {
		t.Fatalf("CreateFleet fleet-a: %v", err)
	}
	if _, err := state.CreateFleet(context.Background(), FleetRecord{
		FleetID:             "fleet-b",
		Name:                "Fleet B",
		Coordinator:         entmoot.NodeInfo{PilotNodeID: 45492, EntmootPubKey: []byte("other")},
		CoordinatorDeviceID: "other-device",
		CreatedAtMS:         2,
	}); err != nil {
		t.Fatalf("CreateFleet fleet-b: %v", err)
	}
	if _, err := state.CreateFleetInvite(context.Background(), FleetInviteRecord{
		InviteID:      "invite-a",
		FleetID:       "fleet-a",
		NodeID:        45493,
		EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("invitee")),
		Status:        FleetMemberInvited,
		Invite:        json.RawMessage(`{"secret":"control-group-invite"}`),
		CreatedAtMS:   3,
	}); err != nil {
		t.Fatalf("CreateFleetInvite: %v", err)
	}
	if _, err := state.UpsertFleetMember(context.Background(), FleetMemberRecord{
		FleetID:       "fleet-a",
		NodeID:        45491,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		Role:          FleetRoleCoordinator,
		Status:        FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	if _, err := state.AppendFleetActivity(context.Background(), FleetActivityRecord{
		FleetID:     "fleet-a",
		Type:        "fleet.created",
		Actor:       coordinator,
		Summary:     "Fleet created",
		CreatedAtMS: 4,
	}); err != nil {
		t.Fatalf("AppendFleetActivity: %v", err)
	}
	handler := testMobileHandlerFull(t, gid, reg, nil, func() time.Time { return time.UnixMilli(10_000) }, nil, state, nil)

	list := doSignedJSONRequest[struct {
		Fleets []FleetRecord `json:"fleets"`
	}](t, handler, coordinatorPriv, http.MethodGet, "/v1/fleets", nil, http.StatusOK, 10_000, "nonce-fleet-list")
	if len(list.Fleets) != 1 || list.Fleets[0].FleetID != "fleet-a" {
		t.Fatalf("coordinator fleets = %+v, want only fleet-a", list.Fleets)
	}
	invites := doSignedJSONRequest[struct {
		Invites []FleetInviteRecord `json:"invites"`
	}](t, handler, coordinatorPriv, http.MethodGet, "/v1/fleets/fleet-a/invites", nil, http.StatusOK, 10_001, "nonce-fleet-invites")
	if len(invites.Invites) != 1 || !bytes.Contains(invites.Invites[0].Invite, []byte("control-group-invite")) {
		t.Fatalf("coordinator invites = %+v", invites.Invites)
	}
	members := doSignedJSONRequest[struct {
		Members []FleetMemberRecord `json:"members"`
	}](t, handler, coordinatorPriv, http.MethodGet, "/v1/fleets/fleet-a/members", nil, http.StatusOK, 10_002, "nonce-fleet-members")
	if len(members.Members) != 1 || members.Members[0].NodeID != 45491 {
		t.Fatalf("coordinator members = %+v", members.Members)
	}
	activity := doSignedJSONRequest[struct {
		Activity []FleetActivityRecord `json:"activity"`
	}](t, handler, coordinatorPriv, http.MethodGet, "/v1/fleets/fleet-a/activity", nil, http.StatusOK, 10_003, "nonce-fleet-activity")
	if len(activity.Activity) != 1 || activity.Activity[0].Type != "fleet.created" {
		t.Fatalf("coordinator activity = %+v", activity.Activity)
	}

	otherList := doSignedJSONRequestFor[struct {
		Fleets []FleetRecord `json:"fleets"`
	}](t, handler, "other-device", otherPriv, http.MethodGet, "/v1/fleets", nil, http.StatusOK, 10_004, "nonce-other-list")
	if len(otherList.Fleets) != 1 || otherList.Fleets[0].FleetID != "fleet-b" {
		t.Fatalf("other fleets = %+v, want only fleet-b", otherList.Fleets)
	}
	errResp := doSignedJSONRequestFor[errorEnvelope](t, handler, "other-device", otherPriv, http.MethodGet, "/v1/fleets/fleet-a/invites", nil, http.StatusForbidden, 10_005, "nonce-other-invites")
	if errResp.Error.Code != "forbidden" {
		t.Fatalf("other invite error code = %q, want forbidden", errResp.Error.Code)
	}
}

func TestHandlerFleetReadsRejectBearerOnly(t *testing.T) {
	gid := testGroupID(1)
	state := NewMemoryStateStore()
	if _, err := state.CreateFleet(context.Background(), FleetRecord{
		FleetID:             "fleet-a",
		Name:                "Fleet A",
		Coordinator:         entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("coordinator")},
		CoordinatorDeviceID: "ios-1-device",
		CreatedAtMS:         1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	handler := testMobileHandlerFull(t, gid, nil, nil, func() time.Time { return time.UnixMilli(10_000) }, nil, state, nil)

	errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodGet, "/v1/fleets", nil, http.StatusForbidden)
	if errResp.Error.Code != "device_signature_required" {
		t.Fatalf("bearer fleet list error code = %q, want device_signature_required", errResp.Error.Code)
	}
}

func TestHandlerFleetListReturnsEmptyArrayWhenNoFleetsVisible(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{
		{ID: "ios-1-device", PublicKey: priv.Public().(ed25519.PublicKey), ClientIDs: []string{"ios-1"}},
	})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	state := NewMemoryStateStore()
	handler := testMobileHandlerFull(t, gid, reg, nil, func() time.Time { return time.UnixMilli(10_000) }, nil, state, nil)

	req := signedDeviceRequest(t, priv, http.MethodGet, "/v1/fleets", nil, 10_000, "nonce-empty-fleets")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("GET /v1/fleets status = %d, want %d\nbody=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(resp.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("Unmarshal response: %v\n%s", err, resp.Body.String())
	}
	if got := strings.TrimSpace(string(envelope["fleets"])); got != "[]" {
		t.Fatalf("fleets JSON = %s, want []\nbody=%s", got, resp.Body.String())
	}
}

func TestHandlerGroupDiagnostics(t *testing.T) {
	gid := testGroupID(1)
	diagnostics := &fakeDiagnostics{result: map[string]any{
		"group_id":   gid.String(),
		"members":    2,
		"suggestion": "ok",
	}}
	handler, err := NewHandler(Config{
		Token:       "secret",
		Service:     mustMailboxService(t, gid),
		Diagnostics: diagnostics,
		GroupExists: func(_ context.Context, got entmoot.GroupID) (bool, error) { return got == gid, nil },
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	resp := doJSONRequest[struct {
		Group map[string]any `json:"group"`
	}](t, handler, http.MethodGet, "/v1/groups/"+url.PathEscape(gid.String())+"/diagnostics?probe=true&timeout=4s", nil, http.StatusOK)
	if !diagnostics.probe || diagnostics.timeout != 4*time.Second || diagnostics.gid != gid {
		t.Fatalf("diagnostics args gid=%s probe=%v timeout=%s", diagnostics.gid, diagnostics.probe, diagnostics.timeout)
	}
	if resp.Group["suggestion"] != "ok" {
		t.Fatalf("diagnostics response = %+v", resp.Group)
	}
}

func TestHandlerGroupSubrouteEscapedSlashInGroupID(t *testing.T) {
	gid := testGroupIDWithSlash()
	if !strings.Contains(gid.String(), "/") {
		t.Fatalf("test group id %q does not contain slash", gid.String())
	}
	catalog := &recordingCatalog{
		fakeCatalog: fakeCatalog{
			members: []MemberSummary{{NodeID: 45491}},
		},
	}
	handler := testMobileHandler(t, gid, nil, catalog, nil)

	members := doJSONRequest[struct {
		Members []MemberSummary `json:"members"`
	}](t, handler, http.MethodGet, "/v1/groups/"+url.PathEscape(gid.String())+"/members", nil, http.StatusOK)
	if len(members.Members) != 1 || members.Members[0].NodeID != 45491 {
		t.Fatalf("members = %+v, want escaped group member", members)
	}
	if catalog.listMembersCalls != 1 {
		t.Fatalf("ListMembers calls = %d, want 1", catalog.listMembersCalls)
	}
	if catalog.listMembersGroup != gid {
		t.Fatalf("ListMembers group = %s, want %s", catalog.listMembersGroup, gid)
	}
}

func TestHandlerGroupHistoryReturnsLatestWithoutAdvancingCursor(t *testing.T) {
	gid := testGroupID(1)
	st := store.NewMemory()
	for _, msg := range []entmoot.Message{
		testMessage(gid, 1, "first"),
		testMessage(gid, 2, "second"),
		testMessage(gid, 3, "third"),
	} {
		if err := st.Put(context.Background(), msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	if err := svc.AckCursorContext(context.Background(), gid, "ios-1", mailbox.Cursor{
		MessageID:   testMessage(gid, 2, "second").ID,
		TimestampMS: 2,
	}); err != nil {
		t.Fatalf("AckCursorContext: %v", err)
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

	history := doJSONRequest[mailbox.HistoryResult](t, handler, http.MethodGet, "/v1/groups/"+gid.String()+"/history?client_id=ios-1&limit=2", nil, http.StatusOK)
	if history.Count != 2 || len(history.Messages) != 2 {
		t.Fatalf("history count/messages = %d/%d, want 2/2", history.Count, len(history.Messages))
	}
	if !history.HasMore || history.NextCursor == "" {
		t.Fatalf("history has_more/cursor = %v/%q, want older page cursor", history.HasMore, history.NextCursor)
	}
	if history.Messages[0].Content != "second" || history.Messages[1].Content != "third" {
		t.Fatalf("history contents = %q, %q; want second, third", history.Messages[0].Content, history.Messages[1].Content)
	}
	older := doJSONRequest[mailbox.HistoryResult](t, handler, http.MethodGet, "/v1/groups/"+gid.String()+"/history?client_id=ios-1&limit=2&cursor="+url.QueryEscape(history.NextCursor), nil, http.StatusOK)
	if older.Count != 1 || len(older.Messages) != 1 || older.Messages[0].Content != "first" {
		t.Fatalf("older history = %+v, want first only", older)
	}
	if older.HasMore || older.NextCursor != "" {
		t.Fatalf("older history has_more/cursor = %v/%q, want exhausted", older.HasMore, older.NextCursor)
	}
	cursor, err := svc.CursorStatus(context.Background(), gid, "ios-1")
	if err != nil {
		t.Fatalf("CursorStatus: %v", err)
	}
	if cursor.Cursor.TimestampMS != 2 {
		t.Fatalf("cursor timestamp = %d, want unchanged 2", cursor.Cursor.TimestampMS)
	}
}

func TestHistoryCursorAcceptsZeroBoundaryFields(t *testing.T) {
	gid := testGroupID(1)
	var msgID entmoot.MessageID
	msgID[0] = 1
	cursor := encodeHistoryCursor(gid, "", &store.PageBoundary{
		TimestampMS:  0,
		AuthorNodeID: 0,
		MessageID:    msgID,
	})
	if cursor == "" {
		t.Fatal("encodeHistoryCursor returned empty cursor")
	}
	boundary, err := parseHistoryCursor(cursor, gid, "")
	if err != nil {
		t.Fatalf("parseHistoryCursor: %v", err)
	}
	if boundary.TimestampMS != 0 || boundary.AuthorNodeID != 0 || boundary.MessageID != msgID {
		t.Fatalf("boundary = %+v, want zero timestamp/node and message id %s", boundary, msgID)
	}
}

func TestHandlerGroupTopicsAndTopicHistory(t *testing.T) {
	gid := testGroupID(1)
	st := store.NewMemory()
	withTopics := func(ts int64, content string, topics ...string) entmoot.Message {
		msg := testMessage(gid, ts, content)
		msg.Topics = topics
		msg.ID = canonical.MessageID(msg)
		return msg
	}
	oldOps := withTopics(1, "old ops", "ops")
	researchOps := withTopics(2, "research ops", "research", "ops")
	chat := withTopics(3, "chat", "chat")
	for _, msg := range []entmoot.Message{chat, oldOps, researchOps} {
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

	topics := doJSONRequest[mailbox.TopicsResult](t, handler, http.MethodGet, "/v1/groups/"+gid.String()+"/topics?client_id=ios-1&limit=10", nil, http.StatusOK)
	if topics.Count != 3 || len(topics.Topics) != 3 {
		t.Fatalf("topics count/messages = %d/%d, want 3/3", topics.Count, len(topics.Topics))
	}
	if topics.Topics[0].Topic != "ops" || topics.Topics[0].Count != 2 || topics.Topics[0].LatestMessageAtMS != 2 {
		t.Fatalf("top topic = %+v, want ops count 2 latest 2", topics.Topics[0])
	}

	history := doJSONRequest[mailbox.HistoryResult](t, handler, http.MethodGet, "/v1/groups/"+gid.String()+"/history?client_id=ios-1&topic=ops&limit=1", nil, http.StatusOK)
	if history.Count != 1 || len(history.Messages) != 1 {
		t.Fatalf("topic history count/messages = %d/%d, want 1/1", history.Count, len(history.Messages))
	}
	if !history.HasMore || history.NextCursor == "" {
		t.Fatalf("topic history has_more/cursor = %v/%q, want older page cursor", history.HasMore, history.NextCursor)
	}
	if history.Messages[0].MessageID != researchOps.ID {
		t.Fatalf("topic history ids = %v, want researchOps", []entmoot.MessageID{history.Messages[0].MessageID})
	}
	older := doJSONRequest[mailbox.HistoryResult](t, handler, http.MethodGet, "/v1/groups/"+gid.String()+"/history?client_id=ios-1&topic=ops&limit=1&cursor="+url.QueryEscape(history.NextCursor), nil, http.StatusOK)
	if older.Count != 1 || len(older.Messages) != 1 || older.Messages[0].MessageID != oldOps.ID {
		t.Fatalf("older topic history = %+v, want oldOps", older)
	}
	if older.HasMore || older.NextCursor != "" {
		t.Fatalf("older topic history has_more/cursor = %v/%q, want exhausted", older.HasMore, older.NextCursor)
	}
}

func TestHandlerGroupHistoryRejectsLimitZero(t *testing.T) {
	gid := testGroupID(1)
	handler := testMobileHandler(t, gid, nil, nil, nil)

	errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodGet, "/v1/groups/"+gid.String()+"/history?client_id=ios-1&limit=0", nil, http.StatusBadRequest)
	if errResp.Error.Code != "bad_request" {
		t.Fatalf("error code = %q, want bad_request", errResp.Error.Code)
	}
	if !strings.Contains(errResp.Error.Message, "between 1 and") {
		t.Fatalf("error message = %q, want limit range", errResp.Error.Message)
	}
}

func TestHandlerMessagePublishSignRequestExecutes(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	publisher := &fakePublisher{}
	handler := testMobileHandlerWithPublisher(t, gid, publisher, func() time.Time { return time.UnixMilli(1_234_000) })

	created := doJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, http.MethodPost, "/v1/groups/"+gid.String()+"/messages", map[string]any{
		"author": entmoot.NodeInfo{
			PilotNodeID:   45491,
			EntmootPubKey: pub,
		},
		"topics":  []string{"chat"},
		"content": []byte("hello from phone"),
	}, http.StatusAccepted)
	req := created.SignRequest
	if req.Kind != signRequestKindMessagePublish || req.CanonicalType != canonicalTypeMessageV1 ||
		req.SignatureAlgorithm != signatureAlgorithmEd25519 || req.SigningPayload == "" || req.SigningPayloadSHA256 == "" {
		t.Fatalf("sign request metadata = %+v", req)
	}
	signingPayload, err := base64.StdEncoding.DecodeString(req.SigningPayload)
	if err != nil {
		t.Fatalf("decode signing payload: %v", err)
	}
	var payload messagePublishPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	wantSigningPayload, err := signing.MessageSigningBytes(payload.Message)
	if err != nil {
		t.Fatalf("MessageSigningBytes: %v", err)
	}
	if !bytes.Equal(signingPayload, wantSigningPayload) {
		t.Fatalf("signing payload mismatch\n got %q\nwant %q", signingPayload, wantSigningPayload)
	}
	sig := ed25519.Sign(priv, signingPayload)
	completed := doJSONRequest[SignRequest](t, handler, http.MethodPost, "/v1/sign-requests/"+req.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(sig),
		"signing_payload_sha256": req.SigningPayloadSHA256,
	}, http.StatusOK)
	if completed.Status != signRequestCompleted || completed.PublishResult == nil {
		t.Fatalf("completed sign request = %+v", completed)
	}
	if completed.PublishResult.MessageID != publisher.got.ID {
		t.Fatalf("publish result message id = %s, want %s", completed.PublishResult.MessageID, publisher.got.ID)
	}
	if publisher.got.ID != canonical.MessageID(publisher.got) {
		t.Fatalf("publisher got non-canonical message id")
	}
	if err := signing.VerifyMessage(publisher.got, publisher.got.Author); err != nil {
		t.Fatalf("publisher got unverifiable message: %v", err)
	}
}

func TestHandlerMessagePublishSignRequestRejectsDigestMismatch(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	handler := testMobileHandlerWithPublisher(t, gid, &fakePublisher{}, func() time.Time { return time.UnixMilli(1_234_000) })
	created := doJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, http.MethodPost, "/v1/groups/"+gid.String()+"/messages", map[string]any{
		"author": entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: pub},
		"topics": []string{"chat"},
	}, http.StatusAccepted)
	signingPayload, err := base64.StdEncoding.DecodeString(created.SignRequest.SigningPayload)
	if err != nil {
		t.Fatalf("decode signing payload: %v", err)
	}
	errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/sign-requests/"+created.SignRequest.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(ed25519.Sign(priv, signingPayload)),
		"signing_payload_sha256": base64.StdEncoding.EncodeToString([]byte("wrong")),
	}, http.StatusBadRequest)
	if errResp.Error.Code != "signing_payload_mismatch" {
		t.Fatalf("error code = %q, want signing_payload_mismatch", errResp.Error.Code)
	}
}

func TestHandlerIdempotencyReplaysSignRequestCreation(t *testing.T) {
	gid := testGroupID(1)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	handler := testMobileHandlerWithPublisher(t, gid, &fakePublisher{}, func() time.Time { return time.UnixMilli(1_234_000) })
	body := map[string]any{
		"author": entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: pub},
		"topics": []string{"chat"},
	}
	first := doJSONRequestWithHeaders[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, http.MethodPost, "/v1/groups/"+gid.String()+"/messages", body, map[string]string{idempotencyHeader: "idem-1"}, http.StatusAccepted)
	second := doJSONRequestWithHeaders[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, http.MethodPost, "/v1/groups/"+gid.String()+"/messages", body, map[string]string{idempotencyHeader: "idem-1"}, http.StatusAccepted)
	if first.SignRequest.ID == "" || first.SignRequest.ID != second.SignRequest.ID {
		t.Fatalf("idempotency replay ids = %q/%q", first.SignRequest.ID, second.SignRequest.ID)
	}
	errResp := doJSONRequestWithHeaders[errorEnvelope](t, handler, http.MethodPost, "/v1/groups/"+gid.String()+"/messages", map[string]any{
		"author": entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: pub},
		"topics": []string{"different"},
	}, map[string]string{idempotencyHeader: "idem-1"}, http.StatusConflict)
	if errResp.Error.Code != "idempotency_conflict" {
		t.Fatalf("error code = %q, want idempotency_conflict", errResp.Error.Code)
	}
}

func TestHandlerExecutableOperationSignRequestExecutes(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	op := &fakeOperationExecutor{result: json.RawMessage(`{"status":"updated"}`)}
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, NewMemoryStateStore(), nil)
	handler.(*Handler).operations = op

	created := doSignedJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, priv, http.MethodPatch, "/v1/groups/"+gid.String(), map[string]any{"name": "ops"}, http.StatusAccepted, 1_234_000, "nonce-create")
	req := created.SignRequest
	if req.Kind != signRequestKindGroupUpdate || req.SigningPayload == "" || req.SigningPayloadSHA256 == "" {
		t.Fatalf("sign request = %+v", req)
	}
	signingPayload, err := base64.StdEncoding.DecodeString(req.SigningPayload)
	if err != nil {
		t.Fatalf("Decode signing payload: %v", err)
	}
	sig := ed25519.Sign(priv, signingPayload)
	completed := doSignedJSONRequest[SignRequest](t, handler, priv, http.MethodPost, "/v1/sign-requests/"+req.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(sig),
		"signing_payload_sha256": req.SigningPayloadSHA256,
	}, http.StatusOK, 1_235_000, "nonce-complete")
	if completed.Status != signRequestCompleted || string(completed.OperationResult) != `{"status":"updated"}` {
		t.Fatalf("completed = %+v", completed)
	}
	if op.req.ID != req.ID || !bytes.Equal(op.signature, sig) {
		t.Fatalf("executor got req=%+v sig_len=%d", op.req, len(op.signature))
	}
}

func TestHandlerAdminOperationsRequireAdminAtCreation(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:        "ios-1-device",
		PublicKey: pub,
		Groups:    []entmoot.GroupID{gid},
		ClientIDs: []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, NewMemoryStateStore(), nil)

	updateErr := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPatch, "/v1/groups/"+gid.String(), map[string]any{"name": "ops"}, http.StatusForbidden, 1_234_000, "nonce-update")
	if updateErr.Error.Code != "forbidden" {
		t.Fatalf("group_update code = %q, want forbidden", updateErr.Error.Code)
	}
	inviteErr := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPost, "/v1/groups/"+gid.String()+"/invites", map[string]any{}, http.StatusForbidden, 1_235_000, "nonce-invite")
	if inviteErr.Error.Code != "forbidden" {
		t.Fatalf("invite_create code = %q, want forbidden", inviteErr.Error.Code)
	}
}

func TestHandlerAdminOperationsRequireGroupMembershipAtCreation(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, NewMemoryStateStore(), nil)

	updateErr := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPatch, "/v1/groups/"+gid.String(), map[string]any{"name": "ops"}, http.StatusForbidden, 1_234_000, "nonce-update")
	if updateErr.Error.Code != "forbidden" {
		t.Fatalf("group_update code = %q, want forbidden", updateErr.Error.Code)
	}
	inviteErr := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPost, "/v1/groups/"+gid.String()+"/invites", map[string]any{}, http.StatusForbidden, 1_235_000, "nonce-invite")
	if inviteErr.Error.Code != "forbidden" {
		t.Fatalf("invite_create code = %q, want forbidden", inviteErr.Error.Code)
	}
}

func TestHandlerAdminOperationRechecksAdminAtCompletion(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	op := &fakeOperationExecutor{result: json.RawMessage(`{"status":"updated"}`)}
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, NewMemoryStateStore(), nil)
	handler.(*Handler).operations = op

	created := doSignedJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, priv, http.MethodPatch, "/v1/groups/"+gid.String(), map[string]any{"name": "ops"}, http.StatusAccepted, 1_234_000, "nonce-create")
	signingPayload, err := base64.StdEncoding.DecodeString(created.SignRequest.SigningPayload)
	if err != nil {
		t.Fatalf("Decode signing payload: %v", err)
	}
	withoutAdmin, err := NewDeviceRegistry([]Device{{
		ID:        "ios-1-device",
		PublicKey: pub,
		Groups:    []entmoot.GroupID{gid},
		ClientIDs: []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry without admin: %v", err)
	}
	reg.Replace(withoutAdmin)
	errResp := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPost, "/v1/sign-requests/"+created.SignRequest.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(ed25519.Sign(priv, signingPayload)),
		"signing_payload_sha256": created.SignRequest.SigningPayloadSHA256,
	}, http.StatusForbidden, 1_235_000, "nonce-complete")
	if errResp.Error.Code != "forbidden" {
		t.Fatalf("error code = %q, want forbidden", errResp.Error.Code)
	}
	if op.req.ID != "" {
		t.Fatalf("executor ran without admin rights: %+v", op.req)
	}
}

func TestHandlerAdminOperationRechecksGroupMembershipAtCompletion(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	op := &fakeOperationExecutor{result: json.RawMessage(`{"status":"updated"}`)}
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, NewMemoryStateStore(), nil)
	handler.(*Handler).operations = op

	created := doSignedJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, priv, http.MethodPatch, "/v1/groups/"+gid.String(), map[string]any{"name": "ops"}, http.StatusAccepted, 1_234_000, "nonce-create")
	signingPayload, err := base64.StdEncoding.DecodeString(created.SignRequest.SigningPayload)
	if err != nil {
		t.Fatalf("Decode signing payload: %v", err)
	}
	adminOnly, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry admin only: %v", err)
	}
	reg.Replace(adminOnly)
	errResp := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPost, "/v1/sign-requests/"+created.SignRequest.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(ed25519.Sign(priv, signingPayload)),
		"signing_payload_sha256": created.SignRequest.SigningPayloadSHA256,
	}, http.StatusForbidden, 1_235_000, "nonce-complete")
	if errResp.Error.Code != "forbidden" {
		t.Fatalf("error code = %q, want forbidden", errResp.Error.Code)
	}
	if op.req.ID != "" {
		t.Fatalf("executor ran without group membership: %+v", op.req)
	}
}

func TestHandlerBearerCompletionRechecksStoredDeviceGroupMembership(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	state := NewMemoryStateStore()
	seeded, err := state.CreateSignRequest(context.Background(), SignRequest{
		DeviceID: "ios-1-device",
		Kind:     signRequestKindGroupUpdate,
		GroupID:  gid,
		Payload:  json.RawMessage(`{"name":"ops"}`),
	})
	if err != nil {
		t.Fatalf("CreateSignRequest: %v", err)
	}
	op := &fakeOperationExecutor{result: json.RawMessage(`{"status":"updated"}`)}
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, state, nil)
	handler.(*Handler).authMode = AuthModeDual
	handler.(*Handler).token = "secret"
	handler.(*Handler).operations = op

	signingPayload, err := base64.StdEncoding.DecodeString(seeded.SigningPayload)
	if err != nil {
		t.Fatalf("Decode signing payload: %v", err)
	}
	errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/sign-requests/"+seeded.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(ed25519.Sign(priv, signingPayload)),
		"signing_payload_sha256": seeded.SigningPayloadSHA256,
	}, http.StatusForbidden)
	if errResp.Error.Code != "forbidden" {
		t.Fatalf("error code = %q, want forbidden", errResp.Error.Code)
	}
	if op.req.ID != "" {
		t.Fatalf("executor ran without stored device group membership: %+v", op.req)
	}
}

func TestHandlerExecutableOperationRejectsBearerCreatedRequest(t *testing.T) {
	gid := testGroupID(1)
	op := &fakeOperationExecutor{result: json.RawMessage(`{"status":"updated"}`)}
	state := NewMemoryStateStore()
	seeded, err := state.CreateSignRequest(context.Background(), SignRequest{
		Kind:    signRequestKindGroupUpdate,
		GroupID: gid,
		Payload: json.RawMessage(`{"name":"ops"}`),
	})
	if err != nil {
		t.Fatalf("CreateSignRequest: %v", err)
	}
	handler := testMobileHandlerFull(t, gid, nil, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, state, nil)
	handler.(*Handler).operations = op

	errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/sign-requests/"+seeded.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
		"signing_payload_sha256": seeded.SigningPayloadSHA256,
	}, http.StatusForbidden)
	if errResp.Error.Code != "device_signature_required" {
		t.Fatalf("error code = %q, want device_signature_required", errResp.Error.Code)
	}
	if op.req.ID != "" {
		t.Fatalf("executor ran for bearer-created operation: %+v", op.req)
	}
}

func TestHandlerExecutableOperationRejectsMissingExecutor(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	state := NewMemoryStateStore()
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, state, nil)

	created := doSignedJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, priv, http.MethodPatch, "/v1/groups/"+gid.String(), map[string]any{"name": "ops"}, http.StatusAccepted, 1_234_000, "nonce-create")
	req := created.SignRequest
	signingPayload, err := base64.StdEncoding.DecodeString(req.SigningPayload)
	if err != nil {
		t.Fatalf("Decode signing payload: %v", err)
	}
	errResp := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPost, "/v1/sign-requests/"+req.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(ed25519.Sign(priv, signingPayload)),
		"signing_payload_sha256": req.SigningPayloadSHA256,
	}, http.StatusServiceUnavailable, 1_235_000, "nonce-complete")
	if errResp.Error.Code != "operation_unavailable" {
		t.Fatalf("error code = %q, want operation_unavailable", errResp.Error.Code)
	}
	stored, ok, err := state.GetSignRequest(context.Background(), req.ID)
	if err != nil || !ok {
		t.Fatalf("GetSignRequest ok=%v err=%v", ok, err)
	}
	if stored.Status != signRequestPending {
		t.Fatalf("stored status = %q, want pending", stored.Status)
	}
}

func TestHandlerExecutableOperationRequiresDigestWithoutExecutor(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, NewMemoryStateStore(), nil)

	created := doSignedJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, priv, http.MethodPatch, "/v1/groups/"+gid.String(), map[string]any{"name": "ops"}, http.StatusAccepted, 1_234_000, "nonce-create")
	signingPayload, err := base64.StdEncoding.DecodeString(created.SignRequest.SigningPayload)
	if err != nil {
		t.Fatalf("Decode signing payload: %v", err)
	}
	errResp := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPost, "/v1/sign-requests/"+created.SignRequest.ID+"/complete", map[string]any{
		"signature": base64.StdEncoding.EncodeToString(ed25519.Sign(priv, signingPayload)),
	}, http.StatusBadRequest, 1_235_000, "nonce-complete")
	if errResp.Error.Code != "bad_request" {
		t.Fatalf("error code = %q, want bad_request", errResp.Error.Code)
	}
}

func TestHandlerExecutableOperationRejectsInvalidSignature(t *testing.T) {
	gid := testGroupID(1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	_, wrongPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey wrong: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   pub,
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	op := &fakeOperationExecutor{result: json.RawMessage(`{"status":"updated"}`)}
	handler := testMobileHandlerFull(t, gid, reg, &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}}, func() time.Time {
		return time.UnixMilli(1_234_000)
	}, nil, NewMemoryStateStore(), nil)
	handler.(*Handler).operations = op

	created := doSignedJSONRequest[struct {
		SignRequest SignRequest `json:"sign_request"`
	}](t, handler, priv, http.MethodPatch, "/v1/groups/"+gid.String(), map[string]any{"name": "ops"}, http.StatusAccepted, 1_234_000, "nonce-create")
	signingPayload, err := base64.StdEncoding.DecodeString(created.SignRequest.SigningPayload)
	if err != nil {
		t.Fatalf("Decode signing payload: %v", err)
	}
	errResp := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPost, "/v1/sign-requests/"+created.SignRequest.ID+"/complete", map[string]any{
		"signature":              base64.StdEncoding.EncodeToString(ed25519.Sign(wrongPriv, signingPayload)),
		"signing_payload_sha256": created.SignRequest.SigningPayloadSHA256,
	}, http.StatusBadRequest, 1_235_000, "nonce-complete")
	if errResp.Error.Code != "invalid_signature" {
		t.Fatalf("error code = %q, want invalid_signature", errResp.Error.Code)
	}
	if op.req.ID != "" {
		t.Fatalf("executor ran for invalid signature: %+v", op.req)
	}
}

func TestHandlerDualAuthBearerCannotCreateExecutableOperation(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   priv.Public().(ed25519.PublicKey),
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	st := store.NewMemory()
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	handler, err := NewHandler(Config{
		Token:    "secret",
		AuthMode: AuthModeDual,
		Devices:  reg,
		Service:  svc,
		Groups:   &fakeCatalog{groups: []GroupSummary{{GroupID: gid}}},
		GroupExists: func(_ context.Context, got entmoot.GroupID) (bool, error) {
			return got == gid, nil
		},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/groups", map[string]any{"name": "ops"}, http.StatusForbidden)
	if errResp.Error.Code != "device_signature_required" {
		t.Fatalf("error code = %q, want device_signature_required", errResp.Error.Code)
	}
}

func TestHandlerNotificationTestClearsInvalidToken(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   priv.Public().(ed25519.PublicKey),
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1-device"},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	state := NewMemoryStateStore()
	handler := testMobileHandlerFull(t, gid, reg, nil, func() time.Time { return time.UnixMilli(1_000_000) }, nil, state, fakeNotifier{
		err: &espnotify.DeliveryError{Code: "BadDeviceToken", Message: "bad token", InvalidToken: true},
	})
	doSignedJSONRequest[DeviceState](t, handler, priv, http.MethodPut, "/v1/devices/current/push-token", map[string]any{
		"platform": "apns",
		"token":    "bad-token",
	}, http.StatusOK, 1_000_000, "push-1")
	errResp := doSignedJSONRequest[errorEnvelope](t, handler, priv, http.MethodPost, "/v1/notifications/test", nil, http.StatusBadGateway, 1_000_000, "test-1")
	if errResp.Error.Code != "provider_unavailable" {
		t.Fatalf("error code = %q, want provider_unavailable", errResp.Error.Code)
	}
	got, err := state.GetDeviceState(context.Background(), "ios-1-device")
	if err != nil {
		t.Fatalf("GetDeviceState: %v", err)
	}
	if got.PushToken != "" {
		t.Fatalf("push token = %q, want cleared", got.PushToken)
	}
}

func TestHandlerDeviceStateAndNotifications(t *testing.T) {
	gid := testGroupID(1)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:          "ios-1-device",
		PublicKey:   priv.Public().(ed25519.PublicKey),
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1-device"},
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
		ID:          "ios-1-device",
		PublicKey:   priv.Public().(ed25519.PublicKey),
		Groups:      []entmoot.GroupID{gid},
		AdminGroups: []entmoot.GroupID{gid},
		ClientIDs:   []string{"ios-1"},
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
	return testMobileHandlerFull(t, gid, reg, catalog, clock, nil, NewMemoryStateStore(), nil)
}

func testMobileHandlerWithPublisher(t *testing.T, gid entmoot.GroupID, publisher Publisher, clock func() time.Time) http.Handler {
	t.Helper()
	return testMobileHandlerFull(t, gid, nil, nil, clock, publisher, NewMemoryStateStore(), nil)
}

func testMobileHandlerFull(t *testing.T, gid entmoot.GroupID, reg *DeviceRegistry, catalog GroupCatalog, clock func() time.Time, publisher Publisher, state StateStore, notifier espnotify.Notifier) http.Handler {
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
		Token:     "secret",
		AuthMode:  mode,
		Devices:   reg,
		Service:   svc,
		Publisher: publisher,
		Notifier:  notifier,
		State:     state,
		Groups:    catalog,
		Clock:     clock,
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

type fakeOperationExecutor struct {
	result    json.RawMessage
	err       error
	req       SignRequest
	signature []byte
}

func (e *fakeOperationExecutor) ExecuteSignRequest(_ context.Context, req SignRequest, signature []byte) (json.RawMessage, error) {
	e.req = req
	e.signature = append([]byte(nil), signature...)
	if e.err != nil {
		return nil, e.err
	}
	return append(json.RawMessage(nil), e.result...), nil
}

type fakeNotifier struct {
	result espnotify.DeliveryResult
	err    error
	got    espnotify.WakeupEvent
}

type fakeDiagnostics struct {
	result  any
	gid     entmoot.GroupID
	probe   bool
	timeout time.Duration
}

func (d *fakeDiagnostics) GroupDiagnostics(_ context.Context, gid entmoot.GroupID, probe bool, timeout time.Duration) (any, error) {
	d.gid = gid
	d.probe = probe
	d.timeout = timeout
	return d.result, nil
}

func (n fakeNotifier) SendWakeup(_ context.Context, _ espnotify.DeviceTarget, event espnotify.WakeupEvent) (espnotify.DeliveryResult, error) {
	n.got = event
	if n.err != nil {
		return espnotify.DeliveryResult{}, n.err
	}
	if n.result.Status == "" {
		return espnotify.DeliveryResult{Status: "sent"}, nil
	}
	return n.result, nil
}

func (p *fakePublisher) PublishSigned(_ context.Context, msg entmoot.Message) (PublishResult, error) {
	p.got = msg
	if p.err != nil {
		return PublishResult{}, p.err
	}
	if p.result.Status == "" {
		return PublishResult{
			Status:      "accepted",
			MessageID:   msg.ID,
			GroupID:     msg.GroupID,
			Author:      msg.Author.PilotNodeID,
			TimestampMS: msg.Timestamp,
		}, nil
	}
	return p.result, nil
}

func mustMailboxService(t *testing.T, gid entmoot.GroupID) *mailbox.Service {
	t.Helper()
	st := store.NewMemory()
	if err := st.Put(context.Background(), testMessage(gid, 1, "first")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	return svc
}

func doJSONRequest[T any](t *testing.T, handler http.Handler, method, path string, body any, wantStatus int) T {
	t.Helper()
	return doJSONRequestWithHeaders[T](t, handler, method, path, body, nil, wantStatus)
}

func doJSONRequestWithHeaders[T any](t *testing.T, handler http.Handler, method, path string, body any, headers map[string]string, wantStatus int) T {
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
	for k, v := range headers {
		req.Header.Set(k, v)
	}
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
	return signedDeviceRequestFor(t, "ios-1-device", priv, method, path, body, timestampMillis, nonce)
}

func signedDeviceRequestFor(t *testing.T, deviceID string, priv ed25519.PrivateKey, method, path string, body any, timestampMillis int64, nonce string) *http.Request {
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
	req.Header.Set(deviceIDHeader, deviceID)
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

func doSignedJSONRequestFor[T any](t *testing.T, handler http.Handler, deviceID string, priv ed25519.PrivateKey, method, path string, body any, wantStatus int, timestampMillis int64, nonce string) T {
	t.Helper()
	req := signedDeviceRequestFor(t, deviceID, priv, method, path, body, timestampMillis, nonce)
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

func testGroupIDWithSlash() entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = 0xff
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

type recordingCatalog struct {
	fakeCatalog
	listMembersCalls int
	listMembersGroup entmoot.GroupID
}

func (c *recordingCatalog) ListMembers(ctx context.Context, gid entmoot.GroupID) ([]MemberSummary, error) {
	c.listMembersCalls++
	c.listMembersGroup = gid
	return c.fakeCatalog.ListMembers(ctx, gid)
}
