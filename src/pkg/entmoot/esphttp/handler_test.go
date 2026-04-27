package esphttp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
