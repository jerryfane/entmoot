package esphttp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"path/filepath"
	"testing"
)

func TestSQLiteStateStorePersistsMobileState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	req, err := store.CreateSignRequest(ctx, SignRequest{
		DeviceID:             "ios-1",
		Kind:                 "message_publish",
		Payload:              json.RawMessage(`{"content":"hello"}`),
		CanonicalType:        canonicalTypeMessageV1,
		SignatureAlgorithm:   signatureAlgorithmEd25519,
		SigningPayload:       "cGF5bG9hZA==",
		SigningPayloadSHA256: "ZGlnZXN0",
	})
	if err != nil {
		t.Fatalf("CreateSignRequest: %v", err)
	}
	if _, err := store.UpsertPushToken(ctx, "ios-1", "apns", "token-1"); err != nil {
		t.Fatalf("UpsertPushToken: %v", err)
	}
	if _, err := store.PatchNotificationPreferences(ctx, "ios-1", NotificationPreferences{
		Enabled: true,
		Topics:  []string{"ops/#"},
	}); err != nil {
		t.Fatalf("PatchNotificationPreferences: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	store, err = OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore reopen: %v", err)
	}
	defer store.Close()
	gotReq, ok, err := store.GetSignRequest(ctx, req.ID)
	if err != nil {
		t.Fatalf("GetSignRequest: %v", err)
	}
	if !ok || gotReq.Kind != "message_publish" || string(gotReq.Payload) != `{"content":"hello"}` ||
		gotReq.CanonicalType != canonicalTypeMessageV1 || gotReq.SigningPayloadSHA256 != "ZGlnZXN0" {
		t.Fatalf("sign request after reopen = %+v ok=%v", gotReq, ok)
	}
	state, err := store.GetDeviceState(ctx, "ios-1")
	if err != nil {
		t.Fatalf("GetDeviceState: %v", err)
	}
	if state.PushToken != "token-1" || len(state.NotificationPreferences.Topics) != 1 ||
		state.NotificationPreferences.Topics[0] != "ops/#" {
		t.Fatalf("device state after reopen = %+v", state)
	}
}

func TestSQLiteStateStoreMigratesSignRequests(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "esp.sqlite")+"?"+q.Encode())
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	_, err = db.Exec(`
CREATE TABLE sign_requests (
  id TEXT PRIMARY KEY,
  device_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  group_id BLOB,
  payload BLOB NOT NULL,
  signature TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE esp_devices_state (
  device_id TEXT PRIMARY KEY,
  push_platform TEXT NOT NULL DEFAULT '',
  push_token TEXT NOT NULL DEFAULT '',
  prefs BLOB NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
`)
	if err != nil {
		t.Fatalf("create old schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close old db: %v", err)
	}

	store, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()
	req, err := store.CreateSignRequest(ctx, SignRequest{
		DeviceID: "ios-1",
		Kind:     "group_create",
		Payload:  json.RawMessage(`{"name":"ops"}`),
	})
	if err != nil {
		t.Fatalf("CreateSignRequest after migration: %v", err)
	}
	if req.CanonicalType != canonicalTypeESPOperationV1 || req.SignatureAlgorithm != signatureAlgorithmEd25519 ||
		req.SigningPayload == "" || req.SigningPayloadSHA256 == "" {
		t.Fatalf("migrated sign request metadata = %+v", req)
	}
}
