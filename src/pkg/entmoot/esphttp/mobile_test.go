package esphttp

import (
	"context"
	"encoding/json"
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
		DeviceID: "ios-1",
		Kind:     "message_publish",
		Payload:  json.RawMessage(`{"content":"hello"}`),
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
	if !ok || gotReq.Kind != "message_publish" || string(gotReq.Payload) != `{"content":"hello"}` {
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
