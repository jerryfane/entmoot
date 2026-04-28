package espnotify

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAPNsNotifierSendsBackgroundWakeup(t *testing.T) {
	keyPath := writeTestAPNsKey(t)
	var gotPath, gotPushType, gotPriority, gotTopic string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotPushType = r.Header.Get("apns-push-type")
		gotPriority = r.Header.Get("apns-priority")
		gotTopic = r.Header.Get("apns-topic")
		if auth := r.Header.Get("authorization"); !strings.HasPrefix(auth, "bearer ") {
			t.Fatalf("authorization header = %q", auth)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("Decode body: %v", err)
		}
		w.Header().Set("apns-id", "apns-1")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	notifier, err := NewAPNsNotifier(APNsConfig{
		TeamID:   "TEAMID",
		KeyID:    "KEYID",
		Topic:    "app.bundle",
		KeyPath:  keyPath,
		Endpoint: srv.URL,
		Now:      func() time.Time { return time.Unix(1_000, 0) },
	})
	if err != nil {
		t.Fatalf("NewAPNsNotifier: %v", err)
	}
	result, err := notifier.SendWakeup(context.Background(), DeviceTarget{Token: "device-token"}, WakeupEvent{
		Type:    EventSignRequest,
		GroupID: "group-1",
		Reason:  "message_publish",
	})
	if err != nil {
		t.Fatalf("SendWakeup: %v", err)
	}
	if result.Status != "sent" || result.ProviderID != "apns-1" {
		t.Fatalf("result = %+v", result)
	}
	if gotPath != "/3/device/device-token" || gotPushType != "background" || gotPriority != "5" || gotTopic != "app.bundle" {
		t.Fatalf("request path/headers = %q %q %q %q", gotPath, gotPushType, gotPriority, gotTopic)
	}
	aps, ok := gotBody["aps"].(map[string]any)
	if !ok || aps["content-available"].(float64) != 1 || gotBody["type"] != string(EventSignRequest) ||
		gotBody["group_id"] != "group-1" || gotBody["reason"] != "message_publish" {
		t.Fatalf("body = %+v", gotBody)
	}
}

func TestAPNsNotifierClassifiesInvalidToken(t *testing.T) {
	keyPath := writeTestAPNsKey(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"reason":"BadDeviceToken"}`))
	}))
	defer srv.Close()
	notifier, err := NewAPNsNotifier(APNsConfig{
		TeamID:   "TEAMID",
		KeyID:    "KEYID",
		Topic:    "app.bundle",
		KeyPath:  keyPath,
		Endpoint: srv.URL,
	})
	if err != nil {
		t.Fatalf("NewAPNsNotifier: %v", err)
	}
	_, err = notifier.SendWakeup(context.Background(), DeviceTarget{Token: "bad"}, WakeupEvent{Type: EventNotificationTest})
	if !IsInvalidToken(err) {
		t.Fatalf("err = %v, want invalid token", err)
	}
}

func writeTestAPNsKey(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	data, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	path := filepath.Join(t.TempDir(), "AuthKey_TEST.p8")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: data}), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}
