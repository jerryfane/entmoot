package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

func TestESPDeviceSignedPublishThroughControlSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir, err := os.MkdirTemp("/tmp", "entmoot-esp-int-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dataDir)
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = byte(i + 1)
	}

	authorID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate author identity: %v", err)
	}
	author := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), authorID.PublicKey...)}
	rost := roster.New(gid)
	if err := rost.Genesis(authorID, author, 1_000); err != nil {
		t.Fatalf("roster.Genesis: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	transports := gossip.NewMemTransports([]entmoot.NodeID{author.PilotNodeID})
	g, err := gossip.New(gossip.Config{
		LocalNode: author.PilotNodeID,
		Identity:  authorID,
		Roster:    rost,
		Store:     st,
		Transport: transports[author.PilotNodeID],
		GroupID:   gid,
		Logger:    slog.Default(),
	})
	if err != nil {
		t.Fatalf("gossip.New: %v", err)
	}

	sockPath := filepath.Join(dataDir, "control.sock")
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("Listen unix: %v", err)
	}
	defer l.Close()
	srv := &ipcServer{
		nodeID:   author.PilotNodeID,
		identity: authorID,
		dataDir:  dataDir,
		gossiper: g,
		roster:   rost,
		groupID:  gid,
		store:    st,
	}
	go srv.acceptLoop(ctx, l)

	cursorStore, err := mailbox.OpenSQLiteCursorStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteCursorStore: %v", err)
	}
	defer cursorStore.Close()
	mbox, err := mailbox.NewWithCursorStore(st, cursorStore, nil)
	if err != nil {
		t.Fatalf("mailbox.NewWithCursorStore: %v", err)
	}

	code, out, stderr := captureCommandOutput(t, func() int {
		return cmdESP(&globalFlags{data: dataDir}, []string{
			"device", "onboard",
			"-id", "ios-1-device",
			"-group", gid.String(),
			"-client", "ios-1",
		})
	})
	if code != exitOK {
		t.Fatalf("esp device onboard exit = %d stderr=%s", code, stderr)
	}
	var onboarding espDeviceOnboardOutput
	if err := json.Unmarshal([]byte(out), &onboarding); err != nil {
		t.Fatalf("Unmarshal onboard output %q: %v", out, err)
	}
	privBytes, err := base64.StdEncoding.DecodeString(onboarding.PrivateKey)
	if err != nil {
		t.Fatalf("Decode onboard private key: %v", err)
	}
	devicePriv := ed25519.PrivateKey(privBytes)
	deviceKeyPath := filepath.Join(dataDir, "ios-1-device.key")
	if err := os.WriteFile(deviceKeyPath, []byte(onboarding.PrivateKey+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile device key: %v", err)
	}
	regPath := filepath.Join(dataDir, "esp-devices.json")
	info, err := os.Stat(regPath)
	if err != nil {
		t.Fatalf("Stat device registry: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("device registry mode = %v, want 0600", got)
	}
	reg, err := esphttp.LoadDeviceRegistry(regPath)
	if err != nil {
		t.Fatalf("LoadDeviceRegistry: %v", err)
	}
	now := time.UnixMilli(1_000_000)
	handler, err := esphttp.NewHandler(esphttp.Config{
		AuthMode: esphttp.AuthModeDevice,
		Devices:  reg,
		Service:  mbox,
		Publisher: controlSocketSignedPublisher{
			socketPath: sockPath,
			timeout:    5 * time.Second,
		},
		GroupExists: func(context.Context, entmoot.GroupID) (bool, error) { return true, nil },
		Clock:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	httpSrv := httptest.NewServer(handler)
	defer httpSrv.Close()

	msgSigner, err := signing.NewLocalSigner(author, authorID)
	if err != nil {
		t.Fatalf("NewLocalSigner: %v", err)
	}
	msg, err := signing.SignMessage(ctx, msgSigner, entmoot.Message{
		GroupID:   gid,
		Timestamp: 1_001,
		Topics:    []string{"esp/integration"},
		Content:   []byte("phone signed integration publish"),
	})
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	body := map[string]entmoot.Message{"message": msg}
	resp := doESPDeviceRequest(t, httpSrv.URL, devicePriv, http.MethodPost, "/v1/messages", body, 1_000_000, "publish-1")
	if resp.StatusCode != http.StatusAccepted {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("POST /v1/messages status = %d, want 202 body=%s", resp.StatusCode, data)
	}
	resp.Body.Close()

	got, err := st.Get(ctx, gid, msg.ID)
	if err != nil {
		t.Fatalf("store.Get published message: %v", err)
	}
	if string(got.Content) != string(msg.Content) {
		t.Fatalf("stored content = %q, want %q", got.Content, msg.Content)
	}

	pullPath := "/v1/mailbox/pull?client_id=ios-1&group_id=" + gid.String()
	headers := signedESPHeadersFromCLI(t, deviceKeyPath, http.MethodGet, pullPath, "", 1_000_000, "pull-1")
	resp = doESPRequestWithHeaders(t, httpSrv.URL, http.MethodGet, pullPath, nil, headers)
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("GET mailbox pull status = %d, want 200 body=%s", resp.StatusCode, data)
	}
	var pull mailbox.PullResult
	if err := json.NewDecoder(resp.Body).Decode(&pull); err != nil {
		resp.Body.Close()
		t.Fatalf("Decode pull: %v", err)
	}
	resp.Body.Close()
	if pull.Count != 1 || len(pull.Messages) != 1 || pull.Messages[0].MessageID != msg.ID {
		t.Fatalf("pull = %+v, want published message", pull)
	}

	resp = doESPDeviceRequest(t, httpSrv.URL, devicePriv, http.MethodGet, "/v1/mailbox/pull?client_id=other&group_id="+gid.String(), nil, 1_000_000, "bad-client-1")
	if resp.StatusCode != http.StatusForbidden {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("unauthorized client status = %d, want 403 body=%s", resp.StatusCode, data)
	}
	resp.Body.Close()

	replay := doESPDeviceRequest(t, httpSrv.URL, devicePriv, http.MethodGet, "/v1/mailbox/pull?client_id=ios-1&group_id="+gid.String(), nil, 1_000_000, "pull-1")
	if replay.StatusCode != http.StatusUnauthorized {
		data, _ := io.ReadAll(replay.Body)
		replay.Body.Close()
		t.Fatalf("replay status = %d, want 401 body=%s", replay.StatusCode, data)
	}
	replay.Body.Close()
}

func signedESPHeadersFromCLI(t *testing.T, keyPath, method, path, bodyPath string, timestampMillis int64, nonce string) map[string]string {
	t.Helper()
	args := []string{
		"sign-request",
		"-device", "ios-1-device",
		"-private-key-file", keyPath,
		"-method", method,
		"-path", path,
		"-timestamp-ms", strconv.FormatInt(timestampMillis, 10),
		"-nonce", nonce,
	}
	if bodyPath != "" {
		args = append(args, "-body", bodyPath)
	}
	code, out, stderr := captureCommandOutput(t, func() int {
		return cmdESP(&globalFlags{}, args)
	})
	if code != exitOK {
		t.Fatalf("esp sign-request exit = %d stderr=%s", code, stderr)
	}
	var signed espSignRequestOutput
	if err := json.Unmarshal([]byte(out), &signed); err != nil {
		t.Fatalf("Unmarshal sign-request output %q: %v", out, err)
	}
	return signed.Headers
}

func doESPRequestWithHeaders(t *testing.T, baseURL string, method, path string, body any, headers map[string]string) *http.Response {
	t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("Marshal body: %v", err)
		}
	}
	req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	return resp
}

func doESPDeviceRequest(t *testing.T, baseURL string, priv ed25519.PrivateKey, method, path string, body any, timestampMillis int64, nonce string) *http.Response {
	t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("Marshal body: %v", err)
		}
	}
	req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	input := esphttp.DeviceSigningInput(method, req.URL.RequestURI(), timestampMillis, nonce, data)
	sig := ed25519.Sign(priv, []byte(input))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Entmoot-Device-ID", "ios-1-device")
	req.Header.Set("X-Entmoot-Timestamp-Ms", strconv.FormatInt(timestampMillis, 10))
	req.Header.Set("X-Entmoot-Nonce", nonce)
	req.Header.Set("X-Entmoot-Signature", base64.StdEncoding.EncodeToString(sig))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	return resp
}
