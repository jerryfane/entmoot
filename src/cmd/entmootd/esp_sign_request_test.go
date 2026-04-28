package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot/esphttp"
)

func TestESPSignRequestDeterministicSignature(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "device.key")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(priv)+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile key: %v", err)
	}
	body := []byte(`{"hello":"world"}`)
	bodyPath := filepath.Join(dir, "body.json")
	if err := os.WriteFile(bodyPath, body, 0o600); err != nil {
		t.Fatalf("WriteFile body: %v", err)
	}

	code, out, stderr := captureCommandOutput(t, func() int {
		return cmdESP(&globalFlags{}, []string{
			"sign-request",
			"-device", "ios-1-device",
			"-private-key-file", keyPath,
			"-method", "post",
			"-path", "/v1/messages",
			"-body", bodyPath,
			"-timestamp-ms", "1000000",
			"-nonce", "nonce-1",
			"-show-input",
		})
	})
	if code != exitOK {
		t.Fatalf("sign-request exit = %d stderr=%s", code, stderr)
	}
	var signed espSignRequestOutput
	if err := json.Unmarshal([]byte(out), &signed); err != nil {
		t.Fatalf("Unmarshal sign output %q: %v", out, err)
	}
	if signed.Method != http.MethodPost || signed.Path != "/v1/messages" {
		t.Fatalf("method/path = %s %s", signed.Method, signed.Path)
	}
	sum := sha256.Sum256(body)
	if signed.BodySHA256 != base64.StdEncoding.EncodeToString(sum[:]) {
		t.Fatalf("body hash = %q", signed.BodySHA256)
	}
	input := esphttp.DeviceSigningInput(http.MethodPost, "/v1/messages", 1_000_000, "nonce-1", body)
	if signed.SigningInput != input {
		t.Fatalf("signing input mismatch")
	}
	sig, err := base64.StdEncoding.DecodeString(signed.Headers["X-Entmoot-Signature"])
	if err != nil {
		t.Fatalf("Decode signature: %v", err)
	}
	if !ed25519.Verify(pub, []byte(input), sig) {
		t.Fatal("signature did not verify")
	}
	if signed.Headers["X-Entmoot-Device-ID"] != "ios-1-device" ||
		signed.Headers["X-Entmoot-Timestamp-Ms"] != "1000000" ||
		signed.Headers["X-Entmoot-Nonce"] != "nonce-1" {
		t.Fatalf("headers = %+v", signed.Headers)
	}
}

func TestESPSignRequestGeneratesNonceAndSignsEmptyBody(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	keyPath := filepath.Join(t.TempDir(), "device.key")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(priv)), 0o600); err != nil {
		t.Fatalf("WriteFile key: %v", err)
	}
	code, out, stderr := captureCommandOutput(t, func() int {
		return cmdESP(&globalFlags{}, []string{
			"sign-request",
			"-device", "ios-1-device",
			"-private-key-file", keyPath,
			"-method", "GET",
			"-path", "/v1/mailbox/pull?client_id=ios-1",
			"-timestamp-ms", "1000000",
		})
	})
	if code != exitOK {
		t.Fatalf("sign-request exit = %d stderr=%s", code, stderr)
	}
	var signed espSignRequestOutput
	if err := json.Unmarshal([]byte(out), &signed); err != nil {
		t.Fatalf("Unmarshal sign output %q: %v", out, err)
	}
	nonce := signed.Headers["X-Entmoot-Nonce"]
	if nonce == "" {
		t.Fatal("nonce was not generated")
	}
	input := esphttp.DeviceSigningInput(http.MethodGet, signed.Path, 1_000_000, nonce, nil)
	sig, err := base64.StdEncoding.DecodeString(signed.Headers["X-Entmoot-Signature"])
	if err != nil {
		t.Fatalf("Decode signature: %v", err)
	}
	if !ed25519.Verify(pub, []byte(input), sig) {
		t.Fatal("signature did not verify")
	}
}

func TestESPSignRequestRejectsInvalidInputs(t *testing.T) {
	dir := t.TempDir()
	shortKeyPath := filepath.Join(dir, "short.key")
	if err := os.WriteFile(shortKeyPath, []byte(base64.StdEncoding.EncodeToString([]byte("short"))), 0o600); err != nil {
		t.Fatalf("WriteFile short key: %v", err)
	}
	for name, args := range map[string][]string{
		"missing device":       {"sign-request", "-private-key-file", shortKeyPath, "-method", "GET", "-path", "/v1/mailbox/pull"},
		"missing private file": {"sign-request", "-device", "ios-1", "-method", "GET", "-path", "/v1/mailbox/pull"},
		"missing method":       {"sign-request", "-device", "ios-1", "-private-key-file", shortKeyPath, "-path", "/v1/mailbox/pull"},
		"missing path":         {"sign-request", "-device", "ios-1", "-private-key-file", shortKeyPath, "-method", "GET"},
		"bad private key":      {"sign-request", "-device", "ios-1", "-private-key-file", shortKeyPath, "-method", "GET", "-path", "/v1/mailbox/pull"},
		"bad timestamp":        {"sign-request", "-device", "ios-1", "-private-key-file", shortKeyPath, "-method", "GET", "-path", "/v1/mailbox/pull", "-timestamp-ms", "-1"},
		"missing body":         {"sign-request", "-device", "ios-1", "-private-key-file", shortKeyPath, "-method", "GET", "-path", "/v1/mailbox/pull", "-body", filepath.Join(dir, "missing.json")},
	} {
		t.Run(name, func(t *testing.T) {
			code, _, stderr := captureCommandOutput(t, func() int {
				return cmdESP(&globalFlags{}, args)
			})
			if code != exitInvalidArgument {
				t.Fatalf("exit = %d, want %d; stderr=%q", code, exitInvalidArgument, stderr)
			}
			if strings.TrimSpace(stderr) == "" {
				t.Fatal("stderr was empty")
			}
		})
	}
}
