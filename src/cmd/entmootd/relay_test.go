package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRelayServeRequiresAllowlistedPeer(t *testing.T) {
	_, code, ok := parseRelayServeConfig(nil)
	if ok || code != exitInvalidArgument {
		t.Fatalf("relay without allowlist: ok=%t code=%d", ok, code)
	}
}

func TestRelayIdentityCreationIsExplicitAndStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay-identity.json")
	if _, err := loadRelayIdentity(path, false); err == nil {
		t.Fatal("missing relay identity was created without explicit permission")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("identity file exists after refused creation: %v", err)
	}
	first, err := loadRelayIdentity(path, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := loadRelayIdentity(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.PrivateKey) != string(second.PrivateKey) {
		t.Fatal("relay identity changed across reload")
	}
}
