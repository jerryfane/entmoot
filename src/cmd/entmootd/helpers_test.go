package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot/keystore"
)

func TestSetupRefusesEmptyPaths(t *testing.T) {
	if _, err := setup(&globalFlags{identity: "identity.json"}); err == nil ||
		err.Error() != "entmootd: data root is empty" {
		t.Fatalf("empty data root error = %v", err)
	}
	if _, err := setup(&globalFlags{data: t.TempDir()}); err == nil ||
		err.Error() != "entmootd: identity path is empty" {
		t.Fatalf("empty identity path error = %v", err)
	}
}

func TestSetupRefusesMissingDataRootWithoutCreatingIt(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	identity := filepath.Join(t.TempDir(), "identity.json")

	_, err := setup(&globalFlags{data: root, identity: identity})
	if err == nil || !strings.Contains(err.Error(), `entmootd: data root "`+root+`" does not exist`) {
		t.Fatalf("setup error = %v, want missing data root", err)
	}
	if _, statErr := os.Stat(root); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("data root stat error = %v, want not exist", statErr)
	}
	if _, statErr := os.Stat(identity); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("identity stat error = %v, want not exist", statErr)
	}
}

func TestSetupRequiresExplicitIdentityCreation(t *testing.T) {
	root := t.TempDir()
	identity := filepath.Join(t.TempDir(), "keys", "identity.json")

	_, err := setup(&globalFlags{data: root, identity: identity})
	if err == nil || !strings.Contains(err.Error(), "pass -allow-new-identity to create it") {
		t.Fatalf("setup error = %v, want explicit identity creation error", err)
	}
	if _, statErr := os.Stat(identity); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("identity stat error = %v, want not exist", statErr)
	}

	result, err := setup(&globalFlags{
		data:             root,
		identity:         identity,
		allowNewIdentity: true,
	})
	if err != nil {
		t.Fatalf("setup with explicit creation: %v", err)
	}
	if result.dataDir != root {
		t.Fatalf("dataDir = %q, want %q", result.dataDir, root)
	}
	if result.identity == nil {
		t.Fatal("setup identity is nil")
	}
	createdPublicKey := append([]byte(nil), result.identity.PublicKey...)

	reloaded, err := setup(&globalFlags{data: root, identity: identity})
	if err != nil {
		t.Fatalf("setup with existing identity: %v", err)
	}
	if string(reloaded.identity.PublicKey) != string(createdPublicKey) {
		t.Fatal("setup replaced the existing identity")
	}
	loaded, err := keystore.Load(identity)
	if err != nil {
		t.Fatalf("load created identity: %v", err)
	}
	if string(loaded.PublicKey) != string(createdPublicKey) {
		t.Fatal("identity on disk differs from setup result")
	}
}

func TestSetupResolvesIndependentAbsolutePaths(t *testing.T) {
	workingDir := t.TempDir()
	oldWorkingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(workingDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWorkingDir) })

	if err := os.Mkdir("data", 0o700); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	result, err := setup(&globalFlags{
		data:             "data",
		identity:         filepath.Join("keys", "identity.json"),
		allowNewIdentity: true,
	})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	wantData := filepath.Join(workingDir, "data")
	if result.dataDir != wantData {
		t.Fatalf("dataDir = %q, want %q", result.dataDir, wantData)
	}
	if _, err := keystore.Load(filepath.Join(workingDir, "keys", "identity.json")); err != nil {
		t.Fatalf("identity outside data root: %v", err)
	}
}
