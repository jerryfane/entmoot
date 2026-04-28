package esphttp

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestDeviceRegistrySaveLoadRoundTrip(t *testing.T) {
	gid := testGroupID(3)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := NewDeviceRegistry([]Device{{
		ID:        "ios-1",
		PublicKey: pub,
		Groups:    []entmoot.GroupID{gid},
		ClientIDs: []string{"ios-1-client"},
		Disabled:  true,
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	path := filepath.Join(t.TempDir(), "nested", "esp-devices.json")
	if err := SaveDeviceRegistry(path, reg); err != nil {
		t.Fatalf("SaveDeviceRegistry: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("registry mode = %v, want 0600", got)
	}
	loaded, err := LoadDeviceRegistry(path)
	if err != nil {
		t.Fatalf("LoadDeviceRegistry: %v", err)
	}
	d, ok := loaded.lookup("ios-1")
	if !ok {
		t.Fatal("loaded device missing")
	}
	if d.ID != "ios-1" || !d.Disabled || len(d.Groups) != 1 || d.Groups[0] != gid ||
		len(d.ClientIDs) != 1 || d.ClientIDs[0] != "ios-1-client" ||
		base64.StdEncoding.EncodeToString(d.PublicKey) != base64.StdEncoding.EncodeToString(pub) {
		t.Fatalf("loaded device = %+v", d)
	}
}

func TestLoadDeviceRegistryOrEmpty(t *testing.T) {
	reg, err := LoadDeviceRegistryOrEmpty(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("LoadDeviceRegistryOrEmpty: %v", err)
	}
	if len(reg.Devices) != 0 {
		t.Fatalf("len(reg.Devices) = %d, want 0", len(reg.Devices))
	}
}

func TestDeviceFromRecordRejectsInvalidFields(t *testing.T) {
	gid := testGroupID(4)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	valid := DeviceRecord{
		ID:        "ios-1",
		PublicKey: base64.StdEncoding.EncodeToString(pub),
		Groups:    []string{gid.String()},
		ClientIDs: []string{"ios-1"},
	}
	for name, mutate := range map[string]func(*DeviceRecord){
		"empty id":        func(r *DeviceRecord) { r.ID = " " },
		"invalid pubkey":  func(r *DeviceRecord) { r.PublicKey = base64.StdEncoding.EncodeToString([]byte("short")) },
		"invalid group":   func(r *DeviceRecord) { r.Groups = []string{base64.StdEncoding.EncodeToString([]byte("short"))} },
		"empty client id": func(r *DeviceRecord) { r.ClientIDs = []string{" "} },
	} {
		t.Run(name, func(t *testing.T) {
			rec := valid
			rec.Groups = append([]string(nil), valid.Groups...)
			rec.ClientIDs = append([]string(nil), valid.ClientIDs...)
			mutate(&rec)
			if _, err := DeviceFromRecord(rec); err == nil {
				t.Fatal("DeviceFromRecord succeeded")
			}
		})
	}
}
