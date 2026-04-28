package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot/esphttp"
)

func TestESPDeviceCLIAddListDisableEnableRemove(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	gid := mailboxTestGroupID(7)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	pubStr := base64.StdEncoding.EncodeToString(pub)

	code, out, stderr := captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{
			"device", "add",
			"-id", "ios-1",
			"-pubkey", pubStr,
			"-group", gid.String(),
		})
	})
	if code != exitOK {
		t.Fatalf("add exit = %d stderr=%s", code, stderr)
	}
	doc := mustDecodeDeviceRegistryOutput(t, out)
	if len(doc.Devices) != 1 || doc.Devices[0].ID != "ios-1" || doc.Devices[0].PublicKey != pubStr ||
		len(doc.Devices[0].ClientIDs) != 1 || doc.Devices[0].ClientIDs[0] != "ios-1" ||
		len(doc.Devices[0].Groups) != 1 || doc.Devices[0].Groups[0] != gid.String() ||
		doc.Devices[0].Disabled {
		t.Fatalf("add output = %+v", doc.Devices)
	}

	code, _, stderr = captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "add", "-id", "ios-1", "-pubkey", pubStr, "-group", gid.String()})
	})
	if code != exitInvalidArgument || !strings.Contains(stderr, "already exists") {
		t.Fatalf("duplicate add exit/stderr = %d/%q", code, stderr)
	}

	code, out, stderr = captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "disable", "-id", "ios-1"})
	})
	if code != exitOK {
		t.Fatalf("disable exit = %d stderr=%s", code, stderr)
	}
	doc = mustDecodeDeviceRegistryOutput(t, out)
	if !doc.Devices[0].Disabled {
		t.Fatalf("disable output = %+v", doc.Devices[0])
	}

	code, out, stderr = captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "enable", "-id", "ios-1"})
	})
	if code != exitOK {
		t.Fatalf("enable exit = %d stderr=%s", code, stderr)
	}
	doc = mustDecodeDeviceRegistryOutput(t, out)
	if doc.Devices[0].Disabled {
		t.Fatalf("enable output = %+v", doc.Devices[0])
	}

	newPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey rotate: %v", err)
	}
	newPubStr := base64.StdEncoding.EncodeToString(newPub)
	code, out, stderr = captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "rotate-key", "-id", "ios-1", "-pubkey", newPubStr})
	})
	if code != exitOK {
		t.Fatalf("rotate-key exit = %d stderr=%s", code, stderr)
	}
	doc = mustDecodeDeviceRegistryOutput(t, out)
	if doc.Devices[0].PublicKey != newPubStr || doc.Devices[0].Groups[0] != gid.String() ||
		doc.Devices[0].ClientIDs[0] != "ios-1" || doc.Devices[0].Disabled {
		t.Fatalf("rotate-key output = %+v", doc.Devices[0])
	}

	code, out, stderr = captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "list"})
	})
	if code != exitOK {
		t.Fatalf("list exit = %d stderr=%s", code, stderr)
	}
	doc = mustDecodeDeviceRegistryOutput(t, out)
	if len(doc.Devices) != 1 || doc.Devices[0].ID != "ios-1" {
		t.Fatalf("list output = %+v", doc.Devices)
	}

	code, out, stderr = captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "remove", "-id", "ios-1"})
	})
	if code != exitOK {
		t.Fatalf("remove exit = %d stderr=%s", code, stderr)
	}
	doc = mustDecodeDeviceRegistryOutput(t, out)
	if len(doc.Devices) != 0 {
		t.Fatalf("remove output = %+v", doc.Devices)
	}
}

func TestESPDeviceCLIRejectsInvalidAdd(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	for name, args := range map[string][]string{
		"missing id":     {"device", "add", "-pubkey", base64.StdEncoding.EncodeToString(pub), "-group", mailboxTestGroupID(1).String()},
		"missing pubkey": {"device", "add", "-id", "ios-1", "-group", mailboxTestGroupID(1).String()},
		"missing group":  {"device", "add", "-id", "ios-1", "-pubkey", base64.StdEncoding.EncodeToString(pub)},
		"bad group":      {"device", "add", "-id", "ios-1", "-pubkey", base64.StdEncoding.EncodeToString(pub), "-group", "bad"},
		"bad pubkey":     {"device", "add", "-id", "ios-1", "-pubkey", "bad", "-group", mailboxTestGroupID(1).String()},
	} {
		t.Run(name, func(t *testing.T) {
			code, _, _ := captureCommandOutput(t, func() int {
				return cmdESP(gf, args)
			})
			if code != exitInvalidArgument {
				t.Fatalf("exit = %d, want %d", code, exitInvalidArgument)
			}
		})
	}
}

func TestESPDeviceCLIUsesExplicitRegistryPath(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	path := filepath.Join(t.TempDir(), "devices.json")
	gid := mailboxTestGroupID(8)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{
			"device", "add",
			"-device-keys", path,
			"-id", "ios-1",
			"-pubkey", base64.StdEncoding.EncodeToString(pub),
			"-group", gid.String(),
			"-client", "phone",
		})
	})
	if code != exitOK {
		t.Fatalf("add explicit path exit = %d stderr=%s", code, stderr)
	}
	code, out, stderr := captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "list", "-device-keys", path})
	})
	if code != exitOK {
		t.Fatalf("list explicit path exit = %d stderr=%s", code, stderr)
	}
	doc := mustDecodeDeviceRegistryOutput(t, out)
	if len(doc.Devices) != 1 || len(doc.Devices[0].ClientIDs) != 1 || doc.Devices[0].ClientIDs[0] != "phone" {
		t.Fatalf("explicit registry output = %+v", doc.Devices)
	}
}

func TestESPDeviceCLIOnboardGeneratesKeyAndStoresPublicOnly(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	gid := mailboxTestGroupID(9)

	code, out, stderr := captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{
			"device", "onboard",
			"-id", "ios-1-device",
			"-group", gid.String(),
			"-client", "ios-1",
		})
	})
	if code != exitOK {
		t.Fatalf("onboard exit = %d stderr=%s", code, stderr)
	}
	var onboarding espDeviceOnboardOutput
	if err := json.Unmarshal([]byte(out), &onboarding); err != nil {
		t.Fatalf("Unmarshal onboard output %q: %v", out, err)
	}
	privBytes, err := base64.StdEncoding.DecodeString(onboarding.PrivateKey)
	if err != nil {
		t.Fatalf("Decode private key: %v", err)
	}
	if len(privBytes) != ed25519.PrivateKeySize {
		t.Fatalf("private key len = %d, want %d", len(privBytes), ed25519.PrivateKeySize)
	}
	pubFromPrivate := ed25519.PrivateKey(privBytes).Public().(ed25519.PublicKey)
	if onboarding.Device.PublicKey != base64.StdEncoding.EncodeToString(pubFromPrivate) {
		t.Fatalf("public key does not match private key")
	}
	if onboarding.Device.ID != "ios-1-device" || len(onboarding.Device.ClientIDs) != 1 ||
		onboarding.Device.ClientIDs[0] != "ios-1" || len(onboarding.Device.Groups) != 1 ||
		onboarding.Device.Groups[0] != gid.String() || onboarding.Device.Disabled {
		t.Fatalf("onboard device = %+v", onboarding.Device)
	}

	registryPath := filepath.Join(gf.data, "esp-devices.json")
	info, err := os.Stat(registryPath)
	if err != nil {
		t.Fatalf("Stat registry: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("registry mode = %v, want 0600", got)
	}
	raw, err := os.ReadFile(registryPath)
	if err != nil {
		t.Fatalf("ReadFile registry: %v", err)
	}
	if strings.Contains(string(raw), onboarding.PrivateKey) {
		t.Fatal("registry contains private key")
	}
	doc := mustDecodeDeviceRegistryFile(t, raw)
	if len(doc.Devices) != 1 || doc.Devices[0].PublicKey != onboarding.Device.PublicKey {
		t.Fatalf("registry doc = %+v", doc.Devices)
	}

	code, _, stderr = captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "onboard", "-id", "ios-1-device", "-group", gid.String()})
	})
	if code != exitInvalidArgument || !strings.Contains(stderr, "already exists") {
		t.Fatalf("duplicate onboard exit/stderr = %d/%q", code, stderr)
	}
}

func TestESPDeviceCLIOnboardDefaultsClientToDeviceID(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	gid := mailboxTestGroupID(10)
	code, out, stderr := captureCommandOutput(t, func() int {
		return cmdESP(gf, []string{"device", "onboard", "-id", "ios-1-device", "-group", gid.String()})
	})
	if code != exitOK {
		t.Fatalf("onboard exit = %d stderr=%s", code, stderr)
	}
	var onboarding espDeviceOnboardOutput
	if err := json.Unmarshal([]byte(out), &onboarding); err != nil {
		t.Fatalf("Unmarshal onboard output %q: %v", out, err)
	}
	if len(onboarding.Device.ClientIDs) != 1 || onboarding.Device.ClientIDs[0] != "ios-1-device" {
		t.Fatalf("client ids = %+v, want default device id", onboarding.Device.ClientIDs)
	}
}

func TestESPDeviceCLIRejectsInvalidOnboard(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	for name, args := range map[string][]string{
		"missing id":    {"device", "onboard", "-group", mailboxTestGroupID(1).String()},
		"missing group": {"device", "onboard", "-id", "ios-1"},
		"bad group":     {"device", "onboard", "-id", "ios-1", "-group", "bad"},
	} {
		t.Run(name, func(t *testing.T) {
			code, _, _ := captureCommandOutput(t, func() int {
				return cmdESP(gf, args)
			})
			if code != exitInvalidArgument {
				t.Fatalf("exit = %d, want %d", code, exitInvalidArgument)
			}
		})
	}
}

func mustDecodeDeviceRegistryOutput(t *testing.T, out string) esphttp.DeviceRegistryDocument {
	t.Helper()
	var doc esphttp.DeviceRegistryDocument
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("Unmarshal output %q: %v", out, err)
	}
	return doc
}

func mustDecodeDeviceRegistryFile(t *testing.T, data []byte) esphttp.DeviceRegistryDocument {
	t.Helper()
	var doc esphttp.DeviceRegistryDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("Unmarshal registry file: %v\n%s", err, data)
	}
	return doc
}
