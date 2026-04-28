package main

import (
	"os"
	"testing"
)

func TestESPServeConfigRequiresToken(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_TOKEN", "")
	cfg, code, ok := parseESPServeConfig(nil)
	if !ok || code != exitOK {
		t.Fatalf("parseESPServeConfig ok/code = %v/%d, want true/%d", ok, code, exitOK)
	}
	if err := validateESPServeConfig(cfg); err == nil {
		t.Fatal("validateESPServeConfig succeeded without token")
	}
}

func TestESPServeConfigDeviceModeDoesNotRequireToken(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_TOKEN", "")
	cfg, code, ok := parseESPServeConfig([]string{"-auth-mode", "device"})
	if !ok || code != exitOK {
		t.Fatalf("parseESPServeConfig ok/code = %v/%d, want true/%d", ok, code, exitOK)
	}
	if cfg.authMode != "device" {
		t.Fatalf("authMode = %q, want device", cfg.authMode)
	}
	if err := validateESPServeConfig(cfg); err != nil {
		t.Fatalf("validateESPServeConfig: %v", err)
	}
}

func TestESPServeConfigDualModeRequiresToken(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_TOKEN", "")
	cfg, code, ok := parseESPServeConfig([]string{"-auth-mode", "dual"})
	if !ok || code != exitOK {
		t.Fatalf("parseESPServeConfig ok/code = %v/%d, want true/%d", ok, code, exitOK)
	}
	if err := validateESPServeConfig(cfg); err == nil {
		t.Fatal("validateESPServeConfig succeeded for dual mode without token")
	}
}

func TestESPServeConfigUsesEnvToken(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_TOKEN", "env-secret")
	cfg, code, ok := parseESPServeConfig(nil)
	if !ok || code != exitOK {
		t.Fatalf("parseESPServeConfig ok/code = %v/%d, want true/%d", ok, code, exitOK)
	}
	if cfg.token != "env-secret" {
		t.Fatalf("token = %q, want env-secret", cfg.token)
	}
	if err := validateESPServeConfig(cfg); err != nil {
		t.Fatalf("validateESPServeConfig: %v", err)
	}
}

func TestESPServeConfigLoadsAPNsEnv(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_TOKEN", "secret")
	t.Setenv("ENTMOOT_APNS_TEAM_ID", "TEAM")
	t.Setenv("ENTMOOT_APNS_KEY_ID", "KEY")
	t.Setenv("ENTMOOT_APNS_TOPIC", "app.bundle")
	t.Setenv("ENTMOOT_APNS_KEY", "~/AuthKey_TEST.p8")
	t.Setenv("ENTMOOT_APNS_SANDBOX", "true")
	cfg, code, ok := parseESPServeConfig(nil)
	if !ok || code != exitOK {
		t.Fatalf("parseESPServeConfig ok/code = %v/%d, want true/%d", ok, code, exitOK)
	}
	if cfg.apnsTeamID != "TEAM" || cfg.apnsKeyID != "KEY" || cfg.apnsTopic != "app.bundle" ||
		cfg.apnsKeyPath != "~/AuthKey_TEST.p8" || !cfg.apnsSandbox {
		t.Fatalf("APNs cfg = %+v", cfg)
	}
	if err := validateESPServeConfig(cfg); err != nil {
		t.Fatalf("validateESPServeConfig: %v", err)
	}
}

func TestESPServeConfigRejectsPartialAPNs(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_TOKEN", "secret")
	cfg, code, ok := parseESPServeConfig([]string{"-apns-team-id", "TEAM"})
	if !ok || code != exitOK {
		t.Fatalf("parseESPServeConfig ok/code = %v/%d, want true/%d", ok, code, exitOK)
	}
	if err := validateESPServeConfig(cfg); err == nil {
		t.Fatal("validateESPServeConfig succeeded for partial APNs config")
	}
}

func TestESPServeConfigRejectsNonLoopbackByDefault(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_TOKEN", "")
	cfg, code, ok := parseESPServeConfig([]string{"-token", "secret", "-addr", "0.0.0.0:8087"})
	if !ok || code != exitOK {
		t.Fatalf("parseESPServeConfig ok/code = %v/%d, want true/%d", ok, code, exitOK)
	}
	if err := validateESPServeConfig(cfg); err == nil {
		t.Fatal("validateESPServeConfig succeeded for non-loopback addr without override")
	}
	cfg.allowNonLoopback = true
	if err := validateESPServeConfig(cfg); err != nil {
		t.Fatalf("validateESPServeConfig with override: %v", err)
	}
}

func TestAddrIsLoopback(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8087", "[::1]:8087", "localhost:8087"} {
		if !addrIsLoopback(addr) {
			t.Fatalf("addrIsLoopback(%q) = false, want true", addr)
		}
	}
	for _, addr := range []string{":8087", "0.0.0.0:8087", "192.0.2.1:8087", "bad"} {
		if addrIsLoopback(addr) {
			t.Fatalf("addrIsLoopback(%q) = true, want false", addr)
		}
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
