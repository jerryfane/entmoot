package main

import (
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot/keystore"
)

// TestDoctorRedactFlagOmitsLocalPaths pins the wiring, not the helper. The
// redaction function, its unit test and the documented -redact flag all
// existed while nothing called it, so `doctor` printed the data directory to
// operators who had been told it would not.
func TestDoctorRedactFlagOmitsLocalPaths(t *testing.T) {
	dir := t.TempDir()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(dir, "identity.json")
	if err := identity.Save(identityPath); err != nil {
		t.Fatal(err)
	}
	gf := &globalFlags{data: dir, identity: identityPath}

	code, plain, stderr := captureCommandOutput(t, func() int {
		return cmdDoctor(gf, []string{"-json"})
	})
	if code != exitOK {
		t.Fatalf("doctor -json exit %d, stderr %q", code, stderr)
	}
	if !strings.Contains(plain, dir) {
		t.Fatalf("doctor -json did not report its data dir, so this test cannot detect redaction: %s", plain)
	}

	code, redacted, stderr := captureCommandOutput(t, func() int {
		return cmdDoctor(gf, []string{"-json", "-redact"})
	})
	if code != exitOK {
		t.Fatalf("doctor -json -redact exit %d, stderr %q", code, stderr)
	}
	if strings.Contains(redacted, dir) {
		t.Fatalf("doctor -redact leaked the data dir: %s", redacted)
	}
	if strings.Contains(redacted, `"runtime"`) {
		t.Fatalf("doctor -redact leaked the runtime report: %s", redacted)
	}
}
