package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	entpolicy "entmoot/pkg/entmoot/policy"
)

func TestCmdGroupPolicySetStatusAndClearJSON(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	gid := testCmdGroupPolicyID(0x51)

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdGroupPolicy(gf, []string{"set", "-group", gid.String(), "-preset", "standard", "--json"})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("set code/stderr = %d/%q", code, stderr)
	}
	var setOut groupPolicyReport
	if err := json.Unmarshal([]byte(stdout), &setOut); err != nil {
		t.Fatalf("set JSON: %v\n%s", err, stdout)
	}
	if !setOut.PolicyConfigured || setOut.Policy == nil || setOut.EffectiveMode != groupPolicyModeStored {
		t.Fatalf("set report = %+v, want configured stored policy", setOut)
	}
	if setOut.Source != "preset:standard" {
		t.Fatalf("set source = %q, want preset:standard", setOut.Source)
	}

	store, err := entpolicy.OpenFileStore(gf.data)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	got, ok, err := store.Get(withTestContext(t), gid)
	if err != nil || !ok {
		t.Fatalf("stored policy ok/err = %t/%v, want true/nil", ok, err)
	}
	if got != entpolicy.Standard() {
		t.Fatalf("stored policy = %+v, want standard", got)
	}

	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdGroupPolicy(gf, []string{"status", "-group", gid.String(), "--json"})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("status code/stderr = %d/%q", code, stderr)
	}
	var statusOut groupPolicyReport
	if err := json.Unmarshal([]byte(stdout), &statusOut); err != nil {
		t.Fatalf("status JSON: %v\n%s", err, stdout)
	}
	if !statusOut.PolicyConfigured || statusOut.PolicySummary == "" || statusOut.RuntimeAppliedKnown {
		t.Fatalf("status report = %+v, want configured summary with unknown runtime", statusOut)
	}

	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdGroupPolicy(gf, []string{"clear", "-group", gid.String(), "--json"})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("clear code/stderr = %d/%q", code, stderr)
	}
	var clearOut groupPolicyReport
	if err := json.Unmarshal([]byte(stdout), &clearOut); err != nil {
		t.Fatalf("clear JSON: %v\n%s", err, stdout)
	}
	if clearOut.PolicyConfigured || clearOut.Policy != nil || clearOut.EffectiveMode != groupPolicyModeLegacy {
		t.Fatalf("clear report = %+v, want cleared legacy mode", clearOut)
	}
	if _, ok, err := store.Get(withTestContext(t), gid); err != nil || ok {
		t.Fatalf("stored after clear ok/err = %t/%v, want false/nil", ok, err)
	}
}

func TestCmdGroupPolicySetFromFileAndPresetNone(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	gid := testCmdGroupPolicyID(0x52)
	path := filepath.Join(t.TempDir(), "relaxed.json")
	raw, err := json.Marshal(entpolicy.Relaxed())
	if err != nil {
		t.Fatalf("Marshal relaxed: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdGroupPolicy(gf, []string{"set", "-group", gid.String(), "-file", path})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("set file code/stderr = %d/%q", code, stderr)
	}
	if !strings.Contains(stdout, "policy_configured: true") || !strings.Contains(stdout, "source: file:") {
		t.Fatalf("set file stdout = %q", stdout)
	}

	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdGroupPolicy(gf, []string{"set", "-group", gid.String(), "-preset", "none"})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("set none code/stderr = %d/%q", code, stderr)
	}
	if !strings.Contains(stdout, "policy_configured: false") || !strings.Contains(stdout, "source: preset:none") {
		t.Fatalf("set none stdout = %q", stdout)
	}
}

func TestCmdGroupPolicyRejectsBadInput(t *testing.T) {
	gf := &globalFlags{data: t.TempDir()}
	gid := testCmdGroupPolicyID(0x53)
	tests := [][]string{
		{"status"},
		{"set", "-group", gid.String()},
		{"set", "-group", gid.String(), "-preset", "standard", "-file", "policy.json"},
		{"set", "-group", gid.String(), "-preset", "unknown"},
		{"clear", "-group", "bad"},
	}
	for _, args := range tests {
		args := args
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			code, _, stderr := captureCommandOutput(t, func() int {
				return cmdGroupPolicy(gf, args)
			})
			if code != exitInvalidArgument {
				t.Fatalf("cmdGroupPolicy(%v) code = %d, want %d; stderr=%q", args, code, exitInvalidArgument, stderr)
			}
			if stderr == "" {
				t.Fatalf("cmdGroupPolicy(%v) stderr empty", args)
			}
		})
	}
}

func withTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := withBackgroundTimeout()
	t.Cleanup(cancel)
	return ctx
}

func testCmdGroupPolicyID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = seed
	}
	return gid
}
