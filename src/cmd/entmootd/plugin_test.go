package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"entmoot/internal/pluginpack"
	"entmoot/internal/subprocess"
)

func TestCmdPluginBuildCreatesDefaultPackage(t *testing.T) {
	home := t.TempDir()
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"build", "codex"})
	})
	if code != exitOK {
		t.Fatalf("cmdPlugin build exit = %d, stderr=%q", code, stderr)
	}
	want := pluginpack.DefaultPackageDir(home, pluginpack.ProviderCodex)
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
	assertFileContains(t, filepath.Join(want, ".codex-plugin", "plugin.json"), `"name": "entmoot"`)
	assertFileContains(t, filepath.Join(want, "skills", "entmoot", "SKILL.md"), "Entmoot")
}

func TestCmdPluginBuildHelpReturnsOK(t *testing.T) {
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: t.TempDir()}, []string{"build", "-h"})
	})
	if code != exitOK {
		t.Fatalf("cmdPlugin build -h exit = %d, want %d", code, exitOK)
	}
	if !strings.Contains(stderr, "Usage: entmootd plugin build") {
		t.Fatalf("stderr = %q, want build usage", stderr)
	}
}

func TestCmdPluginPathUsesHome(t *testing.T) {
	home := t.TempDir()
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: "/unused"}, []string{"path", "claude", "--home", home})
	})
	if code != exitOK {
		t.Fatalf("cmdPlugin path exit = %d, stderr=%q", code, stderr)
	}
	want := pluginpack.DefaultPackageDir(home, pluginpack.ProviderClaude)
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

func TestCmdPluginInstallCodexCommandOrder(t *testing.T) {
	home := t.TempDir()
	runner := &pluginCLIFakeRunner{paths: map[string]string{"codex": "/bin/codex"}}
	restore := replacePluginInstallRunner(runner)
	defer restore()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"install", "codex"})
	})
	if code != exitOK {
		t.Fatalf("cmdPlugin install exit = %d, stdout=%q stderr=%q", code, stdout, stderr)
	}
	wantCalls := []pluginCLIFakeCall{
		{command: "codex", args: []string{"plugin", "marketplace", "add", pluginpack.DefaultMarketplaceDir(home, pluginpack.ProviderCodex)}},
		{command: "codex", args: []string{"plugin", "add", "entmoot@entmoot-local"}},
	}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("calls = %+v, want %+v", runner.calls, wantCalls)
	}
	if !strings.Contains(stdout, "installed codex plugin") {
		t.Fatalf("stdout = %q, want install confirmation", stdout)
	}
}

func TestCmdPluginInstallMissingRuntimePrintsManualCommands(t *testing.T) {
	home := t.TempDir()
	restore := replacePluginInstallRunner(&pluginCLIFakeRunner{})
	defer restore()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"install", "claude"})
	})
	if code != exitOK {
		t.Fatalf("cmdPlugin install exit = %d, stderr=%q", code, stderr)
	}
	for _, want := range []string{
		"claude CLI was not found",
		"manual install commands:",
		"claude plugin validate",
		"claude plugin install entmoot@entmoot-local --scope user",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout = %q, want %q", stdout, want)
		}
	}
}

func TestCmdPluginInstallClaudeRejectsUnknownScope(t *testing.T) {
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: t.TempDir()}, []string{"install", "claude", "--scope", "team"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("cmdPlugin install exit = %d, want %d", code, exitInvalidArgument)
	}
	if !strings.Contains(stderr, "unknown claude plugin scope") {
		t.Fatalf("stderr = %q, want scope error", stderr)
	}
}

func TestCmdPluginDoctorJSON(t *testing.T) {
	home := t.TempDir()
	buildCode, _, buildStderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"build", "codex"})
	})
	if buildCode != exitOK {
		t.Fatalf("build exit = %d, stderr=%q", buildCode, buildStderr)
	}
	restoreLookPath := replacePluginLookPath(func(file string) (string, error) {
		if file == "codex" {
			return "/bin/codex", nil
		}
		return "", errors.New("not found")
	})
	defer restoreLookPath()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"doctor", "codex", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdPlugin doctor exit = %d, stderr=%q", code, stderr)
	}
	var out pluginDoctorOutput
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("doctor json: %v\n%s", err, stdout)
	}
	if out.Home != home || len(out.Runtimes) != 1 || out.Runtimes[0].Runtime != "codex" || !out.Runtimes[0].Healthy {
		t.Fatalf("unexpected doctor output: %+v", out)
	}
}

func TestCmdPluginDoctorFailsExplicitMissingRuntime(t *testing.T) {
	home := t.TempDir()
	restoreLookPath := replacePluginLookPath(func(string) (string, error) {
		return "", errors.New("not found")
	})
	defer restoreLookPath()

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"doctor", "codex"})
	})
	if code != exitTransport {
		t.Fatalf("cmdPlugin doctor exit = %d, want %d", code, exitTransport)
	}
	if !strings.Contains(stderr, "codex runtime is unhealthy") {
		t.Fatalf("stderr = %q, want unhealthy runtime", stderr)
	}
}

func TestCmdPluginDoctorFailsInvalidManifest(t *testing.T) {
	home := t.TempDir()
	buildCode, _, buildStderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"build", "claude"})
	})
	if buildCode != exitOK {
		t.Fatalf("build exit = %d, stderr=%q", buildCode, buildStderr)
	}
	manifest := filepath.Join(home, "plugins", "build", "claude", "entmoot", ".claude-plugin", "plugin.json")
	if err := os.WriteFile(manifest, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	restoreLookPath := replacePluginLookPath(func(file string) (string, error) {
		if file == "claude" {
			return "/bin/claude", nil
		}
		return "", errors.New("not found")
	})
	defer restoreLookPath()
	restoreValidation := replacePluginValidationRunner(&pluginCLIFakeRunner{paths: map[string]string{"claude": "/bin/claude"}})
	defer restoreValidation()

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"doctor", "claude"})
	})
	if code != exitTransport {
		t.Fatalf("cmdPlugin doctor exit = %d, want %d", code, exitTransport)
	}
	if !strings.Contains(stderr, "claude runtime is unhealthy") {
		t.Fatalf("stderr = %q, want unhealthy runtime", stderr)
	}
}

func TestCmdPluginDoctorRunsClaudeValidation(t *testing.T) {
	home := t.TempDir()
	buildCode, _, buildStderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"build", "claude"})
	})
	if buildCode != exitOK {
		t.Fatalf("build exit = %d, stderr=%q", buildCode, buildStderr)
	}
	restoreLookPath := replacePluginLookPath(func(file string) (string, error) {
		if file == "claude" {
			return "/bin/claude", nil
		}
		return "", errors.New("not found")
	})
	defer restoreLookPath()
	runner := &pluginCLIFakeRunner{paths: map[string]string{"claude": "/bin/claude"}}
	restoreValidation := replacePluginValidationRunner(runner)
	defer restoreValidation()

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdPlugin(&globalFlags{data: home}, []string{"doctor", "claude", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdPlugin doctor exit = %d, stderr=%q", code, stderr)
	}
	want := []pluginCLIFakeCall{{
		command: "claude",
		args: []string{
			"plugin",
			"validate",
			filepath.Join(home, "plugins", "build", "claude", "entmoot"),
		},
	}}
	if !reflect.DeepEqual(runner.calls, want) {
		t.Fatalf("calls = %+v, want %+v", runner.calls, want)
	}
}

type pluginCLIFakeRunner struct {
	paths map[string]string
	calls []pluginCLIFakeCall
}

type pluginCLIFakeCall struct {
	command string
	args    []string
}

func (r *pluginCLIFakeRunner) LookPath(file string) (string, error) {
	if path, ok := r.paths[file]; ok {
		return path, nil
	}
	return "", errors.New("not found")
}

func (r *pluginCLIFakeRunner) Run(ctx context.Context, dir string, command string, args ...string) (subprocess.Result, error) {
	r.calls = append(r.calls, pluginCLIFakeCall{command: command, args: append([]string(nil), args...)})
	return subprocess.Result{Command: command, Args: args}, nil
}

func replacePluginInstallRunner(runner subprocess.Runner) func() {
	old := pluginInstallRunner
	pluginInstallRunner = runner
	return func() {
		pluginInstallRunner = old
	}
}

func replacePluginLookPath(fn func(string) (string, error)) func() {
	old := pluginLookPath
	pluginLookPath = fn
	return func() {
		pluginLookPath = old
	}
}

func replacePluginValidationRunner(runner subprocess.Runner) func() {
	old := pluginValidationRunner
	pluginValidationRunner = runner
	return func() {
		pluginValidationRunner = old
	}
}

func assertFileContains(t *testing.T, path, want string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(content), want) {
		t.Fatalf("%s does not contain %q:\n%s", path, want, content)
	}
}
