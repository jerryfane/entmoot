package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCmdVersionPrintsBuildMetadata(t *testing.T) {
	oldVersion, oldCommit, oldDate := version, commit, date
	version = "v-test"
	commit = "abc123"
	date = "2026-04-28T00:00:00Z"
	defer func() {
		version, commit, date = oldVersion, oldCommit, oldDate
	}()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdVersion(&globalFlags{}, nil)
	})
	if code != exitOK {
		t.Fatalf("cmdVersion exit = %d, want %d; stderr=%q", code, exitOK, stderr)
	}

	var got versionOutput
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("version JSON: %v\nstdout=%s", err, stdout)
	}
	if got.Version != version || got.Commit != commit || got.Date != date {
		t.Fatalf("version output = %+v, want version=%q commit=%q date=%q", got, version, commit, date)
	}
}

func TestCmdVersionRejectsExtraArgs(t *testing.T) {
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdVersion(&globalFlags{}, []string{"extra"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("cmdVersion extra exit = %d, want %d", code, exitInvalidArgument)
	}
	if !strings.Contains(stderr, "unexpected arguments") {
		t.Fatalf("stderr = %q, want unexpected arguments", stderr)
	}
}
