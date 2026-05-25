package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot/esphttp"
	entfeatures "entmoot/pkg/entmoot/features"
)

func enableCoordinationFeatures(gf *globalFlags) *globalFlags {
	gf.features = entfeatures.Flags{FleetEnabled: true, TasksEnabled: true}
	return gf
}

func TestRunRejectsInvalidFeatureEnv(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"entmootd", "-identity", filepath.Join(t.TempDir(), "identity.json"), "-data", t.TempDir(), "env", "--json"}
	t.Setenv(entfeatures.EnvEnableFleet, "maybe")

	code, _, stderr := captureCommandOutput(t, run)
	if code != exitInvalidArgument {
		t.Fatalf("run code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, entfeatures.EnvEnableFleet) {
		t.Fatalf("stderr = %q, want env name", stderr)
	}
}

func TestCoordinationCommandsRequireFeatureFlags(t *testing.T) {
	for _, tt := range []struct {
		name string
		run  func(*globalFlags) int
		want string
	}{
		{
			name: "fleet",
			run:  func(gf *globalFlags) int { return cmdFleet(gf, []string{"list"}) },
			want: entfeatures.EnvEnableFleet,
		},
		{
			name: "agent commands",
			run:  func(gf *globalFlags) int { return cmdAgentCommands(gf, []string{"status"}) },
			want: entfeatures.EnvEnableTasks,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, _, stderr := captureCommandOutput(t, func() int {
				return tt.run(&globalFlags{data: t.TempDir()})
			})
			if code != exitInvalidArgument {
				t.Fatalf("code = %d stderr=%s", code, stderr)
			}
			if !strings.Contains(stderr, tt.want) {
				t.Fatalf("stderr = %q, want %s", stderr, tt.want)
			}
		})
	}
}

func TestFleetTasksRequireTaskFeature(t *testing.T) {
	gf := &globalFlags{
		data:     t.TempDir(),
		features: entfeatures.Flags{FleetEnabled: true},
	}
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdFleet(gf, []string{"tasks", "list"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, entfeatures.EnvEnableTasks) {
		t.Fatalf("stderr = %q, want %s", stderr, entfeatures.EnvEnableTasks)
	}
}

func TestFleetCommandMutationsRequireTaskFeature(t *testing.T) {
	gf := &globalFlags{
		data:     t.TempDir(),
		features: entfeatures.Flags{FleetEnabled: true},
	}
	for _, args := range [][]string{
		{"commands", "send"},
		{"commands", "result"},
	} {
		code, _, stderr := captureCommandOutput(t, func() int {
			return cmdFleet(gf, args)
		})
		if code != exitInvalidArgument {
			t.Fatalf("cmdFleet(%v) code = %d stderr=%s", args, code, stderr)
		}
		if !strings.Contains(stderr, entfeatures.EnvEnableTasks) {
			t.Fatalf("cmdFleet(%v) stderr = %q, want %s", args, stderr, entfeatures.EnvEnableTasks)
		}
	}
}

func TestEnableAgentLiveConfigRejectsEmptyExplicitActions(t *testing.T) {
	_, err := enableAgentLiveConfig(context.Background(), esphttp.NewMemoryStateStore(), enableAgentLiveConfigOptions{
		groupID: testAgentLiveGroupID(0x41),
		nodeID:  45491,
		mode:    esphttp.LiveModeOperator,
		actions: []string{""},
	})
	if err == nil {
		t.Fatal("enableAgentLiveConfig err = nil, want empty explicit action rejection")
	}
	if !strings.Contains(err.Error(), "cannot be empty") {
		t.Fatalf("err = %v, want empty action guidance", err)
	}
}

func TestCoordinationCommandsRunWhenEnabled(t *testing.T) {
	gf := enableCoordinationFeatures(&globalFlags{data: t.TempDir()})
	for _, tt := range []struct {
		name string
		run  func() int
	}{
		{name: "fleet list", run: func() int { return cmdFleet(gf, []string{"list"}) }},
		{name: "agent commands status", run: func() int { return cmdAgentCommands(gf, []string{"status"}) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := captureCommandOutput(t, tt.run)
			if code != exitOK {
				t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout, stderr)
			}
			if stdout == "" {
				t.Fatal("stdout is empty, want JSON")
			}
		})
	}
}

func TestRuntimeReportIncludesFeatureCapabilities(t *testing.T) {
	report := collectRuntimeReport(enableCoordinationFeatures(&globalFlags{
		socket:   filepath.Join(t.TempDir(), "pilot.sock"),
		identity: filepath.Join(t.TempDir(), "identity.json"),
		data:     t.TempDir(),
	}), t.TempDir())
	if !report.Features.FleetEnabled || !report.Features.TasksEnabled {
		t.Fatalf("features = %+v, want coordination enabled", report.Features)
	}
}

func TestFilterDisabledLiveActionsRemovesPersistedCoordination(t *testing.T) {
	got := filterDisabledLiveActions([]string{
		liveActionReply,
		liveActionTaskCreate,
		liveActionCommandSend,
		liveActionMetadataUpdate,
	}, entfeatures.Flags{})
	want := strings.Join([]string{liveActionReply, liveActionMetadataUpdate}, ",")
	if strings.Join(got, ",") != want {
		t.Fatalf("actions = %v, want %s", got, want)
	}
}

func TestLiveAllowedActionsForConfigDefaultsReplyActions(t *testing.T) {
	got := liveAllowedActionsForConfig(esphttp.LiveAgentConfig{Mode: esphttp.LiveModeConverse}, entfeatures.Flags{})
	want := strings.Join([]string{liveActionReply, liveActionMessageSummarize}, ",")
	if strings.Join(got, ",") != want {
		t.Fatalf("actions = %v, want %s", got, want)
	}

	operator := liveAllowedActionsForConfig(esphttp.LiveAgentConfig{Mode: esphttp.LiveModeOperator}, entfeatures.Flags{})
	if strings.Contains(strings.Join(operator, ","), liveActionTaskCreate) {
		t.Fatalf("operator actions = %v, want task actions filtered while disabled", operator)
	}
}
