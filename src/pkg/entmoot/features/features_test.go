package features

import (
	"errors"
	"strings"
	"testing"
)

func TestFromLookupDefaultsDisabled(t *testing.T) {
	flags, err := FromLookup(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("FromLookup: %v", err)
	}
	if flags.FleetEnabled || flags.TasksEnabled {
		t.Fatalf("flags = %+v, want both disabled", flags)
	}
}

func TestFromLookupTruthValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "one", value: "1", want: true},
		{name: "true", value: "true", want: true},
		{name: "true uppercase", value: "TRUE", want: true},
		{name: "yes", value: "yes", want: true},
		{name: "on", value: "on", want: true},
		{name: "zero", value: "0", want: false},
		{name: "false", value: "false", want: false},
		{name: "false uppercase", value: "FALSE", want: false},
		{name: "no", value: "no", want: false},
		{name: "off", value: "off", want: false},
		{name: "empty", value: "", want: false},
		{name: "spaces", value: "  yes  ", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags, err := FromLookup(func(name string) (string, bool) {
				switch name {
				case EnvEnableFleet:
					return tt.value, true
				case EnvEnableTasks:
					return tt.value, true
				default:
					return "", false
				}
			})
			if err != nil {
				t.Fatalf("FromLookup: %v", err)
			}
			if flags.FleetEnabled != tt.want {
				t.Fatalf("FleetEnabled = %v, want %v", flags.FleetEnabled, tt.want)
			}
			if tt.want && !flags.TasksEnabled {
				t.Fatalf("TasksEnabled = false, want true when both flags are true")
			}
			if !tt.want && flags.TasksEnabled {
				t.Fatalf("TasksEnabled = true, want false")
			}
		})
	}
}

func TestFromLookupTasksRequireFleet(t *testing.T) {
	flags, err := FromLookup(func(name string) (string, bool) {
		switch name {
		case EnvEnableFleet:
			return "0", true
		case EnvEnableTasks:
			return "1", true
		default:
			return "", false
		}
	})
	if err != nil {
		t.Fatalf("FromLookup: %v", err)
	}
	if flags.FleetEnabled || flags.TasksEnabled {
		t.Fatalf("flags = %+v, want tasks forced disabled when fleet is disabled", flags)
	}
}

func TestFromLookupInvalidValues(t *testing.T) {
	for _, envName := range []string{EnvEnableFleet, EnvEnableTasks} {
		t.Run(envName, func(t *testing.T) {
			_, err := FromLookup(func(name string) (string, bool) {
				if name == envName {
					return "maybe", true
				}
				return "", false
			})
			if err == nil {
				t.Fatal("FromLookup err = nil, want invalid value error")
			}
			if !strings.Contains(err.Error(), envName) {
				t.Fatalf("error = %q, want env name", err)
			}
		})
	}
}

func TestCapabilities(t *testing.T) {
	flags := Flags{FleetEnabled: true, TasksEnabled: false}
	got := flags.Capabilities()
	if !got.FleetEnabled || got.TasksEnabled {
		t.Fatalf("Capabilities = %+v, want fleet only", got)
	}
	got = Flags{FleetEnabled: false, TasksEnabled: true}.Capabilities()
	if got.FleetEnabled || got.TasksEnabled {
		t.Fatalf("Capabilities = %+v, want normalized disabled flags", got)
	}
}

func TestRequireFeatures(t *testing.T) {
	flags := Flags{}
	var disabled DisabledError
	if err := flags.RequireFleet(); !errors.As(err, &disabled) || disabled.Feature != FeatureFleet {
		t.Fatalf("RequireFleet err = %v, want disabled fleet", err)
	}
	if err := flags.RequireTasks(); !errors.As(err, &disabled) || disabled.Feature != FeatureTasks {
		t.Fatalf("RequireTasks err = %v, want disabled tasks", err)
	}
	flags = Flags{FleetEnabled: false, TasksEnabled: true}
	if err := flags.RequireTasks(); !errors.As(err, &disabled) || disabled.Feature != FeatureTasks {
		t.Fatalf("RequireTasks err = %v, want disabled tasks when fleet is disabled", err)
	}
	flags = Flags{FleetEnabled: true, TasksEnabled: true}
	if err := flags.RequireFleet(); err != nil {
		t.Fatalf("RequireFleet enabled: %v", err)
	}
	if err := flags.RequireTasks(); err != nil {
		t.Fatalf("RequireTasks enabled: %v", err)
	}
}
