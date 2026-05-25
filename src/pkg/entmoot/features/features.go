package features

import (
	"fmt"
	"os"
	"strings"
)

const (
	EnvEnableFleet = "ENTMOOT_ENABLE_FLEET"
	EnvEnableTasks = "ENTMOOT_ENABLE_TASKS"

	FeatureFleet = "fleet"
	FeatureTasks = "tasks"
)

type Flags struct {
	FleetEnabled bool `json:"fleet_enabled"`
	TasksEnabled bool `json:"tasks_enabled"`
}

type Capabilities struct {
	FleetEnabled bool `json:"fleet_enabled"`
	TasksEnabled bool `json:"tasks_enabled"`
}

type DisabledError struct {
	Feature string
}

func (e DisabledError) Error() string {
	if e.Feature == "" {
		return "feature disabled"
	}
	return fmt.Sprintf("%s feature disabled", e.Feature)
}

func FeatureDisabledError(feature string) error {
	return DisabledError{Feature: feature}
}

func FromEnv() (Flags, error) {
	return FromLookup(os.LookupEnv)
}

func FromLookup(lookup func(string) (string, bool)) (Flags, error) {
	fleetEnabled, err := boolFromLookup(lookup, EnvEnableFleet)
	if err != nil {
		return Flags{}, err
	}
	tasksEnabled, err := boolFromLookup(lookup, EnvEnableTasks)
	if err != nil {
		return Flags{}, err
	}
	return Flags{
		FleetEnabled: fleetEnabled,
		TasksEnabled: tasksEnabled,
	}.Normalize(), nil
}

func (f Flags) Normalize() Flags {
	if !f.FleetEnabled {
		f.TasksEnabled = false
	}
	return f
}

func (f Flags) Capabilities() Capabilities {
	f = f.Normalize()
	return Capabilities{
		FleetEnabled: f.FleetEnabled,
		TasksEnabled: f.TasksEnabled,
	}
}

func (f Flags) RequireFleet() error {
	if !f.FleetEnabled {
		return FeatureDisabledError(FeatureFleet)
	}
	return nil
}

func (f Flags) RequireTasks() error {
	if !f.FleetEnabled || !f.TasksEnabled {
		return FeatureDisabledError(FeatureTasks)
	}
	return nil
}

func boolFromLookup(lookup func(string) (string, bool), name string) (bool, error) {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return false, nil
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be one of 1,true,yes,on,0,false,no,off", name)
	}
}
