package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"entmoot/pkg/entmoot/esphttp"
	entfeatures "entmoot/pkg/entmoot/features"
)

func requireFleetFeature(gf *globalFlags, command string) int {
	flags := featureFlags(gf)
	if err := flags.RequireFleet(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: Fleet coordination is disabled; set %s=1 to enable it.\n", command, entfeatures.EnvEnableFleet)
		return exitInvalidArgument
	}
	return exitOK
}

func requireTaskFeature(gf *globalFlags, command string) int {
	flags := featureFlags(gf)
	if err := flags.RequireTasks(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: task coordination is disabled; set %s=1 and %s=1 to enable it.\n", command, entfeatures.EnvEnableFleet, entfeatures.EnvEnableTasks)
		return exitInvalidArgument
	}
	return exitOK
}

func featureFlags(gf *globalFlags) entfeatures.Flags {
	if gf == nil {
		return entfeatures.Flags{}
	}
	return gf.features.Normalize()
}

func coordinationLiveActions(actions []string, flags entfeatures.Flags) []string {
	if flags.RequireTasks() == nil {
		return nil
	}
	var disabled []string
	for _, action := range actions {
		action = strings.TrimSpace(strings.ToLower(action))
		if liveActionRequiresTasks(action) {
			disabled = append(disabled, action)
		}
	}
	sort.Strings(disabled)
	return disabled
}

func filterDisabledLiveActions(actions []string, flags entfeatures.Flags) []string {
	disabled := coordinationLiveActions(actions, flags)
	if len(disabled) == 0 {
		return actions
	}
	blocked := make(map[string]struct{}, len(disabled))
	for _, action := range disabled {
		blocked[action] = struct{}{}
	}
	out := make([]string, 0, len(actions)-len(disabled))
	for _, action := range actions {
		if _, ok := blocked[strings.TrimSpace(strings.ToLower(action))]; ok {
			continue
		}
		out = append(out, action)
	}
	return out
}

func liveAllowedActionsForConfig(cfg esphttp.LiveAgentConfig, flags entfeatures.Flags) []string {
	actions := cfg.AllowedActions
	if len(actions) == 0 {
		switch cfg.Mode {
		case esphttp.LiveModeOperator:
			actions = esphttp.DefaultLiveActions()
		case esphttp.LiveModeReplyOnMention, esphttp.LiveModeConverse:
			actions = []string{liveActionReply, liveActionMessageSummarize}
		}
	}
	return filterDisabledLiveActions(actions, flags)
}
