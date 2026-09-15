package main

import (
	"reflect"
	"testing"
)

// The OpenClaw selector and its env-clearing helper moved into
// agent_live_runtime.go when the Fleet agent-commands file was deleted; this
// test moved with them. The precedence decides whether a live reply reaches a
// session, a phone number or a named agent, and every deployment sets these
// variables by hand, so it stays defended.
func TestOpenClawAgentSelectorPrecedence(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{name: "default main agent", want: []string{"--agent", "main"}},
		{name: "agent", env: map[string]string{"ENTMOOT_OPENCLAW_AGENT": "ops"}, want: []string{"--agent", "ops"}},
		{name: "openclaw agent alias", env: map[string]string{"OPENCLAW_AGENT_ID": "alias"}, want: []string{"--agent", "alias"}},
		{name: "to", env: map[string]string{"ENTMOOT_OPENCLAW_AGENT": "ops", "ENTMOOT_OPENCLAW_TO": "+15555550123"}, want: []string{"--to", "+15555550123"}},
		{name: "openclaw to alias", env: map[string]string{"OPENCLAW_AGENT_ID": "alias", "OPENCLAW_TO": "+15555550123"}, want: []string{"--to", "+15555550123"}},
		{name: "session id", env: map[string]string{"ENTMOOT_OPENCLAW_AGENT": "ops", "ENTMOOT_OPENCLAW_TO": "+15555550123", "ENTMOOT_OPENCLAW_SESSION_ID": "sess-1"}, want: []string{"--session-id", "sess-1"}},
		{name: "openclaw session alias", env: map[string]string{"OPENCLAW_AGENT_ID": "alias", "OPENCLAW_TO": "+15555550123", "OPENCLAW_SESSION_ID": "sess-2"}, want: []string{"--session-id", "sess-2"}},
		{name: "entmoot to beats openclaw session alias", env: map[string]string{"ENTMOOT_OPENCLAW_TO": "+15555550123", "OPENCLAW_SESSION_ID": "sess-2"}, want: []string{"--to", "+15555550123"}},
		{name: "entmoot agent beats openclaw session alias", env: map[string]string{"ENTMOOT_OPENCLAW_AGENT": "ops", "OPENCLAW_SESSION_ID": "sess-2"}, want: []string{"--agent", "ops"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearOpenClawRunnerEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			gotFlag, gotValue := openClawAgentSelector()
			got := []string{gotFlag, gotValue}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("selector = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func clearOpenClawRunnerEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"ENTMOOT_OPENCLAW_AGENT",
		"ENTMOOT_OPENCLAW_SESSION_ID",
		"ENTMOOT_OPENCLAW_TO",
		"OPENCLAW_AGENT_ID",
		"OPENCLAW_SESSION_ID",
		"OPENCLAW_TO",
		"OPENCLAW_BIN",
	} {
		t.Setenv(name, "")
	}
}
