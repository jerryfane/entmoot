package main

import (
	"testing"

	"entmoot/pkg/entmoot/esphttp"
)

func TestFleetCommandSendArgsRejectsNullJSON(t *testing.T) {
	cfg := &fleetCommandsFlags{
		action:      esphttp.FleetCommandActionAgentInstruction,
		argsJSON:    "null",
		instruction: "Send a message to Mars Hub",
	}
	if _, _, ok := fleetCommandSendArgs(cfg); ok {
		t.Fatal("fleetCommandSendArgs ok=true for null args JSON, want false")
	}
}

func TestFleetCommandSendArgsMergesInstruction(t *testing.T) {
	cfg := &fleetCommandsFlags{
		action:      esphttp.FleetCommandActionAgentInstruction,
		argsJSON:    `{"context":{"source":"test"}}`,
		instruction: "Send a message to Mars Hub",
		timeoutMS:   60000,
	}
	args, _, ok := fleetCommandSendArgs(cfg)
	if !ok {
		t.Fatal("fleetCommandSendArgs ok=false, want true")
	}
	if args["instruction"] != cfg.instruction {
		t.Fatalf("instruction = %v, want %q", args["instruction"], cfg.instruction)
	}
	if args["timeout_ms"] != cfg.timeoutMS {
		t.Fatalf("timeout_ms = %v, want %d", args["timeout_ms"], cfg.timeoutMS)
	}
}

func TestFleetCommandSendArgsAddsExternalActionFlags(t *testing.T) {
	cfg := &fleetCommandsFlags{
		action:                         esphttp.FleetCommandActionAgentInstruction,
		instruction:                    "Send a message to the owner",
		externalActionID:               "notify-owner",
		externalActionKind:             esphttp.FleetCommandExternalActionMessageSend,
		externalActionChannel:          "telegram",
		externalActionTarget:           "owner",
		externalActionRequired:         true,
		externalActionDeliveryRequired: true,
	}
	args, _, ok := fleetCommandSendArgs(cfg)
	if !ok {
		t.Fatal("fleetCommandSendArgs ok=false, want true")
	}
	actions, ok := args["actions"].([]any)
	if !ok || len(actions) != 1 {
		t.Fatalf("actions = %#v, want one action", args["actions"])
	}
	action, ok := actions[0].(map[string]any)
	if !ok {
		t.Fatalf("action = %#v, want map", actions[0])
	}
	if action["id"] != "notify-owner" || action["kind"] != esphttp.FleetCommandExternalActionMessageSend || action["channel"] != "telegram" || action["target"] != "owner" || action["required"] != true || action["delivery_required"] != true {
		t.Fatalf("action = %#v, want message.send delivery action", action)
	}
}

func TestFleetCommandSendArgsRejectsExternalActionFlagsForNonAgentInstruction(t *testing.T) {
	cfg := &fleetCommandsFlags{
		action:             esphttp.FleetCommandActionEcho,
		argsJSON:           `{"message":"ok"}`,
		externalActionKind: esphttp.FleetCommandExternalActionMessageSend,
	}
	if _, _, ok := fleetCommandSendArgs(cfg); ok {
		t.Fatal("fleetCommandSendArgs ok=true, want false for external action on non-agent command")
	}
}
