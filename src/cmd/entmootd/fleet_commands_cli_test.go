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
