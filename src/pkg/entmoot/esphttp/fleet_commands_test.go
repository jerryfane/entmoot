package esphttp

import (
	"strings"
	"testing"
)

func TestFleetCommandInstructionArgsValidatesInstruction(t *testing.T) {
	_, _, _, err := FleetCommandInstructionArgs(map[string]interface{}{})
	if err == nil || !strings.Contains(err.Error(), "instruction is required") {
		t.Fatalf("FleetCommandInstructionArgs err = %v, want instruction required", err)
	}
}

func TestFleetCommandInstructionArgsParsesBoundedTimeoutAndContext(t *testing.T) {
	instruction, timeoutMS, contextArgs, err := FleetCommandInstructionArgs(map[string]interface{}{
		"instruction": "Send a message to Mars Hub",
		"timeout_ms":  float64(1),
		"context": map[string]interface{}{
			"group": "Mars Hub",
		},
	})
	if err != nil {
		t.Fatalf("FleetCommandInstructionArgs: %v", err)
	}
	if instruction != "Send a message to Mars Hub" {
		t.Fatalf("instruction = %q", instruction)
	}
	if timeoutMS != MinFleetInstructionTimeoutMS {
		t.Fatalf("timeoutMS = %d, want min %d", timeoutMS, MinFleetInstructionTimeoutMS)
	}
	if contextArgs["group"] != "Mars Hub" {
		t.Fatalf("context group = %v", contextArgs["group"])
	}
}

func TestNormalizeFleetCommandResultStatus(t *testing.T) {
	if got := NormalizeFleetCommandResultStatus(" Completed "); got != FleetCommandStatusCompleted {
		t.Fatalf("NormalizeFleetCommandResultStatus = %q, want completed", got)
	}
	if got := NormalizeFleetCommandResultStatus("shell"); got != "" {
		t.Fatalf("NormalizeFleetCommandResultStatus invalid = %q, want empty", got)
	}
}
