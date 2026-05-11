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

func TestFleetCommandInstructionSpecParsesExternalActions(t *testing.T) {
	spec, err := FleetCommandInstructionSpecFromArgs(map[string]interface{}{
		"instruction": "Send a message to Mars Hub",
		"actions": []interface{}{
			map[string]interface{}{
				"id":                "notify-owner",
				"kind":              "message.send",
				"channel":           "Telegram",
				"target":            "owner",
				"required":          true,
				"delivery_required": true,
			},
		},
	})
	if err != nil {
		t.Fatalf("FleetCommandInstructionSpecFromArgs: %v", err)
	}
	if len(spec.Actions) != 1 {
		t.Fatalf("actions len = %d, want 1", len(spec.Actions))
	}
	action := spec.Actions[0]
	if action.ID != "notify-owner" || action.Kind != FleetCommandExternalActionMessageSend || action.Channel != "telegram" || action.Target != "owner" || !action.Required || !action.DeliveryRequired {
		t.Fatalf("action = %+v, want normalized message.send action", action)
	}
}

func TestFleetCommandInstructionSpecRejectsRequiredUnknownAction(t *testing.T) {
	_, err := FleetCommandInstructionSpecFromArgs(map[string]interface{}{
		"instruction": "Do something external",
		"actions": []interface{}{
			map[string]interface{}{
				"kind":     "calendar.create",
				"required": true,
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "not supported for required external actions") {
		t.Fatalf("FleetCommandInstructionSpecFromArgs err = %v, want required unknown action rejection", err)
	}
}

func TestFleetCommandInstructionSpecRejectsDeliveryRequiredUnknownAction(t *testing.T) {
	_, err := FleetCommandInstructionSpecFromArgs(map[string]interface{}{
		"instruction": "Do something external",
		"actions": []interface{}{
			map[string]interface{}{
				"kind":              "calendar.create",
				"delivery_required": true,
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "not supported for required external actions") {
		t.Fatalf("FleetCommandInstructionSpecFromArgs err = %v, want delivery-required unknown action rejection", err)
	}
}

func TestFleetCommandInstructionSpecRejectsRequiredMessageWithoutTarget(t *testing.T) {
	_, err := FleetCommandInstructionSpecFromArgs(map[string]interface{}{
		"instruction": "Send a message",
		"actions": []interface{}{
			map[string]interface{}{
				"kind":              "message.send",
				"channel":           "telegram",
				"delivery_required": true,
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "target is required for message.send") {
		t.Fatalf("FleetCommandInstructionSpecFromArgs err = %v, want target required", err)
	}
}

func TestFleetCommandInstructionSpecRejectsMultipleRequiredMessages(t *testing.T) {
	_, err := FleetCommandInstructionSpecFromArgs(map[string]interface{}{
		"instruction": "Send two messages",
		"actions": []interface{}{
			map[string]interface{}{
				"kind":              "message.send",
				"channel":           "telegram",
				"target":            "owner",
				"delivery_required": true,
			},
			map[string]interface{}{
				"kind":     "message.send",
				"channel":  "telegram",
				"target":   "ops",
				"required": true,
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "only one required message.send action is supported") {
		t.Fatalf("FleetCommandInstructionSpecFromArgs err = %v, want multiple required message rejection", err)
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
