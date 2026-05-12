package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const (
	agentRunnerNone     = "none"
	agentRunnerCustom   = "custom"
	agentRunnerOpenClaw = "openclaw"
)

func normalizeAgentRunnerKind(kind string) string {
	switch strings.TrimSpace(strings.ToLower(kind)) {
	case "", agentRunnerNone:
		return agentRunnerNone
	case agentRunnerCustom:
		return agentRunnerCustom
	case agentRunnerOpenClaw:
		return agentRunnerOpenClaw
	default:
		return ""
	}
}

func agentRunnerCommand(kind, command string) string {
	switch normalizeAgentRunnerKind(kind) {
	case agentRunnerOpenClaw:
		return agentRunnerOpenClaw
	case agentRunnerCustom:
		return strings.TrimSpace(command)
	default:
		return ""
	}
}

func validateAgentRunner(kind, command string) error {
	switch normalizeAgentRunnerKind(kind) {
	case agentRunnerNone:
		if strings.TrimSpace(command) != "" {
			return fmt.Errorf("--runner-command requires --runner custom")
		}
		return nil
	case agentRunnerOpenClaw:
		if strings.TrimSpace(command) != "" {
			return fmt.Errorf("--runner-command requires --runner custom")
		}
		return nil
	case agentRunnerCustom:
		command = strings.TrimSpace(command)
		if command == "" {
			return fmt.Errorf("--runner-command is required when --runner custom")
		}
		return validateCustomAgentRunnerCommand(command)
	default:
		return fmt.Errorf("invalid --runner; use none, custom, or openclaw")
	}
}

func validateCustomAgentRunnerCommand(command string) error {
	command = strings.TrimSpace(command)
	if command == "" {
		return fmt.Errorf("empty runner command")
	}
	if strings.ContainsAny(command, `/\`) {
		info, err := os.Stat(command)
		if err != nil {
			return fmt.Errorf("runner command %q is not accessible: %w", command, err)
		}
		if info.IsDir() {
			return fmt.Errorf("runner command %q is a directory", command)
		}
		if info.Mode()&0o111 == 0 {
			return fmt.Errorf("runner command %q is not executable", command)
		}
		return nil
	}
	if len(strings.Fields(command)) != 1 {
		return fmt.Errorf("runner command must be one executable path or name without arguments")
	}
	if _, err := exec.LookPath(command); err != nil {
		return fmt.Errorf("runner command %q was not found on PATH", command)
	}
	return nil
}

func shellCommand(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		quoted = append(quoted, shellQuoteArg(part))
	}
	return strings.Join(quoted, " ")
}
