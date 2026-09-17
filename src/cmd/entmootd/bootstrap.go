package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type bootstrapAgentOptions struct {
	yes         bool
	interactive bool
	dryRun      bool
	json        bool
	defaultMoot string
}

type bootstrapAgentReport struct {
	DryRun      bool                       `json:"dry_run"`
	Applied     bool                       `json:"applied"`
	DefaultMoot bootstrapDefaultMootReport `json:"default_moot"`
	Commands    []string                   `json:"commands,omitempty"`
	Runtime     runtimeReport              `json:"runtime"`
}

type bootstrapDefaultMootReport struct {
	Choice   string   `json:"choice"`
	Commands []string `json:"commands,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

func cmdBootstrap(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd bootstrap <agent> [flags]")
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "bootstrap: missing op")
			return exitInvalidArgument
		}
		return exitOK
	}
	switch args[0] {
	case "agent":
		return cmdBootstrapAgent(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "bootstrap: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdBootstrapAgent(gf *globalFlags, args []string) int {
	var cfg bootstrapAgentOptions
	fs := flag.NewFlagSet("bootstrap agent", flag.ContinueOnError)
	fs.BoolVar(&cfg.yes, "yes", false, "use unattended safe defaults and never prompt")
	fs.BoolVar(&cfg.interactive, "interactive", false, "ask owner-driven setup questions on a TTY")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "print the setup plan without applying local config")
	fs.BoolVar(&cfg.json, "json", false, "print JSON summary")
	fs.StringVar(&cfg.defaultMoot, "default-moot", "skip", "The Ent Moot owner choice: skip, join, or decline")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if cfg.yes && cfg.interactive {
		fmt.Fprintln(os.Stderr, "bootstrap agent: use either --yes or --interactive, not both")
		return exitInvalidArgument
	}
	if cfg.interactive {
		var err error
		cfg, err = promptBootstrapAgentOptions(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bootstrap agent: %v\n", err)
			return exitInvalidArgument
		}
	}
	if !validBootstrapDefaultMootChoice(cfg.defaultMoot) {
		fmt.Fprintln(os.Stderr, "bootstrap agent: --default-moot must be skip, join, or decline")
		return exitInvalidArgument
	}
	cfg.defaultMoot = normalizeBootstrapDefaultMootChoice(cfg.defaultMoot)
	report := buildBootstrapAgentReport(gf, cfg)
	if !cfg.dryRun && cfg.defaultMoot == defaultMootConsentDeclined {
		declinedState, err := defaultMootDeclinedLocalState(context.Background(), gf.data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bootstrap agent: default moot decline: %v\n", err)
			return exitTransport
		}
		if err := saveDefaultMootLocalState(gf.data, declinedState); err != nil {
			fmt.Fprintf(os.Stderr, "bootstrap agent: default moot decline: %v\n", err)
			return exitTransport
		}
		report.Applied = true
	}
	if cfg.json {
		return printJSON(report)
	}
	printBootstrapAgentReport(report)
	return exitOK
}

func buildBootstrapAgentReport(gf *globalFlags, cfg bootstrapAgentOptions) bootstrapAgentReport {
	runtime := collectRuntimeReport(gf, gf.data)
	report := bootstrapAgentReport{
		DryRun:      cfg.dryRun,
		Runtime:     runtime,
		DefaultMoot: buildBootstrapDefaultMootReport(gf, runtime, cfg.defaultMoot),
	}
	report.Commands = bootstrapAgentCommands(gf, report)
	return report
}

func bootstrapAgentCommands(gf *globalFlags, report bootstrapAgentReport) []string {
	var out []string
	out = append(out, report.DefaultMoot.Commands...)
	out = append(out, entmootCommand(gf, report.Runtime, "serve"))
	return out
}

func buildBootstrapDefaultMootReport(gf *globalFlags, runtime runtimeReport, choice string) bootstrapDefaultMootReport {
	choice = normalizeBootstrapDefaultMootChoice(choice)
	if choice == "" {
		choice = "skip"
	}
	report := bootstrapDefaultMootReport{Choice: choice}
	switch choice {
	case "join":
		report.Commands = append(report.Commands, entmootCommand(gf, runtime, "default-moot", "join"))
		report.Warnings = append(report.Warnings, "joining The Ent Moot is owner-approved but not performed implicitly by bootstrap output; run the command after reviewing the descriptor and intended connectivity profile")
	case defaultMootConsentDeclined:
		report.Commands = append(report.Commands, entmootCommand(gf, runtime, "default-moot", "decline"))
	}
	return report
}

func validBootstrapDefaultMootChoice(choice string) bool {
	switch normalizeBootstrapDefaultMootChoice(choice) {
	case "skip", "join", defaultMootConsentDeclined:
		return true
	default:
		return false
	}
}

func normalizeBootstrapDefaultMootChoice(choice string) string {
	choice = strings.TrimSpace(strings.ToLower(choice))
	if choice == "decline" {
		return defaultMootConsentDeclined
	}
	return choice
}

func entmootCommand(gf *globalFlags, report runtimeReport, args ...string) string {
	binary := firstNonEmpty(report.AgentWrapper, report.Binary, "entmootd")
	parts := []string{
		binary,
		"-identity", gf.identity,
		"-data", gf.data,
	}
	if gf.listenPort > 0 {
		parts = append(parts, "-listen-port", strconv.FormatUint(uint64(gf.listenPort), 10))
	}
	if strings.TrimSpace(gf.logLevel) != "" {
		parts = append(parts, "-log-level", gf.logLevel)
	}
	parts = append(parts, args...)
	return shellCommand(parts...)
}

// shellCommand renders a copy-pasteable command line for the bootstrap
// report: empty parts are dropped so an unset flag does not print as a bare
// dangling name, and every part is quoted for a POSIX shell.
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

func printBootstrapAgentReport(report bootstrapAgentReport) {
	if report.DryRun {
		fmt.Println("bootstrap agent: dry run")
	} else if report.Applied {
		fmt.Println("bootstrap agent: applied")
	} else {
		fmt.Println("bootstrap agent: ready")
	}
	fmt.Printf("default_moot: %s\n", report.DefaultMoot.Choice)
	for _, warning := range report.DefaultMoot.Warnings {
		fmt.Printf("warning: %s\n", warning)
	}
	for _, command := range report.Commands {
		fmt.Printf("command: %s\n", command)
	}
}

func promptBootstrapAgentOptions(cfg bootstrapAgentOptions) (bootstrapAgentOptions, error) {
	if !isTerminal(os.Stdin) || !isTerminal(os.Stdout) {
		return cfg, fmt.Errorf("--interactive requires a terminal; pass flags or use --yes for defaults")
	}
	reader := bufio.NewReader(os.Stdin)
	choice, err := promptChoice(reader, "The Ent Moot [skip/join/decline]", cfg.defaultMoot, map[string]bool{
		"skip":                     true,
		"join":                     true,
		"decline":                  true,
		defaultMootConsentDeclined: true,
	})
	if err != nil {
		return cfg, err
	}
	cfg.defaultMoot = choice
	return cfg, nil
}

func isTerminal(file *os.File) bool {
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func promptChoice(reader *bufio.Reader, label, current string, allowed map[string]bool) (string, error) {
	value, err := promptString(reader, label, current)
	if err != nil {
		return "", err
	}
	value = strings.TrimSpace(strings.ToLower(value))
	if !allowed[value] {
		return "", fmt.Errorf("invalid %s: %s", label, value)
	}
	return value, nil
}

func promptString(reader *bufio.Reader, label, current string) (string, error) {
	if current != "" {
		fmt.Fprintf(os.Stderr, "%s [%s]: ", label, current)
	} else {
		fmt.Fprintf(os.Stderr, "%s: ", label)
	}
	line, err := reader.ReadString('\n')
	if err != nil && len(line) == 0 {
		return "", err
	}
	value := strings.TrimSpace(line)
	if value == "" {
		value = current
	}
	return value, nil
}
