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

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
)

const bootstrapLiveModeOff = "off"

type bootstrapAgentOptions struct {
	yes               bool
	interactive       bool
	dryRun            bool
	json              bool
	runner            string
	runnerCommand     string
	agentInstructions bool
	liveMode          string
	group             string
	node              uint64
	topics            repeatedStringFlag
	actions           repeatedStringFlag
	maxActionsPerScan int
	maxActionBytes    int
	defaultMoot       string
}

type bootstrapAgentReport struct {
	DryRun            bool                       `json:"dry_run"`
	Applied           bool                       `json:"applied"`
	Runner            string                     `json:"runner"`
	RunnerCommand     string                     `json:"runner_command,omitempty"`
	AgentInstructions bool                       `json:"agent_instructions"`
	Live              bootstrapAgentLiveReport   `json:"live"`
	DefaultMoot       bootstrapDefaultMootReport `json:"default_moot"`
	Commands          []string                   `json:"commands,omitempty"`
	Warnings          []string                   `json:"warnings,omitempty"`
	Runtime           runtimeReport              `json:"runtime"`
	AppliedLiveConfig *esphttp.LiveAgentConfig   `json:"applied_live_config,omitempty"`
}

type bootstrapAgentLiveReport struct {
	Enabled           bool     `json:"enabled"`
	Group             string   `json:"group,omitempty"`
	GroupID           string   `json:"group_id,omitempty"`
	NodeID            uint64   `json:"node_id,omitempty"`
	Mode              string   `json:"mode,omitempty"`
	TopicFilters      []string `json:"topic_filters,omitempty"`
	AllowedActions    []string `json:"allowed_actions,omitempty"`
	MaxActionsPerScan int      `json:"max_actions_per_scan,omitempty"`
	MaxActionBytes    int      `json:"max_action_bytes,omitempty"`
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
	cfg := bootstrapAgentOptions{
		runner:   agentRunnerNone,
		liveMode: bootstrapLiveModeOff,
	}
	fs := flag.NewFlagSet("bootstrap agent", flag.ContinueOnError)
	fs.BoolVar(&cfg.yes, "yes", false, "use unattended safe defaults and never prompt")
	fs.BoolVar(&cfg.interactive, "interactive", false, "ask owner-driven setup questions on a TTY")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "print the setup plan without applying local config")
	fs.BoolVar(&cfg.json, "json", false, "print JSON summary")
	fs.StringVar(&cfg.runner, "runner", cfg.runner, "agent runtime: none, custom, or openclaw")
	fs.StringVar(&cfg.runnerCommand, "runner-command", "", "custom agent runner command")
	fs.BoolVar(&cfg.agentInstructions, "agent-instructions", false, "enable instruction-command runtime guidance for entmootd serve")
	fs.StringVar(&cfg.liveMode, "live-mode", cfg.liveMode, "live mode: off, listen, reply_on_mention, converse, operator")
	fs.StringVar(&cfg.group, "group", "", "base64 moot group id for live mode")
	fs.Uint64Var(&cfg.node, "node", 0, "local Pilot node id for live mode")
	fs.Var(&cfg.topics, "topic", "live topic filter; may be repeated")
	fs.Var(&cfg.actions, "action", "operator action; may be repeated")
	fs.IntVar(&cfg.maxActionsPerScan, "max-actions", 0, "optional maximum live actions per scan; 0 means unlimited")
	fs.IntVar(&cfg.maxActionBytes, "max-action-bytes", 0, "optional maximum bytes per live action message; 0 means unlimited")
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
	report, gid, nodeID, err := buildBootstrapAgentReport(gf, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap agent: %v\n", err)
		return exitInvalidArgument
	}
	if !cfg.dryRun && report.Live.Enabled {
		state, err := esphttp.OpenSQLiteStateStore(gf.data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bootstrap agent: %v\n", err)
			return exitTransport
		}
		defer state.Close()
		rec, err := enableAgentLiveConfig(context.Background(), state, enableAgentLiveConfigOptions{
			groupID:           gid,
			nodeID:            nodeID,
			mode:              report.Live.Mode,
			topics:            report.Live.TopicFilters,
			actions:           report.Live.AllowedActions,
			features:          featureFlags(gf),
			maxActionsPerScan: report.Live.MaxActionsPerScan,
			maxActionBytes:    report.Live.MaxActionBytes,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "bootstrap agent: %v\n", err)
			return exitInvalidArgument
		}
		report.Applied = true
		report.AppliedLiveConfig = &rec
	} else {
		report.Applied = false
	}
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

func buildBootstrapAgentReport(gf *globalFlags, cfg bootstrapAgentOptions) (bootstrapAgentReport, entmoot.GroupID, entmoot.NodeID, error) {
	var gid entmoot.GroupID
	var nodeID entmoot.NodeID
	runner := normalizeAgentRunnerKind(cfg.runner)
	if runner == "" {
		return bootstrapAgentReport{}, gid, nodeID, fmt.Errorf("invalid --runner; use none, custom, or openclaw")
	}
	if err := validateAgentRunner(runner, cfg.runnerCommand); err != nil {
		return bootstrapAgentReport{}, gid, nodeID, err
	}
	liveMode := strings.TrimSpace(strings.ToLower(cfg.liveMode))
	if liveMode == "" {
		liveMode = bootstrapLiveModeOff
	}
	if liveMode != bootstrapLiveModeOff {
		liveMode = esphttp.NormalizeLiveMode(liveMode)
		if liveMode == "" {
			return bootstrapAgentReport{}, gid, nodeID, fmt.Errorf("invalid --live-mode; use off, listen, reply_on_mention, converse, or operator")
		}
		var err error
		gid, nodeID, err = parseBootstrapLiveTarget(cfg.group, cfg.node)
		if err != nil {
			return bootstrapAgentReport{}, gid, nodeID, err
		}
	}
	if cfg.maxActionsPerScan < 0 || cfg.maxActionBytes < 0 {
		return bootstrapAgentReport{}, gid, nodeID, fmt.Errorf("--max-actions and --max-action-bytes must be non-negative")
	}
	if unknown := esphttp.UnknownLiveActions([]string(cfg.actions)); len(unknown) > 0 {
		return bootstrapAgentReport{}, gid, nodeID, fmt.Errorf("unknown --action value(s): %s", strings.Join(unknown, ", "))
	}
	if cfg.agentInstructions {
		if err := featureFlags(gf).RequireTasks(); err != nil {
			return bootstrapAgentReport{}, gid, nodeID, fmt.Errorf("agent instruction commands require ENTMOOT_ENABLE_FLEET=1 and ENTMOOT_ENABLE_TASKS=1")
		}
	}
	runnerCommand := agentRunnerCommand(runner, cfg.runnerCommand)
	runtime := collectRuntimeReport(gf, gf.data)
	report := bootstrapAgentReport{
		DryRun:            cfg.dryRun,
		Runner:            runner,
		RunnerCommand:     runnerCommand,
		AgentInstructions: cfg.agentInstructions,
		Runtime:           runtime,
		DefaultMoot:       buildBootstrapDefaultMootReport(gf, runtime, cfg.defaultMoot),
	}
	if cfg.agentInstructions {
		report.Warnings = append(report.Warnings, "run entmootd serve with ENTMOOT_AGENT_INSTRUCTIONS=1; bootstrap cannot change the environment of an already-running daemon")
		if runner == agentRunnerNone {
			report.Warnings = append(report.Warnings, "instruction commands can be queued, but no agent-commands runner is configured")
		}
		if report.Runtime.RunningDaemon != nil {
			report.Warnings = append(report.Warnings, "a running entmootd daemon was detected; restart or update its supervisor if instruction commands should be enabled")
		}
	}
	if liveMode != bootstrapLiveModeOff {
		topics := esphttp.NormalizeLiveTopicFilters([]string(cfg.topics))
		if len(topics) == 0 {
			topics = []string{"#"}
		}
		rawActions := []string(cfg.actions)
		actions := esphttp.NormalizeLiveActions(rawActions)
		if len(rawActions) > 0 && len(actions) == 0 {
			return bootstrapAgentReport{}, gid, nodeID, fmt.Errorf("live action list cannot be empty")
		}
		if liveMode == esphttp.LiveModeOperator && len(actions) == 0 && len(rawActions) == 0 {
			actions = esphttp.DefaultLiveActions()
		}
		if disabled := coordinationLiveActions(actions, featureFlags(gf)); len(disabled) > 0 {
			if len(rawActions) > 0 {
				return bootstrapAgentReport{}, gid, nodeID, fmt.Errorf("live action(s) require ENTMOOT_ENABLE_FLEET=1 and ENTMOOT_ENABLE_TASKS=1: %s", strings.Join(disabled, ", "))
			}
			actions = filterDisabledLiveActions(actions, featureFlags(gf))
		}
		report.Live = bootstrapAgentLiveReport{
			Enabled:           true,
			Group:             cfg.group,
			GroupID:           gid.String(),
			NodeID:            uint64(nodeID),
			Mode:              liveMode,
			TopicFilters:      topics,
			AllowedActions:    actions,
			MaxActionsPerScan: cfg.maxActionsPerScan,
			MaxActionBytes:    cfg.maxActionBytes,
		}
		if agentLiveModeRunsAdapter(liveMode) && runner == agentRunnerNone {
			report.Warnings = append(report.Warnings, "live mode can match events, but no runner is configured; use --runner custom --runner-command PATH or --runner openclaw")
		}
	}
	report.Commands = bootstrapAgentCommands(gf, report)
	return report, gid, nodeID, nil
}

func bootstrapAgentCommands(gf *globalFlags, report bootstrapAgentReport) []string {
	var out []string
	out = append(out, report.DefaultMoot.Commands...)
	coordinationEnv := bootstrapCoordinationEnv(gf, report)
	serve := envPrefixedCommand(coordinationEnv, entmootCommand(gf, report.Runtime, "serve"))
	out = append(out, serve)
	if report.AgentInstructions && report.Runner != agentRunnerNone && featureFlags(gf).RequireTasks() == nil {
		out = append(out, envPrefixedCommand(coordinationEnv, entmootCommand(gf, report.Runtime, "agent-commands", "watch", "-runner", report.RunnerCommand)))
	}
	if report.Live.Enabled {
		parts := []string{"agent-live", "run", "-group", report.Live.Group, "-node", strconv.FormatUint(report.Live.NodeID, 10)}
		if report.Runner != agentRunnerNone {
			parts = append(parts, "-runner", report.RunnerCommand)
		}
		command := envPrefixedCommand(coordinationEnv, entmootCommand(gf, report.Runtime, parts...))
		out = append(out, stackGatedCommand(bootstrapStackHelper(gf, report), bootstrapStackHelperEnv(gf, report), command))
	}
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
		report.Warnings = append(report.Warnings, "joining The Ent Moot is owner-approved but not performed implicitly by bootstrap output; run the command after reviewing descriptor and hide-IP/TURN status")
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

func bootstrapStackHelper(gf *globalFlags, report bootstrapAgentReport) string {
	stackHelper := strings.TrimSpace(report.Runtime.StackHelper)
	if stackHelper == "" || gf == nil {
		return ""
	}
	if strings.TrimSpace(gf.data) != agentEntmootDataPath || strings.TrimSpace(gf.socket) != agentPilotSocketPath {
		return ""
	}
	if gf.listenPort != 0 && gf.listenPort != 1004 {
		return ""
	}
	if logLevel := strings.TrimSpace(gf.logLevel); logLevel != "" && logLevel != "info" {
		return ""
	}
	if gf.traceGossipTransport || gf.traceReconcile {
		return ""
	}
	return stackHelper
}

func bootstrapStackHelperEnv(gf *globalFlags, report bootstrapAgentReport) []string {
	var env []string
	if gf != nil {
		env = append(env,
			shellEnvAssignment("ENTMOOT_DATA", gf.data),
			shellEnvAssignment("ENTMOOT_IDENTITY", gf.identity),
			shellEnvAssignment("PILOT_SOCKET", gf.socket),
			shellEnvAssignment("ENTMOOT_HIDE_IP", strconv.FormatBool(gf.hideIP)),
		)
	}
	if report.AgentInstructions {
		env = append(env, "ENTMOOT_AGENT_INSTRUCTIONS=1")
	}
	env = append(env, featureFlagEnv(gf)...)
	if report.Runner != agentRunnerNone && report.RunnerCommand != "" {
		env = append(env, shellEnvAssignment("ENTMOOT_AGENT_RUNNER", report.RunnerCommand))
	}
	return env
}

func bootstrapCoordinationEnv(gf *globalFlags, report bootstrapAgentReport) []string {
	var env []string
	if report.AgentInstructions {
		env = append(env, "ENTMOOT_AGENT_INSTRUCTIONS=1")
	}
	env = append(env, featureFlagEnv(gf)...)
	return env
}

func featureFlagEnv(gf *globalFlags) []string {
	var env []string
	if featureFlags(gf).FleetEnabled {
		env = append(env, "ENTMOOT_ENABLE_FLEET=1")
	}
	if featureFlags(gf).TasksEnabled {
		env = append(env, "ENTMOOT_ENABLE_TASKS=1")
	}
	return env
}

func envPrefixedCommand(env []string, command string) string {
	if len(env) == 0 {
		return command
	}
	return strings.Join(append(append([]string{}, env...), command), " ")
}

func shellEnvAssignment(name, value string) string {
	return name + "=" + shellQuoteArg(value)
}

func stackGatedCommand(stackHelper string, env []string, command string) string {
	stackHelper = strings.TrimSpace(stackHelper)
	if stackHelper == "" {
		return command
	}
	return strings.Join([]string{
		stackHelperCommand(stackHelper, "ensure", env),
		stackHelperCommand(stackHelper, "check", env),
		command,
	}, " && ")
}

func stackHelperCommand(stackHelper, mode string, env []string) string {
	parts := append([]string{}, env...)
	parts = append(parts, shellCommand(stackHelper, mode))
	return strings.Join(parts, " ")
}

func entmootCommand(gf *globalFlags, report runtimeReport, args ...string) string {
	binary := firstNonEmpty(report.AgentWrapper, report.Binary, "entmootd")
	parts := []string{
		binary,
		"-socket", gf.socket,
		"-identity", gf.identity,
		"-data", gf.data,
	}
	if gf.listenPort > 0 {
		parts = append(parts, "-listen-port", strconv.FormatUint(uint64(gf.listenPort), 10))
	}
	if strings.TrimSpace(gf.logLevel) != "" {
		parts = append(parts, "-log-level", gf.logLevel)
	}
	if gf.hideIP {
		parts = append(parts, "-hide-ip")
	}
	if gf.traceGossipTransport {
		parts = append(parts, "-trace-gossip-transport")
	}
	if gf.traceReconcile {
		parts = append(parts, "-trace-reconcile")
	}
	parts = append(parts, args...)
	return shellCommand(parts...)
}

func parseBootstrapLiveTarget(rawGroup string, rawNode uint64) (entmoot.GroupID, entmoot.NodeID, error) {
	if strings.TrimSpace(rawGroup) == "" {
		return entmoot.GroupID{}, 0, fmt.Errorf("--group is required when --live-mode is not off")
	}
	gid, err := decodeGroupID(rawGroup)
	if err != nil {
		return entmoot.GroupID{}, 0, err
	}
	if rawNode == 0 {
		return entmoot.GroupID{}, 0, fmt.Errorf("--node is required when --live-mode is not off")
	}
	if rawNode > uint64(^uint32(0)) {
		return entmoot.GroupID{}, 0, fmt.Errorf("--node is too large: %s", strconv.FormatUint(rawNode, 10))
	}
	return gid, entmoot.NodeID(rawNode), nil
}

func printBootstrapAgentReport(report bootstrapAgentReport) {
	if report.DryRun {
		fmt.Println("bootstrap agent: dry run")
	} else if report.Applied {
		fmt.Println("bootstrap agent: applied")
	} else {
		fmt.Println("bootstrap agent: ready")
	}
	fmt.Printf("runner: %s\n", report.Runner)
	if report.RunnerCommand != "" {
		fmt.Printf("runner_command: %s\n", report.RunnerCommand)
	}
	fmt.Printf("agent_instructions: %t\n", report.AgentInstructions)
	if report.Live.Enabled {
		fmt.Printf("live: enabled mode=%s group=%s node=%d\n", report.Live.Mode, report.Live.Group, report.Live.NodeID)
		fmt.Printf("live_topics: %s\n", strings.Join(report.Live.TopicFilters, ","))
		if len(report.Live.AllowedActions) > 0 {
			fmt.Printf("live_actions: %s\n", strings.Join(report.Live.AllowedActions, ","))
		}
	} else {
		fmt.Println("live: disabled")
	}
	for _, warning := range report.Warnings {
		fmt.Printf("warning: %s\n", warning)
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
	var err error
	cfg.runner, err = promptChoice(reader, "runner [none/custom/openclaw]", cfg.runner, map[string]bool{agentRunnerNone: true, agentRunnerCustom: true, agentRunnerOpenClaw: true})
	if err != nil {
		return cfg, err
	}
	if cfg.runner == agentRunnerCustom {
		cfg.runnerCommand, err = promptString(reader, "custom runner command", cfg.runnerCommand)
		if err != nil {
			return cfg, err
		}
	}
	cfg.agentInstructions, err = promptBool(reader, "enable instruction commands", cfg.agentInstructions)
	if err != nil {
		return cfg, err
	}
	cfg.defaultMoot, err = promptChoice(reader, "The Ent Moot [skip/join/decline]", cfg.defaultMoot, map[string]bool{
		"skip":                     true,
		"join":                     true,
		"decline":                  true,
		defaultMootConsentDeclined: true,
	})
	if err != nil {
		return cfg, err
	}
	cfg.liveMode, err = promptChoice(reader, "live mode [off/listen/reply_on_mention/converse/operator]", cfg.liveMode, map[string]bool{
		bootstrapLiveModeOff:           true,
		esphttp.LiveModeListen:         true,
		esphttp.LiveModeReplyOnMention: true,
		esphttp.LiveModeConverse:       true,
		esphttp.LiveModeOperator:       true,
	})
	if err != nil {
		return cfg, err
	}
	if cfg.liveMode != bootstrapLiveModeOff {
		cfg.group, err = promptString(reader, "live group id", cfg.group)
		if err != nil {
			return cfg, err
		}
		rawNode, err := promptString(reader, "live node id", nodeDefault(cfg.node))
		if err != nil {
			return cfg, err
		}
		cfg.node, err = strconv.ParseUint(strings.TrimSpace(rawNode), 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("live node id: %w", err)
		}
		rawTopics, err := promptString(reader, "live topics comma-separated", strings.Join(cfg.topics, ","))
		if err != nil {
			return cfg, err
		}
		cfg.topics = repeatedStringFlag(parseTopicList(rawTopics))
		if cfg.liveMode == esphttp.LiveModeOperator {
			rawActions, err := promptString(reader, "operator actions comma-separated; blank means all", strings.Join(cfg.actions, ","))
			if err != nil {
				return cfg, err
			}
			cfg.actions = repeatedStringFlag(parseTopicList(rawActions))
		}
	}
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

func promptBool(reader *bufio.Reader, label string, current bool) (bool, error) {
	def := "n"
	if current {
		def = "y"
	}
	raw, err := promptString(reader, label+" [y/n]", def)
	if err != nil {
		return false, err
	}
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "y", "yes", "true", "1":
		return true, nil
	case "n", "no", "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean for %s", label)
	}
}

func nodeDefault(node uint64) string {
	if node == 0 {
		return ""
	}
	return strconv.FormatUint(node, 10)
}
