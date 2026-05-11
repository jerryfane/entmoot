package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/store"
)

type agentLiveConfig struct {
	group             string
	node              uint64
	mode              string
	topics            repeatedStringFlag
	actions           repeatedStringFlag
	maxActionsPerScan int
	maxActionBytes    int
	json              bool
}

func cmdAgentLive(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd agent-live <enable|disable|status|run> [flags]")
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "agent-live: missing op")
			return exitInvalidArgument
		}
		return exitOK
	}
	switch args[0] {
	case "enable":
		return cmdAgentLiveEnable(gf, args[1:])
	case "disable":
		return cmdAgentLiveDisable(gf, args[1:])
	case "status":
		return cmdAgentLiveStatus(gf, args[1:])
	case "run":
		return cmdAgentLiveRun(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "agent-live: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdAgentLiveEnable(gf *globalFlags, args []string) int {
	cfg := agentLiveConfig{mode: esphttp.LiveModeReplyOnMention}
	fs := flag.NewFlagSet("agent-live enable", flag.ContinueOnError)
	fs.StringVar(&cfg.group, "group", "", "base64 moot group id")
	fs.Uint64Var(&cfg.node, "node", 0, "local Pilot node id")
	fs.StringVar(&cfg.mode, "mode", cfg.mode, "live mode: listen, reply_on_mention, converse, operator")
	fs.Var(&cfg.topics, "topic", "topic filter; may be repeated")
	fs.Var(&cfg.actions, "action", "operator action; may be repeated, defaults to all operator actions")
	fs.IntVar(&cfg.maxActionsPerScan, "max-actions", 0, "optional maximum actions to apply per scan; 0 means unlimited")
	fs.IntVar(&cfg.maxActionBytes, "max-action-bytes", 0, "optional maximum bytes per action message; 0 means unlimited")
	fs.BoolVar(&cfg.json, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, nodeID, ok := parseAgentLiveTarget("agent-live enable", cfg.group, cfg.node)
	if !ok {
		return exitInvalidArgument
	}
	mode := esphttp.NormalizeLiveMode(cfg.mode)
	if mode == "" {
		fmt.Fprintln(os.Stderr, "agent-live enable: invalid -mode")
		return exitInvalidArgument
	}
	if cfg.maxActionsPerScan < 0 || cfg.maxActionBytes < 0 {
		fmt.Fprintln(os.Stderr, "agent-live enable: -max-actions and -max-action-bytes must be non-negative")
		return exitInvalidArgument
	}
	topics := esphttp.NormalizeLiveTopicFilters([]string(cfg.topics))
	if len(topics) == 0 {
		topics = []string{"#"}
	}
	if unknown := esphttp.UnknownLiveActions([]string(cfg.actions)); len(unknown) > 0 {
		fmt.Fprintf(os.Stderr, "agent-live enable: unknown -action value(s): %s\n", strings.Join(unknown, ", "))
		return exitInvalidArgument
	}
	actions := esphttp.NormalizeLiveActions([]string(cfg.actions))
	if mode == esphttp.LiveModeOperator && len(actions) == 0 {
		actions = esphttp.DefaultLiveActions()
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live enable: %v\n", err)
		return exitTransport
	}
	defer state.Close()
	rec, err := state.UpsertLiveAgentConfig(context.Background(), esphttp.LiveAgentConfig{
		GroupID:           gid,
		NodeID:            nodeID,
		Enabled:           true,
		Mode:              mode,
		TopicFilters:      topics,
		AllowedActions:    actions,
		MaxActionsPerScan: cfg.maxActionsPerScan,
		MaxActionBytes:    cfg.maxActionBytes,
		UpdatedAtMS:       time.Now().UnixMilli(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live enable: %v\n", err)
		return exitInvalidArgument
	}
	if cfg.json {
		return printJSON(rec)
	}
	fmt.Fprintf(os.Stdout, "enabled live %s for node %d in group %s\n", rec.Mode, rec.NodeID, cfg.group)
	return exitOK
}

func cmdAgentLiveDisable(gf *globalFlags, args []string) int {
	cfg := agentLiveConfig{}
	fs := flag.NewFlagSet("agent-live disable", flag.ContinueOnError)
	fs.StringVar(&cfg.group, "group", "", "base64 moot group id")
	fs.Uint64Var(&cfg.node, "node", 0, "local Pilot node id")
	fs.BoolVar(&cfg.json, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, nodeID, ok := parseAgentLiveTarget("agent-live disable", cfg.group, cfg.node)
	if !ok {
		return exitInvalidArgument
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live disable: %v\n", err)
		return exitTransport
	}
	defer state.Close()
	if err := state.DeleteLiveAgentConfig(context.Background(), gid, nodeID, time.Now().UnixMilli()); err != nil {
		fmt.Fprintf(os.Stderr, "agent-live disable: %v\n", err)
		return exitTransport
	}
	if cfg.json {
		return printJSON(map[string]any{"group_id": gid, "node_id": nodeID, "enabled": false})
	}
	fmt.Fprintf(os.Stdout, "disabled live mode for node %d in group %s\n", nodeID, cfg.group)
	return exitOK
}

func cmdAgentLiveStatus(gf *globalFlags, args []string) int {
	var rawGroup string
	jsonOut := false
	fs := flag.NewFlagSet("agent-live status", flag.ContinueOnError)
	fs.StringVar(&rawGroup, "group", "", "base64 moot group id")
	fs.BoolVar(&jsonOut, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if strings.TrimSpace(rawGroup) == "" {
		fmt.Fprintln(os.Stderr, "agent-live status: -group is required")
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(rawGroup)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live status: %v\n", err)
		return exitInvalidArgument
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live status: %v\n", err)
		return exitTransport
	}
	defer state.Close()
	configs, err := state.ListLiveAgentConfigs(context.Background(), gid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live status: %v\n", err)
		return exitTransport
	}
	presence, err := state.ListLiveAgentPresence(context.Background(), gid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live status: %v\n", err)
		return exitTransport
	}
	states := esphttp.LiveAgentStatesByNode(configs, presence, time.Now().UnixMilli())
	if jsonOut {
		return printJSON(map[string]any{"group_id": gid, "configs": configs, "presence": presence, "members": states})
	}
	if len(states) == 0 {
		fmt.Fprintln(os.Stdout, "no live agents configured")
		return exitOK
	}
	for nodeID, state := range states {
		fmt.Fprintf(os.Stdout, "node=%d mode=%s status=%s topics=%s\n", nodeID, state.Mode, state.Status, strings.Join(state.TopicFilters, ","))
	}
	return exitOK
}

func cmdAgentLiveRun(gf *globalFlags, args []string) int {
	var rawGroup string
	var node uint64
	var interval, lease time.Duration
	var runner string
	var timeout time.Duration
	var scanLimit int
	var once, jsonOut bool
	fs := flag.NewFlagSet("agent-live run", flag.ContinueOnError)
	fs.StringVar(&rawGroup, "group", "", "base64 moot group id")
	fs.Uint64Var(&node, "node", 0, "local Pilot node id")
	fs.DurationVar(&interval, "interval", 10*time.Second, "heartbeat interval")
	fs.DurationVar(&lease, "lease", 45*time.Second, "presence lease duration")
	fs.StringVar(&runner, "runner", firstNonEmpty(os.Getenv("ENTMOOT_AGENT_RUNNER"), os.Getenv("ENTMOOT_AGENT_COMMAND_HOOK")), "agent runtime adapter executable, or \"openclaw\" for the built-in OpenClaw adapter")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "agent runtime timeout; must be shorter than -lease")
	fs.IntVar(&scanLimit, "limit", 20, "maximum matched messages to send to the agent per scan")
	fs.BoolVar(&once, "once", false, "renew presence once")
	fs.BoolVar(&jsonOut, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, nodeID, ok := parseAgentLiveTarget("agent-live run", rawGroup, node)
	if !ok {
		return exitInvalidArgument
	}
	if interval <= 0 || lease <= 0 {
		fmt.Fprintln(os.Stderr, "agent-live run: -interval and -lease must be positive")
		return exitInvalidArgument
	}
	if timeout <= 0 {
		fmt.Fprintln(os.Stderr, "agent-live run: -timeout must be positive")
		return exitInvalidArgument
	}
	if timeout >= lease {
		fmt.Fprintln(os.Stderr, "agent-live run: -timeout must be shorter than -lease")
		return exitInvalidArgument
	}
	if scanLimit <= 0 {
		fmt.Fprintln(os.Stderr, "agent-live run: -limit must be positive")
		return exitInvalidArgument
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitTransport
	}
	defer state.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runCfg := agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		runner:  strings.TrimSpace(runner),
		timeout: timeout,
		limit:   scanLimit,
	}
	renew := func() (esphttp.LiveAgentConfig, esphttp.LiveAgentPresence, error) {
		cfg, found, err := state.GetLiveAgentConfig(ctx, gid, nodeID)
		if err != nil {
			return esphttp.LiveAgentConfig{}, esphttp.LiveAgentPresence{}, err
		}
		if !found || !cfg.Enabled {
			return esphttp.LiveAgentConfig{}, esphttp.LiveAgentPresence{}, fmt.Errorf("live mode is not enabled for node %d in group %s", nodeID, rawGroup)
		}
		now := time.Now().UnixMilli()
		presence, err := state.UpsertLiveAgentPresence(ctx, esphttp.LiveAgentPresence{
			GroupID:      gid,
			NodeID:       nodeID,
			Status:       esphttp.LiveStatusOnline,
			Mode:         cfg.Mode,
			TopicFilters: cfg.TopicFilters,
			LastSeenAtMS: now,
			LeaseUntilMS: time.UnixMilli(now).Add(lease).UnixMilli(),
			UpdatedAtMS:  now,
		})
		return cfg, presence, err
	}
	cfg, last, err := renew()
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitTransport
	}
	if once {
		if jsonOut {
			return printJSON(last)
		}
		fmt.Fprintf(os.Stdout, "renewed live presence for node %d until %d\n", nodeID, last.LeaseUntilMS)
		return exitOK
	}
	msgStore, err := store.OpenSQLite(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitTransport
	}
	defer msgStore.Close()
	scan, err := runAgentLiveScan(ctx, gf, state, msgStore, cfg, runCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitTransport
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return exitOK
		case <-ticker.C:
			cfg, last, err = renew()
			if err != nil {
				fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
				return exitTransport
			}
			scan, err = runAgentLiveScan(ctx, gf, state, msgStore, cfg, runCfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
				return exitTransport
			}
			if jsonOut {
				_ = printJSON(map[string]any{"presence": last, "scan": scan})
			}
		}
	}
}

func parseAgentLiveTarget(command, rawGroup string, rawNode uint64) (entmoot.GroupID, entmoot.NodeID, bool) {
	if strings.TrimSpace(rawGroup) == "" {
		fmt.Fprintf(os.Stderr, "%s: -group is required\n", command)
		return entmoot.GroupID{}, 0, false
	}
	gid, err := decodeGroupID(rawGroup)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
		return entmoot.GroupID{}, 0, false
	}
	if rawNode == 0 {
		fmt.Fprintf(os.Stderr, "%s: -node is required\n", command)
		return entmoot.GroupID{}, 0, false
	}
	if rawNode > uint64(^uint32(0)) {
		fmt.Fprintf(os.Stderr, "%s: -node is too large: %s\n", command, strconv.FormatUint(rawNode, 10))
		return entmoot.GroupID{}, 0, false
	}
	return gid, entmoot.NodeID(rawNode), true
}
