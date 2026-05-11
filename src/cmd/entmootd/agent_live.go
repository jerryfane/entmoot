package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
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

type agentLiveRunBinding struct {
	Config   esphttp.LiveAgentConfig   `json:"config"`
	Presence esphttp.LiveAgentPresence `json:"presence"`
}

type agentLiveRunGroup struct {
	GroupID entmoot.GroupID
	Mode    string
}

type agentLiveRunGroupScan struct {
	GroupID entmoot.GroupID     `json:"group_id"`
	Scan    agentLiveScanResult `json:"scan"`
}

type agentLiveRunState interface {
	esphttp.StateStore
	esphttp.GroupMetadataStore
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
	var allGroups bool
	var tags repeatedStringFlag
	var once, jsonOut bool
	fs := flag.NewFlagSet("agent-live run", flag.ContinueOnError)
	fs.StringVar(&rawGroup, "group", "", "base64 moot group id")
	fs.Uint64Var(&node, "node", 0, "local Pilot node id")
	fs.BoolVar(&allGroups, "all-groups", false, "run every enabled live config for this node")
	fs.Var(&tags, "tag", "moot metadata tag filter for -all-groups; may be repeated")
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
	nodeID, ok := parseAgentLiveNode("agent-live run", node)
	if !ok {
		return exitInvalidArgument
	}
	if strings.TrimSpace(rawGroup) != "" && allGroups {
		fmt.Fprintln(os.Stderr, "agent-live run: use either -group or -all-groups, not both")
		return exitInvalidArgument
	}
	if strings.TrimSpace(rawGroup) != "" {
		if _, err := decodeGroupID(rawGroup); err != nil {
			fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
			return exitInvalidArgument
		}
	}
	if strings.TrimSpace(rawGroup) == "" && !allGroups {
		fmt.Fprintln(os.Stderr, "agent-live run: -group is required unless -all-groups is set")
		return exitInvalidArgument
	}
	if len(tags) > 0 && !allGroups {
		fmt.Fprintln(os.Stderr, "agent-live run: -tag requires -all-groups")
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
	groups, err := agentLiveRunGroups(ctx, state, rawGroup, allGroups, nodeID, []string(tags))
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitTransport
	}
	if err := validateAgentLiveAllGroupsLease(allGroups, once, groups, interval, timeout, lease); err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitInvalidArgument
	}
	runCfg := agentLiveRuntimeConfig{
		nodeID:  nodeID,
		runner:  strings.TrimSpace(runner),
		timeout: timeout,
		limit:   scanLimit,
	}
	bindings, err := renewAgentLiveRunBindings(ctx, state, groups, nodeID, lease, !allGroups)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitTransport
	}
	if once {
		if jsonOut {
			if allGroups {
				return printJSON(bindings)
			}
			if len(bindings) > 0 {
				return printJSON(bindings[0].Presence)
			}
			return printJSON([]agentLiveRunBinding{})
		}
		if allGroups {
			fmt.Fprintf(os.Stdout, "renewed live presence for node %d in %d group(s)\n", nodeID, len(bindings))
		} else if len(bindings) > 0 {
			fmt.Fprintf(os.Stdout, "renewed live presence for node %d until %d\n", nodeID, bindings[0].Presence.LeaseUntilMS)
		}
		return exitOK
	}
	msgStore, err := store.OpenSQLite(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitTransport
	}
	defer msgStore.Close()
	bindings, scans, err := scanAgentLiveRunGroups(ctx, gf, state, msgStore, groups, nodeID, lease, !allGroups, runCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
		return exitTransport
	}
	if jsonOut {
		_ = printAgentLiveRunJSON(allGroups, bindings, scans)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return exitOK
		case <-ticker.C:
			if allGroups {
				groups, err = agentLiveRunGroups(ctx, state, rawGroup, allGroups, nodeID, []string(tags))
				if err != nil {
					fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
					return exitTransport
				}
				if err := validateAgentLiveAllGroupsLease(allGroups, once, groups, interval, timeout, lease); err != nil {
					fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
					return exitInvalidArgument
				}
			}
			bindings, scans, err = scanAgentLiveRunGroups(ctx, gf, state, msgStore, groups, nodeID, lease, !allGroups, runCfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "agent-live run: %v\n", err)
				return exitTransport
			}
			if jsonOut {
				_ = printAgentLiveRunJSON(allGroups, bindings, scans)
			}
		}
	}
}

func agentLiveRunGroups(ctx context.Context, state agentLiveRunState, rawGroup string, allGroups bool, nodeID entmoot.NodeID, rawTags []string) ([]agentLiveRunGroup, error) {
	if strings.TrimSpace(rawGroup) != "" {
		gid, err := decodeGroupID(rawGroup)
		if err != nil {
			return nil, err
		}
		return []agentLiveRunGroup{{GroupID: gid}}, nil
	}
	if !allGroups {
		return nil, errors.New("-group is required unless -all-groups is set")
	}
	tags := normalizeGroupTags(rawTags)
	configs, err := state.ListLiveAgentConfigsForNode(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	out := make([]agentLiveRunGroup, 0, len(configs))
	seen := make(map[entmoot.GroupID]struct{}, len(configs))
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		if _, ok := seen[cfg.GroupID]; ok {
			continue
		}
		match, err := agentLiveGroupMatchesTags(ctx, state, cfg.GroupID, tags)
		if err != nil {
			return nil, err
		}
		if match {
			seen[cfg.GroupID] = struct{}{}
			out = append(out, agentLiveRunGroup{GroupID: cfg.GroupID, Mode: cfg.Mode})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].GroupID[:], out[j].GroupID[:]) < 0
	})
	return out, nil
}

func validateAgentLiveAllGroupsLease(allGroups, once bool, groups []agentLiveRunGroup, interval, timeout, lease time.Duration) error {
	if !allGroups || once || len(groups) == 0 {
		return nil
	}
	worstWindow := agentLiveAllGroupsWorstLeaseWindow(groups, interval, timeout)
	if worstWindow < lease {
		return nil
	}
	return fmt.Errorf("-lease must be greater than the worst-case all-groups presence renewal window (%s for %d groups)", worstWindow, len(groups))
}

func agentLiveAllGroupsWorstLeaseWindow(groups []agentLiveRunGroup, interval, timeout time.Duration) time.Duration {
	worst := interval
	for i, group := range groups {
		window := interval
		for j, other := range groups {
			if i == j {
				continue
			}
			if agentLiveModeRunsAdapter(other.Mode) {
				window += timeout
			}
		}
		if agentLiveModeRunsAdapter(group.Mode) && timeout > window {
			window = timeout
		}
		if window > worst {
			worst = window
		}
	}
	return worst
}

func agentLiveModeRunsAdapter(mode string) bool {
	normalized := esphttp.NormalizeLiveMode(mode)
	return normalized != "" && normalized != esphttp.LiveModeListen
}

func agentLiveGroupMatchesTags(ctx context.Context, state esphttp.GroupMetadataStore, gid entmoot.GroupID, tags []string) (bool, error) {
	if len(tags) == 0 {
		return true, nil
	}
	if state == nil {
		return false, nil
	}
	raw, ok, err := state.GetGroupMetadata(ctx, gid)
	if err != nil || !ok {
		return false, err
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return false, err
	}
	groupTags := metadataTags(meta["tags"])
	if len(groupTags) == 0 {
		return false, nil
	}
	have := make(map[string]struct{}, len(groupTags))
	for _, tag := range groupTags {
		have[strings.TrimSpace(tag)] = struct{}{}
	}
	for _, tag := range tags {
		if _, ok := have[tag]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func renewAgentLiveRunBindings(ctx context.Context, state esphttp.StateStore, groups []agentLiveRunGroup, nodeID entmoot.NodeID, lease time.Duration, requireAll bool) ([]agentLiveRunBinding, error) {
	out := make([]agentLiveRunBinding, 0, len(groups))
	for _, group := range groups {
		binding, ok, err := renewAgentLiveRunBinding(ctx, state, group.GroupID, nodeID, lease, requireAll)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, binding)
	}
	return out, nil
}

func renewAgentLiveRunBinding(ctx context.Context, state esphttp.StateStore, gid entmoot.GroupID, nodeID entmoot.NodeID, lease time.Duration, requireAll bool) (agentLiveRunBinding, bool, error) {
	cfg, found, err := state.GetLiveAgentConfig(ctx, gid, nodeID)
	if err != nil {
		return agentLiveRunBinding{}, false, err
	}
	if !found || !cfg.Enabled {
		if requireAll {
			return agentLiveRunBinding{}, false, fmt.Errorf("live mode is not enabled for node %d in group %s", nodeID, gid.String())
		}
		return agentLiveRunBinding{}, false, nil
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
	if err != nil {
		return agentLiveRunBinding{}, false, err
	}
	return agentLiveRunBinding{Config: cfg, Presence: presence}, true, nil
}

func scanAgentLiveRunGroups(ctx context.Context, gf *globalFlags, state esphttp.StateStore, msgStore store.MessageStore, groups []agentLiveRunGroup, nodeID entmoot.NodeID, lease time.Duration, requireAll bool, runCfg agentLiveRuntimeConfig) ([]agentLiveRunBinding, []agentLiveRunGroupScan, error) {
	bindings := make([]agentLiveRunBinding, 0, len(groups))
	scans := make([]agentLiveRunGroupScan, 0, len(groups))
	for _, group := range groups {
		binding, ok, err := renewAgentLiveRunBinding(ctx, state, group.GroupID, nodeID, lease, requireAll)
		if err != nil {
			return bindings, scans, err
		}
		if !ok {
			continue
		}
		groupRunCfg := runCfg
		groupRunCfg.groupID = binding.Config.GroupID
		scan, err := runAgentLiveScan(ctx, gf, state, msgStore, binding.Config, groupRunCfg)
		if err != nil {
			return bindings, scans, err
		}
		refreshed, ok, err := renewAgentLiveRunBinding(ctx, state, group.GroupID, nodeID, lease, requireAll)
		if err != nil {
			return bindings, scans, err
		}
		if ok {
			binding = refreshed
		}
		bindings = append(bindings, binding)
		scans = append(scans, agentLiveRunGroupScan{GroupID: binding.Config.GroupID, Scan: scan})
	}
	return bindings, scans, nil
}

func printAgentLiveRunJSON(allGroups bool, bindings []agentLiveRunBinding, scans []agentLiveRunGroupScan) int {
	if allGroups {
		return printJSON(map[string]any{"presence": bindings, "scans": scans})
	}
	var presence esphttp.LiveAgentPresence
	var scan agentLiveScanResult
	if len(bindings) > 0 {
		presence = bindings[0].Presence
	}
	if len(scans) > 0 {
		scan = scans[0].Scan
	}
	return printJSON(map[string]any{"presence": presence, "scan": scan})
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
	nodeID, ok := parseAgentLiveNode(command, rawNode)
	if !ok {
		return entmoot.GroupID{}, 0, false
	}
	return gid, nodeID, true
}

func parseAgentLiveNode(command string, rawNode uint64) (entmoot.NodeID, bool) {
	if rawNode == 0 {
		fmt.Fprintf(os.Stderr, "%s: -node is required\n", command)
		return 0, false
	}
	if rawNode > uint64(^uint32(0)) {
		fmt.Fprintf(os.Stderr, "%s: -node is too large: %s\n", command, strconv.FormatUint(rawNode, 10))
		return 0, false
	}
	return entmoot.NodeID(rawNode), true
}
