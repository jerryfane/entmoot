package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/defaultmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/store"
)

const (
	defaultMootConsentUnconfigured = "unconfigured"
	defaultMootConsentJoined       = "joined"
	defaultMootConsentDeclined     = "declined"
)

type defaultMootLocalState struct {
	Consent              string `json:"consent"`
	GroupID              string `json:"group_id,omitempty"`
	DescriptorURL        string `json:"descriptor_url,omitempty"`
	DescriptorIssuedAtMS int64  `json:"descriptor_issued_at_ms,omitempty"`
	UpdatedAtMS          int64  `json:"updated_at_ms"`
}

type defaultMootStatusReport struct {
	Consent               string                             `json:"consent"`
	DescriptorURL         string                             `json:"descriptor_url"`
	DescriptorVerified    bool                               `json:"descriptor_verified"`
	DescriptorError       string                             `json:"descriptor_error,omitempty"`
	GroupID               string                             `json:"group_id,omitempty"`
	Joined                bool                               `json:"joined"`
	Policy                *entpolicy.Policy                  `json:"policy,omitempty"`
	PolicySummary         string                             `json:"policy_summary,omitempty"`
	LiveEnabled           bool                               `json:"live_enabled"`
	AllowedLiveActions    []string                           `json:"allowed_live_actions,omitempty"`
	HideIPRequested       bool                               `json:"hide_ip_requested"`
	TURNAvailable         bool                               `json:"turn_available"`
	TURNEndpoint          string                             `json:"turn_endpoint,omitempty"`
	TURNError             string                             `json:"turn_error,omitempty"`
	LastLocalMessageAtMS  int64                              `json:"last_local_message_at_ms,omitempty"`
	LocalState            defaultMootLocalState              `json:"local_state"`
	RecommendedLiveConfig *defaultmoot.RecommendedLiveConfig `json:"recommended_live_config,omitempty"`
}

func cmdDefaultMoot(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd default-moot <status|join|decline|leave|live> [flags]")
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "default-moot: missing op")
			return exitInvalidArgument
		}
		return exitOK
	}
	switch args[0] {
	case "status":
		return cmdDefaultMootStatus(gf, args[1:])
	case "join":
		return cmdDefaultMootJoin(gf, args[1:])
	case "decline":
		return cmdDefaultMootDecline(gf, args[1:])
	case "leave":
		return cmdDefaultMootLeave(gf, args[1:])
	case "live":
		return cmdDefaultMootLive(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "default-moot: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdDefaultMootStatus(gf *globalFlags, args []string) int {
	jsonOut := false
	fs := flag.NewFlagSet("default-moot status", flag.ContinueOnError)
	fs.BoolVar(&jsonOut, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	report := buildDefaultMootStatus(context.Background(), gf)
	if jsonOut {
		return printJSON(report)
	}
	printDefaultMootStatus(report)
	return exitOK
}

func cmdDefaultMootJoin(gf *globalFlags, args []string) int {
	dryRun := false
	jsonOut := false
	intro := ""
	timeout := defaultJoinTimeout
	fs := flag.NewFlagSet("default-moot join", flag.ContinueOnError)
	fs.BoolVar(&dryRun, "dry-run", false, "verify descriptor and print the join target without joining")
	fs.BoolVar(&jsonOut, "json", false, "print JSON summary")
	fs.StringVar(&intro, "intro", "", "optional introduction message to publish on the introductions topic after joining")
	fs.DurationVar(&timeout, "timeout", defaultJoinTimeout, "join bootstrap and live-daemon IPC response deadline")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	desc, cfg, err := loadDefaultMootDescriptor(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot join: %v\n", err)
		return exitTransport
	}
	intro = strings.TrimSpace(intro)
	if dryRun {
		return printDefaultMootJoinResult(jsonOut, desc, cfg.URL, "verified", "")
	}
	joinInput, cleanup, err := writeDefaultMootJoinInput(desc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot join: %v\n", err)
		return exitTransport
	}
	defer cleanup()
	code, err := runWithStdoutDiscarded(func() int {
		return cmdJoin(gf, []string{"-timeout", timeout.String(), joinInput})
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot join: %v\n", err)
		return exitTransport
	}
	if code != exitOK {
		return code
	}
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent:              defaultMootConsentJoined,
		GroupID:              desc.GroupID.String(),
		DescriptorURL:        cfg.URL,
		DescriptorIssuedAtMS: desc.IssuedAtMS,
		UpdatedAtMS:          time.Now().UnixMilli(),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "default-moot join: persist consent: %v\n", err)
		return exitTransport
	}
	introPublishReady := intro != "" && controlSocketAlive(controlSocketPath(gf.data), 200*time.Millisecond)
	introStatus, err := publishDefaultMootIntro(context.Background(), gf, desc.GroupID, intro, introPublishReady)
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot join: joined, but publish intro failed: %v\n", err)
		return exitControlUnavail
	}
	return printDefaultMootJoinResult(jsonOut, desc, cfg.URL, "joined", introStatus)
}

func cmdDefaultMootDecline(gf *globalFlags, args []string) int {
	jsonOut := false
	fs := flag.NewFlagSet("default-moot decline", flag.ContinueOnError)
	fs.BoolVar(&jsonOut, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	state, err := defaultMootDeclinedLocalState(context.Background(), gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot decline: %v\n", err)
		return exitTransport
	}
	if err := saveDefaultMootLocalState(gf.data, state); err != nil {
		fmt.Fprintf(os.Stderr, "default-moot decline: %v\n", err)
		return exitTransport
	}
	if jsonOut {
		return printJSON(state)
	}
	fmt.Fprintln(os.Stdout, "declined The Ent Moot")
	return exitOK
}

func cmdDefaultMootLeave(gf *globalFlags, args []string) int {
	jsonOut := false
	fs := flag.NewFlagSet("default-moot leave", flag.ContinueOnError)
	fs.BoolVar(&jsonOut, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := defaultMootGroupIDForLocalCleanup(context.Background(), gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot leave: %v\n", err)
		return exitTransport
	}
	disabled, err := disableDefaultMootLiveConfigs(gf.data, gid, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot leave: disable live: %v\n", err)
		return exitTransport
	}
	state := defaultMootLocalState{Consent: defaultMootConsentDeclined, GroupID: gid.String(), UpdatedAtMS: time.Now().UnixMilli()}
	if err := saveDefaultMootLocalState(gf.data, state); err != nil {
		fmt.Fprintf(os.Stderr, "default-moot leave: persist consent: %v\n", err)
		return exitTransport
	}
	out := map[string]any{"status": "local_participation_disabled", "group_id": gid, "live_configs_disabled": disabled}
	if controlSocketAlive(controlSocketPath(gf.data), 200*time.Millisecond) {
		out["status"] = "restart_required"
		out["warning"] = "local live configs were disabled and declined consent was saved, but an active entmootd daemon may still be serving this group; restart entmootd serve to unload it"
		if jsonOut {
			_ = printJSON(out)
		} else {
			fmt.Fprintf(os.Stderr, "default-moot leave: %s\n", out["warning"])
		}
		return exitControlUnavail
	}
	if jsonOut {
		return printJSON(out)
	}
	fmt.Fprintf(os.Stdout, "disabled local live participation for %s; remote roster removal is not performed by this command\n", gid)
	return exitOK
}

func cmdDefaultMootLive(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd default-moot live <on|off> [flags]")
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "default-moot live: missing op")
			return exitInvalidArgument
		}
		return exitOK
	}
	switch args[0] {
	case "on":
		return cmdDefaultMootLiveOn(gf, args[1:])
	case "off":
		return cmdDefaultMootLiveOff(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "default-moot live: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdDefaultMootLiveOn(gf *globalFlags, args []string) int {
	var node uint64
	jsonOut := false
	fs := flag.NewFlagSet("default-moot live on", flag.ContinueOnError)
	fs.Uint64Var(&node, "node", 0, "local Pilot node id")
	fs.BoolVar(&jsonOut, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	nodeID, ok := parseAgentLiveNode("default-moot live on", node)
	if !ok {
		return exitInvalidArgument
	}
	desc, _, err := loadDefaultMootDescriptor(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot live on: %v\n", err)
		return exitTransport
	}
	if defaultMootLocallyDeclined(gf.data, desc.GroupID) {
		fmt.Fprintln(os.Stderr, "default-moot live on: The Ent Moot is locally declined; run default-moot join first")
		return exitNotMember
	}
	if err := validateDefaultMootLiveMembership(gf, desc.GroupID, nodeID); err != nil {
		fmt.Fprintf(os.Stderr, "default-moot live on: %v\n", err)
		if errors.Is(err, errLocalGroupNotMember) || errors.Is(err, errLocalGroupIdentityMismatch) {
			return exitNotMember
		}
		return exitTransport
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot live on: %v\n", err)
		return exitTransport
	}
	defer state.Close()
	rec, err := enableAgentLiveConfig(context.Background(), state, enableAgentLiveConfigOptions{
		groupID:           desc.GroupID,
		nodeID:            nodeID,
		mode:              desc.RecommendedLiveConfig.Mode,
		topics:            []string{"chat/general"},
		actions:           desc.RecommendedLiveConfig.AllowedActions,
		maxActionsPerScan: desc.RecommendedLiveConfig.MaxActions,
		maxActionBytes:    int(desc.RecommendedLiveConfig.MaxActionBytes),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot live on: %v\n", err)
		return exitInvalidArgument
	}
	if jsonOut {
		return printJSON(rec)
	}
	fmt.Fprintf(os.Stdout, "enabled The Ent Moot live replies for node %d\n", nodeID)
	return exitOK
}

func validateDefaultMootLiveMembership(gf *globalFlags, gid entmoot.GroupID, nodeID entmoot.NodeID) error {
	s, err := setup(gf)
	if err != nil {
		return err
	}
	rlog, ok, err := openExistingRosterLog(s.dataDir, gid)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: The Ent Moot roster is missing for %s", errLocalGroupNotMember, gid.String())
	}
	defer rlog.Close()
	if !rosterHasLocalNodeIdentity(rlog, nodeID, s.identity.PublicKey) {
		if _, ok := rlog.MemberInfo(nodeID); ok {
			return errLocalGroupIdentityMismatch
		}
		return errLocalGroupNotMember
	}
	return nil
}

func cmdDefaultMootLiveOff(gf *globalFlags, args []string) int {
	var node uint64
	jsonOut := false
	fs := flag.NewFlagSet("default-moot live off", flag.ContinueOnError)
	fs.Uint64Var(&node, "node", 0, "local Pilot node id; omit to disable all local The Ent Moot live configs")
	fs.BoolVar(&jsonOut, "json", false, "print JSON summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := defaultMootGroupIDForLocalCleanup(context.Background(), gf.data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot live off: %v\n", err)
		return exitTransport
	}
	var nodeID entmoot.NodeID
	if node != 0 {
		parsed, ok := parseAgentLiveNode("default-moot live off", node)
		if !ok {
			return exitInvalidArgument
		}
		nodeID = parsed
	}
	disabled, err := disableDefaultMootLiveConfigs(gf.data, gid, nodeID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "default-moot live off: %v\n", err)
		return exitTransport
	}
	if jsonOut {
		return printJSON(map[string]any{"group_id": gid, "disabled": disabled})
	}
	fmt.Fprintf(os.Stdout, "disabled %d The Ent Moot live config(s)\n", disabled)
	return exitOK
}

func loadDefaultMootDescriptor(ctx context.Context) (defaultmoot.Descriptor, defaultmoot.Config, error) {
	cfg, err := defaultmoot.LoadConfigFromEnv()
	if err != nil {
		return defaultmoot.Descriptor{}, defaultmoot.Config{}, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	desc, err := defaultmoot.FetchAndVerify(ctx, client, cfg)
	return desc, cfg, err
}

func defaultMootGroupIDForLocalCleanup(ctx context.Context, dataDir string) (entmoot.GroupID, error) {
	if state, ok := loadDefaultMootLocalState(dataDir); ok && strings.TrimSpace(state.GroupID) != "" {
		gid, decErr := decodeGroupID(state.GroupID)
		if decErr != nil {
			return entmoot.GroupID{}, fmt.Errorf("local default moot group id: %w", decErr)
		}
		return gid, nil
	}
	desc, _, err := loadDefaultMootDescriptor(ctx)
	if err == nil {
		return desc.GroupID, nil
	}
	return entmoot.GroupID{}, err
}

func buildDefaultMootStatus(ctx context.Context, gf *globalFlags) defaultMootStatusReport {
	local, _ := loadDefaultMootLocalState(gf.data)
	if strings.TrimSpace(local.Consent) == "" {
		local.Consent = defaultMootConsentUnconfigured
	}
	report := defaultMootStatusReport{
		Consent:         local.Consent,
		HideIPRequested: gf.hideIP,
		LocalState:      local,
	}
	cfg, cfgErr := defaultmoot.LoadConfigFromEnv()
	if cfgErr != nil {
		report.DescriptorError = cfgErr.Error()
		return report
	}
	report.DescriptorURL = cfg.URL
	desc, err := defaultmoot.FetchAndVerify(ctx, &http.Client{Timeout: 5 * time.Second}, cfg)
	if err != nil {
		report.DescriptorError = err.Error()
		if strings.TrimSpace(local.GroupID) != "" {
			report.GroupID = local.GroupID
			if gid, decErr := decodeGroupID(local.GroupID); decErr == nil {
				applyDefaultMootGroupStatus(ctx, gf, gid, &report)
			} else {
				report.DescriptorError = report.DescriptorError + "; local group id: " + decErr.Error()
			}
		}
	} else {
		report.DescriptorVerified = true
		report.GroupID = desc.GroupID.String()
		report.Policy = &desc.Policy
		report.PolicySummary = defaultMootPolicySummary(desc.Policy)
		report.RecommendedLiveConfig = &desc.RecommendedLiveConfig
		applyDefaultMootGroupStatus(ctx, gf, desc.GroupID, &report)
	}
	if pilot, _, _, err := loadPilotDoctorState(ctx, gf.socket); err == nil {
		report.TURNEndpoint = pilot.TURNEndpoint
		report.TURNAvailable = strings.TrimSpace(pilot.TURNEndpoint) != ""
	} else {
		report.TURNError = err.Error()
	}
	return report
}

func applyDefaultMootGroupStatus(ctx context.Context, gf *globalFlags, gid entmoot.GroupID, report *defaultMootStatusReport) {
	report.Joined = defaultMootJoined(gf, gid)
	report.LiveEnabled, report.AllowedLiveActions = defaultMootLiveState(gf.data, gid)
	report.LastLocalMessageAtMS = defaultMootLastLocalMessage(ctx, gf.data, gid)
}

func printDefaultMootStatus(report defaultMootStatusReport) {
	fmt.Printf("consent: %s\n", report.Consent)
	fmt.Printf("descriptor_verified: %t\n", report.DescriptorVerified)
	if report.DescriptorError != "" {
		fmt.Printf("descriptor_error: %s\n", report.DescriptorError)
	}
	if report.GroupID != "" {
		fmt.Printf("group_id: %s\n", report.GroupID)
	}
	fmt.Printf("joined: %t\n", report.Joined)
	fmt.Printf("live_enabled: %t\n", report.LiveEnabled)
	if len(report.AllowedLiveActions) > 0 {
		fmt.Printf("allowed_live_actions: %s\n", strings.Join(report.AllowedLiveActions, ","))
	}
	fmt.Printf("hide_ip_requested: %t\n", report.HideIPRequested)
	fmt.Printf("turn_available: %t\n", report.TURNAvailable)
	if report.TURNEndpoint != "" {
		fmt.Printf("turn_endpoint: %s\n", report.TURNEndpoint)
	}
	if report.PolicySummary != "" {
		fmt.Printf("policy: %s\n", report.PolicySummary)
	}
	if report.LastLocalMessageAtMS > 0 {
		fmt.Printf("last_local_message_at_ms: %d\n", report.LastLocalMessageAtMS)
	}
}

func printDefaultMootJoinResult(jsonOut bool, desc defaultmoot.Descriptor, descriptorURL, status string, introStatus string) int {
	out := map[string]any{"status": status, "name": defaultmoot.Name, "group_id": desc.GroupID, "descriptor_url": descriptorURL}
	switch introStatus {
	case "published":
		out["intro_published"] = true
		out["intro_status"] = introStatus
	case "skipped_no_daemon":
		out["intro_published"] = false
		out["intro_status"] = introStatus
	}
	if jsonOut {
		return printJSON(out)
	}
	fmt.Fprintf(os.Stdout, "%s %s (%s)\n", status, defaultmoot.Name, desc.GroupID)
	switch introStatus {
	case "published":
		fmt.Fprintln(os.Stdout, "published introduction")
	case "skipped_no_daemon":
		fmt.Fprintln(os.Stdout, "skipped introduction publish: no running Entmoot daemon")
	}
	return exitOK
}

func publishDefaultMootIntro(ctx context.Context, gf *globalFlags, gid entmoot.GroupID, intro string, publishReady bool) (string, error) {
	intro = strings.TrimSpace(intro)
	if intro == "" {
		return "", nil
	}
	if !publishReady {
		return "skipped_no_daemon", nil
	}
	if err := publishIPCMessage(ctx, gf, gid, []string{"introductions"}, []byte(intro)); err != nil {
		return "failed", err
	}
	return "published", nil
}

func runWithStdoutDiscarded(fn func() int) (int, error) {
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return exitTransport, err
	}
	defer devNull.Close()
	stdout := os.Stdout
	os.Stdout = devNull
	defer func() { os.Stdout = stdout }()
	return fn(), nil
}

func writeDefaultMootJoinInput(desc defaultmoot.Descriptor) (string, func(), error) {
	raw, err := json.Marshal(desc)
	if err != nil {
		return "", func() {}, fmt.Errorf("marshal verified descriptor: %w", err)
	}
	file, err := os.CreateTemp("", "entmoot-default-moot-*.json")
	if err != nil {
		return "", func() {}, fmt.Errorf("create descriptor join input: %w", err)
	}
	path := file.Name()
	cleanup := func() { _ = os.Remove(path) }
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		cleanup()
		return "", func() {}, fmt.Errorf("write descriptor join input: %w", err)
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("close descriptor join input: %w", err)
	}
	return path, cleanup, nil
}

func defaultMootPolicySummary(p entpolicy.Policy) string {
	return fmt.Sprintf("message_rate=%s burst=%d max_message_bytes=%d live_rate=%s live_burst=%d retention_days=%d",
		p.MessageRatePerAuthor, p.MessageBurstPerAuthor, p.MaxMessageBytes, p.LiveTriggerRate, p.LiveTriggerBurst, p.RetentionDays)
}

func defaultMootJoined(gf *globalFlags, gid entmoot.GroupID) bool {
	id, err := keystore.Load(gf.identity)
	if err != nil {
		return false
	}
	rlog, ok, err := openExistingRosterLog(gf.data, gid)
	if err != nil || !ok {
		return false
	}
	defer rlog.Close()
	return rosterHasLocalIdentityPubKey(rlog, id.PublicKey)
}

func defaultMootDeclinedLocalState(ctx context.Context, dataDir string) (defaultMootLocalState, error) {
	state, _ := loadDefaultMootLocalState(dataDir)
	if strings.TrimSpace(state.GroupID) == "" {
		gid, err := defaultMootGroupIDForLocalCleanup(ctx, dataDir)
		if err != nil {
			if defaultMootHasLocalGroups(dataDir) {
				return defaultMootLocalState{}, fmt.Errorf("resolve default moot group for decline: %w", err)
			}
		} else {
			state.GroupID = gid.String()
		}
	}
	state.Consent = defaultMootConsentDeclined
	state.UpdatedAtMS = time.Now().UnixMilli()
	return state, nil
}

func defaultMootHasLocalGroups(dataDir string) bool {
	gids, err := listGroupIDs(dataDir, nil)
	return err == nil && len(gids) > 0
}

func defaultMootLocallyDeclined(dataDir string, gid entmoot.GroupID) bool {
	state, ok := loadDefaultMootLocalState(dataDir)
	if !ok || strings.TrimSpace(state.Consent) != defaultMootConsentDeclined {
		return false
	}
	if strings.TrimSpace(state.GroupID) == "" {
		return true
	}
	declinedGID, err := decodeGroupID(state.GroupID)
	return err == nil && declinedGID == gid
}

func defaultMootDeclinedGroupID(dataDir string) (entmoot.GroupID, bool) {
	state, ok := loadDefaultMootLocalState(dataDir)
	if !ok || strings.TrimSpace(state.Consent) != defaultMootConsentDeclined {
		return entmoot.GroupID{}, false
	}
	if strings.TrimSpace(state.GroupID) == "" {
		return entmoot.GroupID{}, false
	}
	gid, err := decodeGroupID(state.GroupID)
	if err != nil {
		return entmoot.GroupID{}, false
	}
	return gid, true
}

func defaultMootLiveState(dataDir string, gid entmoot.GroupID) (bool, []string) {
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		return false, nil
	}
	defer state.Close()
	configs, err := state.ListLiveAgentConfigs(context.Background(), gid)
	if err != nil {
		return false, nil
	}
	actions := map[string]struct{}{}
	enabled := false
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		enabled = true
		for _, action := range cfg.AllowedActions {
			actions[action] = struct{}{}
		}
	}
	out := make([]string, 0, len(actions))
	for action := range actions {
		out = append(out, action)
	}
	return enabled, out
}

func defaultMootLastLocalMessage(ctx context.Context, dataDir string, gid entmoot.GroupID) int64 {
	msgs, err := store.OpenSQLite(dataDir)
	if err != nil {
		return 0
	}
	defer msgs.Close()
	latest, err := msgs.Latest(ctx, gid, 1)
	if err != nil || len(latest) == 0 {
		return 0
	}
	return latest[0].Timestamp
}

func disableDefaultMootLiveConfigs(dataDir string, gid entmoot.GroupID, node entmoot.NodeID) (int, error) {
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		return 0, err
	}
	defer state.Close()
	var configs []esphttp.LiveAgentConfig
	if node != 0 {
		cfg, ok, err := state.GetLiveAgentConfig(context.Background(), gid, node)
		if err != nil {
			return 0, err
		}
		if ok {
			configs = []esphttp.LiveAgentConfig{cfg}
		}
	} else {
		configs, err = state.ListLiveAgentConfigs(context.Background(), gid)
		if err != nil {
			return 0, err
		}
	}
	disabled := 0
	now := time.Now().UnixMilli()
	for _, cfg := range configs {
		if err := state.DeleteLiveAgentConfig(context.Background(), gid, cfg.NodeID, now); err != nil {
			return disabled, err
		}
		disabled++
	}
	return disabled, nil
}

func defaultMootStatePath(dataDir string) string {
	return filepath.Join(dataDir, "default_moot.json")
}

func loadDefaultMootLocalState(dataDir string) (defaultMootLocalState, bool) {
	raw, err := os.ReadFile(defaultMootStatePath(dataDir))
	if err != nil {
		return defaultMootLocalState{}, false
	}
	var state defaultMootLocalState
	if err := json.Unmarshal(raw, &state); err != nil {
		return defaultMootLocalState{}, false
	}
	return state, true
}

func saveDefaultMootLocalState(dataDir string, state defaultMootLocalState) error {
	if strings.TrimSpace(state.Consent) == "" {
		state.Consent = defaultMootConsentUnconfigured
	}
	if state.UpdatedAtMS == 0 {
		state.UpdatedAtMS = time.Now().UnixMilli()
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := defaultMootStatePath(dataDir) + "." + strconv.FormatInt(time.Now().UnixNano(), 10) + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, defaultMootStatePath(dataDir))
}
