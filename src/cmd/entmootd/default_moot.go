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
	Consent              string                `json:"consent"`
	DescriptorURL        string                `json:"descriptor_url"`
	DescriptorVerified   bool                  `json:"descriptor_verified"`
	DescriptorError      string                `json:"descriptor_error,omitempty"`
	GroupID              string                `json:"group_id,omitempty"`
	Joined               bool                  `json:"joined"`
	Policy               *entpolicy.Policy     `json:"policy,omitempty"`
	PolicySummary        string                `json:"policy_summary,omitempty"`
	LastLocalMessageAtMS int64                 `json:"last_local_message_at_ms,omitempty"`
	LocalState           defaultMootLocalState `json:"local_state"`
}

func cmdDefaultMoot(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd default-moot <status|join|decline|leave> [flags]")
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
	state := defaultMootLocalState{Consent: defaultMootConsentDeclined, GroupID: gid.String(), UpdatedAtMS: time.Now().UnixMilli()}
	if err := saveDefaultMootLocalState(gf.data, state); err != nil {
		fmt.Fprintf(os.Stderr, "default-moot leave: persist consent: %v\n", err)
		return exitTransport
	}
	out := map[string]any{"status": "local_participation_disabled", "group_id": gid}
	if controlSocketAlive(controlSocketPath(gf.data), 200*time.Millisecond) {
		out["status"] = "restart_required"
		out["warning"] = "declined consent was saved, but an active entmootd daemon may still be serving this group; restart entmootd serve to unload it"
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
	fmt.Fprintf(os.Stdout, "declined The Ent Moot locally for %s; remote roster removal is not performed by this command\n", gid)
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
		Consent:    local.Consent,
		LocalState: local,
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
		applyDefaultMootGroupStatus(ctx, gf, desc.GroupID, &report)
	}
	return report
}

func applyDefaultMootGroupStatus(ctx context.Context, gf *globalFlags, gid entmoot.GroupID, report *defaultMootStatusReport) {
	report.Joined = defaultMootJoined(gf, gid)
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
	return entpolicy.Summary(p)
}

func defaultMootJoined(gf *globalFlags, gid entmoot.GroupID) bool {
	id, err := keystore.Load(gf.identity)
	if err != nil {
		return false
	}
	rlog, ok, err := openExistingGroup(gf.data, gid)
	if err != nil || !ok {
		return false
	}
	defer rlog.Close()
	return groupHasLocalIdentityPubKey(rlog, id.PublicKey)
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
