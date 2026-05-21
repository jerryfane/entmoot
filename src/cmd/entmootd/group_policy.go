package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	entpolicy "entmoot/pkg/entmoot/policy"
)

const (
	groupPolicyModeStored = "stored_policy"
	groupPolicyModeLegacy = "legacy"
)

type groupPolicyReport struct {
	GroupID              entmoot.GroupID   `json:"group_id"`
	PolicyConfigured     bool              `json:"policy_configured"`
	Policy               *entpolicy.Policy `json:"policy,omitempty"`
	PolicySummary        string            `json:"policy_summary,omitempty"`
	EffectiveMode        string            `json:"effective_mode"`
	Source               string            `json:"source,omitempty"`
	Published            bool              `json:"published,omitempty"`
	Sequence             uint64            `json:"sequence,omitempty"`
	UpdatedAtMS          int64             `json:"updated_at_ms,omitempty"`
	RuntimeAppliedKnown  bool              `json:"runtime_applied_known"`
	RuntimeApplied       *bool             `json:"runtime_applied,omitempty"`
	RuntimeAppliedReason string            `json:"runtime_applied_reason,omitempty"`
}

func cmdGroupPolicy(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "group policy: missing op (want: status, set, clear)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "status":
		return cmdGroupPolicyStatus(gf, args[1:])
	case "set":
		return cmdGroupPolicySet(gf, args[1:])
	case "clear":
		return cmdGroupPolicyClear(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "group policy: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdGroupPolicyStatus(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("group policy status", flag.ContinueOnError)
	groupRaw := fs.String("group", "", "group id")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, ok := parseRequiredPolicyGroup("group policy status", *groupRaw)
	if !ok {
		return exitInvalidArgument
	}
	report, code := loadGroupPolicyReport(gf, gid)
	if code != exitOK {
		return code
	}
	if *jsonOut {
		return printJSON(report)
	}
	printGroupPolicyReport(report)
	return exitOK
}

func cmdGroupPolicySet(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("group policy set", flag.ContinueOnError)
	groupRaw := fs.String("group", "", "group id")
	preset := fs.String("preset", "", "policy preset: standard, relaxed, none")
	filePath := fs.String("file", "", "policy JSON file")
	localOnly := fs.Bool("local-only", false, "store locally without publishing through a running daemon")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, ok := parseRequiredPolicyGroup("group policy set", *groupRaw)
	if !ok {
		return exitInvalidArgument
	}
	resolved, err := entpolicy.ResolveSource(*preset, *filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "group policy set: %v\n", err)
		return exitInvalidArgument
	}

	report, code := setGroupPolicy(gf, gid, resolved.Policy, *localOnly)
	if code != exitOK {
		return code
	}
	report.Source = resolved.Source
	if *jsonOut {
		return printJSON(report)
	}
	printGroupPolicyReport(report)
	return exitOK
}

func cmdGroupPolicyClear(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("group policy clear", flag.ContinueOnError)
	groupRaw := fs.String("group", "", "group id")
	localOnly := fs.Bool("local-only", false, "store locally without publishing through a running daemon")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, ok := parseRequiredPolicyGroup("group policy clear", *groupRaw)
	if !ok {
		return exitInvalidArgument
	}
	report, code := setGroupPolicy(gf, gid, nil, *localOnly)
	if code != exitOK {
		return code
	}
	report.Source = "clear"
	if *jsonOut {
		return printJSON(report)
	}
	printGroupPolicyReport(report)
	return exitOK
}

func setGroupPolicy(gf *globalFlags, gid entmoot.GroupID, p *entpolicy.Policy, localOnly bool) (groupPolicyReport, int) {
	store, err := entpolicy.OpenFileStore(gf.data)
	if err != nil {
		slog.Error("group policy set: open store", slog.String("err", err.Error()))
		return groupPolicyReport{}, exitTransport
	}
	ctx, cancel := withBackgroundTimeout()
	defer cancel()

	if !localOnly && controlSocketAlive(controlSocketPath(gf.data), 200*time.Millisecond) {
		report, err := publishGroupPolicyUpdate(ctx, gf, store, gid, p)
		if err != nil {
			if publishIPCErrorCode(err) == ipc.CodeGroupNotFound {
				return applyLocalGroupPolicyUpdate(ctx, store, gid, p)
			}
			fmt.Fprintf(os.Stderr, "group policy set: publish update: %v\n", err)
			return groupPolicyReport{}, publishIPCExitCode(err)
		}
		return report, exitOK
	}

	return applyLocalGroupPolicyUpdate(ctx, store, gid, p)
}

func applyLocalGroupPolicyUpdate(ctx context.Context, store *entpolicy.FileStore, gid entmoot.GroupID, p *entpolicy.Policy) (groupPolicyReport, int) {
	report, err := applyGroupPolicyUpdateLocal(ctx, store, gid, p)
	if err != nil {
		slog.Error("group policy set: apply local", slog.String("err", err.Error()))
		return groupPolicyReport{}, exitTransport
	}
	return report, exitOK
}

func parseRequiredPolicyGroup(prefix, raw string) (entmoot.GroupID, bool) {
	if raw == "" {
		fmt.Fprintf(os.Stderr, "%s: -group is required\n", prefix)
		return entmoot.GroupID{}, false
	}
	gid, err := decodeGroupID(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prefix, err)
		return entmoot.GroupID{}, false
	}
	return gid, true
}

func loadGroupPolicyReport(gf *globalFlags, gid entmoot.GroupID) (groupPolicyReport, int) {
	store, err := entpolicy.OpenFileStore(gf.data)
	if err != nil {
		slog.Error("group policy status: open store", slog.String("err", err.Error()))
		return groupPolicyReport{}, exitTransport
	}
	ctx, cancel := withBackgroundTimeout()
	defer cancel()
	p, ok, err := store.Get(ctx, gid)
	if err != nil {
		slog.Error("group policy status: get", slog.String("err", err.Error()))
		return groupPolicyReport{}, exitTransport
	}
	if !ok {
		report := buildGroupPolicyReport(gid, nil, false)
		if seq, hasSeq, err := store.Sequence(ctx, gid); err == nil && hasSeq {
			report.Sequence = seq
		}
		return report, exitOK
	}
	report := buildGroupPolicyReport(gid, &p, true)
	if seq, hasSeq, err := store.Sequence(ctx, gid); err == nil && hasSeq {
		report.Sequence = seq
	}
	return report, exitOK
}

func buildGroupPolicyReport(gid entmoot.GroupID, p *entpolicy.Policy, configured bool) groupPolicyReport {
	report := groupPolicyReport{
		GroupID:              gid,
		PolicyConfigured:     configured,
		EffectiveMode:        groupPolicyModeLegacy,
		RuntimeAppliedKnown:  false,
		RuntimeAppliedReason: "runtime policy application is not exposed by this daemon yet",
	}
	if configured && p != nil {
		report.Policy = p
		report.PolicySummary = entpolicy.Summary(*p)
		report.EffectiveMode = groupPolicyModeStored
	}
	return report
}

func printGroupPolicyReport(report groupPolicyReport) {
	fmt.Printf("group_id: %s\n", report.GroupID)
	fmt.Printf("policy_configured: %t\n", report.PolicyConfigured)
	fmt.Printf("effective_mode: %s\n", report.EffectiveMode)
	if report.Source != "" {
		fmt.Printf("source: %s\n", report.Source)
	}
	if report.Published {
		fmt.Println("published: true")
	}
	if report.Sequence > 0 {
		fmt.Printf("sequence: %d\n", report.Sequence)
	}
	if report.PolicySummary != "" {
		fmt.Printf("policy: %s\n", report.PolicySummary)
	}
	if report.RuntimeAppliedKnown && report.RuntimeApplied != nil {
		fmt.Printf("runtime_applied: %t\n", *report.RuntimeApplied)
	} else {
		fmt.Printf("runtime_applied: unknown (%s)\n", report.RuntimeAppliedReason)
	}
}

func publishGroupPolicyUpdate(ctx context.Context, gf *globalFlags, store *entpolicy.FileStore, gid entmoot.GroupID, p *entpolicy.Policy) (groupPolicyReport, error) {
	update, err := buildNextPolicyUpdate(ctx, store, gid, p)
	if err != nil {
		return groupPolicyReport{}, err
	}
	body, err := json.Marshal(update)
	if err != nil {
		return groupPolicyReport{}, fmt.Errorf("marshal update: %w", err)
	}
	if err := publishIPCMessage(ctx, gf, gid, []string{entpolicy.UpdateTopic}, body); err != nil {
		return groupPolicyReport{}, err
	}
	report := buildGroupPolicyReport(gid, p, p != nil)
	report.Published = true
	report.Sequence = update.Sequence
	report.UpdatedAtMS = update.UpdatedAtMS
	applied := true
	report.RuntimeApplied = &applied
	report.RuntimeAppliedKnown = true
	report.RuntimeAppliedReason = ""
	return report, nil
}

func applyGroupPolicyUpdateLocal(ctx context.Context, store *entpolicy.FileStore, gid entmoot.GroupID, p *entpolicy.Policy) (groupPolicyReport, error) {
	update, err := buildNextPolicyUpdate(ctx, store, gid, p)
	if err != nil {
		return groupPolicyReport{}, err
	}
	result, err := store.ApplyUpdate(ctx, update)
	if err != nil {
		return groupPolicyReport{}, err
	}
	if !result.Accepted {
		return groupPolicyReport{}, fmt.Errorf("policy update sequence %d was not newer", update.Sequence)
	}
	report := buildGroupPolicyReport(gid, p, p != nil)
	report.Sequence = update.Sequence
	report.UpdatedAtMS = update.UpdatedAtMS
	return report, nil
}

func buildNextPolicyUpdate(ctx context.Context, store *entpolicy.FileStore, gid entmoot.GroupID, p *entpolicy.Policy) (entpolicy.Update, error) {
	if p != nil {
		if err := p.Validate(); err != nil {
			return entpolicy.Update{}, err
		}
	}
	now := time.Now().UnixMilli()
	seq := uint64(now)
	if current, ok, err := store.Sequence(ctx, gid); err != nil {
		return entpolicy.Update{}, err
	} else if ok && current >= seq {
		seq = current + 1
	}
	return entpolicy.NewUpdate(gid, p, now, seq), nil
}
