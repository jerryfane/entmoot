package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"entmoot/pkg/entmoot"
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

	store, err := entpolicy.OpenFileStore(gf.data)
	if err != nil {
		slog.Error("group policy set: open store", slog.String("err", err.Error()))
		return exitTransport
	}
	ctx, cancel := withBackgroundTimeout()
	defer cancel()
	if resolved.Policy == nil {
		if err := store.Delete(ctx, gid); err != nil {
			slog.Error("group policy set: clear", slog.String("err", err.Error()))
			return exitTransport
		}
		report := buildGroupPolicyReport(gid, nil, false)
		report.Source = resolved.Source
		if *jsonOut {
			return printJSON(report)
		}
		printGroupPolicyReport(report)
		return exitOK
	}
	if err := store.Put(ctx, gid, *resolved.Policy); err != nil {
		slog.Error("group policy set: put", slog.String("err", err.Error()))
		return exitTransport
	}
	report := buildGroupPolicyReport(gid, resolved.Policy, true)
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
	store, err := entpolicy.OpenFileStore(gf.data)
	if err != nil {
		slog.Error("group policy clear: open store", slog.String("err", err.Error()))
		return exitTransport
	}
	ctx, cancel := withBackgroundTimeout()
	defer cancel()
	if err := store.Delete(ctx, gid); err != nil {
		slog.Error("group policy clear: delete", slog.String("err", err.Error()))
		return exitTransport
	}
	report := buildGroupPolicyReport(gid, nil, false)
	report.Source = "clear"
	if *jsonOut {
		return printJSON(report)
	}
	printGroupPolicyReport(report)
	return exitOK
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
		return buildGroupPolicyReport(gid, nil, false), exitOK
	}
	return buildGroupPolicyReport(gid, &p, true), exitOK
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
	if report.PolicySummary != "" {
		fmt.Printf("policy: %s\n", report.PolicySummary)
	}
	if report.RuntimeAppliedKnown && report.RuntimeApplied != nil {
		fmt.Printf("runtime_applied: %t\n", *report.RuntimeApplied)
	} else {
		fmt.Printf("runtime_applied: unknown (%s)\n", report.RuntimeAppliedReason)
	}
}
