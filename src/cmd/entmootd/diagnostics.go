package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

const doctorSchemaVersion = 3

const (
	doctorLocalMemberOK               = "ok"
	doctorLocalMemberNotInRoster      = "not_in_roster"
	doctorLocalMemberIdentityMismatch = "identity_mismatch"
)

type doctorReport struct {
	SchemaVersion int                 `json:"schema_version"`
	GeneratedAt   string              `json:"generated_at"`
	Runtime       *runtimeReport      `json:"runtime,omitempty"`
	Entmoot       doctorEntmootReport `json:"entmoot"`
	Groups        []doctorGroupReport `json:"groups"`
}

type doctorEntmootReport struct {
	Running    bool             `json:"running"`
	Error      string           `json:"error,omitempty"`
	MemberID   entmoot.MemberID `json:"member_id"`
	PeerID     string           `json:"peer_id"`
	ListenPort uint16           `json:"listen_port,omitempty"`
	DataDir    string           `json:"data_dir"`
}

type doctorGroupReport struct {
	GroupID           entmoot.GroupID    `json:"group_id"`
	Running           bool               `json:"running"`
	LocalMember       bool               `json:"local_member"`
	LocalMemberStatus string             `json:"local_member_status"`
	Members           int                `json:"members"`
	Messages          int                `json:"messages"`
	MerkleRoot        *[32]byte          `json:"merkle_root,omitempty"`
	Peers             []doctorPeerReport `json:"peers"`
	Error             string             `json:"error,omitempty"`
	Suggestion        string             `json:"suggestion,omitempty"`
	// ProbeStatus is empty without -probe. With it, one of four kinds: "ok",
	// or "incomplete", "runtime_unavailable" or "failed" followed by ": " and
	// the reason - the budget ran out before every member was tried, a probe
	// needs the daemon that owns the host, or the probe itself errored. Only
	// "ok" is bare, so a consumer matches the kind as a prefix.
	ProbeStatus string `json:"probe_status,omitempty"`
}

type doctorPeerReport struct {
	MemberID entmoot.MemberID `json:"member_id"`
	PeerID   string           `json:"peer_id"`
	Self     bool             `json:"self"`
	Roster   bool             `json:"roster"`
	Error    string           `json:"error,omitempty"`
	// Probe is present only with -probe. Without it the rows above describe
	// membership, which says nothing about whether a peer answers.
	Probe *doctorPeerProbe `json:"probe,omitempty"`
}

type doctorPeerProbe struct {
	Reachable bool `json:"reachable"`
	// Answered without Reachable is a peer that replied and refused: the
	// membership it serves does not admit this node. Reporting that as
	// unreachable would send an operator after a network fault instead.
	Answered  bool   `json:"answered,omitempty"`
	Refusal   string `json:"refusal,omitempty"`
	Relayed   bool   `json:"relayed,omitempty"`
	LatencyMS int64  `json:"latency_ms,omitempty"`
	Addresses int    `json:"addresses"`
	Error     string `json:"error,omitempty"`
}

func cmdDoctor(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (optional; defaults to all groups)")
	probe := fs.Bool("probe", false, "ask the running daemon to dial each other member and report which answer")
	timeout := fs.Duration("timeout", defaultProbeBudget, "budget for the whole probe, not per peer; raised to one peer-slice if smaller")
	jsonOutput := fs.Bool("json", false, "print JSON")
	redact := fs.Bool("redact", false, "omit local runtime paths and the data directory, for sharing a report")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	var groupID *entmoot.GroupID
	if *groupStr != "" {
		parsed, err := decodeGroupID(*groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "doctor: %v\n", err)
			return exitInvalidArgument
		}
		groupID = &parsed
	}
	report, err := buildDoctorReport(context.Background(), gf, groupID, *probe, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "doctor: %v\n", err)
		return exitTransport
	}
	if *redact {
		redactDoctorReport(report)
	}
	if *jsonOutput {
		data, err := json.Marshal(report)
		if err != nil {
			fmt.Fprintf(os.Stderr, "doctor: marshal: %v\n", err)
			return exitTransport
		}
		fmt.Println(string(data))
		return exitOK
	}
	printDoctorHuman(report)
	return exitOK
}

func cmdPeers(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("peers", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	probe := fs.Bool("probe", false, "ask the running daemon to dial each other member and report which answer")
	timeout := fs.Duration("timeout", defaultProbeBudget, "budget for the whole probe, not per peer; raised to one peer-slice if smaller")
	jsonOutput := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *groupStr == "" {
		fmt.Fprintln(os.Stderr, "peers: -group is required")
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "peers: %v\n", err)
		return exitInvalidArgument
	}
	report, err := buildDoctorReport(context.Background(), gf, &gid, *probe, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "peers: %v\n", err)
		return exitTransport
	}
	if len(report.Groups) == 0 {
		fmt.Fprintln(os.Stderr, "peers: group not found")
		return exitGroupNotFound
	}
	group := report.Groups[0]
	if *probe && group.ProbeStatus != "" && group.ProbeStatus != "ok" {
		// The status is the difference between "nobody answered" and "no probe
		// ran". Printing only the rows would make -probe indistinguishable
		// from no flag when the daemon is absent. It goes to stderr so the
		// documented JSON shape - an array of peers - is unchanged.
		fmt.Fprintf(os.Stderr, "peers: probe %s\n", group.ProbeStatus)
	}
	if *jsonOutput {
		data, err := json.Marshal(group.Peers)
		if err != nil {
			fmt.Fprintf(os.Stderr, "peers: marshal: %v\n", err)
			return exitTransport
		}
		fmt.Println(string(data))
		return exitOK
	}
	printPeersTable(group.Peers)
	return exitOK
}

func buildDoctorReport(ctx context.Context, gf *globalFlags, groupFilter *entmoot.GroupID, probe bool, timeout time.Duration) (*doctorReport, error) {
	setupResult, err := setup(gf)
	if err != nil {
		return nil, err
	}
	binding, err := libp2ptransport.BindingFromPublicKey(setupResult.identity.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("derive local identity binding: %w", err)
	}
	runtimeReport := collectRuntimeReport(gf, setupResult.dataDir)
	report := &doctorReport{
		SchemaVersion: doctorSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Runtime:       &runtimeReport,
		Entmoot: doctorEntmootReport{
			MemberID: binding.MemberID,
			PeerID:   binding.PeerID.String(),
			DataDir:  setupResult.dataDir,
		},
		Groups: []doctorGroupReport{},
	}
	liveByGroup := make(map[entmoot.GroupID]ipc.GroupInfo)
	if controlSocketAlive(controlSocketPath(setupResult.dataDir), 500*time.Millisecond) {
		live, liveErr := infoOverIPCContext(ctx, controlSocketPath(setupResult.dataDir))
		if liveErr != nil {
			report.Entmoot.Error = liveErr.Error()
		} else {
			report.Entmoot.Running = true
			report.Entmoot.ListenPort = live.ListenPort
			for _, group := range live.Groups {
				liveByGroup[group.GroupID] = group
			}
		}
	}
	groups, err := listGroupIDs(setupResult.dataDir, nil)
	if err != nil {
		return nil, err
	}
	if groupFilter != nil {
		groups = []entmoot.GroupID{*groupFilter}
	}
	messageStore, err := store.OpenSQLite(setupResult.dataDir)
	if err != nil {
		return nil, err
	}
	defer messageStore.Close()
	socket := controlSocketPath(setupResult.dataDir)
	for _, gid := range groups {
		group := buildDoctorGroup(ctx, messageStore, setupResult.dataDir, gid, binding.MemberID, liveByGroup[gid])
		if probe {
			applyPeerProbe(ctx, &group, socket, report.Entmoot.Running, timeout)
		}
		report.Groups = append(report.Groups, group)
	}
	return report, nil
}

func buildDoctorGroup(ctx context.Context, messageStore *store.SQLite, dataDir string, gid entmoot.GroupID, localMemberID entmoot.MemberID, live ipc.GroupInfo) doctorGroupReport {
	group := doctorGroupReport{GroupID: gid, Running: live.GroupID == gid, Peers: []doctorPeerReport{}}
	rlog, err := membership.Open(dataDir, gid)
	if err != nil {
		group.Error = err.Error()
		return group
	}
	defer rlog.Close()
	group.Members = len(rlog.MemberIDs())
	group.LocalMember = rlog.IsMemberID(localMemberID)
	if group.LocalMember {
		group.LocalMemberStatus = doctorLocalMemberOK
	} else {
		group.LocalMemberStatus = doctorLocalMemberNotInRoster
		group.Suggestion = "join this group with a target-bound bootstrap capability"
	}
	memberIDs := rlog.MemberIDs()
	sort.Slice(memberIDs, func(i, j int) bool { return memberIDs[i].String() < memberIDs[j].String() })
	for _, memberID := range memberIDs {
		peerReport := doctorPeerReport{MemberID: memberID, Self: memberID == localMemberID, Roster: true}
		if info, ok := rlog.MemberInfoByID(memberID); ok {
			if remoteBinding, bindErr := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey); bindErr == nil {
				peerReport.PeerID = remoteBinding.PeerID.String()
			} else {
				peerReport.Error = bindErr.Error()
			}
		}
		group.Peers = append(group.Peers, peerReport)
	}
	messages, err := messageStore.Range(ctx, gid, 0, 0)
	if err != nil {
		group.Error = err.Error()
		return group
	}
	group.Messages = len(messages)
	if root, err := messageStore.MerkleRoot(ctx, gid); err == nil {
		group.MerkleRoot = &root
	} else {
		group.Error = err.Error()
	}
	return group
}

func printDoctorHuman(report *doctorReport) {
	if report.Runtime != nil && report.Runtime.NamespaceWarning != "" {
		fmt.Printf("runtime: warning %s\n", report.Runtime.NamespaceWarning)
	}
	if report.Entmoot.Running {
		fmt.Printf("running member=%s peer=%s data=%s\n", report.Entmoot.MemberID.String(), report.Entmoot.PeerID, report.Entmoot.DataDir)
	} else {
		fmt.Printf("not_running member=%s peer=%s data=%s\n", report.Entmoot.MemberID.String(), report.Entmoot.PeerID, report.Entmoot.DataDir)
	}
	for _, group := range report.Groups {
		fmt.Printf("group=%s running=%t local_member=%t members=%d messages=%d", group.GroupID.String(), group.Running, group.LocalMember, group.Members, group.Messages)
		if group.ProbeStatus != "" {
			reachable, refused, probed := 0, 0, 0
			for _, peer := range group.Peers {
				if peer.Self || peer.Probe == nil {
					continue
				}
				probed++
				switch {
				case peer.Probe.Reachable:
					reachable++
				case peer.Probe.Answered:
					refused++
				}
			}
			fmt.Printf(" reachable=%d/%d", reachable, probed)
			if refused > 0 {
				fmt.Printf(" refused=%d", refused)
			}
			fmt.Printf(" probe=%q", group.ProbeStatus)
		}
		if group.Error != "" {
			fmt.Printf(" error=%q", group.Error)
		}
		fmt.Println()
		for _, peer := range group.Peers {
			if peer.Probe == nil || peer.Self {
				continue
			}
			fmt.Printf("  member=%s reachable=%t", peer.MemberID.String(), peer.Probe.Reachable)
			if peer.Probe.Answered && !peer.Probe.Reachable {
				fmt.Printf(" answered=true refusal=%q", peer.Probe.Refusal)
			}
			if peer.Probe.Answered {
				fmt.Printf(" latency_ms=%d relayed=%t", peer.Probe.LatencyMS, peer.Probe.Relayed)
			}
			fmt.Printf(" addresses=%d", peer.Probe.Addresses)
			if peer.Probe.Error != "" {
				fmt.Printf(" error=%q", peer.Probe.Error)
			}
			fmt.Println()
		}
	}
}

func printPeersTable(peers []doctorPeerReport) {
	for _, peer := range peers {
		fmt.Printf("member=%s peer=%s self=%t", peer.MemberID.String(), peer.PeerID, peer.Self)
		if peer.Probe != nil && !peer.Self {
			fmt.Printf(" reachable=%t addresses=%d", peer.Probe.Reachable, peer.Probe.Addresses)
			if peer.Probe.Answered && !peer.Probe.Reachable {
				fmt.Printf(" answered=true refusal=%q", peer.Probe.Refusal)
			}
			if peer.Probe.Answered {
				fmt.Printf(" latency_ms=%d relayed=%t", peer.Probe.LatencyMS, peer.Probe.Relayed)
			}
			if peer.Probe.Error != "" {
				fmt.Printf(" probe_error=%q", peer.Probe.Error)
			}
		}
		if peer.Error != "" {
			fmt.Printf(" error=%q", peer.Error)
		}
		fmt.Println()
	}
}

func redactDoctorReport(report *doctorReport) {
	if report == nil {
		return
	}
	report.Runtime = nil
	report.Entmoot.DataDir = ""
}
