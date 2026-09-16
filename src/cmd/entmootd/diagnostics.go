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
}

type doctorPeerReport struct {
	MemberID entmoot.MemberID `json:"member_id"`
	PeerID   string           `json:"peer_id"`
	Self     bool             `json:"self"`
	Roster   bool             `json:"roster"`
	Error    string           `json:"error,omitempty"`
}

func cmdDoctor(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (optional; defaults to all groups)")
	probe := fs.Bool("probe", false, "include live daemon status")
	timeout := fs.Duration("timeout", 3*time.Second, "diagnostic timeout")
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
	probe := fs.Bool("probe", false, "include live daemon status")
	timeout := fs.Duration("timeout", 3*time.Second, "diagnostic timeout")
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
	if *jsonOutput {
		data, err := json.Marshal(report.Groups[0].Peers)
		if err != nil {
			fmt.Fprintf(os.Stderr, "peers: marshal: %v\n", err)
			return exitTransport
		}
		fmt.Println(string(data))
		return exitOK
	}
	printPeersTable(report.Groups[0].Peers)
	return exitOK
}

func buildDoctorReport(ctx context.Context, gf *globalFlags, groupFilter *entmoot.GroupID, _ bool, _ time.Duration) (*doctorReport, error) {
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
	for _, gid := range groups {
		report.Groups = append(report.Groups, buildDoctorGroup(ctx, messageStore, setupResult.dataDir, gid, binding.MemberID, liveByGroup[gid]))
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
		if group.Error != "" {
			fmt.Printf(" error=%q", group.Error)
		}
		fmt.Println()
	}
}

func printPeersTable(peers []doctorPeerReport) {
	for _, peer := range peers {
		fmt.Printf("member=%s peer=%s self=%t", peer.MemberID.String(), peer.PeerID, peer.Self)
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
