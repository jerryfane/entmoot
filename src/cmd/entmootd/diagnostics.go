package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

const (
	doctorSchemaVersion = 1

	diagProbeMaxPeersPerRequest = 256
	diagProbeConcurrency        = 4
	diagProbeMinTimeout         = 250 * time.Millisecond
	diagProbeDefaultTimeout     = 3 * time.Second
	diagProbeMaxTimeout         = 30 * time.Second
	diagProbeServerSlack        = 2 * time.Second
	diagProbeClientSlack        = 5 * time.Second

	doctorLocalMemberOK               = "ok"
	doctorLocalMemberPilotUnknown     = "pilot_unknown"
	doctorLocalMemberNotInRoster      = "not_in_roster"
	doctorLocalMemberIdentityMismatch = "identity_mismatch"
)

type doctorReport struct {
	SchemaVersion int                 `json:"schema_version"`
	GeneratedAt   string              `json:"generated_at"`
	Pilot         doctorPilotReport   `json:"pilot"`
	Entmoot       doctorEntmootReport `json:"entmoot"`
	Groups        []doctorGroupReport `json:"groups"`
}

type doctorPilotReport struct {
	Reachable      bool             `json:"reachable"`
	Error          string           `json:"error,omitempty"`
	NodeID         entmoot.NodeID   `json:"node_id,omitempty"`
	Hostname       string           `json:"hostname,omitempty"`
	Capabilities   []string         `json:"capabilities,omitempty"`
	TrustedQueryOK bool             `json:"trusted_query_ok"`
	TrustedError   string           `json:"trusted_error,omitempty"`
	Trusted        []entmoot.NodeID `json:"trusted,omitempty"`
	PendingQueryOK bool             `json:"pending_query_ok"`
	PendingError   string           `json:"pending_error,omitempty"`
	Pending        []entmoot.NodeID `json:"pending,omitempty"`
}

type doctorEntmootReport struct {
	Running    bool           `json:"running"`
	Error      string         `json:"error,omitempty"`
	NodeID     entmoot.NodeID `json:"node_id,omitempty"`
	ListenPort uint16         `json:"listen_port,omitempty"`
	DataDir    string         `json:"data_dir"`
}

type doctorGroupReport struct {
	GroupID           entmoot.GroupID    `json:"group_id"`
	LocalMember       bool               `json:"local_member"`
	LocalMemberStatus string             `json:"local_member_status"`
	Members           int                `json:"members"`
	Messages          int                `json:"messages"`
	Suggestion        string             `json:"suggestion,omitempty"`
	NextCommand       string             `json:"next_command,omitempty"`
	Peers             []doctorPeerReport `json:"peers"`
}

type doctorPeerReport struct {
	NodeID      entmoot.NodeID `json:"node_id"`
	Hostname    string         `json:"hostname,omitempty"`
	Roster      bool           `json:"roster"`
	Profile     string         `json:"profile"`
	Transport   string         `json:"transport"`
	Trust       string         `json:"trust"`
	Route       string         `json:"route"`
	RTTMS       int64          `json:"rtt_ms,omitempty"`
	Error       string         `json:"error,omitempty"`
	Diagnosis   string         `json:"diagnosis"`
	Suggestion  string         `json:"suggestion,omitempty"`
	NextCommand string         `json:"next_command,omitempty"`
}

func cmdDoctor(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (optional; defaults to all groups)")
	probe := fs.Bool("probe", false, "actively probe Entmoot streams to peers")
	timeout := fs.Duration("timeout", 3*time.Second, "per-peer probe timeout")
	jsonOut := fs.Bool("json", false, "emit JSON instead of a human summary")
	redact := fs.Bool("redact", false, "omit sensitive fields from JSON output")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	var gid *entmoot.GroupID
	if *groupStr != "" {
		parsed, err := decodeGroupID(*groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "doctor: %v\n", err)
			return exitInvalidArgument
		}
		gid = &parsed
	}
	report, err := buildDoctorReport(gf, gid, *probe, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "doctor: %v\n", err)
		return exitTransport
	}
	if *redact {
		redactDoctorReport(report)
	}
	if *jsonOut {
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
	probe := fs.Bool("probe", false, "actively probe Entmoot streams to peers")
	timeout := fs.Duration("timeout", 3*time.Second, "per-peer probe timeout")
	jsonOut := fs.Bool("json", false, "emit JSON instead of a table")
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
	report, err := buildDoctorReport(gf, &gid, *probe, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "peers: %v\n", err)
		return exitTransport
	}
	if len(report.Groups) == 0 {
		fmt.Fprintf(os.Stderr, "peers: group %s not joined\n", gid)
		return exitGroupNotFound
	}
	if *jsonOut {
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

func buildDoctorReport(gf *globalFlags, groupFilter *entmoot.GroupID, probe bool, probeTimeout time.Duration) (*doctorReport, error) {
	s, err := setup(gf)
	if err != nil {
		return nil, err
	}
	report := &doctorReport{
		SchemaVersion: doctorSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Entmoot: doctorEntmootReport{
			DataDir: s.dataDir,
		},
	}

	trusted := map[entmoot.NodeID]struct{}{}
	pending := map[entmoot.NodeID]struct{}{}
	if pilotInfo, t, p, err := loadPilotDoctorState(gf.socket); err != nil {
		report.Pilot.Error = err.Error()
	} else {
		report.Pilot = pilotInfo
		trusted = t
		pending = p
	}

	running := controlSocketAlive(controlSocketPath(s.dataDir), 500*time.Millisecond)
	report.Entmoot.Running = running
	liveInfoByGroup := map[entmoot.GroupID]ipc.GroupInfo{}
	if running {
		if live, err := infoOverIPC(controlSocketPath(s.dataDir)); err == nil {
			report.Entmoot.NodeID = live.PilotNodeID
			report.Entmoot.ListenPort = live.ListenPort
			report.Entmoot.DataDir = live.DataDir
			for _, gi := range live.Groups {
				liveInfoByGroup[gi.GroupID] = gi
			}
		} else {
			report.Entmoot.Error = err.Error()
			report.Entmoot.Running = false
		}
	}

	gids, err := listGroupIDs(s.dataDir, nil)
	if err != nil {
		return nil, err
	}
	if groupFilter != nil {
		if _, err := os.Stat(groupRosterPath(s.dataDir, *groupFilter)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return report, nil
			}
			return nil, fmt.Errorf("stat roster %s: %w", *groupFilter, err)
		}
		gids = []entmoot.GroupID{*groupFilter}
	}
	sort.Slice(gids, func(i, j int) bool { return gids[i].String() < gids[j].String() })

	st, err := store.OpenSQLite(s.dataDir)
	if err != nil && len(gids) > 0 {
		return nil, fmt.Errorf("open store: %w", err)
	}
	if st != nil {
		defer func() { _ = st.Close() }()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, gid := range gids {
		if _, err := os.Stat(groupRosterPath(s.dataDir, gid)); err != nil {
			if groupFilter != nil && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("stat roster %s: %w", gid, err)
		}
		r, err := roster.OpenJSONL(s.dataDir, gid)
		if err != nil {
			return nil, fmt.Errorf("open roster %s: %w", gid, err)
		}
		localNode := report.Entmoot.NodeID
		if localNode == 0 {
			localNode = report.Pilot.NodeID
		}
		group := buildDoctorGroup(ctx, st, r, gid, localNode, s.identity.PublicKey, report.Pilot, trusted, pending, liveInfoByGroup[gid])
		_ = r.Close()
		report.Groups = append(report.Groups, group)
	}
	if probe {
		applyDoctorProbes(report, s.dataDir, probeTimeout)
	}
	populateDoctorSuggestions(report, gf)
	return report, nil
}

func loadPilotDoctorState(socketPath string) (doctorPilotReport, map[entmoot.NodeID]struct{}, map[entmoot.NodeID]struct{}, error) {
	drv, err := ipcclient.Connect(socketPath)
	if err != nil {
		return doctorPilotReport{}, nil, nil, fmt.Errorf("connect %s: %w", socketPath, err)
	}
	defer func() { _ = drv.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	info, err := drv.InfoStruct(ctx)
	if err != nil {
		return doctorPilotReport{}, nil, nil, err
	}
	report := doctorPilotReport{
		Reachable:    true,
		NodeID:       entmoot.NodeID(info.NodeID),
		Hostname:     info.Hostname,
		Capabilities: append([]string(nil), info.Capabilities...),
	}
	trusted := map[entmoot.NodeID]struct{}{}
	if resp, err := drv.TrustedPeers(ctx); err == nil {
		report.TrustedQueryOK = true
		report.Trusted = parseTrustedNodeIDs(resp)
		for _, id := range report.Trusted {
			trusted[id] = struct{}{}
		}
	} else {
		report.TrustedError = err.Error()
	}
	pending := map[entmoot.NodeID]struct{}{}
	if list, err := drv.PendingHandshakes(ctx); err == nil {
		report.PendingQueryOK = true
		for _, p := range list {
			id := entmoot.NodeID(p.NodeID)
			report.Pending = append(report.Pending, id)
			pending[id] = struct{}{}
		}
		sort.Slice(report.Pending, func(i, j int) bool { return report.Pending[i] < report.Pending[j] })
	} else {
		report.PendingError = err.Error()
	}
	return report, trusted, pending, nil
}

func buildDoctorGroup(ctx context.Context, st *store.SQLite, r *roster.RosterLog, gid entmoot.GroupID, localNode entmoot.NodeID, localPub []byte, pilotReport doctorPilotReport, trusted, pending map[entmoot.NodeID]struct{}, live ipc.GroupInfo) doctorGroupReport {
	members := r.Members()
	group := doctorGroupReport{
		GroupID: gid,
		Members: len(members),
	}
	group.LocalMember, group.LocalMemberStatus = doctorLocalMembership(r, localNode, localPub)
	if live.Messages > 0 {
		group.Messages = live.Messages
	} else if st != nil {
		if msgs, err := st.Range(ctx, gid, 0, 0); err == nil {
			group.Messages = len(msgs)
		}
	}
	now := time.Now()
	for _, id := range members {
		info, _ := r.MemberInfo(id)
		peer := doctorPeerReport{
			NodeID:    id,
			Roster:    true,
			Profile:   "missing",
			Transport: "missing",
			Trust:     doctorPeerTrust(localNode, id, pilotReport, trusted, pending),
			Route:     "not_checked",
		}
		if localNode != 0 && id == localNode {
			peer.Route = "self"
		}
		if st != nil {
			if ad, ok, err := st.GetMemberProfileAd(ctx, gid, id, now); err == nil && ok && memberProfileMatchesRosterInfo(ad, info) {
				peer.Profile = "ok"
				peer.Hostname = ad.Hostname
			}
			if ad, ok, err := st.GetTransportAd(ctx, gid, id); err == nil && ok {
				if ad.NotAfter > now.UnixMilli() {
					peer.Transport = "ok"
				} else {
					peer.Transport = "stale"
				}
			}
		}
		peer.Diagnosis = diagnosePeer(peer, false)
		group.Peers = append(group.Peers, peer)
	}
	return group
}

func doctorLocalMembership(r *roster.RosterLog, localNode entmoot.NodeID, localPub []byte) (bool, string) {
	if localNode == 0 {
		return false, doctorLocalMemberPilotUnknown
	}
	info, ok := r.MemberInfo(localNode)
	if !ok {
		return false, doctorLocalMemberNotInRoster
	}
	if !bytes.Equal(info.EntmootPubKey, localPub) {
		return false, doctorLocalMemberIdentityMismatch
	}
	return true, doctorLocalMemberOK
}

func doctorPeerTrust(localNode, nodeID entmoot.NodeID, pilotReport doctorPilotReport, trusted, pending map[entmoot.NodeID]struct{}) string {
	if localNode != 0 && nodeID == localNode {
		return "self"
	}
	if _, ok := trusted[nodeID]; ok {
		return "trusted"
	}
	if _, ok := pending[nodeID]; ok {
		return "pending"
	}
	if !pilotReport.Reachable || !pilotReport.TrustedQueryOK || !pilotReport.PendingQueryOK {
		return "unknown"
	}
	return "missing"
}

func applyDoctorProbes(report *doctorReport, dataDir string, timeout time.Duration) {
	if !report.Entmoot.Running {
		for gi := range report.Groups {
			for pi := range report.Groups[gi].Peers {
				report.Groups[gi].Peers[pi].Route = "daemon_down"
				report.Groups[gi].Peers[pi].Diagnosis = diagnosePeer(report.Groups[gi].Peers[pi], true)
			}
		}
		return
	}
	for gi := range report.Groups {
		group := &report.Groups[gi]
		peers := make([]entmoot.NodeID, 0, len(group.Peers))
		peerIndexes := make(map[entmoot.NodeID]int, len(group.Peers))
		for i, p := range group.Peers {
			peerIndexes[p.NodeID] = i
			if p.NodeID != report.Entmoot.NodeID {
				peers = append(peers, p.NodeID)
			}
		}
		for _, chunk := range chunkDiagProbePeers(peers) {
			results, err := diagProbeOverIPC(controlSocketPath(dataDir), group.GroupID, chunk, timeout)
			for _, nodeID := range chunk {
				pi, ok := peerIndexes[nodeID]
				if !ok {
					continue
				}
				peer := &group.Peers[pi]
				if err != nil {
					peer.Route = "probe_failed"
					peer.Error = err.Error()
					peer.Diagnosis = diagnosePeer(*peer, true)
					continue
				}
				res, ok := results[nodeID]
				if !ok {
					peer.Route = "unknown"
					peer.Diagnosis = diagnosePeer(*peer, true)
					continue
				}
				if res.OK {
					peer.Route = "ok"
					peer.RTTMS = res.RTTMS
					peer.Error = ""
				} else {
					peer.Route = classifyProbeError(res.Error)
					peer.Error = res.Error
				}
				peer.Diagnosis = diagnosePeer(*peer, true)
			}
		}
		for pi := range group.Peers {
			peer := &group.Peers[pi]
			if peer.NodeID == report.Entmoot.NodeID {
				peer.Route = "self"
				peer.Diagnosis = diagnoseLocalPeer(*group, *peer, true)
				continue
			}
			if peer.Route == "not_checked" {
				peer.Route = "unknown"
			}
			peer.Diagnosis = diagnosePeer(*peer, true)
		}
	}
}

func diagProbeOverIPC(sockPath string, gid entmoot.GroupID, peers []entmoot.NodeID, timeout time.Duration) (map[entmoot.NodeID]ipc.DiagProbePeer, error) {
	timeout = clampDiagProbeTimeout(timeout)
	conn, err := net.DialTimeout("unix", sockPath, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline := time.Now().Add(diagProbeBudget(timeout, len(peers)) + diagProbeClientSlack)
	_ = conn.SetDeadline(deadline)
	if err := ipc.EncodeAndWrite(conn, &ipc.DiagProbeReq{GroupID: gid, Peers: peers, TimeoutMS: timeout.Milliseconds()}); err != nil {
		return nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, err
	}
	switch v := payload.(type) {
	case *ipc.DiagProbeResp:
		out := make(map[entmoot.NodeID]ipc.DiagProbePeer, len(v.Peers))
		for _, p := range v.Peers {
			out[p.NodeID] = p
		}
		return out, nil
	case *ipc.ErrorFrame:
		return nil, fmt.Errorf("%s: %s", v.Code, v.Message)
	default:
		return nil, fmt.Errorf("unexpected response %T", payload)
	}
}

func clampDiagProbeTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return diagProbeDefaultTimeout
	}
	if timeout < diagProbeMinTimeout {
		return diagProbeMinTimeout
	}
	if timeout > diagProbeMaxTimeout {
		return diagProbeMaxTimeout
	}
	return timeout
}

func diagProbeBatchCount(peerCount int) int {
	if peerCount <= 0 {
		return 1
	}
	return (peerCount + diagProbeConcurrency - 1) / diagProbeConcurrency
}

func diagProbeBudget(timeout time.Duration, peerCount int) time.Duration {
	return clampDiagProbeTimeout(timeout)*time.Duration(diagProbeBatchCount(peerCount)) + diagProbeServerSlack
}

func chunkDiagProbePeers(peers []entmoot.NodeID) [][]entmoot.NodeID {
	if len(peers) == 0 {
		return nil
	}
	chunks := make([][]entmoot.NodeID, 0, (len(peers)+diagProbeMaxPeersPerRequest-1)/diagProbeMaxPeersPerRequest)
	for start := 0; start < len(peers); start += diagProbeMaxPeersPerRequest {
		end := start + diagProbeMaxPeersPerRequest
		if end > len(peers) {
			end = len(peers)
		}
		chunks = append(chunks, peers[start:end])
	}
	return chunks
}

func diagnosePeer(p doctorPeerReport, probed bool) string {
	switch {
	case !p.Roster:
		return "not_in_roster"
	case p.Trust == "pending":
		return "trust_pending"
	case p.Trust == "missing":
		return "trust_missing"
	case p.Trust == "unknown":
		return "unknown"
	case p.Profile == "missing":
		return "profile_missing"
	case p.Transport == "missing":
		return "transport_missing"
	case p.Transport == "stale":
		return "transport_stale"
	case probed && p.Route != "ok" && p.Route != "self":
		return p.Route
	default:
		return "ok"
	}
}

func diagnoseLocalPeer(group doctorGroupReport, p doctorPeerReport, probed bool) string {
	if !group.LocalMember {
		if group.LocalMemberStatus != "" {
			return group.LocalMemberStatus
		}
		return doctorLocalMemberNotInRoster
	}
	return diagnosePeer(p, probed)
}

func populateDoctorSuggestions(report *doctorReport, gf *globalFlags) {
	for gi := range report.Groups {
		group := &report.Groups[gi]
		if group.Suggestion == "" && !group.LocalMember {
			group.Suggestion = localMembershipSuggestion(group.LocalMemberStatus)
			if !report.Entmoot.Running {
				group.NextCommand = serveGroupNextCommand(gf, group.GroupID)
			}
		}
		if !report.Entmoot.Running {
			group.Suggestion = "local Entmoot daemon is not running; start the joined group before probing peers"
			group.NextCommand = serveGroupNextCommand(gf, group.GroupID)
		}
		for pi := range group.Peers {
			peer := &group.Peers[pi]
			peer.Suggestion, peer.NextCommand = peerSuggestion(gf, group.GroupID, report.Entmoot.Running, *group, *peer)
		}
	}
}

func localMembershipSuggestion(status string) string {
	switch status {
	case doctorLocalMemberPilotUnknown:
		return "local Pilot node id is unavailable; check that pilot-daemon is running and that entmootd is using the intended Pilot socket"
	case doctorLocalMemberNotInRoster:
		return "current Pilot node is not in this roster; rejoin the group or use the data/identity directory that belongs to this node"
	case doctorLocalMemberIdentityMismatch:
		return "current Pilot node is in the roster with a different Entmoot key; use the matching identity file or rejoin with the current identity"
	default:
		return "local membership could not be verified; check the data directory, identity file, and Pilot socket"
	}
}

func peerSuggestion(gf *globalFlags, gid entmoot.GroupID, daemonRunning bool, group doctorGroupReport, peer doctorPeerReport) (string, string) {
	if peer.Diagnosis == "ok" {
		return "", ""
	}
	if peer.Trust == "self" && !group.LocalMember {
		return localMembershipSuggestion(group.LocalMemberStatus), ""
	}
	switch peer.Diagnosis {
	case "not_in_roster":
		return "peer is not in the local roster; sync the roster or verify the group id", ""
	case "trust_pending":
		return "Pilot trust is pending; approve the incoming handshake if this peer should connect", pilotctlCommand(gf, "approve", fmt.Sprint(peer.NodeID))
	case "trust_missing":
		return "Pilot trust is missing; send a handshake to this peer", pilotctlCommand(gf, "handshake", fmt.Sprint(peer.NodeID), fmt.Sprintf("entmoot group %s", gid.String()))
	case "unknown":
		return "Pilot trust state could not be queried; check pilot-daemon and rerun doctor", ""
	case "profile_missing":
		return "peer has not published a member profile yet; restart or wait for that peer's entmootd", ""
	case "transport_missing":
		return "peer has no current Entmoot transport advertisement; check that the peer daemon is running and joined to this group", ""
	case "transport_stale":
		return "peer transport advertisement is stale; restart or wait for that peer's entmootd to republish", ""
	case "daemon_down":
		return "local Entmoot daemon is not running; start the joined group before probing peers", serveGroupNextCommand(gf, gid)
	case "route_timeout":
		return "Entmoot stream probe timed out; verify the Pilot route and peer daemon", pilotctlCommand(gf, "ping", fmt.Sprint(peer.NodeID))
	case "route_refused":
		return "Entmoot stream was refused or closed; check that the peer entmootd is serving the group", ""
	case "probe_unsupported":
		return "peer or daemon does not support diagnostic probes; update Entmoot on both sides", ""
	case doctorLocalMemberPilotUnknown, doctorLocalMemberIdentityMismatch:
		return localMembershipSuggestion(peer.Diagnosis), ""
	default:
		if daemonRunning {
			return "diagnostic state is not healthy; inspect the row fields and rerun with --probe", ""
		}
		return "local Entmoot daemon is not running; start the joined group before probing peers", serveGroupNextCommand(gf, gid)
	}
}

func pilotctlCommand(gf *globalFlags, args ...string) string {
	parts := []string{"pilotctl"}
	if gf != nil && strings.TrimSpace(gf.socket) != "" {
		parts = append(parts, "-socket", gf.socket)
	}
	parts = append(parts, args...)
	for i, part := range parts {
		parts[i] = shellQuoteArg(part)
	}
	return strings.Join(parts, " ")
}

func serveGroupNextCommand(gf *globalFlags, gid entmoot.GroupID) string {
	args := []string{
		"entmootd",
		"-socket", gf.socket,
		"-identity", gf.identity,
		"-data", gf.data,
		"serve",
		"-group", gid.String(),
	}
	for i, arg := range args {
		args[i] = shellQuoteArg(arg)
	}
	return strings.Join(args, " ")
}

func classifyProbeError(err string) string {
	lower := strings.ToLower(err)
	switch {
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "deadline"):
		return "route_timeout"
	case strings.Contains(lower, "not found"), strings.Contains(lower, "refused"), strings.Contains(lower, "closed"):
		return "route_refused"
	case strings.Contains(lower, "unknown"):
		return "probe_unsupported"
	default:
		return "unknown"
	}
}

func parseTrustedNodeIDs(resp map[string]interface{}) []entmoot.NodeID {
	raw, ok := resp["trusted"].([]interface{})
	if !ok {
		return nil
	}
	out := make([]entmoot.NodeID, 0, len(raw))
	for _, item := range raw {
		obj, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		n, ok := obj["node_id"].(float64)
		if !ok || n < 0 {
			continue
		}
		out = append(out, entmoot.NodeID(uint32(n)))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func printDoctorHuman(report *doctorReport) {
	fmt.Printf("pilot: ")
	if report.Pilot.Reachable {
		fmt.Printf("ok node=%d hostname=%s\n", report.Pilot.NodeID, emptyDash(report.Pilot.Hostname))
	} else {
		fmt.Printf("down %s\n", report.Pilot.Error)
	}
	fmt.Printf("entmoot: ")
	if report.Entmoot.Running {
		fmt.Printf("running node=%d data=%s\n", report.Entmoot.NodeID, report.Entmoot.DataDir)
	} else {
		fmt.Printf("not_running data=%s\n", report.Entmoot.DataDir)
	}
	for _, group := range report.Groups {
		fmt.Printf("\ngroup %s members=%d messages=%d local_member=%t local_member_status=%s\n",
			group.GroupID, group.Members, group.Messages, group.LocalMember, group.LocalMemberStatus)
		if group.Suggestion != "" {
			fmt.Printf("suggestion: %s\n", group.Suggestion)
			if group.NextCommand != "" {
				fmt.Printf("next: %s\n", group.NextCommand)
			}
		}
		printPeersTable(group.Peers)
	}
}

func printPeersTable(peers []doctorPeerReport) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tHOSTNAME\tROSTER\tPROFILE\tTRANSPORT\tTRUST\tROUTE\tDIAGNOSIS\tSUGGESTION")
	for _, p := range peers {
		fmt.Fprintf(w, "%d\t%s\t%t\t%s\t%s\t%s\t%s\t%s\t%s\n",
			p.NodeID, emptyDash(p.Hostname), p.Roster, p.Profile, p.Transport, p.Trust, routeWithRTT(p), p.Diagnosis, suggestionSummary(p))
	}
	_ = w.Flush()
}

func suggestionSummary(p doctorPeerReport) string {
	if p.NextCommand != "" {
		return p.NextCommand
	}
	if p.Suggestion != "" {
		return "see doctor"
	}
	return "-"
}

func routeWithRTT(p doctorPeerReport) string {
	if p.Route == "ok" && p.RTTMS > 0 {
		return fmt.Sprintf("ok/%dms", p.RTTMS)
	}
	return p.Route
}

func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func redactDoctorReport(report *doctorReport) {
	report.Entmoot.DataDir = ""
}
