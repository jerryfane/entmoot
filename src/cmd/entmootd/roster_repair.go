package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// maxRepairFetchRounds bounds how many ceiling-limited pulls one repair will
// chain together. A repair must be able to take a chain longer than a single
// round allows, but it must not page from a peer forever.
const maxRepairFetchRounds = 16

// repairPlan is what a repair would do, or did: the chain to adopt, the local
// entries that chain does not carry, and what became of each of them.
type repairPlan struct {
	peer          peer.AddrInfo
	chain         []entmoot.RosterEntry
	localHead     entmoot.RosterEntryID
	remoteHead    entmoot.RosterEntryID
	shared        int
	discarded     []ipc.RosterRepairEntry
	reissued      []ipc.RosterRepairEntry
	unrecoverable []ipc.RosterRepairEntry
}

// repairRoster ends a fork by adopting one peer's roster chain and re-issuing
// the local changes that chain does not carry.
//
// The log is strictly linear: two authorised signers who write against the
// same head produce two chains, and no amount of retrying merges them. One
// side has to give way. This picks the named peer's chain, re-signs our lost
// changes onto it, and reports anything it could not re-issue so an operator
// knows exactly what is still missing.
//
// A message published in the window between the fork and the repair, naming a
// discarded roster head, cannot be verified against the adopted chain. Repair
// trades that for a group that agrees again.
func (r *groupRuntime) repairRoster(ctx context.Context, session *groupSession, requested string, dryRun bool) (*repairPlan, error) {
	remote, err := r.repairTarget(session, requested)
	if err != nil {
		return nil, err
	}
	local := session.roster.Entries()
	if len(local) == 0 {
		return nil, errors.New("local roster is empty; join or import the group first")
	}
	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	// Ask from the genesis entry: the peer's chain diverges from ours, so only
	// the genesis is guaranteed common ground. A pull stops at a per-round
	// ceiling, so keep asking with what we already hold until the peer says
	// the chain is complete — a repair needs the whole chain, not a prefix.
	chain := []entmoot.RosterEntry{local[0]}
	for round := 0; ; round++ {
		if round == maxRepairFetchRounds {
			return nil, fmt.Errorf("peer %s served more than %d rounds of roster entries without completing its chain",
				remote.ID, maxRepairFetchRounds)
		}
		updates, complete, err := libp2ptransport.FetchRosterUpdates(fetchCtx, r.host, remote, session.groupID, chain)
		if err != nil {
			return nil, fmt.Errorf("fetch chain from %s: %w", remote.ID, err)
		}
		if len(updates) == 0 {
			if round == 0 {
				return nil, fmt.Errorf("peer %s served nothing beyond the genesis entry", remote.ID)
			}
			return nil, fmt.Errorf("peer %s stopped serving its chain before completing it", remote.ID)
		}
		chain = append(chain, updates...)
		if complete {
			break
		}
	}
	plan := &repairPlan{
		peer:       remote,
		chain:      chain,
		localHead:  session.roster.Head(),
		remoteHead: chain[len(chain)-1].ID,
		shared:     session.roster.CommonPrefix(chain),
	}
	keep := make(map[entmoot.RosterEntryID]struct{}, len(chain))
	for _, entry := range chain {
		keep[entry.ID] = struct{}{}
	}
	var discarded []entmoot.RosterEntry
	for _, entry := range local {
		if _, ok := keep[entry.ID]; !ok {
			discarded = append(discarded, entry)
		}
	}
	// A repair is for a fork: two chains, neither extending the other. If the
	// peer's whole chain is a prefix of ours it is simply behind, and adopting
	// it would delete committed history to fix nothing. The ordinary sync path
	// already ignores such a peer; so does this.
	if plan.shared == len(chain) {
		if plan.localHead == plan.remoteHead {
			return plan, nil
		}
		return nil, fmt.Errorf("peer %s is behind this node, not forked from it: its whole chain is already on ours, so there is nothing to repair",
			remote.ID)
	}
	for _, entry := range discarded {
		plan.discarded = append(plan.discarded, describeRepairEntry(entry, ""))
	}
	if dryRun {
		for i := range plan.discarded {
			plan.discarded[i].Reason = "would be discarded"
		}
		return plan, nil
	}

	dropped, err := session.roster.ReplaceChain(chain)
	if err != nil {
		return nil, fmt.Errorf("adopt chain from %s: %w", remote.ID, err)
	}
	// Report what was actually dropped, not the snapshot taken before the
	// fetch: another writer can commit while the chain is in flight, and a
	// change this repair discarded must appear in the report even then.
	// LocalHead stays the head the repair started from, which is what the
	// operator asked about.
	plan.discarded = plan.discarded[:0]
	for _, entry := range dropped {
		plan.discarded = append(plan.discarded, describeRepairEntry(entry, ""))
	}
	r.logger.Warn("roster repaired from peer",
		slog.String("group_id", session.groupID.String()),
		slog.String("peer_id", remote.ID.String()),
		slog.String("previous_head", plan.localHead.String()),
		slog.String("head", session.roster.Head().String()),
		slog.Int("shared_entries", plan.shared),
		slog.Int("discarded", len(dropped)))
	r.reissueDropped(session, dropped, plan)
	// The adopted head is new to this node, so messages held for it can be
	// accepted, and the peer we just synchronized with is no longer forked
	// from our point of view.
	r.drainRosterAhead(ctx, session)
	session.clearRosterSyncFailure(remote.ID)
	return plan, nil
}

// repairOutcome is what became of one discarded change.
type repairOutcome int

const (
	// repairReissued: this node signed the change again onto the adopted head.
	repairReissued repairOutcome = iota
	// repairSatisfied: the adopted chain already says what the change said, so
	// there is nothing to re-issue and nothing missing.
	repairSatisfied
	// repairLost: the change is not in effect and this node could not put it
	// back. An operator has to redo it from a node that can.
	repairLost
)

// reissueDropped re-signs each discarded change onto the adopted head, in the
// order it was originally made. A change the adopted chain already satisfies
// is skipped; one this node cannot put back is reported rather than silently
// forgotten.
func (r *groupRuntime) reissueDropped(session *groupSession, dropped []entmoot.RosterEntry, plan *repairPlan) {
	for _, entry := range dropped {
		reason, outcome := r.reissueEntry(session, entry)
		described := describeRepairEntry(entry, reason)
		switch outcome {
		case repairReissued:
			plan.reissued = append(plan.reissued, described)
		case repairLost:
			plan.unrecoverable = append(plan.unrecoverable, described)
		}
		for i := range plan.discarded {
			if plan.discarded[i].EntryID == described.EntryID {
				plan.discarded[i].Reason = described.Reason
			}
		}
	}
}

// reissueEntry re-applies one dropped change, reporting what became of it and
// a human-readable reason.
//
// Authority is not re-checked here: Apply is the authority of record, and a
// second copy of that policy could disagree with it. A change this node may no
// longer author therefore fails at Apply and is reported with its reason.
func (r *groupRuntime) reissueEntry(session *groupSession, entry entmoot.RosterEntry) (string, repairOutcome) {
	switch entry.Op {
	case "add", "remove":
		if entry.Subject.MemberID == nil {
			return "entry names no member id", repairLost
		}
		// The adopted chain may already say what this change said, in which
		// case re-issuing it would be refused as a no-op anyway.
		isMember := session.roster.IsMemberID(*entry.Subject.MemberID)
		if (entry.Op == "add") == isMember {
			return "already satisfied by the adopted chain", repairSatisfied
		}
	case "policy_change":
	default:
		return "unsupported op", repairLost
	}

	timestamp := time.Now().UnixMilli()
	if head := session.roster.HeadTimestamp(); timestamp <= head {
		timestamp = head + 1
	}
	replacement, err := session.roster.SignEntry(r.identity, entry.Op, entry.Subject, entry.Policy, timestamp)
	if err != nil {
		return "re-sign failed: " + err.Error(), repairLost
	}
	if err := session.roster.Apply(replacement); err != nil {
		return "re-apply failed: " + err.Error(), repairLost
	}
	return "re-issued as " + replacement.ID.String(), repairReissued
}

// repairTarget resolves which peer's chain to adopt. Naming one is the normal
// case; with exactly one divergent peer recorded the daemon can pick it, but
// it never guesses between peers that disagree with each other.
func (r *groupRuntime) repairTarget(session *groupSession, requested string) (peer.AddrInfo, error) {
	candidates := r.rosterSyncPeers(session)
	if requested != "" {
		id, err := peer.Decode(requested)
		if err != nil {
			return peer.AddrInfo{}, fmt.Errorf("peer %q: %w", requested, err)
		}
		for _, candidate := range candidates {
			if candidate.ID == id {
				return candidate, nil
			}
		}
		if addrs := r.host.Peerstore().Addrs(id); len(addrs) > 0 {
			return peer.AddrInfo{ID: id, Addrs: addrs}, nil
		}
		return peer.AddrInfo{}, fmt.Errorf("peer %s has no known address; wait for a connection or name a reachable member", id)
	}

	reports := session.rosterDivergenceReports(session.groupID)
	if len(reports) == 0 {
		return peer.AddrInfo{}, errors.New("no roster divergence recorded; name a peer explicitly to adopt its chain")
	}
	heads := make(map[string]struct{}, len(reports))
	for _, report := range reports {
		heads[report.RemoteHead] = struct{}{}
	}
	if len(reports) > 1 && len(heads) > 1 {
		return peer.AddrInfo{}, fmt.Errorf("%d peers advertise %d different heads; name the peer whose chain to adopt", len(reports), len(heads))
	}
	for _, report := range reports {
		id, err := peer.Decode(report.PeerID)
		if err != nil {
			continue
		}
		for _, candidate := range candidates {
			if candidate.ID == id {
				return candidate, nil
			}
		}
	}
	return peer.AddrInfo{}, errors.New("the divergent peers have no known address; wait for a connection or name a reachable member")
}

func describeRepairEntry(entry entmoot.RosterEntry, reason string) ipc.RosterRepairEntry {
	described := ipc.RosterRepairEntry{EntryID: entry.ID.String(), Op: entry.Op, Reason: reason}
	if entry.Subject.MemberID != nil {
		described.Subject = entry.Subject.MemberID.String()
	}
	if entry.ActorMemberID != nil {
		described.Actor = entry.ActorMemberID.String()
	}
	return described
}

// handleRosterRepair serves `roster repair` from the running daemon. The
// daemon holds the roster writer lease, so this is the only place a repair can
// happen while it runs.
func (s *ipcServer) handleRosterRepair(ctx context.Context, c net.Conn, req *ipc.RosterRepairReq) {
	gid := req.GroupID
	if gid == (entmoot.GroupID{}) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "roster_repair requires group_id"})
		return
	}
	if s.runtime == nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group not joined"})
		return
	}
	session, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group not joined"})
		return
	}
	// One repair at a time per group, and never concurrently with a roster or
	// history maintenance round.
	session.catchup.Lock()
	plan, err := s.runtime.repairRoster(ctx, session, req.Peer, req.DryRun)
	session.catchup.Unlock()
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "roster repair: " + err.Error()})
		return
	}
	status := "repaired"
	switch {
	case req.DryRun:
		status = "dry_run"
	case len(plan.discarded) == 0 && plan.localHead == plan.remoteHead:
		status = "already_converged"
	}
	_ = ipc.EncodeAndWrite(c, &ipc.RosterRepairResp{
		Status:        status,
		GroupID:       gid,
		Peer:          plan.peer.ID.String(),
		LocalHead:     plan.localHead,
		RemoteHead:    plan.remoteHead,
		RosterHead:    session.roster.Head(),
		SharedEntries: plan.shared,
		Members:       len(session.roster.MemberIDs()),
		Discarded:     plan.discarded,
		Reissued:      plan.reissued,
		Unrecoverable: plan.unrecoverable,
	})
}

// cmdRosterRepair implements `roster repair`. The fork it fixes can only be
// seen and fixed by a running daemon: it holds the roster writer lease and the
// peer connections, and it is what recorded the divergence in the first place.
func cmdRosterRepair(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("roster repair", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	peerStr := fs.String("peer", "", "peer id whose roster chain to adopt; optional when exactly one peer diverges")
	dryRun := fs.Bool("dry-run", false, "report what would be discarded and re-issued without changing anything")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster repair: %v\n", err)
		return exitInvalidArgument
	}
	socketPath := controlSocketPath(gf.data)
	if !controlSocketAlive(socketPath, 500*time.Millisecond) {
		fmt.Fprintf(os.Stderr, "roster repair: no running daemon at %s; a repair needs the live roster and peer connections, so start `serve` first\n", socketPath)
		return exitTransport
	}
	resp, err := rosterRepairOverIPC(context.Background(), socketPath, &ipc.RosterRepairReq{GroupID: gid, Peer: *peerStr, DryRun: *dryRun})
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster repair: %v\n", err)
		return exitTransport
	}
	data, err := json.Marshal(resp)
	if err != nil {
		slog.Error("roster repair: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	if len(resp.Unrecoverable) > 0 {
		fmt.Fprintf(os.Stderr, "roster repair: warning: %d discarded change(s) could not be re-issued from this node; redo them from a node that may author them\n", len(resp.Unrecoverable))
		for _, entry := range resp.Unrecoverable {
			fmt.Fprintf(os.Stderr, "  %s %s: %s\n", entry.Op, entry.Subject, entry.Reason)
		}
		return exitTransport
	}
	return exitOK
}

func rosterRepairOverIPC(ctx context.Context, socketPath string, req *ipc.RosterRepairReq) (*ipc.RosterRepairResp, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "unix", socketPath)
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	defer conn.Close()
	// A repair dials a peer and downloads its whole chain, so it needs more
	// than the ordinary request budget.
	if err := conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return nil, err
	}
	if err := ipc.EncodeAndWrite(conn, req); err != nil {
		return nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, err
	}
	switch v := payload.(type) {
	case *ipc.RosterRepairResp:
		return v, nil
	case *ipc.ErrorFrame:
		return nil, operationIPCError(v)
	default:
		return nil, fmt.Errorf("unexpected roster repair response %T", payload)
	}
}
