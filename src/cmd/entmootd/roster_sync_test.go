package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/roster"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// A founder that never pulls would never learn about an admin-authored add, so
// the candidate list has to include other members and exclude this node.
func TestRosterSyncPeersCoverOtherMembersFromTheFounder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity, founder, founderBinding := mustTestIdentity(t)
	memberIdentity, member, memberBinding := mustTestIdentity(t)

	host, _, err := libp2ptransport.NewHost(ctx, founderIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	groupID := entmoot.GroupID{0x33}
	groupRoster := roster.New(groupID)
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	add, err := groupRoster.SignEntry(founderIdentity, "add", member, nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := groupRoster.Apply(add); err != nil {
		t.Fatal(err)
	}
	// The member is only reachable because its address is in the peerstore.
	address := multiaddr.StringCast("/ip4/127.0.0.1/tcp/45999")
	host.Peerstore().AddAddr(memberBinding.PeerID, address, time.Hour)

	runtime := &groupRuntime{
		identity: founderIdentity, binding: founderBinding, host: host, dataDir: t.TempDir(),
		sessions: map[entmoot.GroupID]*groupSession{groupID: {groupID: groupID, roster: groupRoster}},
	}
	candidates := runtime.rosterSyncPeers(runtime.sessions[groupID])
	if len(candidates) != 1 {
		t.Fatalf("candidates = %+v, want exactly the other member", candidates)
	}
	if candidates[0].ID != memberBinding.PeerID {
		t.Fatalf("candidate = %s, want the member %s", candidates[0].ID, memberBinding.PeerID)
	}
	for _, candidate := range candidates {
		if candidate.ID == host.ID() {
			t.Fatal("candidate list includes this node")
		}
	}
	_ = memberIdentity
}

// A member with no known address is not a sync candidate, and the founder is
// tried first when it is reachable.
func TestRosterSyncPeersSkipUnreachableAndPreferFounder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity, founder, founderBinding := mustTestIdentity(t)
	localIdentity, local, localBinding := mustTestIdentity(t)
	_, silent, _ := mustTestIdentity(t)

	host, _, err := libp2ptransport.NewHost(ctx, localIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	groupID := entmoot.GroupID{0x34}
	groupRoster := roster.New(groupID)
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	for i, subject := range []entmoot.NodeInfo{local, silent} {
		entry, err := groupRoster.SignEntry(founderIdentity, "add", subject, nil, int64(2_000+i))
		if err != nil {
			t.Fatal(err)
		}
		if err := groupRoster.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	host.Peerstore().AddAddr(founderBinding.PeerID, multiaddr.StringCast("/ip4/127.0.0.1/tcp/45998"), time.Hour)

	runtime := &groupRuntime{
		identity: localIdentity, binding: localBinding, host: host, dataDir: t.TempDir(),
		sessions: map[entmoot.GroupID]*groupSession{groupID: {groupID: groupID, roster: groupRoster}},
	}
	candidates := runtime.rosterSyncPeers(runtime.sessions[groupID])
	if len(candidates) != 1 || candidates[0].ID != founderBinding.PeerID {
		t.Fatalf("candidates = %+v, want only the reachable founder", candidates)
	}
}

// A peer whose chain cannot be taken must not be re-downloaded every tick:
// without a backoff, one forked or hostile member makes every maintenance
// round expensive.
func TestRosterSyncBacksOffAFailingPeer(t *testing.T) {
	session := &groupSession{}
	var id peer.ID = "12D3KooWBdvL92Hd76R1LN5qswuXSgQf7ZWZNwHhKeS4tDoHGzuA"
	now := time.Now()
	if !session.rosterSyncReady(id, now) {
		t.Fatal("an unseen peer should be ready")
	}
	first := session.noteRosterSyncFailure(id, now, entmoot.RosterEntryID{1}, entmoot.RosterEntryID{2}, "pull failed: test", true)
	if first != rosterSyncBackoffBase {
		t.Fatalf("first backoff = %s, want %s", first, rosterSyncBackoffBase)
	}
	if session.rosterSyncReady(id, now.Add(first/2)) {
		t.Fatal("a backed-off peer was retried inside its window")
	}
	if !session.rosterSyncReady(id, now.Add(first+time.Second)) {
		t.Fatal("a backed-off peer was never retried")
	}
	second := session.noteRosterSyncFailure(id, now, entmoot.RosterEntryID{1}, entmoot.RosterEntryID{2}, "pull failed: test", true)
	if second <= first {
		t.Fatalf("backoff did not grow: %s then %s", first, second)
	}
	for i := 0; i < 20; i++ {
		if capped := session.noteRosterSyncFailure(id, now, entmoot.RosterEntryID{1}, entmoot.RosterEntryID{2}, "pull failed: test", true); capped > rosterSyncBackoffMax {
			t.Fatalf("backoff %s exceeds the cap %s", capped, rosterSyncBackoffMax)
		}
	}
	// Converging with a peer clears its record, so a transient failure does
	// not keep penalising a healthy member.
	session.clearRosterSyncFailure(id)
	if !session.rosterSyncReady(id, now) {
		t.Fatal("clearing the failure did not make the peer ready")
	}
}

// A fork is not repaired by retrying, so it has to be visible as state and
// not only as a log line that scrolls away.
func TestRosterDivergenceIsReportedAsGroupState(t *testing.T) {
	founderIdentity, founder, _ := mustTestIdentity(t)
	groupID := entmoot.GroupID{0x41}
	groupRoster := roster.New(groupID)
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	session := &groupSession{groupID: groupID, roster: groupRoster}
	var id peer.ID = "12D3KooWBdvL92Hd76R1LN5qswuXSgQf7ZWZNwHhKeS4tDoHGzuA"
	if reports := session.rosterDivergenceReports(groupID); len(reports) != 0 {
		t.Fatalf("healthy session reported %+v", reports)
	}
	now := time.Now()
	local := entmoot.RosterEntryID{7}
	remote := entmoot.RosterEntryID{9}
	session.noteRosterSyncFailure(id, now, local, remote, "apply rejected: parents must reference current head", true)
	reports := session.rosterDivergenceReports(groupID)
	if len(reports) != 1 {
		t.Fatalf("reports = %+v, want one", reports)
	}
	report := reports[0]
	if report.PeerID != id.String() || report.LocalHead != local.String() || report.RemoteHead != remote.String() {
		t.Fatalf("report = %+v", report)
	}
	if report.Reason == "" || report.SinceMS == 0 {
		t.Fatalf("report lacks a reason or first-seen time: %+v", report)
	}
	// The first-seen time must not reset on every retry, or a persistent fork
	// would always look new.
	session.noteRosterSyncFailure(id, now.Add(time.Minute), local, remote, "apply rejected: parents must reference current head", true)
	if again := session.rosterDivergenceReports(groupID); again[0].SinceMS != report.SinceMS {
		t.Fatalf("since_ms moved from %d to %d", report.SinceMS, again[0].SinceMS)
	}
	session.clearRosterSyncFailure(id)
	if reports := session.rosterDivergenceReports(groupID); len(reports) != 0 {
		t.Fatalf("converged session still reports %+v", reports)
	}
}

// Divergence means "this peer's chain does not extend ours", which no retry
// repairs. A timeout, an exhausted server snapshot or a rotated snapshot earns
// the same backoff but is not a fork: reporting those as divergence would send
// an operator to repair a healthy group.
func TestOnlyANonExtendingChainCountsAsDivergence(t *testing.T) {
	founderIdentity, founder, _ := mustTestIdentity(t)
	groupID := entmoot.GroupID{0x42}
	groupRoster := roster.New(groupID)
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	session := &groupSession{groupID: groupID, roster: groupRoster}
	var id peer.ID = "12D3KooWBdvL92Hd76R1LN5qswuXSgQf7ZWZNwHhKeS4tDoHGzuA"
	now := time.Now()

	transient := []error{
		context.DeadlineExceeded,
		errors.New("libp2p: roster page request failed: resource_exhausted"),
		errors.New("libp2p: roster snapshot changed"),
		errors.New("libp2p: roster pull exceeds 4096 new entries"),
		errors.New("failed to open stream: context canceled"),
	}
	for _, err := range transient {
		if rosterChainDiverged(err, true) {
			t.Fatalf("%v was classified as a fork", err)
		}
		session.noteRosterSyncFailure(id, now, groupRoster.Head(), entmoot.RosterEntryID{9}, "pull unavailable: "+err.Error(), false)
		if reports := session.rosterDivergenceReports(groupID); len(reports) != 0 {
			t.Fatalf("%v produced a divergence report: %+v", err, reports)
		}
		if session.rosterSyncReady(id, now) {
			t.Fatalf("%v did not back the peer off", err)
		}
	}

	forks := []error{
		fmt.Errorf("%w: parents must reference current head", entmoot.ErrRosterReject),
		errors.New("libp2p: roster head mismatch"),
	}
	for _, err := range forks {
		if !rosterChainDiverged(err, true) {
			t.Fatalf("%v was not classified as a fork", err)
		}
	}
	session.noteRosterSyncFailure(id, now, groupRoster.Head(), entmoot.RosterEntryID{9}, "pull failed: forked", true)
	if reports := session.rosterDivergenceReports(groupID); len(reports) != 1 {
		t.Fatalf("a fork produced %+v, want one report", reports)
	}
	// A timeout after a fork does not mean the fork healed, so the standing
	// report must survive it.
	session.noteRosterSyncFailure(id, now, groupRoster.Head(), entmoot.RosterEntryID{9}, "pull unavailable: context deadline exceeded", false)
	if reports := session.rosterDivergenceReports(groupID); len(reports) != 1 {
		t.Fatalf("a transient failure erased a standing fork report: %+v", reports)
	}

	// A report must stop being asserted once the head it names is on our
	// chain, even before the peer is retried.
	member := mustTestIdentityInfo(t)
	entry, err := groupRoster.SignEntry(founderIdentity, "add", member, nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := groupRoster.Apply(entry); err != nil {
		t.Fatal(err)
	}
	session.noteRosterSyncFailure(id, now, groupRoster.Head(), entry.ID, "pull failed: forked", true)
	if reports := session.rosterDivergenceReports(groupID); len(reports) != 0 {
		t.Fatalf("a head now on our chain is still reported as divergence: %+v", reports)
	}
}

func mustTestIdentityInfo(t *testing.T) entmoot.NodeInfo {
	t.Helper()
	_, info, _ := mustTestIdentity(t)
	return info
}

// A forked peer can be SHORTER than we are: it holds fewer entries and a head
// we do not have. The server reports that its chain ends before our prefix,
// and that must read as a fork, or the split is invisible and `roster repair`
// has nothing to name.
func TestAShorterForkedChainIsReportedAsDivergence(t *testing.T) {
	shortChain := fmt.Errorf("libp2p: roster sync: %s", libp2ptransport.SyncShortChain)
	if !rosterChainDiverged(shortChain, true) {
		t.Fatalf("%v was not classified as a fork", shortChain)
	}
	// The server cannot know our head, so the conjunct is the caller's: a
	// short chain from a peer whose head we DO hold is just a peer that pruned
	// or restarted, not a fork.
	if rosterChainDiverged(shortChain, false) {
		t.Fatal("a short chain from a peer whose head is on our chain was called a fork")
	}
	// And it must still be distinguishable from an ordinary malformed reply,
	// which says nothing about chains.
	if rosterChainDiverged(fmt.Errorf("libp2p: roster sync: %s", libp2ptransport.SyncMalformed), true) {
		t.Fatal("a malformed reply was classified as a fork")
	}
}

// What a finished pull leaves behind decides whether a fork stays visible and
// whether a node catching up gets delayed. A pull that applied some entries
// and then hit a rejection has still met a chain it cannot take: clearing its
// record because something applied would erase the evidence in the round it
// was found.
func TestRosterSyncOutcomeKeepsForkEvidenceAndFreesProgress(t *testing.T) {
	founderIdentity, founder, _ := mustTestIdentity(t)
	groupID := entmoot.GroupID{0x43}
	groupRoster := roster.New(groupID)
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	var id peer.ID = "12D3KooWBdvL92Hd76R1LN5qswuXSgQf7ZWZNwHhKeS4tDoHGzuA"
	now := time.Now()
	remoteHead := entmoot.RosterEntryID{0x5c}
	forked := func() *groupSession {
		session := &groupSession{groupID: groupID, roster: groupRoster}
		session.noteRosterSyncFailure(id, now, groupRoster.Head(), remoteHead,
			"apply rejected: parents must reference current head", true)
		if reports := session.rosterDivergenceReports(groupID); len(reports) != 1 {
			t.Fatalf("fixture did not record the fork: %+v", reports)
		}
		return session
	}

	// Applied some, then rejected: the fork stays reported and backed off.
	partial := forked()
	partial.noteRosterSyncOutcome(id, 3, true, false)
	if reports := partial.rosterDivergenceReports(groupID); len(reports) != 1 {
		t.Fatalf("a partially applied pull erased its fork record: %+v", reports)
	}
	if partial.rosterSyncReady(id, now) {
		t.Fatal("a partially applied pull cleared the backoff")
	}

	// Applied nothing at all: likewise untouched.
	empty := forked()
	empty.noteRosterSyncOutcome(id, 0, false, true)
	if reports := empty.rosterDivergenceReports(groupID); len(reports) != 1 {
		t.Fatalf("a pull that applied nothing erased its fork record: %+v", reports)
	}

	// Applied and complete: the peers agree, so nothing is left standing.
	agreed := forked()
	agreed.noteRosterSyncOutcome(id, 3, false, true)
	if reports := agreed.rosterDivergenceReports(groupID); len(reports) != 0 {
		t.Fatalf("a completed pull left a fork record: %+v", reports)
	}
	if !agreed.rosterSyncReady(id, now) {
		t.Fatal("a completed pull left the peer backed off")
	}

	// Applied, more to take: progress must not be delayed.
	progressing := forked()
	progressing.noteRosterSyncOutcome(id, 3, false, false)
	if !progressing.rosterSyncReady(id, now) {
		t.Fatal("a peer serving progress was backed off")
	}
}
