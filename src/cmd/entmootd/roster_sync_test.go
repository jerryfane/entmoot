package main

import (
	"context"
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
	first := session.noteRosterSyncFailure(id, now, entmoot.RosterEntryID{1}, entmoot.RosterEntryID{2}, "pull failed: test")
	if first != rosterSyncBackoffBase {
		t.Fatalf("first backoff = %s, want %s", first, rosterSyncBackoffBase)
	}
	if session.rosterSyncReady(id, now.Add(first/2)) {
		t.Fatal("a backed-off peer was retried inside its window")
	}
	if !session.rosterSyncReady(id, now.Add(first+time.Second)) {
		t.Fatal("a backed-off peer was never retried")
	}
	second := session.noteRosterSyncFailure(id, now, entmoot.RosterEntryID{1}, entmoot.RosterEntryID{2}, "pull failed: test")
	if second <= first {
		t.Fatalf("backoff did not grow: %s then %s", first, second)
	}
	for i := 0; i < 20; i++ {
		if capped := session.noteRosterSyncFailure(id, now, entmoot.RosterEntryID{1}, entmoot.RosterEntryID{2}, "pull failed: test"); capped > rosterSyncBackoffMax {
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
	groupID := entmoot.GroupID{0x41}
	session := &groupSession{groupID: groupID}
	var id peer.ID = "12D3KooWBdvL92Hd76R1LN5qswuXSgQf7ZWZNwHhKeS4tDoHGzuA"
	if reports := session.rosterDivergenceReports(groupID); len(reports) != 0 {
		t.Fatalf("healthy session reported %+v", reports)
	}
	now := time.Now()
	local := entmoot.RosterEntryID{7}
	remote := entmoot.RosterEntryID{9}
	session.noteRosterSyncFailure(id, now, local, remote, "apply rejected: parents must reference current head")
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
	session.noteRosterSyncFailure(id, now.Add(time.Minute), local, remote, "apply rejected: parents must reference current head")
	if again := session.rosterDivergenceReports(groupID); again[0].SinceMS != report.SinceMS {
		t.Fatalf("since_ms moved from %d to %d", report.SinceMS, again[0].SinceMS)
	}
	session.clearRosterSyncFailure(id)
	if reports := session.rosterDivergenceReports(groupID); len(reports) != 0 {
		t.Fatalf("converged session still reports %+v", reports)
	}
}
