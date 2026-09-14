package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// repairFixture is a real fork across two hosts: a founder serving the winning
// chain over the roster protocol, and this node holding a chain that branched
// at the same head.
type repairFixture struct {
	ctx     context.Context
	runtime *groupRuntime
	session *groupSession
	remote  peer.AddrInfo
	groupID entmoot.GroupID
	winning []entmoot.RosterEntry
	// remoteLog is the founder's chain, served over the roster protocol.
	remoteLog *roster.RosterLog
	// founderIdentity signs entries on the founder's chain.
	founderIdentity *keystore.Identity
	// losing is the member this node added on the branch that loses.
	losing entmoot.NodeInfo
	// winner is the member the founder's chain carries instead.
	winner entmoot.NodeInfo
}

// repairOptions shapes the fork a fixture builds.
type repairOptions struct {
	// localIsAdmin decides whether this node may author membership changes on
	// the adopted chain, which decides whether a lost change can be re-issued.
	localIsAdmin bool
	// sameSubject makes both branches admit the same person, which is what two
	// admins acting on the same request produce.
	sameSubject bool
}

// newRepairFixture builds the fork described by opts.
func newRepairFixture(t *testing.T, opts repairOptions) *repairFixture {
	t.Helper()
	// Generous: one of these tests repairs a chain longer than a single pull,
	// which signs and validates several thousand entries, and that is slow
	// under the race detector.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	founderIdentity, founder, _ := mustTestIdentity(t)
	localIdentity, local, localBinding := mustTestIdentity(t)

	founderHost, _, err := libp2ptransport.NewHost(ctx, founderIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = founderHost.Close() })
	localHost, _, err := libp2ptransport.NewHost(ctx, localIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = localHost.Close() })

	groupID := entmoot.GroupID{0x7f}
	timestamp := int64(1_000)
	next := func() int64 { timestamp += 10; return timestamp }

	// Shared prefix: the founder's genesis, this node as a member, and
	// optionally the delegation that lets it author changes.
	remoteLog := roster.New(groupID)
	if err := remoteLog.Genesis(founderIdentity, founder, next()); err != nil {
		t.Fatal(err)
	}
	apply := func(log *roster.RosterLog, signer *keystore.Identity, op string, subject entmoot.NodeInfo, policy []byte, at int64) entmoot.RosterEntry {
		t.Helper()
		entry, err := log.SignEntry(signer, op, subject, policy, at)
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Apply(entry); err != nil {
			t.Fatal(err)
		}
		return entry
	}
	apply(remoteLog, founderIdentity, "add", local, nil, next())
	if opts.localIsAdmin {
		policy, err := roster.MarshalAdminPolicy([]entmoot.MemberID{localBinding.MemberID})
		if err != nil {
			t.Fatal(err)
		}
		apply(remoteLog, founderIdentity, "policy_change", entmoot.NodeInfo{}, policy, next())
	}

	// Replay the shared prefix into this node's log.
	localLog := roster.New(groupID)
	for i, entry := range remoteLog.Entries() {
		if i == 0 {
			if err := localLog.AcceptGenesis(entry); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := localLog.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}

	// The fork: both sides add a different member against the same head.
	winnerIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	winner := mustNodeInfoFor(t, winnerIdentity)
	apply(remoteLog, founderIdentity, "add", winner, nil, next())

	loserIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	losing := mustNodeInfoFor(t, loserIdentity)
	if opts.sameSubject {
		// Both admins admit the same person: two different entries naming one
		// subject, so the adopted chain already says what this node lost.
		losing = winner
	}
	signer := founderIdentity
	if opts.localIsAdmin {
		signer = localIdentity
	}
	apply(localLog, signer, "add", losing, nil, next())

	server := &libp2ptransport.SyncServer{
		Host:      founderHost,
		Admission: libp2ptransport.NewBootstrapAdmission(),
		Store:     store.NewMemory(),
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) {
			if id != groupID {
				return nil, false
			}
			return remoteLog, true
		},
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}

	remote := peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}
	localHost.Peerstore().AddAddrs(remote.ID, remote.Addrs, time.Hour)
	session := &groupSession{groupID: groupID, roster: localLog}
	runtime := &groupRuntime{
		identity: localIdentity, binding: localBinding, host: localHost,
		dataDir: t.TempDir(), logger: slog.New(slog.DiscardHandler),
		sessions: map[entmoot.GroupID]*groupSession{groupID: session},
	}
	return &repairFixture{
		ctx: ctx, runtime: runtime, session: session, remote: remote,
		groupID: groupID, winning: remoteLog.Entries(), remoteLog: remoteLog,
		founderIdentity: founderIdentity, losing: losing, winner: winner,
	}
}

func mustNodeInfoFor(t *testing.T, identity *keystore.Identity) entmoot.NodeInfo {
	t.Helper()
	binding, err := libp2ptransport.BindingFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return entmoot.NodeInfo{MemberID: &binding.MemberID, PeerID: binding.PeerID.String(), EntmootPubKey: identity.PublicKey}
}

// The repair an operator runs: adopt the peer's chain over the network and
// re-issue the change this node lost, so both sides end up with everything.
func TestRepairAdoptsPeerChainAndReissuesLostChange(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true})
	adopted := f.winning[len(f.winning)-1].ID

	plan, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), false)
	if err != nil {
		t.Fatal(err)
	}
	if plan.remoteHead != adopted {
		t.Fatalf("remote head = %s, want %s", plan.remoteHead, adopted)
	}
	if len(plan.discarded) != 1 {
		t.Fatalf("discarded = %+v, want the one local add", plan.discarded)
	}
	if len(plan.reissued) != 1 || len(plan.unrecoverable) != 0 {
		t.Fatalf("reissued = %+v unrecoverable = %+v", plan.reissued, plan.unrecoverable)
	}
	if plan.shared != len(f.winning)-1 {
		t.Fatalf("shared entries = %d, want %d", plan.shared, len(f.winning)-1)
	}
	// Both members are present, which is the point: the repair adopted the
	// winner and put the loser's change back on top.
	if !f.session.roster.IsMemberID(*f.winner.MemberID) {
		t.Fatal("the adopted chain's member is missing")
	}
	if !f.session.roster.IsMemberID(*f.losing.MemberID) {
		t.Fatal("the discarded change was not re-issued")
	}
	if f.session.roster.CommonPrefix(f.winning) != len(f.winning) {
		t.Fatal("the repaired chain does not contain the adopted chain")
	}
}

// A node that cannot author the change it lost must say so. Reporting success
// there would leave a member silently missing from the group.
func TestRepairReportsChangesItCannotReissue(t *testing.T) {
	f := newRepairFixture(t, repairOptions{})

	plan, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.reissued) != 0 {
		t.Fatalf("an ordinary member re-issued a roster change: %+v", plan.reissued)
	}
	if len(plan.unrecoverable) != 1 {
		t.Fatalf("unrecoverable = %+v, want the one lost add", plan.unrecoverable)
	}
	if !strings.HasPrefix(plan.unrecoverable[0].Reason, "re-apply failed:") {
		t.Fatalf("reason = %q, want the refusal from the roster", plan.unrecoverable[0].Reason)
	}
	if f.session.roster.IsMemberID(*f.losing.MemberID) {
		t.Fatal("the lost member is present without a re-issued entry")
	}
	// The adoption still happened: convergence does not wait on authority.
	if f.session.roster.Head() != f.winning[len(f.winning)-1].ID {
		t.Fatal("the chain was not adopted")
	}
}

// A dry run is for deciding, so it must change nothing while still naming what
// would be lost.
func TestRepairDryRunChangesNothing(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true})
	before := f.session.roster.Head()
	entries := len(f.session.roster.Entries())

	plan, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.discarded) != 1 || plan.discarded[0].Reason != "would be discarded" {
		t.Fatalf("discarded = %+v", plan.discarded)
	}
	if len(plan.reissued) != 0 {
		t.Fatalf("a dry run re-issued %+v", plan.reissued)
	}
	if f.session.roster.Head() != before || len(f.session.roster.Entries()) != entries {
		t.Fatal("a dry run changed the roster")
	}
}

// Without a named peer and without recorded divergence there is nothing to
// adopt, and guessing would be worse than refusing.
func TestRepairRefusesWithoutATarget(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true})
	before := f.session.roster.Head()

	if _, err := f.runtime.repairRoster(f.ctx, f.session, "", false); err == nil {
		t.Fatal("a repair with no target and no divergence was accepted")
	}
	if f.session.roster.Head() != before {
		t.Fatal("a refused repair changed the roster")
	}

	// Two peers advertising different heads is exactly the case an operator
	// has to resolve, so the daemon must not choose for them.
	other, _, _ := mustTestIdentity(t)
	otherBinding, err := libp2ptransport.BindingFromPublicKey(other.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	f.session.noteRosterSyncFailure(f.remote.ID, now, before, entmoot.RosterEntryID{0x01}, "pull failed: test", true)
	f.session.noteRosterSyncFailure(otherBinding.PeerID, now, before, entmoot.RosterEntryID{0x02}, "pull failed: test", true)
	_, err = f.runtime.repairRoster(f.ctx, f.session, "", false)
	if err == nil || !strings.Contains(err.Error(), "different heads") {
		t.Fatalf("ambiguous divergence produced %v, want a refusal naming the disagreement", err)
	}
	if f.session.roster.Head() != before {
		t.Fatal("a refused repair changed the roster")
	}
}

// Two admins admitting the same person is the common concurrent case. After
// the repair the adopted chain already carries that member, so re-issuing the
// lost entry would be refused as a duplicate binding; the repair reports it as
// satisfied instead of as a failure an operator has to chase.
func TestRepairSkipsChangesTheAdoptedChainAlreadySatisfies(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true, sameSubject: true})

	plan, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.discarded) != 1 {
		t.Fatalf("discarded = %+v, want the one local add", plan.discarded)
	}
	if len(plan.reissued) != 0 {
		t.Fatalf("a satisfied change was re-issued: %+v", plan.reissued)
	}
	if len(plan.unrecoverable) != 0 {
		t.Fatalf("a satisfied change was reported unrecoverable: %+v", plan.unrecoverable)
	}
	if plan.discarded[0].Reason != "already satisfied by the adopted chain" {
		t.Fatalf("reason = %q", plan.discarded[0].Reason)
	}
	if !f.session.roster.IsMemberID(*f.winner.MemberID) {
		t.Fatal("the member both branches admitted is missing after the repair")
	}
	if len(f.session.roster.Entries()) != len(f.winning) {
		t.Fatalf("entries = %d, want the adopted %d with no duplicate re-issue", len(f.session.roster.Entries()), len(f.winning))
	}
}

// A peer that is merely behind is not a fork. Adopting its chain would delete
// committed history to fix nothing, so the repair refuses and says why.
func TestRepairRefusesAPeerThatIsMerelyBehind(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true})
	// Take the peer's chain, so this node is strictly ahead of it: the peer's
	// head is now on our chain and it has nothing we lack.
	if _, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), false); err != nil {
		t.Fatal(err)
	}
	head := f.session.roster.Head()
	entries := len(f.session.roster.Entries())

	_, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), false)
	if err == nil {
		t.Fatal("a repair against a peer that is behind was accepted")
	}
	if !strings.Contains(err.Error(), "is behind this node") {
		t.Fatalf("error = %v, want it to name the peer as behind rather than forked", err)
	}
	if f.session.roster.Head() != head || len(f.session.roster.Entries()) != entries {
		t.Fatalf("a refused repair rewrote the chain: head %s -> %s, %d -> %d entries",
			head, f.session.roster.Head(), entries, len(f.session.roster.Entries()))
	}
}

// A pull stops at a per-round ceiling, so a repair whose adopted chain is
// longer than that must chain several pulls. Failing here would make the
// documented escape hatch for a long-lived group unusable, which is the case
// most likely to need it.
func TestRepairAdoptsAChainLongerThanOnePull(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true})
	// Grow the peer's chain past one pull's ceiling with founder-signed policy
	// entries of a family this build does not interpret: cheap to produce and
	// accepted exactly like any other entry.
	foreign := []byte(`{"type":"legacy-identity-upgrade/v1"}`)
	timestamp := f.remoteLog.HeadTimestamp()
	// Comfortably past one round, so a single pull cannot coincidentally
	// cover the whole chain.
	for len(f.remoteLog.Entries()) < libp2ptransport.MaxRosterSyncEntries()+600 {
		timestamp++
		entry, err := f.remoteLog.SignEntry(f.founderIdentity, "policy_change", entmoot.NodeInfo{}, foreign, timestamp)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.remoteLog.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	want := f.remoteLog.Entries()
	if len(want) <= libp2ptransport.MaxRosterSyncEntries()+1 {
		t.Fatalf("peer chain is %d entries, want more than one pull's ceiling", len(want))
	}

	plan, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), false)
	if err != nil {
		t.Fatalf("repairing against a chain longer than one pull: %v", err)
	}
	if plan.remoteHead != want[len(want)-1].ID {
		t.Fatalf("adopted head %s, want %s", plan.remoteHead, want[len(want)-1].ID)
	}
	if got := len(f.session.roster.Entries()); got != len(want)+len(plan.reissued) {
		t.Fatalf("adopted %d entries, want %d plus %d re-issued", got, len(want), len(plan.reissued))
	}
	if !f.session.roster.IsMemberID(*f.winner.MemberID) {
		t.Fatal("the adopted chain's member is missing")
	}
}

// syncRoster is where the classification is composed: it decides the
// head-off-chain conjunct and the rejected/complete flags. The helpers are
// unit-tested, but only a real round proves the composition, and swapping
// either value re-opens a fixed defect.
func TestSyncRosterRecordsARealForkAndClearsItAfterRepair(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true})

	// A real round against a genuinely forked peer must record divergence.
	f.runtime.syncRoster(f.ctx, f.session)
	reports := f.session.rosterDivergenceReports(f.groupID)
	if len(reports) != 1 {
		t.Fatalf("a real round against a forked peer reported %+v, want one divergence", reports)
	}
	if reports[0].PeerID != f.remote.ID.String() {
		t.Fatalf("report names %s, want the forked peer %s", reports[0].PeerID, f.remote.ID)
	}
	if reports[0].LocalHead == reports[0].RemoteHead {
		t.Fatalf("report has one head twice: %+v", reports[0])
	}
	// The peer is backed off, so the next tick does not re-download its chain.
	if f.session.rosterSyncReady(f.remote.ID, time.Now()) {
		t.Fatal("a forked peer was not backed off")
	}

	// Repairing adopts the peer's chain and re-issues what was lost, so the
	// next round agrees with the peer and the report is gone.
	if _, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), false); err != nil {
		t.Fatal(err)
	}
	if reports := f.session.rosterDivergenceReports(f.groupID); len(reports) != 0 {
		t.Fatalf("the repair left a divergence report: %+v", reports)
	}
	// The report list is filtered, so it cannot tell a cleared record from one
	// hidden by the filter. Readiness can: a peer left backed off is not
	// pulled from for up to fifteen minutes.
	if !f.session.rosterSyncReady(f.remote.ID, time.Now()) {
		t.Fatal("the repair left the repaired peer backed off")
	}
	// And a further round against the same peer stays clean: our chain now
	// contains its whole chain, so it is behind, not forked.
	f.runtime.syncRoster(f.ctx, f.session)
	if reports := f.session.rosterDivergenceReports(f.groupID); len(reports) != 0 {
		t.Fatalf("a peer that is merely behind was reported as divergent: %+v", reports)
	}
	if !f.session.rosterSyncReady(f.remote.ID, time.Now()) {
		t.Fatal("a peer that is merely behind was backed off")
	}
}

// A peer that is behind us holds a head we already have and a shorter chain.
// The round must recognise that from the head probe alone: no pull, no
// divergence, no backoff. Classifying by the error text without the
// head conjunct, or dropping the equal-or-behind check, both land here.
func TestSyncRosterIgnoresAPeerThatIsSimplyBehind(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true})
	// Adopt the peer's chain, then grow ours past it, so the peer's head is on
	// our chain and its chain is shorter than our prefix.
	if _, err := f.runtime.repairRoster(f.ctx, f.session, f.remote.ID.String(), false); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		extra := mustNodeInfoFor(t, mustTestIdentityValue(t))
		entry, err := f.session.roster.SignEntry(f.runtime.identity, "add", extra, nil, f.session.roster.HeadTimestamp()+10)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.session.roster.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	if !f.session.roster.HasEntry(f.winning[len(f.winning)-1].ID) {
		t.Fatal("fixture: the peer's head is not on our chain")
	}

	head := f.session.roster.Head()
	entries := len(f.session.roster.Entries())
	f.runtime.syncRoster(f.ctx, f.session)

	if reports := f.session.rosterDivergenceReports(f.groupID); len(reports) != 0 {
		t.Fatalf("a peer behind us was reported as divergent: %+v", reports)
	}
	if !f.session.rosterSyncReady(f.remote.ID, time.Now()) {
		t.Fatal("a peer behind us was backed off, so later rounds will skip it")
	}
	if f.session.roster.Head() != head || len(f.session.roster.Entries()) != entries {
		t.Fatalf("a round against a peer behind us changed our chain: head %s -> %s", head, f.session.roster.Head())
	}
}

// The short-chain path: the forked peer holds FEWER entries than our prefix,
// so the server answers short_chain rather than serving a page. Only the
// caller knows whether the peer's head is on our chain, and the report depends
// on that conjunct being supplied here.
func TestSyncRosterRecordsAForkAgainstAShorterPeer(t *testing.T) {
	f := newRepairFixture(t, repairOptions{localIsAdmin: true})
	// Grow this node's branch past the peer's, so a pull asks for entries the
	// peer does not have.
	for i := 0; i < 2; i++ {
		extra := mustNodeInfoFor(t, mustTestIdentityValue(t))
		entry, err := f.session.roster.SignEntry(f.runtime.identity, "add", extra, nil, f.session.roster.HeadTimestamp()+10)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.session.roster.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.session.roster.Entries()) <= len(f.winning) {
		t.Fatalf("local chain is %d entries, want more than the peer's %d", len(f.session.roster.Entries()), len(f.winning))
	}

	f.runtime.syncRoster(f.ctx, f.session)
	reports := f.session.rosterDivergenceReports(f.groupID)
	if len(reports) != 1 {
		t.Fatalf("a shorter forked peer reported %+v, want one divergence", reports)
	}
	if reports[0].PeerID != f.remote.ID.String() {
		t.Fatalf("report names %s, want %s", reports[0].PeerID, f.remote.ID)
	}
}

func mustTestIdentityValue(t *testing.T) *keystore.Identity {
	t.Helper()
	identity, _, _ := mustTestIdentity(t)
	return identity
}
