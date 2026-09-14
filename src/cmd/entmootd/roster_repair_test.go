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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		groupID: groupID, winning: remoteLog.Entries(), losing: losing, winner: winner,
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
	f.session.noteRosterSyncFailure(f.remote.ID, now, before, entmoot.RosterEntryID{0x01}, "pull failed: test")
	f.session.noteRosterSyncFailure(otherBinding.PeerID, now, before, entmoot.RosterEntryID{0x02}, "pull failed: test")
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
