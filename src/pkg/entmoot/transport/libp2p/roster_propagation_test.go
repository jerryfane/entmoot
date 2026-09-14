package libp2ptransport

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

// Delegated admins author membership changes, so roster state must travel in
// both directions: a founder that only ever served its own chain would never
// learn about an admin-authored add, and the group would run on two heads.
func TestFounderPullsAdminAuthoredRosterEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	founderIdentity := mustIdentity(t)
	adminIdentity := mustIdentity(t)
	joinerIdentity := mustIdentity(t)

	adminHost, _, err := NewHost(ctx, adminIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer adminHost.Close()
	founderHost, _, err := NewHost(ctx, founderIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()

	// Both nodes start from the same chain: founder, admin member, admin grant.
	groupID := entmoot.GroupID{0x77}
	adminLog := buildAdminRoster(t, groupID, founderIdentity, adminIdentity)
	founderLog := replayRoster(t, groupID, adminLog.Entries())

	// The admin admits a new member on its own authority.
	joiner := mustNodeInfo(t, joinerIdentity.PublicKey)
	entry, err := adminLog.SignEntry(adminIdentity, "add", joiner, nil, adminLog.HeadTimestamp()+1)
	if err != nil {
		t.Fatal(err)
	}
	if err := adminLog.Apply(entry); err != nil {
		t.Fatal(err)
	}
	if founderLog.Head() == adminLog.Head() {
		t.Fatal("fixture did not diverge")
	}

	server := SyncServer{
		Host:      adminHost,
		Admission: NewBootstrapAdmission(),
		Roster: func(want entmoot.GroupID) (*roster.RosterLog, bool) {
			return adminLog, want == groupID
		},
		Store: store.NewMemory(),
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: adminHost.ID(), Addrs: adminHost.Addrs()}

	// The founder is a member, so it authorizes on membership alone and can
	// pull the entry the admin authored.
	head, err := FetchRosterHead(ctx, founderHost, remote, groupID)
	if err != nil {
		t.Fatal(err)
	}
	if head == founderLog.Head() {
		t.Fatal("admin peer advertised the founder's stale head")
	}
	if founderLog.HasEntry(head) {
		t.Fatal("advertised head is already known locally")
	}
	updates, err := FetchRosterUpdates(ctx, founderHost, remote, groupID, founderLog.Entries())
	if err != nil {
		t.Fatalf("founder could not pull admin-authored entries: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("pulled %d entries, want the admin's add", len(updates))
	}
	for _, update := range updates {
		if err := founderLog.Apply(update); err != nil {
			t.Fatalf("founder rejected an admin-authored entry: %v", err)
		}
	}
	if founderLog.Head() != adminLog.Head() {
		t.Fatalf("heads still differ: founder=%s admin=%s", founderLog.Head(), adminLog.Head())
	}
	if !founderLog.IsMemberID(*joiner.MemberID) {
		t.Fatal("founder does not see the admin-admitted member")
	}
}

// buildAdminRoster returns a log whose founder has delegated admin authority
// to the second identity.
func buildAdminRoster(t *testing.T, groupID entmoot.GroupID, founderIdentity, adminIdentity *keystore.Identity) *roster.RosterLog {
	t.Helper()
	log := roster.New(groupID)
	founder := mustNodeInfo(t, founderIdentity.PublicKey)
	if err := log.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	admin := mustNodeInfo(t, adminIdentity.PublicKey)
	add, err := log.SignEntry(founderIdentity, "add", admin, nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Apply(add); err != nil {
		t.Fatal(err)
	}
	policy, err := roster.MarshalAdminPolicy([]entmoot.MemberID{*admin.MemberID})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := log.SignEntry(founderIdentity, "policy_change", entmoot.NodeInfo{}, policy, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Apply(grant); err != nil {
		t.Fatal(err)
	}
	return log
}

// replayRoster rebuilds a second view of the same chain, as a peer loading it
// from disk would.
func replayRoster(t *testing.T, groupID entmoot.GroupID, entries []entmoot.RosterEntry) *roster.RosterLog {
	t.Helper()
	log := roster.New(groupID)
	if err := log.AcceptGenesis(entries[0]); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries[1:] {
		if err := log.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	return log
}
