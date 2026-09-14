package roster

import (
	"errors"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// forkFixture is two chains that share a genesis and then disagree: what two
// authorised signers produce when both write against the same head.
type forkFixture struct {
	local   *RosterLog
	winning []entmoot.RosterEntry
	founder *keystore.Identity
	groupID entmoot.GroupID
	// losing is the member this node added on the chain that loses.
	losing entmoot.NodeInfo
	// winner is the member the adopted chain carries instead.
	winner entmoot.NodeInfo
}

func newForkFixture(t *testing.T) *forkFixture {
	t.Helper()
	f := newAdminFixture(t)
	// Both sides share the genesis plus one committed add.
	f.member(t)

	// Replay the shared prefix into a second log and extend it there.
	other := New(f.log.groupID)
	for i, entry := range f.log.Entries() {
		var err error
		if i == 0 {
			err = other.AcceptGenesis(entry)
		} else {
			err = other.Apply(entry)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	winnerIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	winner := mustInfo(t, winnerIdentity)
	remote, err := other.SignEntry(f.founder, "add", winner, nil, f.nextTime+10)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Apply(remote); err != nil {
		t.Fatal(err)
	}

	// This node adds a different member against the same head, and loses.
	loserIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	losing := mustInfo(t, loserIdentity)
	if err := f.sign(t, f.founder, "add", losing, nil); err != nil {
		t.Fatal(err)
	}
	fixture := &forkFixture{
		local: f.log, winning: other.Entries(), founder: f.founder,
		groupID: f.log.groupID, losing: losing, winner: winner,
	}
	if fixture.local.Head() == fixture.winning[len(fixture.winning)-1].ID {
		t.Fatal("fixture did not fork")
	}
	return fixture
}

// Adopting the winning chain is the whole point: afterwards the losing node
// holds exactly the winner's chain and knows what it lost.
func TestReplaceChainAdoptsTheWinningChainAndReportsWhatItDropped(t *testing.T) {
	f := newForkFixture(t)
	adopted := f.winning[len(f.winning)-1].ID

	dropped, err := f.local.ReplaceChain(f.winning)
	if err != nil {
		t.Fatal(err)
	}
	if len(dropped) != 1 || dropped[0].Subject.MemberID == nil || *dropped[0].Subject.MemberID != *f.losing.MemberID {
		t.Fatalf("dropped = %+v, want the local add of %s", dropped, f.losing.MemberID)
	}
	if f.local.Head() != adopted {
		t.Fatalf("head = %s, want the adopted %s", f.local.Head(), adopted)
	}
	if len(f.local.Entries()) != len(f.winning) {
		t.Fatalf("entries = %d, want %d", len(f.local.Entries()), len(f.winning))
	}
	if !f.local.IsMemberID(*f.winner.MemberID) {
		t.Fatal("the adopted chain's member is missing from the projection")
	}
	if f.local.IsMemberID(*f.losing.MemberID) {
		t.Fatal("the discarded member survived the repair")
	}

	// The repaired log must keep working: re-issuing the lost change has to
	// apply against the adopted head, which is what a repair does next.
	reissue, err := f.local.SignEntry(f.founder, "add", f.losing, nil, f.local.HeadTimestamp()+1)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.local.Apply(reissue); err != nil {
		t.Fatalf("re-issuing the lost change onto the adopted head: %v", err)
	}
	if !f.local.IsMemberID(*f.losing.MemberID) {
		t.Fatal("the re-issued member is not in the projection")
	}
	if f.local.CommonPrefix(f.winning) != len(f.winning) {
		t.Fatal("the repaired chain no longer contains the adopted chain as a prefix")
	}
}

// A repair must not be a way to move a group somewhere else, or to accept a
// chain the ordinary rules would refuse. Every refusal leaves the log intact.
func TestReplaceChainRefusesForeignAndInvalidChains(t *testing.T) {
	f := newForkFixture(t)
	before := f.local.Head()
	entries := len(f.local.Entries())

	stranger := newAdminFixture(t)
	cases := []struct {
		name  string
		chain []entmoot.RosterEntry
	}{
		{"different genesis", stranger.log.Entries()},
		{"empty chain", nil},
		{"broken signature", tamperedChain(f.winning)},
		{"missing middle entry", append(append([]entmoot.RosterEntry(nil), f.winning[0]), f.winning[2:]...)},
	}
	for _, tc := range cases {
		if _, err := f.local.ReplaceChain(tc.chain); !errors.Is(err, entmoot.ErrRosterReject) {
			t.Fatalf("%s was accepted: %v", tc.name, err)
		}
		if f.local.Head() != before || len(f.local.Entries()) != entries {
			t.Fatalf("%s changed the log", tc.name)
		}
	}
}

// The repair has to survive a restart, or the fork returns on reopen.
func TestReplaceChainPersistsTheAdoptedChain(t *testing.T) {
	root := t.TempDir()
	f := newForkFixture(t)

	persisted, err := OpenJSONL(root, f.groupID)
	if err != nil {
		t.Fatal(err)
	}
	// Commit the shared prefix, then this node's losing change, durably.
	shared := f.local.CommonPrefix(f.winning)
	for i, entry := range f.local.Entries() {
		if i == 0 {
			if err := persisted.AcceptGenesis(entry); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := persisted.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	forkedHead := persisted.Head()
	if forkedHead != f.local.Head() {
		t.Fatalf("durable fork head = %s, want %s", forkedHead, f.local.Head())
	}

	dropped, err := persisted.ReplaceChain(f.winning)
	if err != nil {
		t.Fatal(err)
	}
	if len(dropped) != len(f.local.Entries())-shared {
		t.Fatalf("dropped %d entries, want %d", len(dropped), len(f.local.Entries())-shared)
	}
	if err := persisted.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJSONL(root, f.groupID)
	if err != nil {
		t.Fatalf("reopen after repair: %v", err)
	}
	defer reopened.Close()
	if reopened.Head() != f.winning[len(f.winning)-1].ID {
		t.Fatalf("reopened head = %s, want the adopted %s", reopened.Head(), f.winning[len(f.winning)-1].ID)
	}
	if len(reopened.Entries()) != len(f.winning) {
		t.Fatalf("reopened entries = %d, want %d", len(reopened.Entries()), len(f.winning))
	}
	if !reopened.IsMemberID(*f.winner.MemberID) || reopened.IsMemberID(*f.losing.MemberID) {
		t.Fatal("the reopened projection does not match the adopted chain")
	}
}

func tamperedChain(chain []entmoot.RosterEntry) []entmoot.RosterEntry {
	out := append([]entmoot.RosterEntry(nil), chain...)
	broken := cloneEntry(out[len(out)-1])
	broken.Signature[0] ^= 0xff
	out[len(out)-1] = broken
	return out
}
