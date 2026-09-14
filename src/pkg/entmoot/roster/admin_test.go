package roster

import (
	"encoding/json"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

type adminFixture struct {
	log      *RosterLog
	founder  *keystore.Identity
	nextTime int64
}

func newAdminFixture(t *testing.T) *adminFixture {
	t.Helper()
	founder, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	var groupID entmoot.GroupID
	groupID[0] = 0x51
	log := New(groupID)
	if err := log.Genesis(founder, mustInfo(t, founder), 1_000); err != nil {
		t.Fatal(err)
	}
	return &adminFixture{log: log, founder: founder, nextTime: 1_001}
}

func mustInfo(t *testing.T, identity *keystore.Identity) entmoot.NodeInfo {
	t.Helper()
	memberID, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := entmoot.PeerIDFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return entmoot.NodeInfo{EntmootPubKey: identity.PublicKey, MemberID: &memberID, PeerID: peerID}
}

// sign applies one entry signed by the given identity, returning the apply
// error so tests can assert refusals.
func (f *adminFixture) sign(t *testing.T, signer *keystore.Identity, op string, subject entmoot.NodeInfo, policy []byte) error {
	t.Helper()
	f.nextTime++
	entry, err := f.log.SignEntry(signer, op, subject, policy, f.nextTime)
	if err != nil {
		return err
	}
	return f.log.Apply(entry)
}

func (f *adminFixture) member(t *testing.T) (*keystore.Identity, entmoot.NodeInfo) {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	info := mustInfo(t, identity)
	if err := f.sign(t, f.founder, "add", info, nil); err != nil {
		t.Fatal(err)
	}
	return identity, info
}

func (f *adminFixture) grant(t *testing.T, admins ...entmoot.MemberID) {
	t.Helper()
	payload, err := MarshalAdminPolicy(admins)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.sign(t, f.founder, "policy_change", entmoot.NodeInfo{}, payload); err != nil {
		t.Fatal(err)
	}
}

// The point of delegated admins: membership can change while the founder is
// away, and only for the operations the founder delegated.
func TestDelegatedAdminAddsAndEvictsOrdinaryMembers(t *testing.T) {
	f := newAdminFixture(t)
	adminIdentity, admin := f.member(t)
	strangerIdentity, _ := keystore.Generate()
	stranger := mustInfo(t, strangerIdentity)

	if err := f.sign(t, adminIdentity, "add", stranger, nil); err == nil {
		t.Fatal("an ordinary member signed a roster add")
	}
	f.grant(t, *admin.MemberID)
	if !f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("granted admin cannot administer")
	}
	if err := f.sign(t, adminIdentity, "add", stranger, nil); err != nil {
		t.Fatalf("delegated admin add refused: %v", err)
	}
	if !f.log.IsMemberID(*stranger.MemberID) {
		t.Fatal("admin-added member is missing")
	}
	if err := f.sign(t, adminIdentity, "remove", stranger, nil); err != nil {
		t.Fatalf("delegated admin remove refused: %v", err)
	}
	if f.log.IsMemberID(*stranger.MemberID) {
		t.Fatal("admin-removed member is still present")
	}
}

// Delegation must not become a takeover: an admin cannot evict the founder,
// evict a peer admin, or hand authority to itself.
func TestDelegatedAdminCannotEscalate(t *testing.T) {
	f := newAdminFixture(t)
	firstIdentity, first := f.member(t)
	_, second := f.member(t)
	outsiderIdentity, _ := keystore.Generate()
	f.grant(t, *first.MemberID, *second.MemberID)

	founderInfo := mustInfo(t, f.founder)
	if err := f.sign(t, firstIdentity, "remove", founderInfo, nil); err == nil {
		t.Fatal("an admin removed the founder")
	}
	if err := f.sign(t, firstIdentity, "remove", second, nil); err == nil {
		t.Fatal("an admin removed another admin")
	}
	payload, err := MarshalAdminPolicy([]entmoot.MemberID{*first.MemberID, *second.MemberID})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.sign(t, firstIdentity, "policy_change", entmoot.NodeInfo{}, payload); err == nil {
		t.Fatal("an admin rewrote the admin set")
	}
	outsider := mustInfo(t, outsiderIdentity)
	if err := f.sign(t, outsiderIdentity, "add", outsider, nil); err == nil {
		t.Fatal("a non-member added itself")
	}
}

// Revoking delegation, and removing a delegated admin, both have to take the
// authority away immediately.
func TestAdminAuthorityEndsWithRevocationOrRemoval(t *testing.T) {
	f := newAdminFixture(t)
	adminIdentity, admin := f.member(t)
	f.grant(t, *admin.MemberID)

	f.grant(t)
	if f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("revoked admin still administers")
	}
	candidateIdentity, _ := keystore.Generate()
	candidate := mustInfo(t, candidateIdentity)
	if err := f.sign(t, adminIdentity, "add", candidate, nil); err == nil {
		t.Fatal("revoked admin signed a roster add")
	}

	f.grant(t, *admin.MemberID)
	if err := f.sign(t, f.founder, "remove", admin, nil); err != nil {
		t.Fatal(err)
	}
	if f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("removed admin still administers")
	}
	if len(f.log.Admins()) != 0 {
		t.Fatalf("admins after removal = %v, want none", f.log.Admins())
	}
	if err := f.sign(t, adminIdentity, "add", candidate, nil); err == nil {
		t.Fatal("removed admin signed a roster add")
	}
}

// Policy payloads that are not admin sets must pass through untouched: the
// legacy identity-upgrade checkpoint uses the same entry op.
func TestUnrelatedPolicyChangeLeavesAdminSetAlone(t *testing.T) {
	f := newAdminFixture(t)
	_, admin := f.member(t)
	f.grant(t, *admin.MemberID)

	other, err := json.Marshal(map[string]any{"type": "legacy-identity-upgrade/v1", "legacy_history_count": 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.sign(t, f.founder, "policy_change", entmoot.NodeInfo{}, other); err != nil {
		t.Fatalf("unrelated policy refused: %v", err)
	}
	if !f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("an unrelated policy change cleared the admin set")
	}
	broken, err := json.Marshal(map[string]any{"type": AdminPolicyType, "admins": []string{"not-base64"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.sign(t, f.founder, "policy_change", entmoot.NodeInfo{}, broken); err == nil {
		t.Fatal("an unreadable admin policy was accepted")
	}
}

// A policy payload this build cannot read must never leave delegated
// authority standing: validation refuses it, and a log loaded without
// validation reduces the admin set rather than keeping it.
func TestUndecodablePolicyIsRefusedAndAuthorityReducing(t *testing.T) {
	f := newAdminFixture(t)
	_, admin := f.member(t)
	f.grant(t, *admin.MemberID)

	truncated := []byte(`{"type":"admins/v1","admins":[`)
	if err := f.sign(t, f.founder, "policy_change", entmoot.NodeInfo{}, truncated); err == nil {
		t.Fatal("a policy payload that is not valid JSON was accepted")
	}
	if !f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("a rejected entry changed the admin set")
	}

	// The same bytes reaching the projection without validation, as a log
	// written by a broken writer would. This build cannot even sign such a
	// payload (canonical encoding refuses it), so the entry is assembled by
	// hand from a valid one.
	valid, err := MarshalAdminPolicy([]entmoot.MemberID{*admin.MemberID})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := f.log.SignEntry(f.founder, "policy_change", entmoot.NodeInfo{}, valid, f.nextTime+50)
	if err != nil {
		t.Fatal(err)
	}
	entry.Policy = truncated
	f.log.mu.Lock()
	f.log.applyLocked(entry)
	f.log.mu.Unlock()
	if f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("an unreadable policy left delegated authority standing")
	}
}

// A policy that claims to change the admin set in a version this build cannot
// read must be refused. Accepting it would leave this node honouring admins
// the founder may have just removed, while a newer peer applied the change:
// the two would then disagree about who may sign the next entry.
func TestUnknownAdminPolicyVersionIsRefusedAndAuthorityReducing(t *testing.T) {
	f := newAdminFixture(t)
	adminIdentity, admin := f.member(t)
	f.grant(t, *admin.MemberID)
	if !f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("the delegated admin was not granted")
	}

	future := []byte(`{"type":"admins/v2","admins":[],"quorum":2}`)
	if !IsUnknownAdminPolicy(future) {
		t.Fatal("a future admin version was not recognised as unreadable")
	}
	if IsUnknownAdminPolicy([]byte(`{"type":"legacy-identity-upgrade/v1"}`)) {
		t.Fatal("an unrelated policy was treated as an admin policy")
	}
	if err := f.sign(t, f.founder, "policy_change", entmoot.NodeInfo{}, future); err == nil {
		t.Fatal("an admin policy version this build cannot apply was accepted")
	}
	if !f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("a refused entry changed the admin set")
	}

	// Reaching the projection without validation (an older or broken writer)
	// must reduce authority rather than keep admins the payload does not state.
	entry, err := f.log.SignEntry(f.founder, "policy_change", entmoot.NodeInfo{}, future, f.nextTime+50)
	if err != nil {
		t.Fatal(err)
	}
	f.log.mu.Lock()
	f.log.applyLocked(entry)
	f.log.mu.Unlock()
	if f.log.CanAdminister(*admin.MemberID) {
		t.Fatal("an unreadable admin policy left delegated authority standing")
	}
	// The admin's ordinary membership is untouched; only delegation is lost.
	if !f.log.IsMemberID(*admin.MemberID) {
		t.Fatal("clearing delegation removed the member")
	}
	_ = adminIdentity
}
