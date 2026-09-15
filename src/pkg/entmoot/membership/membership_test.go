package membership

import (
	"bytes"
	"crypto/rand"
	"errors"
	"math/big"
	"slices"
	"sort"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// fixture is a group plus the identities a test admits, removes and bans.
type fixture struct {
	t         *testing.T
	root      string
	groupID   entmoot.GroupID
	founder   *keystore.Identity
	group     *Group
	clockMS   int64
	nextNonce byte
}

func newFixture(t *testing.T, policy Policy) *fixture {
	t.Helper()
	founder := mustIdentity(t)
	f := &fixture{
		t:       t,
		root:    t.TempDir(),
		groupID: entmoot.GroupID{0x4d},
		founder: founder,
		clockMS: 1_000,
	}
	group, err := Create(f.root, founder, f.info(founder), f.groupID, policy, f.clockMS)
	if err != nil {
		t.Fatal(err)
	}
	group.SetNow(func() time.Time { return time.UnixMilli(f.clockMS) })
	t.Cleanup(func() { _ = group.Close() })
	f.group = group
	return f
}

func mustIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func (f *fixture) info(identity *keystore.Identity) entmoot.NodeInfo {
	f.t.Helper()
	info, err := identityInfo(identity)
	if err != nil {
		f.t.Fatal(err)
	}
	return info
}

func (f *fixture) memberID(identity *keystore.Identity) entmoot.MemberID {
	f.t.Helper()
	id, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f *fixture) tick(ms int64) { f.clockMS += ms }

// invite mints a capability the way a founder or admin does.
func (f *fixture) invite(issuer *keystore.Identity, target *keystore.Identity, maxUses int) entmoot.BootstrapCapability {
	f.t.Helper()
	capability := entmoot.BootstrapCapability{
		GroupID:     f.groupID,
		Founder:     f.info(f.founder),
		MaxUses:     maxUses,
		IssuedAtMS:  f.clockMS - 1,
		ExpiresAtMS: f.clockMS + 3_600_000,
	}
	if issuer != f.founder {
		issuerInfo := f.info(issuer)
		capability.Issuer = &issuerInfo
	}
	if target != nil {
		info := f.info(target)
		capability.TargetPublicKey = info.EntmootPubKey
		capability.TargetMemberID = *info.MemberID
		capability.TargetPeerID = info.PeerID
	}
	f.nextNonce++
	capability.Nonce[0] = f.nextNonce
	if _, err := rand.Read(capability.Nonce[1:]); err != nil {
		f.t.Fatal(err)
	}
	if err := SignInvite(issuer, &capability); err != nil {
		f.t.Fatal(err)
	}
	return capability
}

// sign builds and signs a record without applying it, so a test can apply the
// same set in different orders.
func (f *fixture) sign(identity *keystore.Identity, rec Record) Record {
	f.t.Helper()
	rec.GroupID = f.groupID
	rec.Actor = f.info(identity)
	if rec.Kind == KindJoin || rec.Kind == KindLeave {
		rec.Subject = rec.Actor
	}
	if rec.Timestamp == 0 {
		f.tick(10)
		rec.Timestamp = f.clockMS
	}
	signed, err := SignRecord(identity, rec)
	if err != nil {
		f.t.Fatal(err)
	}
	return signed
}

// join admits an identity with a fresh founder-issued invite.
func (f *fixture) join(identity *keystore.Identity) Record {
	f.t.Helper()
	capability := f.invite(f.founder, identity, 1)
	rec := f.sign(identity, Record{Kind: KindJoin, Invite: &capability})
	if _, err := f.group.Apply(rec); err != nil {
		f.t.Fatal(err)
	}
	return rec
}

// joinWith admits an identity by redeeming a caller-supplied invite, so a test
// can drive one invite's use limit.
func (f *fixture) joinWith(identity *keystore.Identity, capability entmoot.BootstrapCapability) Record {
	f.t.Helper()
	rec := f.sign(identity, Record{Kind: KindJoin, Invite: &capability})
	if _, err := f.group.Apply(rec); err != nil {
		f.t.Fatal(err)
	}
	return rec
}

func (f *fixture) apply(rec Record) {
	f.t.Helper()
	if _, err := f.group.Apply(rec); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) grantAdmin(admins ...entmoot.MemberID) Record {
	f.t.Helper()
	policy := f.group.Policy()
	policy.Admins = SortAdmins(admins)
	rec, err := f.group.SignRecord(f.founder, Record{Kind: KindPolicy, Policy: &policy})
	if err != nil {
		f.t.Fatal(err)
	}
	return rec
}

func shuffle(t *testing.T, records []Record) []Record {
	t.Helper()
	out := append([]Record(nil), records...)
	for i := len(out) - 1; i > 0; i-- {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			t.Fatal(err)
		}
		j := int(n.Int64())
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// The property the whole design rests on: two nodes that hold the same records
// agree on the membership, whatever order those records arrived in. Without
// it, concurrent writers fork the group the way the linear chain did.
func TestProjectionIsOrderIndependent(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	base := f.group.Canonical()

	var records []Record
	var joiners []*keystore.Identity
	for i := 0; i < 8; i++ {
		joiner := mustIdentity(t)
		joiners = append(joiners, joiner)
		capability := f.invite(f.founder, joiner, 1)
		records = append(records, f.sign(joiner, Record{Kind: KindJoin, Invite: &capability}))
	}
	// An admin, a removal, a ban, an unban, a leave, a rekey and a revoke, so
	// every record kind is in the mix.
	admin := joiners[0]
	policy := DefaultPolicy()
	policy.Admins = SortAdmins([]entmoot.MemberID{f.memberID(admin)})
	f.tick(10)
	records = append(records, f.sign(f.founder, Record{Kind: KindPolicy, Policy: &policy}))
	records = append(records, f.sign(admin, Record{Kind: KindRemove, Subject: f.info(joiners[1])}))
	records = append(records, f.sign(f.founder, Record{Kind: KindRemove, Subject: f.info(joiners[2]), Banned: true}))
	records = append(records, f.sign(f.founder, Record{Kind: KindRemove, Subject: f.info(joiners[3]), Banned: true}))
	f.tick(10)
	records = append(records, f.sign(f.founder, Record{Kind: KindUnban, Subject: f.info(joiners[3])}))
	records = append(records, f.sign(joiners[4], Record{Kind: KindLeave}))
	rekeyed := mustIdentity(t)
	records = append(records, f.sign(joiners[5], Record{Kind: KindRekey, Subject: f.info(rekeyed)}))
	revoked := f.invite(f.founder, nil, 4)
	records = append(records, f.sign(f.founder, Record{Kind: KindRevokeInvite, InviteNonce: revoked.Nonce}))
	stranger := mustIdentity(t)
	records = append(records, f.sign(stranger, Record{Kind: KindJoin, Invite: &revoked}))

	want, wantEffective := Project(base, records)
	for round := 0; round < 20; round++ {
		got, gotEffective := Project(base, shuffle(t, records))
		if len(gotEffective) != len(wantEffective) {
			t.Fatalf("round %d: %d effective records, want %d", round, len(gotEffective), len(wantEffective))
		}
		if !sameMembership(got, want) {
			t.Fatalf("round %d projected a different membership: %v vs %v", round, got.MemberIDs(), want.MemberIDs())
		}
	}

	// And the result is the one a reader would expect: the founder, the admin,
	// the two untouched joiners and the rekeyed identity. An unban clears the
	// bar to rejoining; it does not put the member back, so joiners[3] is out
	// until it redeems an invite again.
	if len(want.Members) != 5 {
		t.Fatalf("members = %d, want 5 (founder, admin, 2 untouched joiners, 1 rekeyed)", len(want.Members))
	}
	for _, absent := range []*keystore.Identity{joiners[1], joiners[2], joiners[3], joiners[4], joiners[5], stranger} {
		if _, member := want.Members[f.memberID(absent)]; member {
			t.Fatalf("identity %s should not be a member", f.memberID(absent))
		}
	}
	if _, member := want.Members[f.memberID(rekeyed)]; !member {
		t.Fatal("the rekeyed identity is not a member")
	}
	if _, banned := want.Banned[f.memberID(joiners[2])]; !banned {
		t.Fatal("a banned member is not banned")
	}
	if _, banned := want.Banned[f.memberID(joiners[3])]; banned {
		t.Fatal("an unbanned member is still banned")
	}
}

// An invite is the only thing that admits a stranger to a private group, so
// the rules around it decide who gets in.
func TestJoinNeedsAValidInviteUnlessOpen(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	stranger := mustIdentity(t)

	bare := f.sign(stranger, Record{Kind: KindJoin})
	f.apply(bare)
	if f.group.IsMemberID(f.memberID(stranger)) {
		t.Fatal("an invite-only group admitted a join with no invite")
	}

	// An invite signed by someone who is not the founder and not an admin.
	outsider := mustIdentity(t)
	forged := f.invite(outsider, stranger, 1)
	f.apply(f.sign(stranger, Record{Kind: KindJoin, Invite: &forged}))
	if f.group.IsMemberID(f.memberID(stranger)) {
		t.Fatal("a join with an unauthorised issuer was admitted")
	}

	// An invite bound to somebody else cannot even be signed into a join: the
	// binding is part of what a record has to be internally consistent about,
	// so it never reaches the group.
	other := mustIdentity(t)
	bound := f.invite(f.founder, other, 1)
	misused := Record{
		Kind: KindJoin, GroupID: f.groupID, Actor: f.info(stranger),
		Subject: f.info(stranger), Invite: &bound, Timestamp: f.clockMS + 1,
	}
	if _, err := SignRecord(stranger, misused); err == nil {
		t.Fatal("a join redeeming another identity's invite was signed")
	}

	// A previously delegated admin whose delegation was withdrawn.
	admin := mustIdentity(t)
	f.join(admin)
	f.grantAdmin(f.memberID(admin))
	adminInvite := f.invite(admin, stranger, 1)
	f.grantAdmin()
	f.apply(f.sign(stranger, Record{Kind: KindJoin, Invite: &adminInvite}))
	if f.group.IsMemberID(f.memberID(stranger)) {
		t.Fatal("a demoted admin's invite still admitted a member")
	}

	// The same bare join works once the group is open.
	policy := f.group.Policy()
	policy.JoinRule = JoinRuleOpen
	if _, err := f.group.SignRecord(f.founder, Record{Kind: KindPolicy, Policy: &policy}); err != nil {
		t.Fatal(err)
	}
	newcomer := mustIdentity(t)
	f.apply(f.sign(newcomer, Record{Kind: KindJoin}))
	if !f.group.IsMemberID(f.memberID(newcomer)) {
		t.Fatal("an open group refused a join with no invite")
	}
}

// A use limit has to hold without a coordinator counting redemptions, and it
// has to survive the records being folded into a checkpoint.
func TestInviteUsesAreCountedDeterministically(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	shared := f.invite(f.founder, nil, 2)

	var joins []Record
	var joiners []*keystore.Identity
	for i := 0; i < 3; i++ {
		joiner := mustIdentity(t)
		joiners = append(joiners, joiner)
		f.tick(10)
		joins = append(joins, f.sign(joiner, Record{Kind: KindJoin, Invite: &shared, Timestamp: f.clockMS}))
	}
	for _, join := range joins {
		f.apply(join)
	}

	admitted := 0
	for _, joiner := range joiners {
		if f.group.IsMemberID(f.memberID(joiner)) {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatalf("%d joiners admitted on a two-use invite, want 2", admitted)
	}
	// The two earliest win, so every node admits the same two.
	if !f.group.IsMemberID(f.memberID(joiners[0])) || !f.group.IsMemberID(f.memberID(joiners[1])) {
		t.Fatal("the admitted pair is not the two earliest joins")
	}

	// The count survives a checkpoint: a fourth attempt after the records are
	// folded away must still be refused.
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("checkpoint: signed=%t err=%v", signed, err)
	}
	if uses := f.group.Canonical().InviteUses; len(uses) != 1 || uses[0].Uses != 2 {
		t.Fatalf("checkpoint invite uses = %+v, want one nonce with 2 uses", uses)
	}
	late := mustIdentity(t)
	f.tick(10)
	f.apply(f.sign(late, Record{Kind: KindJoin, Invite: &shared, Timestamp: f.clockMS}))
	if f.group.IsMemberID(f.memberID(late)) {
		t.Fatal("an exhausted invite admitted another member after a checkpoint")
	}
}

// Removal must beat a concurrent join, and a member removed without a ban may
// come back with a later invite.
func TestRemoveWinsAndRejoinAfterRemoveNeedsLaterTimestamp(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)

	f.tick(10)
	removal := f.sign(f.founder, Record{Kind: KindRemove, Subject: f.info(member)})
	// A join authored before the removal, arriving after it, must not undo it.
	stale := f.invite(f.founder, member, 1)
	earlier := f.sign(member, Record{Kind: KindJoin, Invite: &stale, Timestamp: removal.Timestamp - 5})
	f.apply(removal)
	f.apply(earlier)
	if f.group.IsMemberID(f.memberID(member)) {
		t.Fatal("a join from before the removal re-admitted a removed member")
	}

	// A join after it does re-admit: plain removal is not a ban.
	f.tick(10)
	fresh := f.invite(f.founder, member, 1)
	f.apply(f.sign(member, Record{Kind: KindJoin, Invite: &fresh, Timestamp: f.clockMS}))
	if !f.group.IsMemberID(f.memberID(member)) {
		t.Fatal("a removed member could not rejoin with a later invite")
	}
}

func TestBanBlocksJoinUntilUnban(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)
	admin := mustIdentity(t)
	f.join(admin)
	f.grantAdmin(f.memberID(admin))

	if _, err := f.group.SignRecord(f.founder, Record{Kind: KindRemove, Subject: f.info(member), Banned: true}); err != nil {
		t.Fatal(err)
	}
	f.tick(10)
	again := f.invite(f.founder, member, 1)
	f.apply(f.sign(member, Record{Kind: KindJoin, Invite: &again, Timestamp: f.clockMS}))
	if f.group.IsMemberID(f.memberID(member)) {
		t.Fatal("a banned identity rejoined")
	}

	// An admin cannot lift a ban: that is the founder's decision.
	f.apply(f.sign(admin, Record{Kind: KindUnban, Subject: f.info(member)}))
	if !f.group.IsBanned(f.memberID(member)) {
		t.Fatal("an admin lifted a ban")
	}

	if _, err := f.group.SignRecord(f.founder, Record{Kind: KindUnban, Subject: f.info(member)}); err != nil {
		t.Fatal(err)
	}
	if f.group.IsBanned(f.memberID(member)) {
		t.Fatal("the founder's unban did not take effect")
	}
	f.tick(10)
	third := f.invite(f.founder, member, 1)
	f.apply(f.sign(member, Record{Kind: KindJoin, Invite: &third, Timestamp: f.clockMS}))
	if !f.group.IsMemberID(f.memberID(member)) {
		t.Fatal("an unbanned identity could not rejoin")
	}
}

func TestAdminCannotRemoveFounderOrAnotherAdmin(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	first := mustIdentity(t)
	second := mustIdentity(t)
	f.join(first)
	f.join(second)
	f.grantAdmin(f.memberID(first), f.memberID(second))

	f.apply(f.sign(first, Record{Kind: KindRemove, Subject: f.info(f.founder)}))
	if !f.group.IsMemberID(f.memberID(f.founder)) {
		t.Fatal("an admin removed the founder")
	}
	f.apply(f.sign(first, Record{Kind: KindRemove, Subject: f.info(second)}))
	if !f.group.IsMemberID(f.memberID(second)) {
		t.Fatal("an admin removed another admin")
	}
	// An admin may stand down.
	f.apply(f.sign(first, Record{Kind: KindRemove, Subject: f.info(first)}))
	if f.group.IsMemberID(f.memberID(first)) {
		t.Fatal("an admin could not remove itself")
	}
	// An ordinary member cannot remove anyone.
	ordinary := mustIdentity(t)
	f.join(ordinary)
	f.apply(f.sign(ordinary, Record{Kind: KindRemove, Subject: f.info(second)}))
	if !f.group.IsMemberID(f.memberID(second)) {
		t.Fatal("an ordinary member removed an admin")
	}
}

// Rotating a key is a member's own business, and a ban follows the person, not
// the key.
func TestRekeyMovesMembershipAndInheritsBan(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)
	replacement := mustIdentity(t)

	f.apply(f.sign(member, Record{Kind: KindRekey, Subject: f.info(replacement)}))
	if f.group.IsMemberID(f.memberID(member)) {
		t.Fatal("the old identity is still a member after a rekey")
	}
	if !f.group.IsMemberID(f.memberID(replacement)) {
		t.Fatal("the new identity is not a member after a rekey")
	}

	// A banned member cannot escape the ban by rekeying.
	banned := mustIdentity(t)
	f.join(banned)
	if _, err := f.group.SignRecord(f.founder, Record{Kind: KindRemove, Subject: f.info(banned), Banned: true}); err != nil {
		t.Fatal(err)
	}
	escape := mustIdentity(t)
	f.tick(10)
	f.apply(f.sign(banned, Record{Kind: KindRekey, Subject: f.info(escape), Timestamp: f.clockMS}))
	if f.group.IsMemberID(f.memberID(escape)) {
		t.Fatal("a banned member rekeyed its way back in")
	}
}

// A checkpoint is what makes the records behind it disposable, so a record
// from before one must never be applied again: that is what would let a
// discarded change come back.
func TestStaleRecordCannotResurrectAMember(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)
	rejoin := f.invite(f.founder, member, 1)
	f.tick(10)
	old := f.sign(member, Record{Kind: KindJoin, Invite: &rejoin, Timestamp: f.clockMS})

	if _, err := f.group.SignRecord(f.founder, Record{Kind: KindRemove, Subject: f.info(member)}); err != nil {
		t.Fatal(err)
	}
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("checkpoint: signed=%t err=%v", signed, err)
	}
	if f.group.IsMemberID(f.memberID(member)) {
		t.Fatal("the removed member survived the checkpoint")
	}

	_, err := f.group.Apply(old)
	if !errors.Is(err, ErrStale) {
		t.Fatalf("applying a pre-checkpoint join returned %v, want ErrStale", err)
	}
	if f.group.IsMemberID(f.memberID(member)) {
		t.Fatal("a stale join resurrected a removed member")
	}
}

func TestCheckpointRequiresAuthorisedSignerAndMatchingState(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)
	base := f.group.Canonical()

	// An ordinary member cannot sign a checkpoint.
	if _, _, err := f.group.SignCheckpoint(member, true); !errors.Is(err, ErrNotAuthorised) {
		t.Fatalf("an ordinary member signing a checkpoint returned %v, want ErrNotAuthorised", err)
	}

	// A checkpoint whose membership disagrees with the records we hold is
	// refused: otherwise a wrong checkpoint would rewrite what we know.
	state := f.group.State()
	delete(state.Members, f.memberID(member))
	f.tick(10)
	wrong := state.Checkpoint(f.groupID, base.Sequence+1, base.ID, 1, f.clockMS)
	signedWrong, err := SignCheckpoint(f.founder, f.info(f.founder), wrong)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(signedWrong); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("a mismatched checkpoint returned %v, want ErrCheckpointMismatch", err)
	}

	// A checkpoint chaining onto one we do not hold cannot be checked yet.
	orphan := f.group.State().Checkpoint(f.groupID, 9, entmoot.RosterEntryID{0x99}, 0, f.clockMS+1)
	signedOrphan, err := SignCheckpoint(f.founder, f.info(f.founder), orphan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(signedOrphan); !errors.Is(err, ErrUnknownPrevious) {
		t.Fatalf("an orphan checkpoint returned %v, want ErrUnknownPrevious", err)
	}
}

// Two admins may checkpoint the same sequence. Every node must pick the same
// one without asking anybody.
func TestConcurrentCheckpointsSettleOnOneCanonical(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	first, second := mustIdentity(t), mustIdentity(t)
	f.join(first)
	f.join(second)
	f.grantAdmin(f.memberID(first), f.memberID(second))
	f.tick(10)
	// A checkpoint that names both admins: a signer is judged by the
	// checkpoint before it, so the grant has to be in the base before either
	// admin can sign.
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("base checkpoint: signed=%t err=%v", signed, err)
	}
	base := f.group.Canonical()
	state := f.group.State()

	f.tick(10)
	third := mustIdentity(t)
	f.join(third)
	state, _ = Project(base, f.group.Pending())
	early := state.Checkpoint(f.groupID, base.Sequence+1, base.ID, 1, f.clockMS)
	earlySigned, err := SignCheckpoint(first, f.info(first), early)
	if err != nil {
		t.Fatal(err)
	}
	late := state.Checkpoint(f.groupID, base.Sequence+1, base.ID, 1, f.clockMS+5)
	lateSigned, err := SignCheckpoint(second, f.info(second), late)
	if err != nil {
		t.Fatal(err)
	}

	// Apply the later one first: the earlier must still win, or two nodes that
	// received them in different orders would disagree.
	if _, err := f.group.ApplyCheckpoint(lateSigned); err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(earlySigned); err != nil {
		t.Fatal(err)
	}
	if got := f.group.Canonical().ID; got != earlySigned.ID {
		t.Fatalf("canonical checkpoint is %s, want the earlier %s", got, earlySigned.ID)
	}
	if !f.group.IsMemberID(f.memberID(third)) {
		t.Fatal("settling lost the member the checkpoints folded in")
	}
}

// A founder-signed checkpoint wins its sequence even when an admin signed one
// earlier: it is the only kind a node holding no group state can adopt, so
// preferring it is what keeps a group joinable while admins keep checkpointing.
func TestFounderSignedCheckpointWinsItsSequence(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	admin := mustIdentity(t)
	f.join(admin)
	f.grantAdmin(f.memberID(admin))
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("base checkpoint: signed=%t err=%v", signed, err)
	}
	base := f.group.Canonical()

	f.tick(10)
	f.join(mustIdentity(t))
	state, _ := Project(base, f.group.Pending())
	adminBody := state.Checkpoint(f.groupID, base.Sequence+1, base.ID, 1, f.clockMS)
	adminSigned, err := SignCheckpoint(admin, f.info(admin), adminBody)
	if err != nil {
		t.Fatal(err)
	}
	founderBody := state.Checkpoint(f.groupID, base.Sequence+1, base.ID, 1, f.clockMS+50)
	founderSignedCP, err := SignCheckpoint(f.founder, f.info(f.founder), founderBody)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(adminSigned); err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(founderSignedCP); err != nil {
		t.Fatal(err)
	}
	if got := f.group.Canonical().ID; got != founderSignedCP.ID {
		t.Fatalf("canonical is %s, want the founder-signed %s", got, founderSignedCP.ID)
	}
	// And a node starting from nothing can adopt what this group now offers,
	// which is the whole reason for the preference.
	adopted, err := Adopt(t.TempDir(), f.group.Canonical())
	if err != nil {
		t.Fatalf("a joiner could not adopt the canonical checkpoint: %v", err)
	}
	defer adopted.Close()
}

// A member granted admin authority inside the window a checkpoint covers may
// not sign that checkpoint: a peer judges a checkpoint by its predecessor
// alone, so one signed on the strength of a grant the predecessor does not
// carry would be unverifiable — and would then make every later checkpoint
// unreachable for naming an unknown previous.
func TestFreshlyGrantedAdminWaitsOneCheckpoint(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	admin := mustIdentity(t)
	f.join(admin)
	f.grantAdmin(f.memberID(admin))
	f.tick(10)
	if _, _, err := f.group.SignCheckpoint(admin, true); !errors.Is(err, ErrNotAuthorised) {
		t.Fatalf("a freshly granted admin signed the checkpoint carrying its own grant: %v", err)
	}
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("founder checkpoint: signed=%t err=%v", signed, err)
	}
	// Now the base names it, so the next one is its to sign.
	f.tick(10)
	f.join(mustIdentity(t))
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(admin, true); err != nil || !signed {
		t.Fatalf("the admin could not sign the following checkpoint: signed=%t err=%v", signed, err)
	}
}

func TestRetentionKeepsOneCheckpointLag(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	admin := mustIdentity(t)
	f.join(admin)
	f.grantAdmin(f.memberID(admin))
	f.tick(10)
	// A checkpoint that carries the grant: a signer is judged by the
	// checkpoint before it, so the admin can only sign once one names it.
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("grant checkpoint: signed=%t err=%v", signed, err)
	}
	first := mustIdentity(t)
	firstJoin := f.join(first)
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("first checkpoint: signed=%t err=%v", signed, err)
	}

	// The same body, signed by the other admin: checking it requires the
	// records behind the head, which a node that dropped them could not do.
	held := f.group.Canonical()
	competing := held
	competing.ID = entmoot.RosterEntryID{}
	competing.Signer = entmoot.NodeInfo{}
	competing.Signature = nil
	signedCompeting, err := SignCheckpoint(admin, f.info(admin), competing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(signedCompeting); err != nil {
		t.Fatalf("a checkpoint at the held sequence was refused: %v", err)
	}
	if !f.group.IsMemberID(f.memberID(first)) {
		t.Fatal("settling two checkpoints at one sequence lost a member")
	}

	second := mustIdentity(t)
	f.join(second)
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("second checkpoint: signed=%t err=%v", signed, err)
	}
	if !f.group.IsMemberID(f.memberID(first)) || !f.group.IsMemberID(f.memberID(second)) {
		t.Fatal("retiring records lost a member")
	}
	if got := f.group.Canonical().Sequence; got != 3 {
		t.Fatalf("canonical sequence = %d, want 3", got)
	}
	// A member admitted after the newest checkpoint: its record is still live,
	// so the served set is not empty and the assertions below are about what a
	// receiver is actually handed.
	third := mustIdentity(t)
	thirdJoin := f.join(third)

	// What a consumer sees of retirement: a record the canonical checkpoint has
	// folded in is inside it, so re-offering one is never taken as new and
	// changes no membership; and a fresh peer handed exactly what this node
	// serves reaches the same members — including the one admitted since the
	// checkpoint — and can still admit the next one.
	before := f.group.MemberIDs()
	reapplied, err := f.group.Apply(firstJoin)
	if err != nil && !errors.Is(err, ErrStale) {
		t.Fatalf("re-applying a record the checkpoint covers: %v", err)
	}
	if reapplied {
		t.Fatal("a record the canonical checkpoint already covers was accepted as new")
	}
	if got := f.group.MemberIDs(); !slices.Equal(got, before) {
		t.Fatalf("replaying a covered record changed the members to %v from %v", got, before)
	}

	// The folded record is also not handed to anyone else, while the record the
	// checkpoint does not cover still is: the served set is what a peer
	// receives, and a record already inside the canonical checkpoint is one the
	// receiver would have to refuse.
	offered := f.group.Pending()
	if !slices.ContainsFunc(offered, func(rec Record) bool { return rec.ID == thirdJoin.ID }) {
		t.Fatalf("the join that no checkpoint covers is not offered to peers: served %d records", len(offered))
	}
	for _, rec := range offered {
		if rec.ID == firstJoin.ID {
			t.Fatalf("the canonical checkpoint at sequence %d covers the %s record at %d, but it is still offered to peers",
				f.group.Canonical().Sequence, firstJoin.Kind, firstJoin.Timestamp)
		}
	}

	fresh := adoptServedSet(t, f.group)
	if got := fresh.MemberIDs(); !slices.Equal(got, f.group.MemberIDs()) {
		t.Fatalf("a peer given the served set holds %v, this node holds %v", got, f.group.MemberIDs())
	}
	newcomer := mustIdentity(t)
	welcome := f.invite(f.founder, newcomer, 1)
	if _, err := fresh.Apply(f.sign(newcomer, Record{Kind: KindJoin, Invite: &welcome})); err != nil {
		t.Fatalf("the fresh peer refused a join against its adopted checkpoint: %v", err)
	}
	if !fresh.IsMemberID(f.memberID(newcomer)) {
		t.Fatal("the fresh peer did not admit a newcomer joining under the checkpoint it adopted")
	}
}

// adoptServedSet builds the group a fresh peer ends up with when it is handed
// what a member serves: the retained checkpoints and the pending records. The
// peer starts from the canonical head, which is the only thing a node with no
// group state can check, and then takes what it can of the rest — an
// admin-signed checkpoint is believable only to a node that holds the records
// granting that admin, so a receiver skips those instead of trusting them.
// What must hold is that this is enough to reach the same members, and that
// every record the source serves is one the receiver can apply: a record the
// canonical checkpoint has already folded in would be refused here.
func adoptServedSet(t *testing.T, source *Group) *Group {
	t.Helper()
	served := source.CheckpointsSince(0)
	if len(served) == 0 {
		t.Fatal("the source group served no checkpoints, so a fresh peer could not start")
	}
	head := source.Canonical()
	fresh, err := Adopt(t.TempDir(), head)
	if err != nil {
		t.Fatalf("adopting the served head at sequence %d: %v", head.Sequence, err)
	}
	t.Cleanup(func() { _ = fresh.Close() })
	for _, checkpoint := range served {
		if checkpoint.ID == head.ID {
			continue
		}
		if _, err := fresh.ApplyCheckpoint(checkpoint); err != nil && !errors.Is(err, ErrNotAuthorised) && !errors.Is(err, ErrUnknownPrevious) {
			t.Fatalf("a peer given served checkpoint %d failed for an unexpected reason: %v", checkpoint.Sequence, err)
		}
	}
	if got := fresh.Canonical().ID; got != head.ID {
		t.Fatalf("a peer given the served set settled on checkpoint %s, the source holds %s", got, head.ID)
	}
	for _, record := range source.Pending() {
		if _, err := fresh.Apply(record); err != nil {
			t.Fatalf("a peer given the served set refused the %s record at %d: %v", record.Kind, record.Timestamp, err)
		}
	}
	return fresh
}

func TestOpenReloadsExactState(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)
	admin := mustIdentity(t)
	f.join(admin)
	f.grantAdmin(f.memberID(admin))
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("checkpoint: signed=%t err=%v", signed, err)
	}
	pending := mustIdentity(t)
	f.join(pending)

	wantCanonical := f.group.Canonical()
	wantMembers := f.group.MemberIDs()
	wantPending := len(f.group.Pending())
	if err := f.group.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(f.root, f.groupID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Canonical().ID; got != wantCanonical.ID {
		t.Fatalf("reopened canonical %s, want %s", got, wantCanonical.ID)
	}
	if got := len(reopened.Pending()); got != wantPending {
		t.Fatalf("reopened pending = %d, want %d", got, wantPending)
	}
	if got := reopened.MemberIDs(); len(got) != len(wantMembers) {
		t.Fatalf("reopened members = %d, want %d", len(got), len(wantMembers))
	}
	if !reopened.CanAdminister(f.memberID(admin)) {
		t.Fatal("the reopened group lost its delegated admin")
	}
	if !reopened.IsMemberID(f.memberID(pending)) {
		t.Fatal("the reopened group lost a member admitted after the checkpoint")
	}
}

// A join that cannot take effect must say why, so a joiner is told rather than
// left guessing. The sentences are user-facing prose and may be reworded; what
// has to hold is that the join really is refused, that the group state carries
// the cause, that each cause earns its own non-empty explanation, and that a
// join which does take effect is explained as nothing at all.
func TestExplainJoinNamesTheReason(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	reasons := make(map[string]string, 4)

	// refused records the reason the group gives for a join, having proved the
	// join does not take effect.
	refused := func(cause string, rec Record) {
		t.Helper()
		subject, err := rec.SubjectMemberID()
		if err != nil {
			t.Fatalf("%s: join record names no usable subject: %v", cause, err)
		}
		reason := ExplainJoin(f.group.State(), rec)
		if reason == "" {
			t.Fatalf("%s: the group gave no reason for a join it refuses", cause)
		}
		if _, err := f.group.Apply(rec); err != nil {
			t.Fatalf("%s: apply: %v", cause, err)
		}
		if f.group.IsMemberID(subject) {
			t.Fatalf("%s: the join took effect anyway (reason %q)", cause, reason)
		}
		reasons[cause] = reason
	}

	stranger := mustIdentity(t)
	bare := f.sign(stranger, Record{Kind: KindJoin})
	if rule := f.group.Policy().JoinRule; rule != JoinRuleInvite || bare.Invite != nil {
		t.Fatalf("fixture: want an invite-only group and an inviteless join, got rule %q invite=%t", rule, bare.Invite != nil)
	}
	refused("no invite", bare)

	capability := f.invite(f.founder, stranger, 1)
	if _, err := f.group.SignRecord(f.founder, Record{Kind: KindRevokeInvite, InviteNonce: capability.Nonce}); err != nil {
		t.Fatal(err)
	}
	f.tick(10)
	revoked := f.sign(stranger, Record{Kind: KindJoin, Invite: &capability, Timestamp: f.clockMS})
	if _, gone := f.group.State().RevokedInvites[capability.Nonce]; !gone {
		t.Fatal("fixture: the invite is not revoked in the group state")
	}
	refused("revoked invite", revoked)

	shared := f.invite(f.founder, nil, 1)
	firstUser := mustIdentity(t)
	if _, err := f.group.Apply(f.sign(firstUser, Record{Kind: KindJoin, Invite: &shared})); err != nil {
		t.Fatal(err)
	}
	if !f.group.IsMemberID(f.memberID(firstUser)) {
		t.Fatal("fixture: the single-use invite admitted nobody")
	}
	latecomer := mustIdentity(t)
	late := f.sign(latecomer, Record{Kind: KindJoin, Invite: &shared})
	if used := f.group.State().InviteUses[shared.Nonce]; used < shared.Uses() {
		t.Fatalf("fixture: the invite has %d of %d uses spent", used, shared.Uses())
	}
	refused("invite exhausted", late)

	banned := mustIdentity(t)
	f.join(banned)
	if _, err := f.group.SignRecord(f.founder, Record{Kind: KindRemove, Subject: f.info(banned), Banned: true}); err != nil {
		t.Fatal(err)
	}
	f.tick(10)
	again := f.invite(f.founder, banned, 1)
	attempt := f.sign(banned, Record{Kind: KindJoin, Invite: &again, Timestamp: f.clockMS})
	if !f.group.IsBanned(f.memberID(banned)) {
		t.Fatal("fixture: the identity is not banned in the group state")
	}
	refused("banned", attempt)

	// A join that does take effect leaves nothing to explain, so a caller can
	// read the empty reason as "no problem".
	admitted := f.join(mustIdentity(t))
	if reason := ExplainJoin(f.group.State(), admitted); reason != "" {
		t.Fatalf("an effective join was explained as %q", reason)
	}

	// Each cause earns its own sentence: one reason serving two different
	// problems tells a caller nothing it can act on.
	byReason := make(map[string]string, len(reasons))
	for cause, reason := range reasons {
		if other, clash := byReason[reason]; clash {
			t.Fatalf("%q and %q are both explained as %q", cause, other, reason)
		}
		byReason[reason] = cause
	}
}

// Project itself must ignore a record the base checkpoint already accounts
// for, not only the store that calls it: verifying a peer's checkpoint means
// projecting our own records onto it, and a replayed older record leaking into
// that calculation would quietly undo a change the group has moved past.
func TestProjectIgnoresRecordsTheCheckpointAlreadyCovers(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("checkpoint: signed=%t err=%v", signed, err)
	}
	base := f.group.Canonical()
	if !f.group.IsMemberID(f.memberID(member)) {
		t.Fatal("fixture: the member should be in the checkpoint")
	}

	// A founder-signed removal from before the checkpoint. It is perfectly
	// valid in isolation, which is the point: only its age disqualifies it.
	replayed, err := SignRecord(f.founder, Record{
		Kind: KindRemove, GroupID: f.groupID, Actor: f.info(f.founder),
		Subject: f.info(member), Timestamp: base.Timestamp - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	state, effective := Project(base, []Record{replayed})
	if len(effective) != 0 {
		t.Fatalf("a record older than the checkpoint was effective: %+v", effective)
	}
	if _, stillMember := state.Members[f.memberID(member)]; !stillMember {
		t.Fatal("a replayed record from before the checkpoint removed a current member")
	}

	// The store refuses it for the same reason, so it cannot be persisted and
	// then applied on the next reload.
	if _, err := f.group.Apply(replayed); !errors.Is(err, ErrStale) {
		t.Fatalf("applying it returned %v, want ErrStale", err)
	}
}

// ApplyCheckpoint must judge the signer itself, not rely on the signing path:
// checkpoints arrive from peers, and one signed by someone with no authority
// would otherwise replace this node's view of the membership.
func TestApplyCheckpointRefusesAnUnauthorisedSigner(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)
	base := f.group.Canonical()

	f.tick(10)
	body := f.group.State().Checkpoint(f.groupID, base.Sequence+1, base.ID, 1, f.clockMS)
	forged, err := SignCheckpoint(member, f.info(member), body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(forged); !errors.Is(err, ErrNotAuthorised) {
		t.Fatalf("a checkpoint signed by an ordinary member returned %v, want ErrNotAuthorised", err)
	}
	if got := f.group.Canonical().ID; got != base.ID {
		t.Fatalf("a refused checkpoint became canonical: %s", got)
	}

	// An outsider's checkpoint is refused too, even though it is well formed.
	outsider := mustIdentity(t)
	outsiderBody := f.group.State().Checkpoint(f.groupID, base.Sequence+1, base.ID, 1, f.clockMS+1)
	outsiderSigned, err := SignCheckpoint(outsider, f.info(outsider), outsiderBody)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(outsiderSigned); !errors.Is(err, ErrNotAuthorised) {
		t.Fatalf("an outsider's checkpoint returned %v, want ErrNotAuthorised", err)
	}
}

// Records made in the same millisecond still need one settled outcome, and
// which one it is matters: admitting somebody by mistake is recoverable,
// failing to remove them is not, and a member who leaves must stay gone.
func TestRecordsInTheSameInstantResolveSafely(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	base := f.group.Canonical()

	// A join and a removal of that joiner, at the same instant: out.
	joiner := mustIdentity(t)
	capability := f.invite(f.founder, joiner, 1)
	at := f.clockMS + 50
	join := f.sign(joiner, Record{Kind: KindJoin, Invite: &capability, Timestamp: at})
	removal := f.sign(f.founder, Record{Kind: KindRemove, Subject: f.info(joiner), Timestamp: at})
	state, _ := Project(base, []Record{removal, join})
	if _, member := state.Members[f.memberID(joiner)]; member {
		t.Fatal("a removal lost to a join made in the same instant")
	}

	// A join and the joiner's own leave, at the same instant: out.
	quitter := mustIdentity(t)
	quitInvite := f.invite(f.founder, quitter, 1)
	quitJoin := f.sign(quitter, Record{Kind: KindJoin, Invite: &quitInvite, Timestamp: at})
	leave := f.sign(quitter, Record{Kind: KindLeave, Timestamp: at})
	state, _ = Project(base, []Record{leave, quitJoin})
	if _, member := state.Members[f.memberID(quitter)]; member {
		t.Fatal("a leave lost to a join made in the same instant")
	}

	// And a plain join in that instant still works, so the ordering is not
	// simply refusing everything.
	plain := mustIdentity(t)
	plainInvite := f.invite(f.founder, plain, 1)
	plainJoin := f.sign(plain, Record{Kind: KindJoin, Invite: &plainInvite, Timestamp: at})
	state, _ = Project(base, []Record{plainJoin})
	if _, member := state.Members[f.memberID(plain)]; !member {
		t.Fatal("an uncontested join in the same instant was refused")
	}
}

// A group that keeps checkpointing must keep converging. The node used to
// anchor its forward walk on the sequence-zero checkpoint, which retention
// then deleted: from that moment the canonical checkpoint could never advance
// and no record was ever retired again.
func TestCheckpointingPastTheRetentionWindowKeepsConverging(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	for round := 0; round < 6; round++ {
		f.join(mustIdentity(t))
		f.tick(10)
		checkpoint, signed, err := f.group.SignCheckpoint(f.founder, true)
		if err != nil || !signed {
			t.Fatalf("round %d: signed=%t err=%v", round, signed, err)
		}
		if got := f.group.Canonical().ID; got != checkpoint.ID {
			t.Fatalf("round %d: canonical is %s, want the checkpoint just signed %s", round, got, checkpoint.ID)
		}
		if got, want := f.group.Canonical().Sequence, uint64(round+1); got != want {
			t.Fatalf("round %d: canonical sequence = %d, want %d", round, got, want)
		}
	}
	// Seven members: the founder plus one per round. Retiring records must not
	// lose any of them.
	if got := len(f.group.MemberIDs()); got != 7 {
		t.Fatalf("membership = %d members, want 7", got)
	}
	// And the records behind the second-newest checkpoint are gone, which is
	// the storage claim the whole design rests on.
	canonical := f.group.Canonical()
	for _, rec := range f.group.Pending() {
		if rec.Timestamp <= canonical.Timestamp {
			t.Fatalf("checkpoint %d covers %s at %d, but it was never retired",
				canonical.Sequence, rec.Kind, rec.Timestamp)
		}
	}
}

// A checkpoint's own claim about who may sign it is worth nothing: its signer
// writes that claim. A node with no record in the covered window used to
// believe it, which let any peer install itself as an admin.
func TestCheckpointSignerCannotAuthoriseItself(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	member := mustIdentity(t)
	f.join(member)
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("base checkpoint: signed=%t err=%v", signed, err)
	}
	base := f.group.Canonical()

	// A quiet node: it holds the checkpoint but none of the records behind the
	// next one, so its only defence is refusing to read authority from the
	// claim itself.
	quietRoot := t.TempDir()
	quiet, err := Adopt(quietRoot, base)
	if err != nil {
		t.Fatal(err)
	}
	defer quiet.Close()

	stranger := mustIdentity(t)
	strangerID := f.memberID(stranger)
	forged := base
	forged.ID = entmoot.RosterEntryID{}
	forged.Sequence = base.Sequence + 1
	forged.Previous = base.ID
	forged.Timestamp = base.Timestamp + 1
	forged.Covered = 1
	forged.Members = append(append([]entmoot.NodeInfo(nil), base.Members...), f.info(stranger))
	sortCheckpointMembers(&forged)
	forged.Policy = base.Policy.Clone()
	forged.Policy.Admins = SortAdmins(append(append([]entmoot.MemberID(nil), base.Policy.Admins...), strangerID))
	forged.Signature = nil
	signedForgery, err := SignCheckpoint(stranger, f.info(stranger), forged)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := quiet.ApplyCheckpoint(signedForgery); !errors.Is(err, ErrNotAuthorised) {
		t.Fatalf("a stranger's self-authorised checkpoint was accepted: err=%v, admins now %v",
			err, quiet.Admins())
	}
	if quiet.CanAdminister(strangerID) {
		t.Fatal("the stranger became an admin")
	}
	if quiet.IsMemberID(strangerID) {
		t.Fatal("the stranger became a member")
	}
}

// sortCheckpointMembers puts a hand-built member list in the order a real
// checkpoint carries, so the signature covers a well-formed body.
func sortCheckpointMembers(cp *Checkpoint) {
	sort.Slice(cp.Members, func(i, j int) bool {
		left, _ := entmoot.ResolvedMemberID(cp.Members[i])
		right, _ := entmoot.ResolvedMemberID(cp.Members[j])
		return bytes.Compare(left[:], right[:]) < 0
	})
}

// A record dated exactly at the checkpoint's timestamp is inside it. The
// projection used a strict comparison while the store used an inclusive one,
// so such a record survived retention and was replayed on top of a checkpoint
// that had already folded it in. Invite-use counting is the one effect that is
// not idempotent, so the replay inflated it and split membership.
func TestRecordsAtTheCheckpointTimestampAreNotReplayed(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	first, second := mustIdentity(t), mustIdentity(t)
	invite := f.invite(f.founder, nil, 2)
	f.joinWith(first, invite)
	checkpoint, signed, err := f.group.SignCheckpoint(f.founder, true)
	if err != nil || !signed {
		t.Fatalf("checkpoint: signed=%t err=%v", signed, err)
	}
	joinRecord := f.group.Pending()
	if len(joinRecord) != 0 {
		t.Fatalf("checkpoint left %d records uncovered, want the join folded in", len(joinRecord))
	}
	if got := f.group.InviteUses(invite.Nonce); got != 1 {
		t.Fatalf("invite uses = %d, want 1", got)
	}

	// Retire it for real: a second checkpoint drops the records behind the
	// first, which is the state a peer one checkpoint behind re-delivers into.
	f.tick(10)
	f.join(mustIdentity(t))
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("second checkpoint: signed=%t err=%v", signed, err)
	}
	replay, err := SignRecord(first, Record{
		Version: Version, GroupID: f.groupID, Kind: KindJoin,
		Actor: f.info(first), Subject: f.info(first), Invite: &invite,
		Timestamp: checkpoint.Timestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.Apply(replay); !errors.Is(err, ErrStale) {
		t.Fatalf("a record at the checkpoint timestamp was accepted: %v", err)
	}
	if got := f.group.InviteUses(invite.Nonce); got != 1 {
		t.Fatalf("the replay inflated invite uses to %d", got)
	}
	// The invite's second use is still available to somebody who has not used
	// it, which is what an inflated count would have denied.
	f.joinWith(second, invite)
	if !f.group.IsMemberID(f.memberID(second)) {
		t.Fatal("the invite's remaining use was denied")
	}
}

// A checkpoint dated far in the future would make every legitimate record
// stale and freeze the node until that date arrived, so it is refused.
func TestCheckpointFarAheadOfTheLocalClockIsRefused(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	f.join(mustIdentity(t))
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("base checkpoint: signed=%t err=%v", signed, err)
	}
	base := f.group.Canonical()
	future := base
	future.ID = entmoot.RosterEntryID{}
	future.Sequence = base.Sequence + 1
	future.Previous = base.ID
	future.Timestamp = f.clockMS + (24 * time.Hour).Milliseconds()
	future.Covered = 0
	future.Signature = nil
	signedFuture, err := SignCheckpoint(f.founder, f.info(f.founder), future)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(signedFuture); err == nil {
		t.Fatal("a checkpoint dated a day ahead was accepted")
	}
	if got := f.group.Canonical().ID; got != base.ID {
		t.Fatalf("canonical moved to %s", got)
	}
	// And the group still accepts ordinary records, which a future-dated
	// checkpoint would have made stale.
	late := mustIdentity(t)
	f.tick(10)
	f.join(late)
	if !f.group.IsMemberID(f.memberID(late)) {
		t.Fatal("a legitimate join was refused after the future-dated checkpoint")
	}
}

// LegacyHead is signed into checkpoint 0 to bind an upgrade to the chain it
// replaces. A checkpoint naming a chain this node does not hold, or naming a
// different head, is refused rather than believed.
func TestCheckpointMustNameTheRosterChainItReplaces(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	base := f.group.Canonical()
	other := entmoot.RosterEntryID{0x7f}
	claiming := base
	claiming.ID = entmoot.RosterEntryID{}
	claiming.LegacyHead = &other
	claiming.Signature = nil
	signedClaim, err := SignCheckpoint(f.founder, f.info(f.founder), claiming)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh node adopting it: there is no chain here, so the claim is a
	// fabrication and must be refused rather than installed.
	group, err := Adopt(t.TempDir(), signedClaim)
	if err == nil {
		_ = group.Close()
		t.Fatal("a root checkpoint claiming an upgrade with no chain behind it was adopted")
	}
	if !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("refusal was %v, want a roster rejection", err)
	}
}

// A checkpoint that arrives for a sequence the group has already passed — what
// a node coming back from a partition produces — must not pull the canonical
// checkpoint backwards. It used to: the walk picked the best child at each
// step, so the late sibling beat the one its successors were built on, the
// walk stopped there because nothing chained onto it, and the membership the
// later checkpoints carried was retired and gone.
func TestLateCheckpointForAPassedSequenceDoesNotRewindMembership(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	admin := mustIdentity(t)
	f.join(admin)
	f.grantAdmin(f.memberID(admin))
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("grant checkpoint: signed=%t err=%v", signed, err)
	}
	base := f.group.Canonical()

	// The admin checkpoints the next two sequences, folding in two members.
	firstMember, secondMember := mustIdentity(t), mustIdentity(t)
	f.tick(10)
	f.join(firstMember)
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(admin, true); err != nil || !signed {
		t.Fatalf("admin checkpoint: signed=%t err=%v", signed, err)
	}
	f.tick(10)
	f.join(secondMember)
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(admin, true); err != nil || !signed {
		t.Fatalf("second admin checkpoint: signed=%t err=%v", signed, err)
	}
	head := f.group.Canonical()
	if head.Sequence != base.Sequence+2 {
		t.Fatalf("canonical sequence = %d, want %d", head.Sequence, base.Sequence+2)
	}

	// Now the founder's own checkpoint for the sequence the admin already
	// passed arrives. It is perfectly valid, and preferred at its own
	// sequence, but it is not where the group is.
	stale, _ := Project(base, nil)
	staleBody := stale.Checkpoint(f.groupID, base.Sequence+1, base.ID, 0, base.Timestamp+1)
	staleSigned, err := SignCheckpoint(f.founder, f.info(f.founder), staleBody)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.group.ApplyCheckpoint(staleSigned); err != nil {
		t.Fatalf("a valid checkpoint for a passed sequence was refused: %v", err)
	}
	if got := f.group.Canonical().Sequence; got != head.Sequence {
		t.Fatalf("canonical rewound to sequence %d, want %d", got, head.Sequence)
	}
	for _, member := range []*keystore.Identity{admin, firstMember, secondMember} {
		if !f.group.IsMemberID(f.memberID(member)) {
			t.Fatalf("member %s was lost", f.memberID(member).String())
		}
	}
}

// A node that joined after the group retired its early checkpoints holds no
// sequence zero at all. Its chain must still advance, which is why the anchor
// is the oldest checkpoint held rather than the sequence-zero one.
func TestANodeWithoutSequenceZeroKeepsCheckpointing(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	f.join(mustIdentity(t))
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("first checkpoint: signed=%t err=%v", signed, err)
	}
	// A joiner adopts the group as it stands: sequence 1, no zero behind it.
	joined, err := Adopt(t.TempDir(), f.group.Canonical())
	if err != nil {
		t.Fatal(err)
	}
	defer joined.Close()
	if joined.Canonical().Sequence == 0 {
		t.Fatal("the fixture adopted a sequence-zero checkpoint, so the test proves nothing")
	}

	// It then follows the group forward. Each step has to become canonical, or
	// this node is frozen at the checkpoint it adopted.
	for round := 0; round < 3; round++ {
		f.tick(10)
		newcomer := mustIdentity(t)
		f.join(newcomer)
		f.tick(10)
		checkpoint, signed, err := f.group.SignCheckpoint(f.founder, true)
		if err != nil || !signed {
			t.Fatalf("round %d: signed=%t err=%v", round, signed, err)
		}
		for _, rec := range f.group.Pending() {
			if _, err := joined.Apply(rec); err != nil && !errors.Is(err, ErrStale) {
				t.Fatalf("round %d: apply record: %v", round, err)
			}
		}
		if _, err := joined.ApplyCheckpoint(checkpoint); err != nil {
			t.Fatalf("round %d: apply checkpoint: %v", round, err)
		}
		if got := joined.Canonical().ID; got != checkpoint.ID {
			t.Fatalf("round %d: canonical is %s, want %s", round, got, checkpoint.ID)
		}
		if !joined.IsMemberID(f.memberID(newcomer)) {
			t.Fatalf("round %d: the newcomer never arrived", round)
		}
	}
}

// Adoption is the one moment a node has nothing of its own to check against,
// so it accepts only the founder's signature: an admin-signed checkpoint would
// have to be taken on the strength of its own claim about who the admins are.
func TestAdoptRefusesACheckpointTheFounderDidNotSign(t *testing.T) {
	f := newFixture(t, DefaultPolicy())
	admin := mustIdentity(t)
	f.join(admin)
	f.grantAdmin(f.memberID(admin))
	f.tick(10)
	if _, signed, err := f.group.SignCheckpoint(f.founder, true); err != nil || !signed {
		t.Fatalf("base checkpoint: signed=%t err=%v", signed, err)
	}
	f.tick(10)
	f.join(mustIdentity(t))
	f.tick(10)
	adminSigned, signed, err := f.group.SignCheckpoint(admin, true)
	if err != nil || !signed {
		t.Fatalf("admin checkpoint: signed=%t err=%v", signed, err)
	}
	group, err := Adopt(t.TempDir(), adminSigned)
	if err == nil {
		_ = group.Close()
		t.Fatal("an admin-signed checkpoint was adopted as a starting point")
	}
	if !errors.Is(err, ErrNotAuthorised) {
		t.Fatalf("refusal was %v, want ErrNotAuthorised", err)
	}
	// The founder's own checkpoint at that sequence is adoptable, so the rule
	// refuses the signature rather than the sequence.
	founderSigned, signed, err := f.group.SignCheckpoint(f.founder, true)
	if err != nil || !signed {
		t.Fatalf("founder checkpoint: signed=%t err=%v", signed, err)
	}
	adopted, err := Adopt(t.TempDir(), founderSigned)
	if err != nil {
		t.Fatalf("the founder's checkpoint was not adoptable: %v", err)
	}
	defer adopted.Close()
}
