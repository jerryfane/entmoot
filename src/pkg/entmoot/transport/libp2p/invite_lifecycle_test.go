package libp2ptransport

import (
	"errors"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

// tryJoinWithInvite signs a join the way a joiner does and reports whether the
// group admitted it. A join that carries an unusable invite is stored and
// ignored rather than rejected, so "was it applied" is the wrong question:
// membership is.
func tryJoinWithInvite(t *testing.T, group *membership.Group, joiner *keystore.Identity, capability entmoot.BootstrapCapability) (entmoot.MemberID, error) {
	t.Helper()
	member := *mustNode(t, joiner).MemberID
	if _, err := group.SignRecord(joiner, membership.Record{Kind: membership.KindJoin, Invite: &capability}); err != nil {
		return member, err
	}
	return member, nil
}

// A multi-use invite is what lets an operator hand one link to a small team.
func TestMultiUseInviteAdmitsDistinctPeersUpToItsLimit(t *testing.T) {
	founder := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, nil, 2, nil)

	for i := range 2 {
		member, err := tryJoinWithInvite(t, group, mustIdentity(t), capability)
		if err != nil {
			t.Fatalf("multi-use invite refused an applicant within its limit: %v", err)
		}
		if !group.IsMemberID(member) {
			t.Fatalf("applicant %d within the limit was not admitted", i)
		}
	}
	if uses := group.InviteUses(capability.Nonce); uses != 2 {
		t.Fatalf("invite uses = %d, want 2", uses)
	}

	extra, err := tryJoinWithInvite(t, group, mustIdentity(t), capability)
	if err != nil {
		t.Fatalf("join past the limit was rejected instead of ignored: %v", err)
	}
	if group.IsMemberID(extra) {
		t.Fatal("multi-use invite admitted more identities than its limit")
	}
	if uses := group.InviteUses(capability.Nonce); uses != 2 {
		t.Fatalf("invite uses after the refused join = %d, want 2", uses)
	}
	// Three members: the founder and the two admitted applicants.
	if members := group.MemberIDs(); len(members) != 3 {
		t.Fatalf("group holds %d members, want 3", len(members))
	}
}

// Single use stays the default: an invite without MaxUses admits one identity.
func TestInviteWithoutMaxUsesAdmitsOneIdentity(t *testing.T) {
	founder := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, nil, 0, nil)

	first, err := tryJoinWithInvite(t, group, mustIdentity(t), capability)
	if err != nil {
		t.Fatal(err)
	}
	if !group.IsMemberID(first) {
		t.Fatal("default invite did not admit its first holder")
	}
	second, err := tryJoinWithInvite(t, group, mustIdentity(t), capability)
	if err != nil {
		t.Fatalf("second join was rejected instead of ignored: %v", err)
	}
	if group.IsMemberID(second) {
		t.Fatal("default invite admitted a second identity")
	}
}

// A target-bound invite is not transferable, so the same link cannot be handed
// to someone else: the stranger cannot even produce a valid join record.
func TestTargetBoundInviteRefusesAnotherApplicant(t *testing.T) {
	founder := mustIdentity(t)
	target := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, target.PublicKey, 1, nil)

	stranger, err := tryJoinWithInvite(t, group, mustIdentity(t), capability)
	if !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("stranger's join error = %v, want a rejected record", err)
	}
	if group.IsMemberID(stranger) {
		t.Fatal("target-bound invite admitted a different identity")
	}
	admitted, err := tryJoinWithInvite(t, group, target, capability)
	if err != nil {
		t.Fatalf("target-bound invite refused its own target: %v", err)
	}
	if !group.IsMemberID(admitted) {
		t.Fatal("target-bound invite did not admit its target")
	}
}

// Revocation is the lever an operator needs: an invite can be withdrawn before
// expiry with uses still remaining, and the remaining uses stop working.
func TestRevokedInviteStopsRemainingUses(t *testing.T) {
	founder := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, nil, 3, nil)

	admitted, err := tryJoinWithInvite(t, group, mustIdentity(t), capability)
	if err != nil {
		t.Fatal(err)
	}
	if !group.IsMemberID(admitted) {
		t.Fatal("first holder of a three-use invite was not admitted")
	}
	if _, err := group.SignRecord(founder, membership.Record{
		Kind:        membership.KindRevokeInvite,
		InviteNonce: capability.Nonce,
	}); err != nil {
		t.Fatalf("sign revocation: %v", err)
	}

	refused, err := tryJoinWithInvite(t, group, mustIdentity(t), capability)
	if err != nil {
		t.Fatalf("join on a revoked invite was rejected instead of ignored: %v", err)
	}
	if group.IsMemberID(refused) {
		t.Fatal("revoked invite still admitted an identity")
	}
	if uses := group.InviteUses(capability.Nonce); uses != 1 {
		t.Fatalf("invite uses = %d, want the one use it had before revocation", uses)
	}
	if err := group.CheckInvite(capability, time.Now().UnixMilli()); !errors.Is(err, membership.ErrInviteDenied) {
		t.Fatalf("revoked invite still passes the pre-membership gate: %v", err)
	}
	// The member admitted before the revocation keeps its membership.
	if !group.IsMemberID(admitted) {
		t.Fatal("revoking an invite removed the member it had already admitted")
	}
}

// The local ledger is a record of what this node handed out, not an authority,
// so it must tell "withdrawn now" apart from "never issued here".
func TestInviteLedgerMarkRevokedReportsUnknownInvite(t *testing.T) {
	ledger, err := OpenInviteLedger(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ledger.Close() })

	founder := mustIdentity(t)
	groupID, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, nil, 1, nil)

	var unknown [32]byte
	unknown[0] = 42
	noted, err := ledger.MarkRevoked(groupID, unknown)
	if err != nil {
		t.Fatal(err)
	}
	if noted {
		t.Fatal("ledger claimed to withdraw an invite it never issued")
	}

	if err := ledger.RecordIssuedInvite(capability); err != nil {
		t.Fatal(err)
	}
	noted, err = ledger.MarkRevoked(groupID, capability.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	if !noted {
		t.Fatal("ledger did not withdraw an invite it had issued")
	}
	again, err := ledger.MarkRevoked(groupID, capability.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	if again {
		t.Fatal("ledger withdrew the same invite twice")
	}
}
