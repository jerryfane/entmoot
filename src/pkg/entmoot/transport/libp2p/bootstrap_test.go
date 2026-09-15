package libp2ptransport

import (
	"errors"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

// An invite is verifiable on its own: signature, shape and validity window.
// Nothing else about it can be checked before a joiner has group state, so
// this is the whole of the host-free gate.
func TestBootstrapCapabilityVerifiesInsideItsWindowOnly(t *testing.T) {
	founder := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, nil, 1, nil)
	holder := mustIdentity(t)
	holderBinding, err := BindingFromPublicKey(holder.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyBootstrapCapability(capability, holderBinding.PeerID, time.Now()); err != nil {
		t.Fatalf("live bearer invite refused: %v", err)
	}
	expired := time.UnixMilli(capability.ExpiresAtMS).Add(time.Millisecond)
	if err := VerifyBootstrapCapability(capability, holderBinding.PeerID, expired); !errors.Is(err, ErrBootstrapDenied) {
		t.Fatalf("expired invite error = %v, want %v", err, ErrBootstrapDenied)
	}
	tooEarly := time.UnixMilli(capability.IssuedAtMS).Add(-time.Millisecond)
	if err := VerifyBootstrapCapability(capability, holderBinding.PeerID, tooEarly); !errors.Is(err, ErrBootstrapDenied) {
		t.Fatalf("not-yet-valid invite error = %v, want %v", err, ErrBootstrapDenied)
	}
}

// A target-bound invite is bound to the secure transport identity redeeming
// it, so a leaked link cannot be used from another peer.
func TestBootstrapCapabilityRejectsInvalidPeerBinding(t *testing.T) {
	founder := mustIdentity(t)
	target := mustIdentity(t)
	attacker := mustIdentity(t)
	targetBinding, err := BindingFromPublicKey(target.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	attackerBinding, err := BindingFromPublicKey(attacker.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, target.PublicKey, 1, nil)
	now := time.Now()
	if err := VerifyBootstrapCapability(capability, attackerBinding.PeerID, now); !errors.Is(err, ErrBootstrapDenied) {
		t.Fatalf("capability accepted from a different secure transport identity: %v", err)
	}
	if err := VerifyBootstrapCapability(capability, targetBinding.PeerID, now); err != nil {
		t.Fatalf("target-bound invite refused its own target: %v", err)
	}
}

// The signature on an invite only means something once it is bound to who may
// administer the group right now: the founder always, a delegated admin while
// it holds that delegation.
func TestAuthorizedIssuerRequiresCurrentAdministrator(t *testing.T) {
	founder := mustIdentity(t)
	admin := mustIdentity(t)
	stranger := mustIdentity(t)
	_, group := mustOpenGroup(t, founder, admin)
	adminNode := mustNode(t, admin)
	setInviteTestAdmins(t, group, founder, *adminNode.MemberID)

	if err := AuthorizedIssuer(group, mustNode(t, founder)); err != nil {
		t.Fatalf("founder refused as issuer: %v", err)
	}
	if err := AuthorizedIssuer(group, adminNode); err != nil {
		t.Fatalf("delegated admin refused as issuer: %v", err)
	}
	if err := AuthorizedIssuer(group, mustNode(t, stranger)); !errors.Is(err, ErrBootstrapDenied) {
		t.Fatalf("non-member accepted as issuer: %v", err)
	}

	setInviteTestAdmins(t, group, founder)
	if err := AuthorizedIssuer(group, adminNode); !errors.Is(err, ErrBootstrapDenied) {
		t.Fatalf("demoted admin accepted as issuer: %v", err)
	}
	if !group.IsMemberID(*adminNode.MemberID) {
		t.Fatal("demotion removed the member as well as the delegation")
	}
}

// CheckInvite is the pre-membership gate, and it answers from the group's own
// signed state so every node answers the same way.
func TestCheckInviteAcceptsLiveInviteAndRefusesForeignGroup(t *testing.T) {
	founder := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	live := mustInvite(t, group, founder, nil, 1, nil)
	if err := group.CheckInvite(live, time.Now().UnixMilli()); err != nil {
		t.Fatalf("live invite refused: %v", err)
	}

	otherFounder := mustIdentity(t)
	_, other := mustInviteOnlyGroup(t, t.TempDir(), otherFounder)
	foreign := mustInvite(t, other, otherFounder, nil, 1, nil)
	if err := group.CheckInvite(foreign, time.Now().UnixMilli()); !errors.Is(err, membership.ErrInviteDenied) {
		t.Fatalf("invite for another group accepted: %v", err)
	}
}

// Revocation travels as a signed record, so an invite withdrawn with uses left
// stops working from the group's state rather than from a local note.
func TestCheckInviteRefusesRevokedInvite(t *testing.T) {
	founder := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, nil, 3, nil)
	if _, err := group.SignRecord(founder, membership.Record{
		Kind:        membership.KindRevokeInvite,
		InviteNonce: capability.Nonce,
	}); err != nil {
		t.Fatalf("sign revocation: %v", err)
	}
	if !group.IsInviteRevoked(capability.Nonce) {
		t.Fatal("group state does not report the invite as revoked")
	}
	if err := group.CheckInvite(capability, time.Now().UnixMilli()); !errors.Is(err, membership.ErrInviteDenied) {
		t.Fatalf("revoked invite accepted: %v", err)
	}
}

// Uses are counted from the joins that actually landed, so an exhausted invite
// is refused even though its window is still open.
func TestCheckInviteRefusesExhaustedInvite(t *testing.T) {
	founder := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	capability := mustInvite(t, group, founder, nil, 1, nil)
	joiner := mustIdentity(t)
	mustJoinWithInvite(t, group, joiner, capability)
	if !group.IsMemberID(*mustNode(t, joiner).MemberID) {
		t.Fatal("redeeming a live invite did not admit the joiner")
	}
	if uses := group.InviteUses(capability.Nonce); uses != 1 {
		t.Fatalf("invite uses = %d, want 1", uses)
	}
	if err := group.CheckInvite(capability, time.Now().UnixMilli()); !errors.Is(err, membership.ErrInviteDenied) {
		t.Fatalf("exhausted invite accepted: %v", err)
	}
}

// An admin's outstanding invites are worth exactly its authority now: removing
// the admin withdraws them without anybody revoking them one by one.
func TestCheckInviteRefusesInviteFromRemovedAdmin(t *testing.T) {
	founder := mustIdentity(t)
	admin := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	adminNode := mustNode(t, admin)
	mustJoinWithInvite(t, group, admin, mustInvite(t, group, founder, admin.PublicKey, 1, nil))
	setInviteTestAdmins(t, group, founder, *adminNode.MemberID)

	delegated := mustInvite(t, group, admin, nil, 2, nil)
	if err := group.CheckInvite(delegated, time.Now().UnixMilli()); err != nil {
		t.Fatalf("invite from a serving admin refused: %v", err)
	}
	if _, err := group.SignRecord(founder, membership.Record{
		Kind:    membership.KindRemove,
		Subject: adminNode,
	}); err != nil {
		t.Fatalf("remove admin: %v", err)
	}
	if err := group.CheckInvite(delegated, time.Now().UnixMilli()); !errors.Is(err, membership.ErrInviteDenied) {
		t.Fatalf("invite from a removed admin accepted: %v", err)
	}
}

// setInviteTestAdmins replaces the delegated-admin set with a founder-signed
// policy record, leaving the rest of the policy as it stands.
func setInviteTestAdmins(t *testing.T, group *membership.Group, founder *keystore.Identity, admins ...entmoot.MemberID) {
	t.Helper()
	policy := group.Policy()
	policy.Admins = membership.SortAdmins(admins)
	if _, err := group.SignRecord(founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy}); err != nil {
		t.Fatalf("set admins: %v", err)
	}
	for _, admin := range admins {
		if !group.CanAdminister(admin) {
			t.Fatalf("policy record did not grant admin %s", admin.String())
		}
	}
}

func mustIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func mustNodeInfo(t *testing.T, publicKey []byte) entmoot.NodeInfo {
	t.Helper()
	memberID, err := entmoot.MemberIDFromPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := entmoot.PeerIDFromPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return entmoot.NodeInfo{EntmootPubKey: append([]byte(nil), publicKey...), MemberID: &memberID, PeerID: peerID}
}
