package libp2ptransport

import (
	"crypto/rand"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

// mustGroupID derives a deterministic-enough group id for a test.
func mustGroupID(t *testing.T) entmoot.GroupID {
	t.Helper()
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	return gid
}

// mustNode builds the full-width identity record for a key.
func mustNode(t *testing.T, identity *keystore.Identity) entmoot.NodeInfo {
	t.Helper()
	binding, err := BindingFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	memberID := binding.MemberID
	return entmoot.NodeInfo{
		EntmootPubKey: append([]byte(nil), identity.PublicKey...),
		MemberID:      &memberID,
		PeerID:        binding.PeerID.String(),
	}
}

// mustOpenGroup creates a group whose join rule is open, so a test can admit
// members with one self-signed record and without minting invites.
func mustOpenGroup(t *testing.T, founder *keystore.Identity, members ...*keystore.Identity) (entmoot.GroupID, *membership.Group) {
	t.Helper()
	groupID := mustGroupID(t)
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	group, err := membership.Create(t.TempDir(), founder, mustNode(t, founder), groupID, policy, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })
	for _, member := range members {
		if _, err := group.SignRecord(member, membership.Record{Kind: membership.KindJoin}); err != nil {
			t.Fatalf("admit member: %v", err)
		}
	}
	return groupID, group
}

// mustInviteOnlyGroup creates an invite-only group holding just its founder,
// rooted in dir so a caller can reopen it.
func mustInviteOnlyGroup(t *testing.T, dir string, founder *keystore.Identity) (entmoot.GroupID, *membership.Group) {
	t.Helper()
	groupID := mustGroupID(t)
	group, err := membership.Create(dir, founder, mustNode(t, founder), groupID, membership.DefaultPolicy(), time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })
	return groupID, group
}

// mustInvite mints an invite for a group. targetKey nil makes it a bearer
// invite; maxUses 0 means the default of one use.
func mustInvite(t *testing.T, group *membership.Group, issuer *keystore.Identity, targetKey []byte, maxUses int, allowedPeerIDs []string) entmoot.BootstrapCapability {
	t.Helper()
	founder := group.Founder()
	capability := entmoot.BootstrapCapability{
		GroupID:        group.GroupID(),
		Founder:        founder,
		RosterHead:     group.Canonical().ID,
		AllowedPeerIDs: allowedPeerIDs,
		MaxUses:        maxUses,
		IssuedAtMS:     time.Now().Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:    time.Now().Add(time.Hour).UnixMilli(),
	}
	if targetKey != nil {
		binding, err := BindingFromPublicKey(targetKey)
		if err != nil {
			t.Fatal(err)
		}
		capability.TargetPublicKey = append([]byte(nil), targetKey...)
		capability.TargetMemberID = binding.MemberID
		capability.TargetPeerID = binding.PeerID.String()
	}
	issuerNode := mustNode(t, issuer)
	if founderID, err := entmoot.ResolvedMemberID(founder); err == nil {
		if id, err := entmoot.ResolvedMemberID(issuerNode); err == nil && id != founderID {
			capability.Issuer = &issuerNode
		}
	}
	if _, err := rand.Read(capability.Nonce[:]); err != nil {
		t.Fatal(err)
	}
	if err := membership.SignInvite(issuer, &capability); err != nil {
		t.Fatal(err)
	}
	return capability
}

// mustJoinWithInvite admits an identity by redeeming an invite, which is the
// production path: the joiner signs its own admission.
func mustJoinWithInvite(t *testing.T, group *membership.Group, joiner *keystore.Identity, capability entmoot.BootstrapCapability) membership.Record {
	t.Helper()
	record, err := group.SignRecord(joiner, membership.Record{Kind: membership.KindJoin, Invite: &capability})
	if err != nil {
		t.Fatalf("join with invite: %v", err)
	}
	return record
}
