package main

import (
	"crypto/rand"
	"path/filepath"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

// mustDaemonIdentity returns a fresh identity together with the node info the
// membership package expects for it.
func mustDaemonIdentity(t *testing.T) (*keystore.Identity, entmoot.NodeInfo) {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate: %v", err)
	}
	return identity, mustDaemonNodeInfo(t, identity)
}

func mustDaemonNodeInfo(t *testing.T, identity *keystore.Identity) entmoot.NodeInfo {
	t.Helper()
	memberID, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatalf("MemberIDFromPublicKey: %v", err)
	}
	peerID, err := entmoot.PeerIDFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatalf("PeerIDFromPublicKey: %v", err)
	}
	return entmoot.NodeInfo{MemberID: &memberID, PeerID: peerID, EntmootPubKey: identity.PublicKey}
}

// mustCreateGroup writes a group's checkpoint 0 under dataDir and leaves the
// store closed, which is what a CLI command expects to find.
func mustCreateGroup(t *testing.T, dataDir string, gid entmoot.GroupID, founder *keystore.Identity, policy membership.Policy) {
	t.Helper()
	group, err := membership.Create(dataDir, founder, mustDaemonNodeInfo(t, founder), gid, policy, 1_700_000_000_000)
	if err != nil {
		t.Fatalf("membership.Create: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("membership close: %v", err)
	}
}

// mustAdoptCheckpointZero mints and adopts a founder-signed checkpoint 0 over
// a group that still has only the linear chain. It is what `membership
// upgrade` does, expressed against a chain whose genesis predates member ids.
func mustAdoptCheckpointZero(t *testing.T, dataDir string, gid entmoot.GroupID, founder *keystore.Identity) {
	t.Helper()
	legacy, err := membership.LoadLegacyChain(dataDir, gid)
	if err != nil {
		t.Fatalf("LoadLegacyChain: %v", err)
	}
	founderInfo := mustDaemonNodeInfo(t, founder)
	policy := membership.DefaultPolicy()
	policy.Admins = membership.SortAdmins(legacy.Admins())
	state := membership.State{
		Founder: founderInfo,
		Members: map[entmoot.MemberID]entmoot.NodeInfo{*founderInfo.MemberID: founderInfo},
		Policy:  policy,
	}
	head := legacy.Head()
	body := state.Checkpoint(gid, 0, entmoot.RosterEntryID{}, 0, time.Now().UnixMilli())
	body.LegacyHead = &head
	signed, err := membership.SignCheckpoint(founder, founderInfo, body)
	if err != nil {
		t.Fatalf("SignCheckpoint: %v", err)
	}
	group, err := membership.Adopt(dataDir, signed)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("membership close: %v", err)
	}
}

// mustDaemonInvite mints an invite for target against the group's canonical
// checkpoint, signed by issuer.
func mustDaemonInvite(t *testing.T, group *membership.Group, issuer *keystore.Identity, target entmoot.NodeInfo, maxUses int) entmoot.BootstrapCapability {
	t.Helper()
	capability := entmoot.BootstrapCapability{
		GroupID:         group.GroupID(),
		Founder:         group.Founder(),
		RosterHead:      group.Canonical().ID,
		TargetPublicKey: append([]byte(nil), target.EntmootPubKey...),
		TargetMemberID:  *target.MemberID,
		TargetPeerID:    target.PeerID,
		MaxUses:         maxUses,
		IssuedAtMS:      time.Now().Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     time.Now().Add(time.Hour).UnixMilli(),
	}
	issuerInfo := mustDaemonNodeInfo(t, issuer)
	if founder := group.Founder(); founder.MemberID == nil || *founder.MemberID != *issuerInfo.MemberID {
		capability.Issuer = &issuerInfo
	}
	if _, err := rand.Read(capability.Nonce[:]); err != nil {
		t.Fatalf("invite nonce: %v", err)
	}
	if err := membership.SignInvite(issuer, &capability); err != nil {
		t.Fatalf("SignInvite: %v", err)
	}
	return capability
}

// mustJoinWithInvite admits joiner the way the daemon does: the joiner signs
// its own join record and cites the invite it was handed.
func mustJoinWithInvite(t *testing.T, group *membership.Group, joiner *keystore.Identity, capability entmoot.BootstrapCapability) membership.Record {
	t.Helper()
	record, err := group.SignRecord(joiner, membership.Record{Kind: membership.KindJoin, Invite: &capability})
	if err != nil {
		t.Fatalf("join record: %v", err)
	}
	info := mustDaemonNodeInfo(t, joiner)
	if !group.IsMemberID(*info.MemberID) {
		t.Fatalf("joiner %s is not a member after its join record", info.MemberID)
	}
	return record
}

// mustIdentityFile saves an identity where setup() will load it from, so a
// test can drive the real command functions.
func mustIdentityFile(t *testing.T, identity *keystore.Identity) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "identity.json")
	if err := identity.Save(path); err != nil {
		t.Fatalf("save identity: %v", err)
	}
	return path
}

// daemonFlags builds the global flags a command needs to reach dataDir with a
// specific signing identity.
func daemonFlags(t *testing.T, dataDir string, identity *keystore.Identity) *globalFlags {
	t.Helper()
	return &globalFlags{data: dataDir, identity: mustIdentityFile(t, identity)}
}
