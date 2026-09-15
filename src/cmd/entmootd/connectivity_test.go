package main

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

func TestDaemonHostConfigSupportsRelayOnly(t *testing.T) {
	relayIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	relayPeerID, err := entmoot.PeerIDFromPublicKey(relayIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	relayAddress := "/ip4/127.0.0.1/tcp/41004/p2p/" + relayPeerID
	config, err := daemonHostConfig(&globalFlags{
		connectivity:     "relay-only",
		controlledRelays: stringListFlag{relayAddress},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.Mode != libp2ptransport.RelayOnlyConnectivity || len(config.ControlledRelays) != 1 || config.ControlledRelays[0].ID.String() != relayPeerID {
		t.Fatalf("relay-only host config = %+v", config)
	}
	if len(config.ListenAddrs) != 0 {
		t.Fatalf("relay-only host exposed direct listen addresses: %v", config.ListenAddrs)
	}
}

func TestDaemonHostConfigRejectsUncontrolledRelayOnly(t *testing.T) {
	if _, err := daemonHostConfig(&globalFlags{connectivity: "relay-only"}); err == nil {
		t.Fatal("relay-only daemon config accepted no controlled relay")
	}
}

func TestDaemonHostConfigKeepsDirectProfileHolePunchRelays(t *testing.T) {
	relayIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	relayPeerID, err := entmoot.PeerIDFromPublicKey(relayIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	config, err := daemonHostConfig(&globalFlags{
		connectivity:     "direct",
		listenPort:       41005,
		controlledRelays: stringListFlag{"/ip4/127.0.0.1/tcp/41004/p2p/" + relayPeerID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.Mode != libp2ptransport.DirectConnectivity || len(config.ListenAddrs) != 1 {
		t.Fatalf("direct host config = %+v", config)
	}
	if len(config.ControlledRelays) != 1 || config.ControlledRelays[0].ID.String() != relayPeerID {
		t.Fatalf("direct profile discarded its hole-punch relay: %+v", config.ControlledRelays)
	}
}

func TestDaemonHostConfigRejectsMalformedRelayInDirectProfile(t *testing.T) {
	if _, err := daemonHostConfig(&globalFlags{
		connectivity:     "direct",
		listenPort:       41006,
		controlledRelays: stringListFlag{"/ip4/127.0.0.1/tcp/41004"},
	}); err == nil {
		t.Fatal("direct profile accepted a relay address without a peer id")
	}
}

// One join can reach the same node twice: the joiner hands it over itself,
// and a member that already took it gossips the same record on. Redelivery is
// the norm, not a fault — the second arrival must be recognised as the same
// statement, reported as not applied, and leave one member and one record
// behind rather than a duplicate admission.
func TestDuplicateJoinDeliveryIsNotAppliedTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	gid := daemonTestGroupID(0x2a)
	founder, _ := mustDaemonIdentity(t)
	joiner, joinerInfo := mustDaemonIdentity(t)

	founderRoot := t.TempDir()
	mustCreateGroup(t, founderRoot, gid, founder, membership.DefaultPolicy())

	joinerHost, _, err := libp2ptransport.NewHost(ctx, joiner, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer joinerHost.Close()

	founderRuntime, founderSession, founderHost := startTestRuntime(t, ctx, founderRoot, founder, gid)
	defer founderRuntime.Close()
	defer founderHost.Close()
	founderAddr := peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}

	capability := mustBootstrapInvite(t, founderSession.group, founder, joinerInfo, founderHost.ID().String())

	// The real join: read the checkpoint with the invite, sign the join, hand
	// it to the founder. That is the first delivery.
	joinerGroup, err := libp2ptransport.JoinGroup(ctx, joinerHost, founderAddr, t.TempDir(), joiner, capability, joinerInfo)
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	defer mustCloseGroup(t, joinerGroup)
	var join membership.Record
	for _, record := range joinerGroup.Pending() {
		if record.Kind == membership.KindJoin && record.Subject.MemberID != nil && *record.Subject.MemberID == *joinerInfo.MemberID {
			join = record
		}
	}
	if join.Kind != membership.KindJoin {
		t.Fatalf("the joiner kept no join record: %+v", joinerGroup.Pending())
	}
	if !founderSession.group.IsMemberID(*joinerInfo.MemberID) {
		t.Fatal("the founder did not take the pushed join")
	}

	// Second delivery of the very same record, as gossip performs it.
	if err := libp2ptransport.PushMembershipRecord(ctx, joinerHost, founderAddr, gid, join, &capability); err != nil {
		t.Fatalf("redelivering a held join failed: %v", err)
	}

	// The receiver reports it as not applied rather than refusing it or
	// taking it again.
	applied, err := founderSession.group.Apply(join)
	if err != nil {
		t.Fatalf("re-applying a held join errored: %v", err)
	}
	if applied {
		t.Fatal("a record the group already held was reported as applied")
	}

	members := founderSession.group.MemberIDs()
	if len(members) != 2 || !founderSession.group.IsMemberID(*joinerInfo.MemberID) {
		t.Fatalf("after three deliveries of one join the group holds %d members", len(members))
	}
	if pending := founderSession.group.Pending(); len(pending) != 1 {
		t.Fatalf("one join produced %d stored records", len(pending))
	}
}

// mustBootstrapInvite mints an invite the joiner may redeem against a named
// serving peer, which is what a `-bootstrap` invite carries.
func mustBootstrapInvite(t *testing.T, group *membership.Group, issuer *keystore.Identity, target entmoot.NodeInfo, allowedPeerID string) entmoot.BootstrapCapability {
	t.Helper()
	capability := entmoot.BootstrapCapability{
		GroupID:         group.GroupID(),
		Founder:         group.Founder(),
		RosterHead:      group.Canonical().ID,
		TargetPublicKey: append([]byte(nil), target.EntmootPubKey...),
		TargetMemberID:  *target.MemberID,
		TargetPeerID:    target.PeerID,
		AllowedPeerIDs:  []string{allowedPeerID},
		MaxUses:         1,
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
