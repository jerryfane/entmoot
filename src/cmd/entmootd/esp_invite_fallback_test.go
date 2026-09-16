package main

import (
	"encoding/json"
	"strings"
	"testing"

	entmoot "entmoot/pkg/entmoot"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// TestESPInvitePayloadsCarryTheFallbackOptOut pins that the escape hatch is
// reachable from the ESP, which is the only shipped IPC client. The flag
// existed on the IPC request while nothing set it, so the documented opt-out
// could not be exercised by any caller.
func TestESPInvitePayloadsCarryTheFallbackOptOut(t *testing.T) {
	var targeted inviteCreatePayload
	if err := json.Unmarshal([]byte(`{"no_fallback_peers":true}`), &targeted); err != nil {
		t.Fatalf("unmarshal targeted: %v", err)
	}
	if !targeted.NoFallbackPeers {
		t.Fatal("invite_create ignores no_fallback_peers")
	}
	var open openInviteCreatePayload
	if err := json.Unmarshal([]byte(`{"max_uses":1,"no_fallback_peers":true}`), &open); err != nil {
		t.Fatalf("unmarshal open: %v", err)
	}
	if !open.NoFallbackPeers {
		t.Fatal("open_invite_create ignores no_fallback_peers")
	}
}

// TestOpenInviteBootstrapIsValidatedAtCreation pins where the check belongs.
// The token an open invite returns is what gets shared, and the capability is
// minted only at redemption, so a bad list used to yield a link that failed
// for every joiner with an error naming no cause.
func TestOpenInviteBootstrapIsValidatedAtCreation(t *testing.T) {
	_, info := mustDaemonIdentity(t)
	binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	good := "/ip4/37.27.59.89/tcp/1004/p2p/" + binding.PeerID.String()
	members := []string{binding.PeerID.String()}
	if err := validateOpenInviteBootstrap([]string{good}, members, "", false); err != nil {
		t.Fatalf("a well-formed address was refused: %v", err)
	}
	if err := validateOpenInviteBootstrap([]string{"not-a-multiaddr"}, members, "", false); err == nil {
		t.Fatal("a malformed multiaddr was accepted")
	}
	if err := validateOpenInviteBootstrap([]string{"/ip4/37.27.59.89/tcp/1004"}, members, "", false); err == nil {
		t.Fatal("an address with no /p2p/ component was accepted")
	}

	var many []string
	for i := 0; i < 200; i++ {
		many = append(many, good)
	}
	if err := validateOpenInviteBootstrap(many, members, "", false); err == nil {
		t.Fatalf("a list too long to redeem was accepted (limit %d bytes)", libp2ptransport.MaxCapabilityBytes)
	}
}

// TestOpenInviteBootstrapRefusesANonMember pins the check redemption already
// performed: an address naming nobody is knowable at creation, and the token
// is shared before any capability exists, so accepting it hands out a link
// every redemption refuses.
func TestOpenInviteBootstrapRefusesANonMember(t *testing.T) {
	_, memberInfo := mustDaemonIdentity(t)
	_, strangerInfo := mustDaemonIdentity(t)
	member, err := libp2ptransport.BindingFromPublicKey(memberInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	stranger, err := libp2ptransport.BindingFromPublicKey(strangerInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	members := []string{member.PeerID.String()}
	if err := validateOpenInviteBootstrap([]string{"/ip4/37.27.59.89/tcp/1004/p2p/" + member.PeerID.String()}, members, "", false); err != nil {
		t.Fatalf("a member's address was refused: %v", err)
	}
	if err := validateOpenInviteBootstrap([]string{"/ip4/37.27.59.89/tcp/1004/p2p/" + stranger.PeerID.String()}, members, "", false); err == nil {
		t.Fatal("an address naming no member was accepted")
	}
}

// TestOpenInviteSizeEstimateCoversTheMintedCapability pins that creation
// refuses what the mint would refuse. Measuring the bare address list
// undercounted by the capability's fixed fields and the fallback slots, so a
// list of about 25 addresses passed creation and then failed every redemption.
func TestOpenInviteSizeEstimateCoversTheMintedCapability(t *testing.T) {
	_, info := mustDaemonIdentity(t)
	binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	members := []string{binding.PeerID.String()}
	addr := "/ip4/203.0.113.7/tcp/1004/p2p/" + binding.PeerID.String()

	var list []string
	for i := 0; i < 25; i++ {
		list = append(list, addr)
	}
	if err := validateOpenInviteBootstrap(list, members, "", false); err == nil {
		t.Fatal("25 addresses passed creation; the mint refuses that list, so the link would fail for every joiner")
	}
	// A short list must still be accepted, or the check is just a refusal.
	if err := validateOpenInviteBootstrap([]string{addr}, members, "", false); err != nil {
		t.Fatalf("a single address was refused: %v", err)
	}
}

// TestCreationEstimateIsAnUpperBoundOnTheMint pins the property the estimate
// needs and twice did not have: whatever passes creation must pass the mint.
// Sizing the fallback slots from the operator's own short addresses let a
// 15-address list through while the daemon, filling those slots from a peer
// cache of long webtransport addresses, signed something the joiner could not
// send.
func TestCreationEstimateIsAnUpperBoundOnTheMint(t *testing.T) {
	_, info := mustDaemonIdentity(t)
	binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	members := []string{binding.PeerID.String()}
	named := "/ip4/203.0.113.7/tcp/1004/p2p/" + binding.PeerID.String()

	for n := 1; n <= 40; n++ {
		list := make([]string, 0, n)
		for i := 0; i < n; i++ {
			list = append(list, named)
		}
		creationOK := validateOpenInviteBootstrap(list, members, "", false) == nil
		if !creationOK {
			continue
		}
		// The worst capability the mint could sign from this list: every
		// fallback and relay slot filled at the width attachment enforces.
		minted := mintedWorstCase(t, list, binding.PeerID.String())
		if size, tooLarge := libp2ptransport.CapabilityTooLarge(minted); tooLarge {
			t.Fatalf("%d addresses passed creation while the worst-case mint is %d bytes, over %d", n, size, libp2ptransport.MaxCapabilityBytes)
		}
	}
}

func mintedWorstCase(t *testing.T, named []string, peerID string) entmoot.BootstrapCapability {
	t.Helper()
	var key [32]byte
	var signature [64]byte
	node := entmoot.NodeInfo{EntmootPubKey: key[:], MemberID: &entmoot.MemberID{}, PeerID: peerID}
	capability := entmoot.BootstrapCapability{
		GroupID:           entmoot.GroupID{7},
		AllowedMultiaddrs: append([]string(nil), named...),
		AllowedPeerIDs:    []string{peerID},
		Founder:           node,
		Issuer:            &node,
		TargetPublicKey:   key[:],
		TargetPeerID:      peerID,
		Signature:         signature[:],
		RosterHead:        entmoot.RosterEntryID{9},
	}
	capability.Relays = append(capability.Relays, strings.Repeat("r", maxInviteFallbackBytes))
	capability.AllowedMultiaddrs = append(capability.AllowedMultiaddrs, strings.Repeat("a", maxInviteFallbackBytes))
	for i := 0; i < maxInviteFallbackPeers; i++ {
		capability.AllowedPeerIDs = append(capability.AllowedPeerIDs, peerID)
	}
	return capability
}
