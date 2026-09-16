package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	entmoot "entmoot/pkg/entmoot"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
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

// TestAttachmentHonoursTheBytePremiseTheEstimateRelies On is the property the
// creation-time estimate is built on: whatever the daemon attaches without
// being asked — fallback member addresses and relay hints — never exceeds the
// byte bounds the estimate models. The previous version of this test built its
// own worst case from the same constants the estimate uses, so it asserted the
// premise against itself and passed while the real mint, whose relays come
// from relayHints() with no byte bound, signed capabilities creation had
// accepted.
func TestAttachmentHonoursTheBytePremiseTheEstimateReliesOn(t *testing.T) {
	// A webtransport address with two certhashes, the shape a real peer cache
	// holds: each under the per-address bound, the set far over the total.
	long := "/ip4/203.0.113.7/udp/4001/quic-v1/webtransport/certhash/" +
		strings.Repeat("u", 64) + "/certhash/" + strings.Repeat("v", 64) +
		"/p2p/12D3KooWGu8QgDWWsThK4JqbwXDBAQmTr9NXsomethinglongenough"
	if len(long) > maxInviteAddrBytes {
		t.Fatalf("fixture address is %d bytes, over the per-address bound; pick a shorter one", len(long))
	}

	var hints []string
	for i := 0; i < 16; i++ {
		hints = append(hints, long)
	}
	bounded := boundInviteRelays(hints)
	total := 0
	for _, hint := range bounded {
		total += len(hint)
	}
	if total > maxInviteFallbackBytes {
		t.Fatalf("relay hints attach %d bytes, over the %d the estimate models: %d hints of %d bytes each",
			total, maxInviteFallbackBytes, len(bounded), len(long))
	}
	if len(bounded) == 0 {
		t.Fatal("no relay hint survived, so the bound is a refusal rather than a trim")
	}
}

// TestCapabilityOverheadIsBounded measures the constant the creation-time
// check charges for everything that is not an address or a peer id. Three
// earlier versions of that check reasoned about JSON byte counts in prose and
// undercounted each time — the fallback slots, the relay bytes, then the nonce
// and timestamps. This measures instead: a capability shaped the way the mint
// builds one, with every fixed field at its widest, minus the address and
// peer-id bytes the check charges separately.
func TestCapabilityOverheadIsBounded(t *testing.T) {
	_, info := mustDaemonIdentity(t)
	binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	node := entmoot.NodeInfo{EntmootPubKey: info.EntmootPubKey, MemberID: info.MemberID, PeerID: binding.PeerID.String()}
	var signature [64]byte
	for i := range signature {
		signature[i] = 0xff
	}
	capability := entmoot.BootstrapCapability{
		GroupID:         entmoot.GroupID{0xff},
		Founder:         node,
		Issuer:          &node,
		RosterHead:      entmoot.RosterEntryID{0xff},
		TargetPublicKey: info.EntmootPubKey,
		TargetMemberID:  *info.MemberID,
		TargetPeerID:    binding.PeerID.String(),
		MaxUses:         1 << 31,
		IssuedAtMS:      1 << 44,
		ExpiresAtMS:     1 << 44,
		Signature:       signature[:],
	}
	for i := range capability.Nonce {
		capability.Nonce[i] = 0xff
	}
	encoded, err := json.Marshal(capability)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxInviteCapabilityOverhead {
		t.Fatalf("a capability with no addresses encodes to %d bytes, over the %d charged as overhead: the creation-time check would undercount",
			len(encoded), maxInviteCapabilityOverhead)
	}
}

// TestRelayHintsFromTheRuntimeAreBounded pins the bound at its producer. It
// used to live at each mint site, so reverting one of them left every test
// green while the capability-size check silently stopped being an upper bound.
func TestRelayHintsFromTheRuntimeAreBounded(t *testing.T) {
	_, info := mustDaemonIdentity(t)
	binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	// A long but valid address: dnsaddr names are the widest real shape.
	long := "/dnsaddr/" + strings.Repeat("relay-host-segment.", 6) + "example.com/tcp/4001"
	relay := peer.AddrInfo{ID: binding.PeerID}
	for i := 0; i < 16; i++ {
		relay.Addrs = append(relay.Addrs, multiaddr.StringCast(long))
	}
	runtime := &groupRuntime{controlledRelays: []peer.AddrInfo{relay}}

	hints := runtime.relayHints()
	total := 0
	for _, hint := range hints {
		total += len(hint)
	}
	if total > maxInviteFallbackBytes {
		t.Fatalf("relayHints returned %d bytes over %d: the size check models this bound", total, maxInviteFallbackBytes)
	}
	if len(hints) == 0 {
		t.Fatal("relayHints returned nothing, so the bound refuses rather than trims")
	}
	for _, hint := range hints {
		if len(hint) > maxInviteAddrBytes {
			t.Fatalf("relayHints returned a %d-byte hint, over the %d-byte per-address bound", len(hint), maxInviteAddrBytes)
		}
	}
}

// TestTheDaemonsOwnAddressFillIsBounded pins the set the daemon substitutes
// when a request names no bootstrap address — which is what every
// group_create open invite does, since that path stores no list at all. A
// libp2p host on a multi-homed machine reports dozens of addresses, and this
// fill bypassed every bound, so those tokens minted capabilities too large to
// redeem.
func TestTheDaemonsOwnAddressFillIsBounded(t *testing.T) {
	_, info := mustDaemonIdentity(t)
	binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	suffix := "/p2p/" + binding.PeerID.String()

	var own []string
	for i := 0; i < 12; i++ {
		own = append(own, fmt.Sprintf("/ip4/172.%d.0.1/tcp/1004", 17+i)+suffix)
	}
	for i := 0; i < 12; i++ {
		own = append(own, fmt.Sprintf("/ip4/203.0.113.%d/tcp/1004", 10+i)+suffix)
	}
	own = append(own, "/ip4/127.0.0.1/tcp/1004"+suffix)

	bounded := boundInviteAddresses(own)
	if len(bounded) == 0 || len(bounded) > maxInviteFallbackAddrs {
		t.Fatalf("filled %d addresses, want between 1 and %d", len(bounded), maxInviteFallbackAddrs)
	}
	total := 0
	for _, addr := range bounded {
		total += len(addr)
	}
	if total > maxInviteFallbackBytes {
		t.Fatalf("filled %d bytes, over the %d the size check models", total, maxInviteFallbackBytes)
	}
	// Routable first: a newcomer elsewhere can only use those.
	for _, addr := range bounded {
		if strings.HasPrefix(addr, "/ip4/172.") || strings.HasPrefix(addr, "/ip4/127.") {
			t.Fatalf("filled %q while routable addresses were available", addr)
		}
	}

	// A host with nothing but loopback must still produce something, or its
	// invites cannot be redeemed at all on a development machine.
	loopbackOnly := boundInviteAddresses([]string{"/ip4/127.0.0.1/tcp/1004" + suffix})
	if len(loopbackOnly) != 1 {
		t.Fatalf("a loopback-only host filled %d addresses, want 1 as a last resort", len(loopbackOnly))
	}
}
