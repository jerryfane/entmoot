package main

import (
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// TestInviteBootstrapAcceptsAnyMemberPeer pins the rule that makes an invite
// survive its issuer going down. Before this, the only address an invite could
// name was the issuing node's own, and a peer serves a newcomer only when the
// capability names it — so the issuer had to be running for its own invite to
// work, which is the one thing self-signed admission was meant to remove.
func TestInviteBootstrapAcceptsAnyMemberPeer(t *testing.T) {
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, root, gid, founder, policy)

	group, err := membership.Open(root, gid)
	if err != nil {
		t.Fatalf("membership.Open: %v", err)
	}
	defer group.Close()

	member, memberInfo := mustDaemonIdentity(t)
	if _, err := group.SignRecord(member, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if !group.IsMemberID(*memberInfo.MemberID) {
		t.Fatal("the second member did not join")
	}

	peers, err := groupMemberPeerIDs(group)
	if err != nil {
		t.Fatalf("groupMemberPeerIDs: %v", err)
	}
	memberBinding, err := libp2ptransport.BindingFromPublicKey(memberInfo.EntmootPubKey)
	if err != nil {
		t.Fatalf("BindingFromPublicKey: %v", err)
	}
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founderInfo.EntmootPubKey)
	if err != nil {
		t.Fatalf("BindingFromPublicKey: %v", err)
	}
	if _, ok := peers[memberBinding.PeerID]; !ok {
		t.Fatal("a current member's peer id is not serveable")
	}
	if _, ok := peers[founderBinding.PeerID]; !ok {
		t.Fatal("the founder's own peer id is not serveable")
	}

	// A non-member's peer id must stay unserveable: widening the bootstrap
	// list must not turn it into an arbitrary redirect.
	stranger, strangerInfo := mustDaemonIdentity(t)
	_ = stranger
	strangerBinding, err := libp2ptransport.BindingFromPublicKey(strangerInfo.EntmootPubKey)
	if err != nil {
		t.Fatalf("BindingFromPublicKey: %v", err)
	}
	if _, ok := peers[strangerBinding.PeerID]; ok {
		t.Fatal("a non-member's peer id is serveable")
	}

	// A removed member stops being serveable at once, like every other
	// authority question in this design.
	if _, err := group.SignRecord(founder, membership.Record{Kind: membership.KindRemove, Subject: memberInfo}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	peers, err = groupMemberPeerIDs(group)
	if err != nil {
		t.Fatalf("groupMemberPeerIDs: %v", err)
	}
	if _, ok := peers[memberBinding.PeerID]; ok {
		t.Fatal("a removed member is still serveable")
	}
}

// TestFallbackPeersAreBoundedAndRoutable pins the two properties the bound
// needs. The first version capped MEMBERS, not addresses: a multi-homed member
// contributed every address it had, so four such members put the capability
// past the 8 KiB request frame — the invite this feature exists to make robust
// became one that cannot be redeemed at all. It also enumerated Docker and
// CGNAT ranges into invites whose links get shared.
func TestFallbackPeersAreBoundedAndRoutable(t *testing.T) {
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, root, gid, founder, policy)
	group, err := membership.Open(root, gid)
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	members := make(map[peer.ID]struct{})
	for i := 0; i < 6; i++ {
		identity, info := mustDaemonIdentity(t)
		if _, err := group.SignRecord(identity, membership.Record{Kind: membership.KindJoin}); err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
		binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
		if err != nil {
			t.Fatal(err)
		}
		members[binding.PeerID] = struct{}{}
		// Each member is multi-homed the way a real host is: many private
		// addresses and one public.
		addrs := []multiaddr.Multiaddr{}
		for j := 0; j < 12; j++ {
			addrs = append(addrs, mustMultiaddr(t, fmt.Sprintf("/ip4/172.%d.0.%d/tcp/1004", 17+j, i+1)))
		}
		addrs = append(addrs,
			mustMultiaddr(t, "/ip4/127.0.0.1/tcp/1004"),
			mustMultiaddr(t, "/ip4/100.106.218.88/tcp/1004"),
			mustMultiaddr(t, fmt.Sprintf("/ip4/37.27.59.%d/tcp/1004", 80+i)),
			mustMultiaddr(t, fmt.Sprintf("/ip4/37.27.59.%d/tcp/2004", 80+i)),
			mustMultiaddr(t, fmt.Sprintf("/ip4/37.27.59.%d/tcp/3004", 80+i)),
		)
		if err := persistGroupPeer(root, gid, peer.AddrInfo{ID: binding.PeerID, Addrs: addrs}); err != nil {
			t.Fatalf("persistGroupPeer: %v", err)
		}
	}

	selfBinding, err := libp2ptransport.BindingFromPublicKey(founderInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	addresses, peerIDs, private := addKnownMemberPeers(root, gid, members, selfBinding.PeerID, nil, nil, map[peer.ID]struct{}{})
	if private != 0 {
		t.Fatalf("reported %d private-only fallbacks, want 0: every member here has a routable address", private)
	}

	if len(peerIDs) > maxInviteFallbackPeers {
		t.Fatalf("attached %d peers, want at most %d", len(peerIDs), maxInviteFallbackPeers)
	}
	if len(addresses) > maxInviteFallbackAddrs {
		t.Fatalf("attached %d addresses, want at most %d: bounding members alone let an invite outgrow the request frame", len(addresses), maxInviteFallbackAddrs)
	}
	if len(addresses) == 0 {
		t.Fatal("attached nothing, so the fallback would never help")
	}
	for _, addr := range addresses {
		for _, leak := range []string{"/ip4/172.", "/ip4/10.", "/ip4/192.168.", "/ip4/127.", "/ip4/100.106."} {
			if strings.HasPrefix(addr, leak) {
				t.Fatalf("attached a non-routable address %q while this member has a routable one: routable addresses come first", addr)
			}
		}
	}
}

func mustMultiaddr(t *testing.T, value string) multiaddr.Multiaddr {
	t.Helper()
	addr, err := multiaddr.NewMultiaddr(value)
	if err != nil {
		t.Fatalf("NewMultiaddr(%q): %v", value, err)
	}
	return addr
}

// TestPrivateOnlyMemberStillGetsOneFallbackSlot covers the case the routability
// filter got wrong on its own: on a LAN or an overlay network a member's
// private address is exactly the door that works, and dropping it left the
// fallback empty while the operator believed members were attached — so the
// invite quietly went back to depending on the issuer's uptime.
func TestPrivateOnlyMemberStillGetsOneFallbackSlot(t *testing.T) {
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, root, gid, founder, policy)
	group, err := membership.Open(root, gid)
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	identity, info := mustDaemonIdentity(t)
	if _, err := group.SignRecord(identity, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("join: %v", err)
	}
	binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	members := map[peer.ID]struct{}{binding.PeerID: {}}
	// Reachable only on a LAN address, plus a loopback that must never be
	// attached because it names the newcomer's own machine.
	if err := persistGroupPeer(root, gid, peer.AddrInfo{ID: binding.PeerID, Addrs: []multiaddr.Multiaddr{
		mustMultiaddr(t, "/ip4/127.0.0.1/tcp/1004"),
		mustMultiaddr(t, "/ip4/192.168.1.40/tcp/1004"),
	}}); err != nil {
		t.Fatalf("persistGroupPeer: %v", err)
	}

	selfBinding, err := libp2ptransport.BindingFromPublicKey(founderInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	addresses, peerIDs, private := addKnownMemberPeers(root, gid, members, selfBinding.PeerID, nil, nil, map[peer.ID]struct{}{})
	if len(peerIDs) != 1 || len(addresses) != 1 {
		t.Fatalf("attached %d peers / %d addresses, want 1 and 1: a private-only member must still be reachable", len(peerIDs), len(addresses))
	}
	if !strings.HasPrefix(addresses[0], "/ip4/192.168.1.40/") {
		t.Fatalf("attached %q, want the LAN address", addresses[0])
	}
	if private != 1 {
		t.Fatalf("reported %d private-only fallbacks, want 1 so the operator is told", private)
	}
}

// TestAnInviteTooLargeToRedeemIsRefused pins the mint-time guard. A capability
// travels inside one membership-sync request; past the frame limit the mint
// would succeed and every redemption fail with a size error naming no cause.
func TestAnInviteTooLargeToRedeemIsRefused(t *testing.T) {
	capability := entmoot.BootstrapCapability{GroupID: entmoot.GroupID{1}}
	if _, tooLarge := libp2ptransport.CapabilityTooLarge(capability); tooLarge {
		t.Fatal("an empty capability is reported as too large")
	}
	for i := 0; i < 200; i++ {
		capability.AllowedMultiaddrs = append(capability.AllowedMultiaddrs,
			fmt.Sprintf("/ip4/37.27.59.%d/tcp/1004/p2p/12D3KooWGu8QgDWWsThK4JqbwXDBAQmTr9NXsomethinglong%d", i%250, i))
	}
	size, tooLarge := libp2ptransport.CapabilityTooLarge(capability)
	if !tooLarge {
		t.Fatalf("a %d-byte capability is not reported as too large (limit %d)", size, libp2ptransport.MaxCapabilityBytes)
	}
}
