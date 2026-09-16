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
	addresses, peerIDs := addKnownMemberPeers(root, gid, members, selfBinding.PeerID, nil, nil, map[peer.ID]struct{}{})

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
				t.Fatalf("attached a non-routable address %q: an invite should not enumerate a member's internal network", addr)
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
