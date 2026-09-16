package main

import (
	"crypto/rand"
	"testing"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
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
