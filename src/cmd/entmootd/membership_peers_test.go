package main

import (
	"context"
	"crypto/rand"
	"testing"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"

	"github.com/libp2p/go-libp2p"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// A node whose coverage bound has been pulled backwards has to reach the
// members it lost - they hold the records that restore them - and the sync
// fan-out is smaller than a busy group's membership. With the current
// projection filling every slot, the peers that can repair anything are never
// dialed at all, so the repair is dead on arrival in exactly the groups where
// it matters.
func TestMembershipPeersReachesTheMembersARewindDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())

	host, binding, err := libp2ptransport.NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close()
	messages, err := store.OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer messages.Close()
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: founder, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: host, Binding: binding, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer runtime.Close()
	session, _, err := runtime.AddLocalGroup(ctx, gid)
	if err != nil {
		t.Fatalf("AddLocalGroup: %v", err)
	}

	reachable := func(identity *keystore.Identity) {
		t.Helper()
		info := mustDaemonNodeInfo(t, identity)
		peerBinding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
		if err != nil {
			t.Fatal(err)
		}
		address, err := multiaddr.NewMultiaddr("/ip4/203.0.113.9/tcp/4001")
		if err != nil {
			t.Fatal(err)
		}
		host.Peerstore().AddAddrs(peerBinding.PeerID, []multiaddr.Multiaddr{address}, 24*60*60*1_000_000_000)
	}

	// The member that the rewind will drop, folded in and retired.
	if _, signed, err := session.group.SignCheckpoint(founder, true); err != nil || !signed {
		t.Fatalf("base checkpoint: signed=%t err=%v", signed, err)
	}
	base := session.group.Canonical()
	lost, lostInfo := mustDaemonIdentity(t)
	mustJoinWithInvite(t, session.group, lost, mustDaemonInvite(t, session.group, founder, lostInfo, 1))
	reachable(lost)
	if _, signed, err := session.group.SignCheckpoint(founder, true); err != nil || !signed {
		t.Fatalf("folding checkpoint: signed=%t err=%v", signed, err)
	}

	// Enough other reachable members to fill the fan-out on their own.
	for i := 0; i < maxMembershipSyncPeers+2; i++ {
		member, memberInfo := mustDaemonIdentity(t)
		mustJoinWithInvite(t, session.group, member, mustDaemonInvite(t, session.group, founder, memberInfo, 1))
		reachable(member)
	}
	if _, signed, err := session.group.SignCheckpoint(founder, true); err != nil || !signed {
		t.Fatalf("crowd checkpoint: signed=%t err=%v", signed, err)
	}
	head := session.group.Canonical()
	if len(runtime.membershipPeers(session)) != maxMembershipSyncPeers {
		t.Fatalf("the fixture has %d addressable peers, want the fan-out full at %d",
			len(runtime.membershipPeers(session)), maxMembershipSyncPeers)
	}

	// The rewind: a branch forking from the base that reaches further while
	// dated earlier, carrying only the founder.
	previous := base
	for sequence := base.Sequence + 1; sequence <= head.Sequence+1; sequence++ {
		body := previous
		body.ID = entmoot.RosterEntryID{}
		body.Sequence = sequence
		body.Previous = previous.ID
		body.Timestamp = previous.Timestamp + 1
		body.Covered = 0
		body.Members = []entmoot.NodeInfo{founderInfo}
		body.Signature = nil
		branch, err := membership.SignCheckpoint(founder, founderInfo, body)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.group.ApplyCheckpoint(branch); err != nil {
			t.Fatalf("branch at sequence %d: %v", sequence, err)
		}
		previous = branch
	}
	if session.group.IsMemberID(*lostInfo.MemberID) {
		t.Fatal("the rewind did not drop the member")
	}

	lostBinding, err := libp2ptransport.BindingFromPublicKey(lostInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	peers := runtime.membershipPeers(session)
	for _, candidate := range peers {
		if candidate.ID == lostBinding.PeerID {
			return
		}
	}
	t.Fatalf("the member the rewind dropped is not among the %d peers this node will pull from", len(peers))
}
