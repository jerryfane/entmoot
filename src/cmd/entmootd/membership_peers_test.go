package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"sort"
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

	// Several members that the rewind will drop, folded in and retired.
	if _, signed, err := session.group.SignCheckpoint(founder, true); err != nil || !signed {
		t.Fatalf("base checkpoint: signed=%t err=%v", signed, err)
	}
	base := session.group.Canonical()
	lostInfos := make([]entmoot.NodeInfo, 0, maxRewoundSyncPeers+2)
	for i := 0; i < maxRewoundSyncPeers+2; i++ {
		identity, info := mustDaemonIdentity(t)
		mustJoinWithInvite(t, session.group, identity, mustDaemonInvite(t, session.group, founder, info, 1))
		reachable(identity)
		lostInfos = append(lostInfos, info)
	}
	if _, signed, err := session.group.SignCheckpoint(founder, true); err != nil || !signed {
		t.Fatalf("folding checkpoint: signed=%t err=%v", signed, err)
	}

	// Enough other reachable members to fill the fan-out on their own, folded
	// in and retired too, so the branch below contradicts no record we hold.
	keptInfos := []entmoot.NodeInfo{founderInfo}
	for i := 0; i < maxMembershipSyncPeers+2; i++ {
		identity, info := mustDaemonIdentity(t)
		mustJoinWithInvite(t, session.group, identity, mustDaemonInvite(t, session.group, founder, info, 1))
		reachable(identity)
		keptInfos = append(keptInfos, info)
	}
	if _, signed, err := session.group.SignCheckpoint(founder, true); err != nil || !signed {
		t.Fatalf("crowd checkpoint: signed=%t err=%v", signed, err)
	}
	head := session.group.Canonical()
	if got := len(runtime.membershipPeers(session)); got != maxMembershipSyncPeers {
		t.Fatalf("the fixture has %d addressable peers, want the fan-out full at %d", got, maxMembershipSyncPeers)
	}

	// The rewind: a branch forking from the base that reaches further while
	// dated earlier, carrying the crowd but not the members above.
	previous := base
	for sequence := base.Sequence + 1; sequence <= head.Sequence+1; sequence++ {
		body := previous
		body.ID = entmoot.RosterEntryID{}
		body.Sequence = sequence
		body.Previous = previous.ID
		body.Timestamp = previous.Timestamp + 1
		body.Covered = 0
		body.Members = append([]entmoot.NodeInfo(nil), keptInfos...)
		sort.Slice(body.Members, func(i, j int) bool {
			left, _ := entmoot.ResolvedMemberID(body.Members[i])
			right, _ := entmoot.ResolvedMemberID(body.Members[j])
			return bytes.Compare(left[:], right[:]) < 0
		})
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
	for _, info := range lostInfos {
		if session.group.IsMemberID(*info.MemberID) {
			t.Fatalf("the rewind did not drop %s", info.MemberID)
		}
	}

	peers := runtime.membershipPeers(session)
	dropped := 0
	for _, info := range lostInfos {
		binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range peers {
			if candidate.ID == binding.PeerID {
				dropped++
			}
		}
	}
	if dropped == 0 {
		t.Fatalf("no member the rewind dropped is among the %d peers this node will pull from", len(peers))
	}

	// And the reservation has a ceiling, which is the half the constant
	// exists for: repair peers must not take the whole fan-out, or a long
	// rewind would stop this node syncing with the members it still has.
	// Literal numbers on purpose: this is the only guard on the reservation's
	// ceiling, and an assertion written in terms of maxRewoundSyncPeers moves
	// with the constant it is supposed to pin. Eight slots, at most three of
	// them reserved for repair, so at least five remain for the members this
	// node still has - or a long rewind would stop ordinary sync.
	if dropped != 3 {
		t.Fatalf("%d of %d peers are members the rewind dropped, want exactly the 3 reserved slots", dropped, len(peers))
	}
	if len(peers)-dropped != 5 {
		t.Fatalf("%d of %d peers are current members, want the remaining 5", len(peers)-dropped, len(peers))
	}
}
