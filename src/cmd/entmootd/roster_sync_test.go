package main

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/roster"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// A founder that never pulls would never learn about an admin-authored add, so
// the candidate list has to include other members and exclude this node.
func TestRosterSyncPeersCoverOtherMembersFromTheFounder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity, founder, founderBinding := mustTestIdentity(t)
	memberIdentity, member, memberBinding := mustTestIdentity(t)

	host, _, err := libp2ptransport.NewHost(ctx, founderIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	groupID := entmoot.GroupID{0x33}
	groupRoster := roster.New(groupID)
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	add, err := groupRoster.SignEntry(founderIdentity, "add", member, nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := groupRoster.Apply(add); err != nil {
		t.Fatal(err)
	}
	// The member is only reachable because its address is in the peerstore.
	address := multiaddr.StringCast("/ip4/127.0.0.1/tcp/45999")
	host.Peerstore().AddAddr(memberBinding.PeerID, address, time.Hour)

	runtime := &groupRuntime{
		identity: founderIdentity, binding: founderBinding, host: host, dataDir: t.TempDir(),
		sessions: map[entmoot.GroupID]*groupSession{groupID: {groupID: groupID, roster: groupRoster}},
	}
	candidates := runtime.rosterSyncPeers(runtime.sessions[groupID])
	if len(candidates) != 1 {
		t.Fatalf("candidates = %+v, want exactly the other member", candidates)
	}
	if candidates[0].ID != memberBinding.PeerID {
		t.Fatalf("candidate = %s, want the member %s", candidates[0].ID, memberBinding.PeerID)
	}
	for _, candidate := range candidates {
		if candidate.ID == host.ID() {
			t.Fatal("candidate list includes this node")
		}
	}
	_ = memberIdentity
}

// A member with no known address is not a sync candidate, and the founder is
// tried first when it is reachable.
func TestRosterSyncPeersSkipUnreachableAndPreferFounder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity, founder, founderBinding := mustTestIdentity(t)
	localIdentity, local, localBinding := mustTestIdentity(t)
	_, silent, _ := mustTestIdentity(t)

	host, _, err := libp2ptransport.NewHost(ctx, localIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	groupID := entmoot.GroupID{0x34}
	groupRoster := roster.New(groupID)
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	for i, subject := range []entmoot.NodeInfo{local, silent} {
		entry, err := groupRoster.SignEntry(founderIdentity, "add", subject, nil, int64(2_000+i))
		if err != nil {
			t.Fatal(err)
		}
		if err := groupRoster.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	host.Peerstore().AddAddr(founderBinding.PeerID, multiaddr.StringCast("/ip4/127.0.0.1/tcp/45998"), time.Hour)

	runtime := &groupRuntime{
		identity: localIdentity, binding: localBinding, host: host, dataDir: t.TempDir(),
		sessions: map[entmoot.GroupID]*groupSession{groupID: {groupID: groupID, roster: groupRoster}},
	}
	candidates := runtime.rosterSyncPeers(runtime.sessions[groupID])
	if len(candidates) != 1 || candidates[0].ID != founderBinding.PeerID {
		t.Fatalf("candidates = %+v, want only the reachable founder", candidates)
	}
}
