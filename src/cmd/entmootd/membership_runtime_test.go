package main

import (
	"context"
	"errors"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// A founder that never pulls would never learn about a join another admin
// already holds, so the candidate list covers the other members and never
// this node.
func TestMembershipPeersCoverOtherMembersAndExcludeThisNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dataDir := t.TempDir()
	gid := daemonTestGroupID(0x33)
	founder, _ := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	openPolicy := membership.DefaultPolicy()
	openPolicy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, dataDir, gid, founder, openPolicy)

	group := mustOpenGroup(t, dataDir, gid)
	defer mustCloseGroup(t, group)
	if _, err := group.SignRecord(member, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("member join: %v", err)
	}

	host, binding, err := libp2ptransport.NewHost(ctx, founder, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close()
	memberBinding, err := libp2ptransport.BindingFromPublicKey(memberInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	// The member is a candidate only because its address is known.
	host.Peerstore().AddAddr(memberBinding.PeerID, multiaddr.StringCast("/ip4/127.0.0.1/tcp/45999"), time.Hour)

	runtime := &groupRuntime{identity: founder, binding: binding, host: host, dataDir: dataDir}
	candidates := runtime.membershipPeers(&groupSession{groupID: gid, group: group})
	if len(candidates) != 1 || candidates[0].ID != memberBinding.PeerID {
		t.Fatalf("candidates = %+v, want exactly the other member %s", candidates, memberBinding.PeerID)
	}
}

// The founder is the most likely node to be reachable, so it is asked first;
// a member whose address is unknown is not a candidate at all.
func TestMembershipPeersPreferTheFounderAndSkipUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dataDir := t.TempDir()
	gid := daemonTestGroupID(0x34)
	founder, founderInfo := mustDaemonIdentity(t)
	local, _ := mustDaemonIdentity(t)
	other, otherInfo := mustDaemonIdentity(t)
	silent, _ := mustDaemonIdentity(t)
	openPolicy := membership.DefaultPolicy()
	openPolicy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, dataDir, gid, founder, openPolicy)

	group := mustOpenGroup(t, dataDir, gid)
	defer mustCloseGroup(t, group)
	for _, joiner := range []*keystore.Identity{local, other, silent} {
		if _, err := group.SignRecord(joiner, membership.Record{Kind: membership.KindJoin}); err != nil {
			t.Fatalf("join: %v", err)
		}
	}

	host, binding, err := libp2ptransport.NewHost(ctx, local, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close()
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founderInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	otherBinding, err := libp2ptransport.BindingFromPublicKey(otherInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	host.Peerstore().AddAddr(otherBinding.PeerID, multiaddr.StringCast("/ip4/127.0.0.1/tcp/45997"), time.Hour)
	host.Peerstore().AddAddr(founderBinding.PeerID, multiaddr.StringCast("/ip4/127.0.0.1/tcp/45998"), time.Hour)

	runtime := &groupRuntime{identity: local, binding: binding, host: host, dataDir: dataDir}
	candidates := runtime.membershipPeers(&groupSession{groupID: gid, group: group})
	if len(candidates) != 2 {
		t.Fatalf("candidates = %+v, want the founder and the one reachable member", candidates)
	}
	if candidates[0].ID != founderBinding.PeerID {
		t.Fatalf("first candidate = %s, want the founder %s", candidates[0].ID, founderBinding.PeerID)
	}
	if candidates[1].ID != otherBinding.PeerID {
		t.Fatalf("second candidate = %s, want the reachable member %s", candidates[1].ID, otherBinding.PeerID)
	}
}

// An evicted node has to find out. It cannot be told by the record it will
// never be served, so the peer that refuses it hands over the signed removal;
// once applied, the node stops treating itself as a member and stops
// publishing into a group that no longer accepts it.
func TestBannedNodeLearnsItsRemovalOverMembershipSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	gid := daemonTestGroupID(0x08)
	founder, founderInfo := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)

	founderRoot := t.TempDir()
	memberRoot := t.TempDir()
	mustCreateGroup(t, founderRoot, gid, founder, membership.DefaultPolicy())

	founderGroup := mustOpenGroup(t, founderRoot, gid)
	join := mustJoinWithInvite(t, founderGroup, member, mustDaemonInvite(t, founderGroup, founder, memberInfo, 1))
	checkpoint := founderGroup.Canonical()
	mustCloseGroup(t, founderGroup)

	// The member holds the same group: checkpoint 0 plus its own join.
	memberGroup, err := membership.Adopt(memberRoot, checkpoint)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if _, err := memberGroup.Apply(join); err != nil {
		t.Fatalf("apply join on the member: %v", err)
	}
	mustCloseGroup(t, memberGroup)

	founderRuntime, founderSession, founderHost := startTestRuntime(t, ctx, founderRoot, founder, gid)
	defer founderRuntime.Close()
	defer founderHost.Close()
	memberRuntime, memberSession, memberHost := startTestRuntime(t, ctx, memberRoot, member, gid)
	defer memberRuntime.Close()
	defer memberHost.Close()

	founderAddr := peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}
	if err := memberHost.Connect(ctx, founderAddr); err != nil {
		t.Fatalf("connect to founder: %v", err)
	}

	if _, err := founderSession.group.SignRecord(founder, membership.Record{
		Kind: membership.KindRemove, Subject: memberInfo, Banned: true,
	}); err != nil {
		t.Fatalf("ban record: %v", err)
	}
	if founderSession.group.IsMemberID(*memberInfo.MemberID) {
		t.Fatal("the founder still lists the banned member")
	}

	memberRuntime.syncMembership(ctx, memberSession)

	if memberSession.group.IsMemberID(*memberInfo.MemberID) {
		t.Fatal("the removed node still lists itself as a member")
	}
	if !memberSession.group.IsBanned(*memberInfo.MemberID) {
		t.Fatal("the removed node did not learn that it is banned")
	}

	message := mustSignedMessage(t, ctx, gid, memberInfo, member, memberSession.group.Canonical().ID)
	if _, err := memberSession.live.Publish(ctx, message); err == nil {
		t.Fatal("a removed node published into the group")
	} else if !errors.Is(err, entmoot.ErrNotMember) {
		t.Fatalf("publish error = %v, want %v", err, entmoot.ErrNotMember)
	}

	// The founder is unaffected: it is still the group.
	if !founderSession.group.IsMemberID(*founderInfo.MemberID) {
		t.Fatal("the founder lost its own membership")
	}
}

// startTestRuntime brings up a daemon group runtime over a real libp2p host,
// the way serve does, and returns its live session for the group.
func startTestRuntime(t *testing.T, ctx context.Context, root string, identity *keystore.Identity, gid entmoot.GroupID) (*groupRuntime, *groupSession, host.Host) {
	t.Helper()
	host, binding, err := libp2ptransport.NewHost(ctx, identity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	messages, err := store.OpenSQLite(root)
	if err != nil {
		host.Close()
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = messages.Close() })
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: identity, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: host, Binding: binding, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		host.Close()
		t.Fatalf("newGroupRuntime: %v", err)
	}
	session, _, err := runtime.AddLocalGroup(ctx, gid)
	if err != nil {
		runtime.Close()
		host.Close()
		t.Fatalf("AddLocalGroup: %v", err)
	}
	return runtime, session, host
}

func mustSignedMessage(t *testing.T, ctx context.Context, gid entmoot.GroupID, author entmoot.NodeInfo, identity *keystore.Identity, head entmoot.RosterEntryID) entmoot.Message {
	t.Helper()
	message := entmoot.Message{
		Version: 2, GroupID: gid, Author: author, Timestamp: time.Now().UnixMilli(),
		Topics: []string{"chat"}, Content: []byte("still here?"), RosterHead: &head,
	}
	signer, err := signing.NewLocalSigner(author, identity)
	if err != nil {
		t.Fatalf("NewLocalSigner: %v", err)
	}
	signed, err := signing.SignMessage(ctx, signer, message)
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	return signed
}
