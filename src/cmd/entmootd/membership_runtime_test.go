package main

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
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

// startTestRuntimeWithProfiles is startTestRuntime plus the ESP state store
// the daemon passes in serve, so observed member profiles are recorded.
func startTestRuntimeWithProfiles(t *testing.T, ctx context.Context, root string, identity *keystore.Identity, gid entmoot.GroupID, profiles esphttp.StateStore) (*groupRuntime, *groupSession, host.Host) {
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
		Host: host, Binding: binding, Mode: libp2ptransport.DirectConnectivity, Profiles: profiles,
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

// Checkpointing is the daemon's job, not an operator's. It has to happen on
// the cadence even when this node hears from nobody: the records it signs
// itself count towards the cadence, and a founder alone in a group still has
// to retire them.
func TestDaemonCheckpointsOnCadenceWithNoPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := t.TempDir()
	founder, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	policy.CheckpointEvery = 3
	mustCreateGroup(t, root, gid, founder, policy)

	runtime, session, host := startTestRuntime(t, ctx, root, founder, gid)
	defer host.Close()
	defer runtime.Close()

	// Three joins, signed locally: nothing arrives from any peer, and there is
	// no peer to arrive from.
	for i := 0; i < 3; i++ {
		joiner, err := keystore.Generate()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.group.SignRecord(joiner, membership.Record{Kind: membership.KindJoin}); err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
	}
	if got := session.group.Canonical().Sequence; got != 0 {
		t.Fatalf("canonical sequence = %d before any maintenance round, want 0", got)
	}
	if got := len(runtime.membershipPeers(session)); got != 0 {
		t.Fatalf("the fixture has %d reachable peers, so it does not test the no-peer path", got)
	}

	runtime.syncMembership(ctx, session)

	canonical := session.group.Canonical()
	if canonical.Sequence != 1 {
		t.Fatalf("canonical sequence = %d after the cadence was reached, want 1", canonical.Sequence)
	}
	if canonical.Covered != 3 {
		t.Fatalf("checkpoint covered %d records, want 3", canonical.Covered)
	}
	if got := len(session.group.MemberIDs()); got != 4 {
		t.Fatalf("membership = %d, want the founder plus three joiners", got)
	}
	if got := session.group.EffectivePendingCount(); got != 0 {
		t.Fatalf("%d records are still pending after the checkpoint", got)
	}
}

// A follower adopting checkpoint 0 has only its own chain to judge it by, so
// that judgement is the whole defence: this is the one moment a node holds no
// checkpoint to compare against, and it is exactly when a hostile peer would
// try to rewrite who is in the group.
func TestFollowerRefusesACheckpointThatDisagreesWithItsChain(t *testing.T) {
	founder, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	member, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	stranger, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	founderInfo := mustDaemonNodeInfo(t, founder)
	memberInfo := mustDaemonNodeInfo(t, member)
	strangerInfo := mustDaemonNodeInfo(t, stranger)

	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	head := entmoot.RosterEntryID{0x11}
	chain := map[entmoot.MemberID]struct{}{
		*founderInfo.MemberID: {},
		*memberInfo.MemberID:  {},
	}
	runtime := &groupRuntime{logger: slog.Default()}

	good := membership.Checkpoint{
		Version:    membership.Version,
		GroupID:    gid,
		Sequence:   0,
		Founder:    founderInfo,
		Members:    []entmoot.NodeInfo{founderInfo, memberInfo},
		LegacyHead: &head,
	}
	if err := runtime.checkpointZeroMatchesChain(good, *founderInfo.MemberID, founderInfo, head, chain); err != nil {
		t.Fatalf("a checkpoint describing this node's own chain was refused: %v", err)
	}

	otherHead := entmoot.RosterEntryID{0x22}
	cases := map[string]func(membership.Checkpoint) membership.Checkpoint{
		"names another chain": func(cp membership.Checkpoint) membership.Checkpoint {
			cp.LegacyHead = &otherHead
			return cp
		},
		"names no chain at all": func(cp membership.Checkpoint) membership.Checkpoint {
			cp.LegacyHead = nil
			return cp
		},
		"adds a member the chain never carried": func(cp membership.Checkpoint) membership.Checkpoint {
			cp.Members = append(append([]entmoot.NodeInfo(nil), cp.Members...), strangerInfo)
			return cp
		},
		"drops a member the chain carries": func(cp membership.Checkpoint) membership.Checkpoint {
			cp.Members = []entmoot.NodeInfo{founderInfo}
			return cp
		},
		"substitutes one member for another": func(cp membership.Checkpoint) membership.Checkpoint {
			cp.Members = []entmoot.NodeInfo{founderInfo, strangerInfo}
			return cp
		},
		"claims a different founder": func(cp membership.Checkpoint) membership.Checkpoint {
			cp.Founder = strangerInfo
			return cp
		},
		"is not a starting point": func(cp membership.Checkpoint) membership.Checkpoint {
			cp.Sequence = 3
			return cp
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if err := runtime.checkpointZeroMatchesChain(mutate(good), *founderInfo.MemberID, founderInfo, head, chain); err == nil {
				t.Fatalf("a checkpoint that %s was accepted", name)
			}
		})
	}
}
