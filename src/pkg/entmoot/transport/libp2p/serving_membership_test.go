package libp2ptransport

import (
	"context"
	"testing"
	"time"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestARemovedMemberStopsServingPreMembershipReads pins the rule that makes it
// safe for an invite to name members other than its issuer. An invite is
// minted against the membership of the moment and then lives for its whole
// TTL, so being named cannot be sufficient: a node removed afterwards would
// otherwise stay a working admission channel for every invite that named it.
func TestARemovedMemberStopsServingPreMembershipReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	founder, server, joiner := mustIdentity(t), mustIdentity(t), mustIdentity(t)
	serverHost, serverBinding, err := NewHost(ctx, server, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	clientHost, _, err := NewHost(ctx, joiner, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()

	groupID := mustGroupID(t)
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	group, err := membership.Create(t.TempDir(), founder, mustNode(t, founder), groupID, policy, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	serverInfo := mustNode(t, server)
	if _, err := group.SignRecord(server, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if !group.IsMemberID(serverBinding.MemberID) {
		t.Fatal("the serving node did not join")
	}

	messages, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer messages.Close()
	sync := SyncServer{
		Host: serverHost, Store: messages,
		Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID },
	}
	if err := sync.Install(); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}

	// The founder mints an invite naming the serving member, not itself.
	capability := mustInvite(t, group, founder, joiner.PublicKey, 0, []string{serverHost.ID().String()})

	read := func(name string) (MembershipSyncResponse, error) {
		return RequestMembership(ctx, clientHost, remote, MembershipSyncRequest{
			Version: 1, RequestID: name, GroupID: groupID, Capability: &capability,
		})
	}

	response, err := read("member-serves")
	if err != nil || len(response.Checkpoints) == 0 {
		t.Fatalf("a current member must serve a named invite: response=%+v err=%v", response, err)
	}

	// The founder removes it. The invite is unchanged and still names it.
	if _, err := group.SignRecord(founder, membership.Record{Kind: membership.KindRemove, Subject: serverInfo}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if group.IsMemberID(serverBinding.MemberID) {
		t.Fatal("the removal did not apply")
	}

	response, err = read("removed-serves")
	if err == nil || len(response.Checkpoints) != 0 {
		t.Fatalf("a removed member still served a pre-membership read: response=%+v err=%v", response, err)
	}
}

// TestAnIssuerServesWithoutBeingAMember covers the exemption the rule needs: a
// founder may issue after removing itself, and must still be able to serve the
// invite it signed.
func TestAnIssuerServesWithoutBeingAMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	founder, joiner := mustIdentity(t), mustIdentity(t)
	serverHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	clientHost, _, err := NewHost(ctx, joiner, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()

	groupID, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	messages, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer messages.Close()
	sync := SyncServer{
		Host: serverHost, Store: messages,
		Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID },
	}
	if err := sync.Install(); err != nil {
		t.Fatal(err)
	}

	capability := mustInvite(t, group, founder, joiner.PublicKey, 0, []string{serverHost.ID().String()})
	if _, err := group.SignRecord(founder, membership.Record{Kind: membership.KindLeave}); err != nil {
		t.Fatalf("leave: %v", err)
	}

	response, err := RequestMembership(ctx, clientHost,
		peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()},
		MembershipSyncRequest{Version: 1, RequestID: "founder-left", GroupID: groupID, Capability: &capability})
	if err != nil || len(response.Checkpoints) == 0 {
		t.Fatalf("a founder that left could not serve its own invite: response=%+v err=%v", response, err)
	}
}
