package libp2ptransport

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store/storetest"
)

func TestFreshPeerJoinsMigratedGroupWithoutLegacyRoster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	founder, joiner := mustIdentity(t), mustIdentity(t)
	groupID := mustGroupID(t)
	root := t.TempDir()
	chain := []entmoot.RosterEntry{legacyEntry(t, founder, nil, "add", mustNodeInfo(t, founder.PublicKey), nil, 1000)}
	writeLegacyChain(t, root, groupID, chain)
	group := mustUpgradeLegacyGroup(t, root, groupID, founder)
	defer group.Close()
	serverHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	joinerHost, binding, err := NewHost(ctx, joiner, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer joinerHost.Close()
	messages := storetest.New(t)
	defer messages.Close()
	server := SyncServer{Host: serverHost, Store: messages, Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID }}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	invite := mustInvite(t, group, founder, joiner.PublicKey, 0, []string{serverHost.ID().String()})
	localRoot := t.TempDir()
	local, err := JoinGroup(ctx, joinerHost, peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}, localRoot, joiner, invite, mustNode(t, joiner))
	if err != nil {
		t.Fatal(err)
	}
	if !local.IsMemberID(binding.MemberID) || !group.IsMemberID(binding.MemberID) {
		local.Close()
		t.Fatal("join did not admit the fresh member on both peers")
	}
	if local.Canonical().ID != group.Canonical().ID || local.Legacy() != nil {
		local.Close()
		t.Fatal("join must preserve the founder checkpoint without fabricating a local legacy chain")
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := membership.Open(localRoot, groupID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.IsMemberID(binding.MemberID) {
		t.Fatal("admission was lost on reopen")
	}
	legacyRoot := t.TempDir()
	writeLegacyChain(t, legacyRoot, groupID, chain)
	adopted, err := JoinGroup(ctx, joinerHost, peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}, legacyRoot, joiner, invite, mustNode(t, joiner))
	if err == nil {
		adopted.Close()
		t.Fatal("join path replaced local legacy state")
	}
	if membership.Exists(legacyRoot, groupID) {
		t.Fatal("rejected join left an operational membership store")
	}
	// The guarded conversion path remains usable and binds the local chain.
	converted, err := membership.Adopt(legacyRoot, group.Canonical())
	if err != nil {
		t.Fatal(err)
	}
	defer converted.Close()
	if converted.Legacy() == nil {
		t.Fatal("conversion lost its local legacy chain")
	}
}
