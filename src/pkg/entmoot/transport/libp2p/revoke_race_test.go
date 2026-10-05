package libp2ptransport

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store/storetest"
)

// A revoke is sealed with a checkpoint at once, so a joiner that signed its
// join just before it - against the checkpoint it had just read - has that
// join refused as stale when it arrives. That joiner did nothing wrong: running
// the join again must re-read the group, sign above the new checkpoint and be
// admitted, from the same data root the failed attempt used.
func TestJoinRacingASealedRevokeSucceedsWhenRerun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	founder, joiner, other := mustIdentity(t), mustIdentity(t), mustIdentity(t)
	serverHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	joinerHost, joinerBinding, err := NewHost(ctx, joiner, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer joinerHost.Close()
	groupID, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	messages := storetest.New(t)
	defer messages.Close()
	server := SyncServer{
		Host:  serverHost,
		Store: messages,
		Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	allowed := []string{serverHost.ID().String()}
	invite := mustInvite(t, group, founder, joiner.PublicKey, 0, allowed)
	revoked := mustInvite(t, group, founder, other.PublicKey, 0, allowed)

	// The founder revokes somebody else's invite between the joiner's read
	// and its push, which is exactly the window a concurrent revoke hits.
	var raced atomic.Bool
	serverHost.SetStreamHandler(MembershipPushProtocol, func(stream network.Stream) {
		if raced.CompareAndSwap(false, true) {
			// The revoke must be dated after the joiner's record. It is
			// sealed the way the revoke paths seal it: one checkpoint dated
			// after it, which makes the in-flight join stale.
			time.Sleep(5 * time.Millisecond)
			if _, err := group.SignRecord(founder, membership.Record{
				Kind: membership.KindRevokeInvite, InviteNonce: revoked.Nonce,
			}); err != nil {
				t.Errorf("revoke: %v", err)
			}
			if _, _, err := group.SignCheckpoint(founder, true); err != nil {
				t.Errorf("checkpoint the revoke: %v", err)
			}
		}
		server.handleMembershipPush(stream)
	})

	remote := peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}
	root := t.TempDir()
	local, err := JoinGroup(ctx, joinerHost, remote, root, joiner, invite, mustNode(t, joiner))
	if err == nil {
		_ = local.Close()
		t.Fatal("a join signed before the sealed revoke was accepted")
	}
	if !strings.Contains(err.Error(), "predates the current checkpoint") {
		t.Fatalf("the refusal does not say the join was stale: %v", err)
	}
	if !raced.Load() {
		t.Fatal("the join never reached the push")
	}
	if group.IsMemberID(joinerBinding.MemberID) {
		t.Fatal("the founder admitted the stale join")
	}

	local, err = JoinGroup(ctx, joinerHost, remote, root, joiner, invite, mustNode(t, joiner))
	if err != nil {
		t.Fatalf("re-running the join failed: %v", err)
	}
	defer local.Close()
	if !group.IsMemberID(joinerBinding.MemberID) || !local.IsMemberID(joinerBinding.MemberID) {
		t.Fatal("re-running the join did not admit the joiner on both sides")
	}
}
