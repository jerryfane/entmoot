package libp2ptransport

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/store/storetest"
)

// A browser page that reloads dials back with the same key while the node
// still holds the old page's webrtc-direct connection. The new page must get
// live messages without waiting for the old connection to time out (live
// node, 2026-11-17: ~35 s, after which the page's subscription was gone and it
// got no live messages for the rest of its session).
func TestReloadedWebRTCPageReceivesLiveMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nodeIdentity, pageIdentity := mustIdentity(t), mustIdentity(t)
	groupID, group := mustOpenGroup(t, nodeIdentity, pageIdentity)

	node, _, err := NewConfiguredHost(ctx, nodeIdentity, HostConfig{ListenAddrs: []string{"/ip4/127.0.0.1/udp/0/webrtc-direct"}})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	var dial []ma.Multiaddr
	for _, addr := range node.Network().ListenAddresses() {
		if _, err := addr.ValueForProtocol(ma.P_WEBRTC_DIRECT); err == nil {
			dial = append(dial, addr)
		}
	}
	nodeGroup, err := NewLiveGroup(ctx, LiveConfig{Host: node, GroupID: groupID, Group: group, Store: storetest.New(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeGroup.Close()

	openPage := func() store.MessageStore {
		page, _, err := NewHost(ctx, pageIdentity, libp2p.NoListenAddrs)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = page.Close() })
		if err := page.Connect(ctx, peer.AddrInfo{ID: node.ID(), Addrs: dial}); err != nil {
			t.Fatal(err)
		}
		messages := storetest.New(t)
		live, err := NewLiveGroup(ctx, LiveConfig{Host: page, GroupID: groupID, Group: group, Store: messages})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = live.Close() })
		time.Sleep(1500 * time.Millisecond) // subscription and mesh setup
		return messages
	}

	firstPage := openPage()
	before := signedLiveMessage(t, nodeIdentity, group, groupID, 10_000, "before reload")
	if _, err := nodeGroup.Publish(ctx, before); err != nil {
		t.Fatal(err)
	}
	waitForStoredMessage(t, ctx, firstPage, groupID, before.ID)

	// The page reloads: a new connection under the same peer ID while the
	// old one stays open, as a crashed tab's does until WebRTC times it out.
	secondPage := openPage()
	after := signedLiveMessage(t, nodeIdentity, group, groupID, 11_000, "after reload")
	if _, err := nodeGroup.Publish(ctx, after); err != nil {
		t.Fatal(err)
	}
	wait, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	waitForStoredMessage(t, wait, secondPage, groupID, after.ID)
}
