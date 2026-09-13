package libp2ptransport

import (
	"context"
	"io"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/record"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
)

func TestRelayOnlyRawIdentifyAndSignedRecordsExcludeDirectHints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	relay, _, err := NewHost(ctx, mustIdentity(t), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.ForceReachabilityPublic(), libp2p.EnableRelayService())
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	controlled := peer.AddrInfo{ID: relay.ID(), Addrs: relay.Addrs()}
	app, _, err := NewHost(ctx, mustIdentity(t), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	if _, err := relayclient.Reserve(ctx, app, controlled); err != nil {
		t.Fatal(err)
	}
	private, _, err := NewConfiguredHost(ctx, mustIdentity(t), HostConfig{Mode: RelayOnlyConnectivity, ControlledRelays: []peer.AddrInfo{controlled}})
	if err != nil {
		t.Fatal(err)
	}
	defer private.Close()
	if err := private.Connect(ctx, peer.AddrInfo{ID: app.ID(), Addrs: app.Addrs()}); err == nil {
		t.Fatal("direct application dial succeeded")
	}
	if addresses := private.Peerstore().Addrs(app.ID()); len(addresses) != 0 {
		t.Fatalf("rejected dial leaked raw hints: %v", addresses)
	}
	events, err := private.EventBus().Subscribe(new(event.EvtPeerIdentificationCompleted))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	circuit := controlled.Addrs[0].Encapsulate(ma.StringCast("/p2p/" + relay.ID().String() + "/p2p-circuit"))
	app.SetStreamHandler("/entmoot/privacy-regression/1", func(s network.Stream) { defer s.Close(); _, _ = s.Write([]byte("private circuit")) })
	if err := private.Connect(ctx, peer.AddrInfo{ID: app.ID(), Addrs: []ma.Multiaddr{circuit}}); err != nil {
		t.Fatal(err)
	}
identified:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("application identify did not complete")
		case e := <-events.Out():
			if e.(event.EvtPeerIdentificationCompleted).Peer == app.ID() {
				break identified
			}
		}
	}
	stream, err := private.NewStream(network.WithAllowLimitedConn(ctx, "privacy regression"), app.ID(), "/entmoot/privacy-regression/1")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	payload, err := io.ReadAll(stream)
	if err != nil || string(payload) != "private circuit" || !stream.Conn().Stat().Limited {
		t.Fatalf("real circuit payload=%q err=%v", payload, err)
	}
	assertPrivate := func() {
		t.Helper()
		addresses := private.Peerstore().Addrs(app.ID())
		if len(addresses) == 0 {
			t.Fatal("identify erased the usable controlled circuit hint")
		}
		for _, address := range addresses {
			if !isCircuitAddress(address) {
				t.Fatalf("raw peerstore leaked %s", address)
			}
		}
	}
	assertPrivate()
	certified, ok := peerstore.GetCertifiedAddrBook(private.Peerstore())
	if !ok {
		t.Fatal("configured host has no certified address book")
	}
	key := app.Peerstore().PrivKey(app.ID())
	safe := peer.NewPeerRecord()
	safe.PeerID = app.ID()
	safe.Addrs = []ma.Multiaddr{circuit}
	envelope, err := record.Seal(safe, key)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := certified.ConsumePeerRecord(envelope, time.Minute); err != nil || !accepted {
		t.Fatalf("safe signed record: accepted=%v err=%v", accepted, err)
	}
	unsafe := *safe
	unsafe.Seq++
	unsafe.Addrs = append([]ma.Multiaddr{circuit}, app.Addrs()...)
	envelope, err = record.Seal(&unsafe, key)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := certified.ConsumePeerRecord(envelope, time.Minute); err != nil || accepted {
		t.Fatalf("mixed signed record: accepted=%v err=%v", accepted, err)
	}
	retained, err := certified.GetPeerRecord(app.ID()).Record()
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range retained.(*peer.PeerRecord).Addrs {
		if !isCircuitAddress(address) {
			t.Fatalf("certified record leaked %s", address)
		}
	}
	private.Peerstore().AddAddrs(app.ID(), app.Addrs(), peerstore.PermanentAddrTTL)
	private.Peerstore().SetAddrs(app.ID(), app.Addrs(), peerstore.PermanentAddrTTL)
	assertPrivate()
	addresses := private.Peerstore().AddrStream(ctx, app.ID())
	select {
	case address := <-addresses:
		if address == nil || !isCircuitAddress(address) {
			t.Fatalf("address stream leaked %v", address)
		}
	case <-ctx.Done():
		t.Fatal("safe circuit address was not observable")
	}
}
