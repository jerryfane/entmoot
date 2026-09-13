package libp2ptransport

import (
	"context"
	"io"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
)

type fixedConnMultiaddrs struct {
	local  multiaddr.Multiaddr
	remote multiaddr.Multiaddr
}

func (a fixedConnMultiaddrs) LocalMultiaddr() multiaddr.Multiaddr  { return a.local }
func (a fixedConnMultiaddrs) RemoteMultiaddr() multiaddr.Multiaddr { return a.remote }

func TestDirectAndRelayOnlyProfilesAreIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	relayIdentity := mustIdentity(t)
	relayHost, _, err := NewHost(ctx, relayIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer relayHost.Close()
	applicationIdentity := mustIdentity(t)
	applicationHost, _, err := NewHost(ctx, applicationIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer applicationHost.Close()
	directIdentity := mustIdentity(t)
	directHost, _, err := NewConfiguredHost(ctx, directIdentity, HostConfig{Mode: DirectConnectivity, ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatal(err)
	}
	defer directHost.Close()
	if err := directHost.Connect(ctx, peer.AddrInfo{ID: applicationHost.ID(), Addrs: applicationHost.Addrs()}); err != nil {
		t.Fatalf("direct profile could not dial application peer: %v", err)
	}

	relayInfo := peer.AddrInfo{ID: relayHost.ID(), Addrs: relayHost.Addrs()}
	privateIdentity := mustIdentity(t)
	privateHost, _, err := NewConfiguredHost(ctx, privateIdentity, HostConfig{Mode: RelayOnlyConnectivity, ControlledRelays: []peer.AddrInfo{relayInfo}})
	if err != nil {
		t.Fatal(err)
	}
	defer privateHost.Close()
	if len(privateHost.Addrs()) != 0 {
		t.Fatalf("relay-only host advertised direct addresses: %v", privateHost.Addrs())
	}
	if err := privateHost.Connect(ctx, peer.AddrInfo{ID: applicationHost.ID(), Addrs: applicationHost.Addrs()}); err == nil {
		t.Fatal("relay-only host silently dialed an application peer directly")
	}
	if err := privateHost.Connect(ctx, relayInfo); err != nil {
		t.Fatalf("relay-only host could not dial its approved relay: %v", err)
	}
}

func TestRelayOnlyPeersConnectThroughControlledRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	firstIdentity := mustIdentity(t)
	firstBinding, err := BindingFromPublicKey(firstIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	secondIdentity := mustIdentity(t)
	secondBinding, err := BindingFromPublicKey(secondIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	relayHost, _, err := NewRelayServer(ctx, mustIdentity(t), RelayServerConfig{
		ListenAddrs:           []string{"/ip4/127.0.0.1/tcp/0"},
		AllowedPeers:          []peer.ID{firstBinding.PeerID, secondBinding.PeerID},
		ReservationTTL:        time.Hour,
		CircuitDuration:       time.Minute,
		CircuitBytes:          1 << 20,
		MaxReservations:       8,
		MaxCircuitsPerPeer:    2,
		MaxReservationsPerIP:  8,
		MaxReservationsPerASN: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relayHost.Close()
	relayInfo := peer.AddrInfo{ID: relayHost.ID(), Addrs: relayHost.Addrs()}

	unlisted, _, err := NewConfiguredHost(ctx, mustIdentity(t), HostConfig{Mode: RelayOnlyConnectivity, ControlledRelays: []peer.AddrInfo{relayInfo}})
	if err != nil {
		t.Fatal(err)
	}
	defer unlisted.Close()
	if err := unlisted.Connect(ctx, relayInfo); err != nil {
		t.Fatalf("unlisted peer could not reach relay admission endpoint: %v", err)
	}
	if _, err := relayclient.Reserve(ctx, unlisted, relayInfo); err == nil {
		t.Fatal("relay accepted a reservation from an unlisted peer")
	}

	first, _, err := NewConfiguredHost(ctx, firstIdentity, HostConfig{Mode: RelayOnlyConnectivity, ControlledRelays: []peer.AddrInfo{relayInfo}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, _, err := NewConfiguredHost(ctx, secondIdentity, HostConfig{Mode: RelayOnlyConnectivity, ControlledRelays: []peer.AddrInfo{relayInfo}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := relayclient.Reserve(ctx, second, relayInfo); err != nil {
		t.Fatal(err)
	}
	relayAddrs, err := peer.AddrInfoToP2pAddrs(&relayInfo)
	if err != nil || len(relayAddrs) == 0 {
		t.Fatalf("relay address: %v", err)
	}
	circuitAddress := multiaddr.Join(relayAddrs[0], multiaddr.StringCast("/p2p-circuit"))
	const echoProtocol = protocol.ID("/entmoot/relay-check/1")
	second.SetStreamHandler(echoProtocol, func(stream network.Stream) {
		defer stream.Close()
		_, _ = stream.Write([]byte("relayed"))
	})
	if err := first.Connect(ctx, peer.AddrInfo{ID: second.ID(), Addrs: []multiaddr.Multiaddr{circuitAddress}}); err != nil {
		t.Fatalf("connect through relay: %v", err)
	}
	if connections := first.Network().ConnsToPeer(second.ID()); len(connections) == 0 {
		t.Fatal("relay connect returned without an application-peer connection")
	}
	stream, err := first.NewStream(network.WithAllowLimitedConn(ctx, "relay-only test"), second.ID(), echoProtocol)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(stream)
	_ = stream.Close()
	if err != nil || string(payload) != "relayed" {
		t.Fatalf("relayed stream payload=%q err=%v", payload, err)
	}
	if !isCircuitAddress(stream.Conn().RemoteMultiaddr()) {
		t.Fatalf("application connection was not relayed: %s", stream.Conn().RemoteMultiaddr())
	}
	for _, address := range VisiblePeerAddresses(first, second.ID(), RelayOnlyConnectivity, []peer.AddrInfo{relayInfo}) {
		if !isCircuitAddress(address) {
			t.Fatalf("relay-only peerstore exposed direct address %s", address)
		}
	}
}

func TestRelayCircuitLimitAppliesPerPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	identities := make([]*keystore.Identity, 4)
	allowed := make([]peer.ID, 4)
	for i := range identities {
		identities[i] = mustIdentity(t)
		binding, err := BindingFromPublicKey(identities[i].PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		allowed[i] = binding.PeerID
	}
	relayHost, _, err := NewRelayServer(ctx, mustIdentity(t), RelayServerConfig{
		ListenAddrs:           []string{"/ip4/127.0.0.1/tcp/0"},
		AllowedPeers:          allowed,
		ReservationTTL:        time.Hour,
		CircuitDuration:       time.Minute,
		CircuitBytes:          1 << 20,
		MaxReservations:       8,
		MaxCircuitsPerPeer:    1,
		MaxReservationsPerIP:  8,
		MaxReservationsPerASN: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relayHost.Close()
	relayInfo := peer.AddrInfo{ID: relayHost.ID(), Addrs: relayHost.Addrs()}

	clients := make([]host.Host, len(identities))
	for i, identity := range identities {
		clients[i], _, err = NewConfiguredHost(ctx, identity, HostConfig{
			Mode:             RelayOnlyConnectivity,
			ControlledRelays: []peer.AddrInfo{relayInfo},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer clients[i].Close()
		if _, err := relayclient.Reserve(ctx, clients[i], relayInfo); err != nil {
			t.Fatal(err)
		}
	}
	relayAddrs, err := peer.AddrInfoToP2pAddrs(&relayInfo)
	if err != nil || len(relayAddrs) == 0 {
		t.Fatalf("relay address: %v", err)
	}
	circuitAddress := multiaddr.Join(relayAddrs[0], multiaddr.StringCast("/p2p-circuit"))
	connect := func(source, destination int) error {
		return clients[source].Connect(ctx, peer.AddrInfo{
			ID:    clients[destination].ID(),
			Addrs: []multiaddr.Multiaddr{circuitAddress},
		})
	}
	if err := connect(0, 1); err != nil {
		t.Fatalf("first peer pair: %v", err)
	}
	if err := connect(2, 3); err != nil {
		t.Fatalf("independent peer pair was rejected by a relay-wide circuit limit: %v", err)
	}
	if err := connect(0, 2); err == nil {
		t.Fatal("peer opened a second circuit despite its per-peer limit")
	}
}

func TestVerifiedAddressHintsRespectIdentityAndPrivacyProfile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	localIdentity := mustIdentity(t)
	localHost, _, err := NewHost(ctx, localIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer localHost.Close()
	memberIdentity := mustIdentity(t)
	memberBinding, err := BindingFromPublicKey(memberIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	member := mustNodeInfo(t, memberIdentity.PublicKey)
	var groupID entmoot.GroupID
	groupID[0] = 1
	rosterLog := roster.New(groupID)
	if err := rosterLog.Genesis(memberIdentity, member, 1_000); err != nil {
		t.Fatal(err)
	}
	direct := multiaddr.StringCast("/ip4/203.0.113.10/tcp/4001")
	if err := InstallVerifiedPeer(localHost, rosterLog, member, memberBinding.PeerID, []multiaddr.Multiaddr{direct}, time.Hour, DirectConnectivity, nil); err != nil {
		t.Fatal(err)
	}
	if got := VisiblePeerAddresses(localHost, memberBinding.PeerID, DirectConnectivity, nil); len(got) != 1 {
		t.Fatalf("direct addresses = %v", got)
	}
	attacker := mustIdentity(t)
	attackerBinding, _ := BindingFromPublicKey(attacker.PublicKey)
	if err := InstallVerifiedPeer(localHost, rosterLog, member, attackerBinding.PeerID, []multiaddr.Multiaddr{direct}, time.Hour, DirectConnectivity, nil); err == nil {
		t.Fatal("address hint with invalid PeerID/key binding accepted")
	}
	relay := mustIdentity(t)
	relayBinding, _ := BindingFromPublicKey(relay.PublicKey)
	relayInfo := peer.AddrInfo{ID: relayBinding.PeerID}
	circuit := multiaddr.StringCast("/ip4/198.51.100.2/tcp/4001/p2p/" + relayBinding.PeerID.String() + "/p2p-circuit")
	unapprovedRelay := mustIdentity(t)
	unapprovedBinding, _ := BindingFromPublicKey(unapprovedRelay.PublicKey)
	unapprovedCircuit := multiaddr.StringCast("/ip4/198.51.100.3/tcp/4001/p2p/" + unapprovedBinding.PeerID.String() + "/p2p-circuit")
	gater := newRelayOnlyGater([]peer.AddrInfo{relayInfo})
	if !gater.InterceptAddrDial(memberBinding.PeerID, circuit) {
		t.Fatal("controlled relay circuit was rejected")
	}
	if gater.InterceptAddrDial(memberBinding.PeerID, unapprovedCircuit) {
		t.Fatal("unapproved relay circuit was accepted for dial")
	}
	unapprovedConnection := fixedConnMultiaddrs{remote: unapprovedCircuit}
	if gater.InterceptAccept(unapprovedConnection) {
		t.Fatal("unapproved relay circuit was accepted inbound")
	}
	if gater.InterceptSecured(network.DirInbound, memberBinding.PeerID, unapprovedConnection) {
		t.Fatal("unapproved relay circuit was accepted after authentication")
	}
	localHost.Peerstore().ClearAddrs(memberBinding.PeerID)
	if err := InstallVerifiedPeer(localHost, rosterLog, member, memberBinding.PeerID, []multiaddr.Multiaddr{direct, unapprovedCircuit, circuit}, time.Hour, RelayOnlyConnectivity, []peer.AddrInfo{relayInfo}); err != nil {
		t.Fatal(err)
	}
	visible := VisiblePeerAddresses(localHost, memberBinding.PeerID, RelayOnlyConnectivity, []peer.AddrInfo{relayInfo})
	if len(visible) != 1 || !visible[0].Equal(circuit) {
		t.Fatalf("relay-only diagnostics exposed unapproved or direct addresses: %v", visible)
	}
	localHost.Peerstore().ClearAddrs(memberBinding.PeerID)
	if err := InstallVerifiedPeer(localHost, rosterLog, member, memberBinding.PeerID, []multiaddr.Multiaddr{unapprovedCircuit}, time.Hour, RelayOnlyConnectivity, []peer.AddrInfo{relayInfo}); err == nil {
		t.Fatal("unapproved relay was the only surviving address")
	}
}

func TestRelayOnlyRequiresControlledRelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, _, err := NewConfiguredHost(ctx, mustIdentity(t), HostConfig{Mode: RelayOnlyConnectivity}); err == nil {
		t.Fatal("relay-only host started without controlled relay")
	}
}
