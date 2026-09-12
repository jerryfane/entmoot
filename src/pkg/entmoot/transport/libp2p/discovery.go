package libp2ptransport

import (
	"context"
	"errors"
	"fmt"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
)

type ConnectivityMode string

const (
	DirectConnectivity    ConnectivityMode = "direct"
	RelayOnlyConnectivity ConnectivityMode = "relay_only"
)

type HostConfig struct {
	Mode             ConnectivityMode
	ListenAddrs      []string
	ControlledRelays []peer.AddrInfo
}

// NewConfiguredHost applies the selected address/privacy profile. Relay-only
// never enables mDNS, public discovery, hole punching, AutoNAT service, or a
// direct application listener.
func NewConfiguredHost(ctx context.Context, identity *keystore.Identity, cfg HostConfig) (host.Host, Binding, error) {
	manager, err := connmgr.NewConnManager(48, 64)
	if err != nil {
		return nil, Binding{}, err
	}
	options := []libp2p.Option{libp2p.ConnectionManager(manager)}
	switch cfg.Mode {
	case "", DirectConnectivity:
		if len(cfg.ListenAddrs) > 0 {
			options = append(options, libp2p.ListenAddrStrings(cfg.ListenAddrs...))
		}
	case RelayOnlyConnectivity:
		if len(cfg.ControlledRelays) == 0 {
			return nil, Binding{}, errors.New("libp2p: relay-only mode requires a controlled relay")
		}
		gater := newRelayOnlyGater(cfg.ControlledRelays)
		options = append(options,
			libp2p.NoListenAddrs,
			libp2p.DisableIdentifyAddressDiscovery(),
			libp2p.ForceReachabilityPrivate(),
			libp2p.EnableRelay(),
			libp2p.EnableAutoRelayWithStaticRelays(cfg.ControlledRelays),
			libp2p.ConnectionGater(gater),
			libp2p.AddrsFactory(func(addresses []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				return filterControlledCircuitAddresses(addresses, gater.relays)
			}),
		)
	default:
		return nil, Binding{}, fmt.Errorf("libp2p: unsupported connectivity mode %q", cfg.Mode)
	}
	return NewHost(ctx, identity, options...)
}

// InstallVerifiedPeer accepts invite/static/identify hints only after the
// roster key binds both full-width identifiers. Stale hints expire in 30 min.
func InstallVerifiedPeer(h host.Host, r *roster.RosterLog, member entmoot.NodeInfo, peerID peer.ID, addresses []multiaddr.Multiaddr, ttl time.Duration, mode ConnectivityMode, controlledRelays []peer.AddrInfo) error {
	if h == nil || r == nil || member.MemberID == nil || !r.IsMemberID(*member.MemberID) {
		return errors.New("libp2p: address hint is not for a current member")
	}
	if err := VerifyBinding(member.EntmootPubKey, *member.MemberID, peerID); err != nil {
		return err
	}
	if ttl <= 0 || ttl > 30*time.Minute {
		ttl = 30 * time.Minute
	}
	if mode == RelayOnlyConnectivity {
		allowed := make(map[peer.ID]struct{}, len(controlledRelays))
		for _, relay := range controlledRelays {
			allowed[relay.ID] = struct{}{}
		}
		filtered := addresses[:0]
		for _, address := range addresses {
			if peerIDIsControlledRelay(peerID, allowed) || circuitUsesControlledRelay(address, allowed) {
				filtered = append(filtered, address)
			}
		}
		addresses = filtered
	}
	if len(addresses) == 0 {
		return errors.New("libp2p: no address survives the connectivity profile")
	}
	h.Peerstore().AddAddrs(peerID, addresses, ttl)
	return nil
}

// VisiblePeerAddresses is the only diagnostics-facing peerstore projection.
func VisiblePeerAddresses(h host.Host, peerID peer.ID, mode ConnectivityMode, controlledRelays []peer.AddrInfo) []multiaddr.Multiaddr {
	if h == nil {
		return nil
	}
	addresses := h.Peerstore().Addrs(peerID)
	if mode != RelayOnlyConnectivity {
		return addresses
	}
	allowed := make(map[peer.ID]struct{}, len(controlledRelays))
	for _, relay := range controlledRelays {
		allowed[relay.ID] = struct{}{}
	}
	if peerIDIsControlledRelay(peerID, allowed) {
		return addresses
	}
	return filterControlledCircuitAddresses(addresses, allowed)
}

// StartMemberMDNS enables LAN discovery only after an explicit direct-profile
// call. Discovered addresses are still accepted only for current roster peers.
func StartMemberMDNS(h host.Host, r *roster.RosterLog, groupID entmoot.GroupID) (mdns.Service, error) {
	if h == nil || r == nil {
		return nil, errors.New("libp2p: host and roster are required for mDNS")
	}
	topicSuffix := GroupTopic(groupID)[len("/entmoot/group/2/"):]
	service := mdns.NewMdnsService(h, "_em-"+topicSuffix[:8]+"._udp", &memberMDNSNotifee{host: h, roster: r})
	if err := service.Start(); err != nil {
		return nil, err
	}
	return service, nil
}

type memberMDNSNotifee struct {
	host   host.Host
	roster *roster.RosterLog
}

func (n *memberMDNSNotifee) HandlePeerFound(info peer.AddrInfo) {
	if !peerIsMember(n.host, n.roster, info.ID) {
		return
	}
	n.host.Peerstore().AddAddrs(info.ID, info.Addrs, 30*time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = n.host.Connect(ctx, info)
}

type relayOnlyGater struct {
	relays map[peer.ID]struct{}
}

func newRelayOnlyGater(relays []peer.AddrInfo) *relayOnlyGater {
	allowed := make(map[peer.ID]struct{}, len(relays))
	for _, relay := range relays {
		allowed[relay.ID] = struct{}{}
	}
	return &relayOnlyGater{relays: allowed}
}

func (*relayOnlyGater) InterceptPeerDial(peer.ID) bool { return true }

func (g *relayOnlyGater) InterceptAddrDial(peerID peer.ID, address multiaddr.Multiaddr) bool {
	return peerIDIsControlledRelay(peerID, g.relays) || circuitUsesControlledRelay(address, g.relays)
}

func (g *relayOnlyGater) InterceptAccept(addresses network.ConnMultiaddrs) bool {
	return connectionUsesControlledRelay(addresses, g.relays)
}

func (g *relayOnlyGater) InterceptSecured(_ network.Direction, peerID peer.ID, addresses network.ConnMultiaddrs) bool {
	return peerIDIsControlledRelay(peerID, g.relays) || connectionUsesControlledRelay(addresses, g.relays)
}

func (g *relayOnlyGater) InterceptUpgraded(connection network.Conn) (bool, control.DisconnectReason) {
	allowed := peerIDIsControlledRelay(connection.RemotePeer(), g.relays) ||
		connectionUsesControlledRelay(connection, g.relays)
	return allowed, 0
}

func peerIDIsControlledRelay(peerID peer.ID, relays map[peer.ID]struct{}) bool {
	_, ok := relays[peerID]
	return ok
}

func isCircuitAddress(address multiaddr.Multiaddr) bool {
	found := false
	if address != nil {
		multiaddr.ForEach(address, func(component multiaddr.Component) bool {
			found = component.Protocol().Code == multiaddr.P_CIRCUIT
			return !found
		})
	}
	return found
}

func circuitUsesControlledRelay(address multiaddr.Multiaddr, relays map[peer.ID]struct{}) bool {
	var precedingPeer peer.ID
	allowed := false
	if address == nil {
		return false
	}
	multiaddr.ForEach(address, func(component multiaddr.Component) bool {
		switch component.Protocol().Code {
		case multiaddr.P_P2P:
			decoded, err := peer.Decode(component.Value())
			if err != nil {
				precedingPeer = ""
			} else {
				precedingPeer = decoded
			}
		case multiaddr.P_CIRCUIT:
			allowed = peerIDIsControlledRelay(precedingPeer, relays)
			return false
		}
		return true
	})
	return allowed
}

func connectionUsesControlledRelay(addresses network.ConnMultiaddrs, relays map[peer.ID]struct{}) bool {
	return addresses != nil &&
		(circuitUsesControlledRelay(addresses.LocalMultiaddr(), relays) ||
			circuitUsesControlledRelay(addresses.RemoteMultiaddr(), relays))
}

func filterControlledCircuitAddresses(addresses []multiaddr.Multiaddr, relays map[peer.ID]struct{}) []multiaddr.Multiaddr {
	filtered := make([]multiaddr.Multiaddr, 0, len(addresses))
	for _, address := range addresses {
		if circuitUsesControlledRelay(address, relays) {
			filtered = append(filtered, address)
		}
	}
	return filtered
}

// RelayReservationManager renews controlled-relay reservations at half-life.
type RelayReservationManager struct {
	Host    host.Host
	Relays  []peer.AddrInfo
	OnError func(peer.ID, error)
}

func (m *RelayReservationManager) Run(ctx context.Context) error {
	if m == nil || m.Host == nil || len(m.Relays) == 0 {
		return errors.New("libp2p: relay reservation manager is not configured")
	}
	for _, relay := range m.Relays {
		relay := relay
		go m.renew(ctx, relay)
	}
	return nil
}

func (m *RelayReservationManager) renew(ctx context.Context, relay peer.AddrInfo) {
	backoff := time.Second
	for ctx.Err() == nil {
		reservation, err := relayclient.Reserve(ctx, m.Host, relay)
		if err != nil {
			if m.OnError != nil {
				m.OnError(relay.ID, err)
			}
			if !sleepContext(ctx, backoff) {
				return
			}
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			continue
		}
		backoff = time.Second
		delay := time.Until(reservation.Expiration) / 2
		if delay <= 0 {
			delay = time.Second
		}
		if !sleepContext(ctx, delay) {
			return
		}
	}
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
