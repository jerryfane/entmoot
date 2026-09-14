package libp2ptransport

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
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

const (
	maxHostConnections = 64
	maxPeerConnections = 8
	maxPeerStreams     = 64
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
	if cfg.Mode != "" && cfg.Mode != DirectConnectivity && cfg.Mode != RelayOnlyConnectivity {
		return nil, Binding{}, fmt.Errorf("libp2p: unsupported connectivity mode %q", cfg.Mode)
	}
	if cfg.Mode == RelayOnlyConnectivity && len(cfg.ControlledRelays) == 0 {
		return nil, Binding{}, errors.New("libp2p: relay-only mode requires a controlled relay")
	}
	manager, err := connmgr.NewConnManager(maxHostConnections*3/4, maxHostConnections)
	if err != nil {
		return nil, Binding{}, err
	}
	limits := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Conns: maxHostConnections, ConnsInbound: maxHostConnections, ConnsOutbound: maxHostConnections,
		},
		PeerDefault: rcmgr.ResourceLimits{
			Conns: maxPeerConnections, ConnsInbound: maxPeerConnections, ConnsOutbound: maxPeerConnections,
			Streams: maxPeerStreams, StreamsInbound: maxPeerStreams, StreamsOutbound: maxPeerStreams,
		},
	}
	resources, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.Build(rcmgr.DefaultLimits.AutoScale())))
	if err != nil {
		_ = manager.Close()
		return nil, Binding{}, err
	}
	options := []libp2p.Option{libp2p.ConnectionManager(manager), libp2p.ResourceManager(resources)}
	var privateAddresses *relayOnlyPeerstore
	var relayAddrs *advertisedRelayAddrs
	switch cfg.Mode {
	case "", DirectConnectivity:
		if len(cfg.ListenAddrs) > 0 {
			options = append(options, libp2p.ListenAddrStrings(cfg.ListenAddrs...))
		}
		// DCUtR upgrades a relayed connection to a direct one. Both ends must
		// speak it: a publicly reachable peer is the punch target for a NATed
		// peer, so the protocol is enabled even without local relays. Relay
		// rendezvous itself stays opt-in through ControlledRelays.
		options = append(options, libp2p.EnableRelay(), libp2p.EnableHolePunching())
		if len(cfg.ControlledRelays) > 0 {
			// libp2p only folds relay addresses into Addrs() once AutoNAT
			// reports no reachable address. Members must be able to publish a
			// circuit address as soon as the reservation exists, so mirror the
			// reservation manager's addresses unconditionally.
			relayAddrs = &advertisedRelayAddrs{}
			options = append(options, libp2p.AddrsFactory(func(addresses []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				return multiaddr.Unique(append(slices.Clone(addresses), relayAddrs.snapshot()...))
			}))
		}
	case RelayOnlyConnectivity:
		gater := newRelayOnlyGater(cfg.ControlledRelays)
		privateAddresses, err = newRelayOnlyPeerstore(gater.relays)
		if err != nil {
			_ = manager.Close()
			_ = resources.Close()
			return nil, Binding{}, err
		}
		options = append(options,
			libp2p.NoListenAddrs,
			libp2p.DisableIdentifyAddressDiscovery(),
			libp2p.ForceReachabilityPrivate(),
			libp2p.EnableRelay(),
			libp2p.ConnectionGater(gater),
			libp2p.Peerstore(privateAddresses),
			libp2p.AddrsFactory(func(addresses []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				return filterControlledCircuitAddresses(addresses, gater.relays)
			}),
		)
	}
	hostCtx := ctx
	var privateHost *relayOnlyHost
	if privateAddresses != nil {
		privateHost = &relayOnlyHost{addresses: privateAddresses}
		hostCtx, privateHost.cancel = context.WithCancel(ctx)
	}
	h, binding, err := NewHost(hostCtx, identity, options...)
	if err != nil {
		if privateHost != nil {
			privateHost.cancel()
		}
		_ = manager.Close()
		_ = resources.Close()
		if privateAddresses != nil {
			_ = privateAddresses.Close()
		}
		return nil, Binding{}, err
	}
	if privateHost != nil {
		privateHost.Host = h
		h = privateHost
	}
	if relayAddrs != nil {
		if err := relayAddrs.watch(hostCtx, h.EventBus()); err != nil {
			_ = h.Close()
			return nil, Binding{}, err
		}
	}
	if len(cfg.ControlledRelays) > 0 {
		reservations := &RelayReservationManager{Host: h, Relays: cfg.ControlledRelays}
		if err := reservations.Run(hostCtx); err != nil {
			_ = h.Close()
			return nil, Binding{}, err
		}
	}
	return h, binding, err
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
	if ttl <= 0 || ttl > maxPeerAddressAge {
		ttl = maxPeerAddressAge
	}
	addresses = profileAddresses(peerID, addresses, mode, controlledRelays)
	if len(addresses) == 0 {
		return errors.New("libp2p: no address survives the connectivity profile")
	}
	h.Peerstore().AddAddrs(peerID, addresses, ttl)
	return nil
}

// profileAddresses returns the addresses a connectivity profile permits for a
// peer. It never mutates its input: callers hold address sets that must stay
// intact when nothing survives.
func profileAddresses(peerID peer.ID, addresses []multiaddr.Multiaddr, mode ConnectivityMode, controlledRelays []peer.AddrInfo) []multiaddr.Multiaddr {
	if mode != RelayOnlyConnectivity {
		return slices.Clone(addresses)
	}
	allowed := make(map[peer.ID]struct{}, len(controlledRelays))
	for _, relay := range controlledRelays {
		allowed[relay.ID] = struct{}{}
	}
	filtered := make([]multiaddr.Multiaddr, 0, len(addresses))
	for _, address := range addresses {
		if peerIDIsControlledRelay(peerID, allowed) || circuitUsesControlledRelay(address, allowed) {
			filtered = append(filtered, address)
		}
	}
	return filtered
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
	n.host.Peerstore().AddAddrs(info.ID, info.Addrs, maxPeerAddressAge)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = n.host.Connect(ctx, info)
}

// advertisedRelayAddrs mirrors the reservation manager's circuit addresses so a
// direct-profile host advertises them without waiting for AutoNAT to report
// private reachability.
type advertisedRelayAddrs struct {
	mu    sync.RWMutex
	addrs []multiaddr.Multiaddr
}

func (a *advertisedRelayAddrs) snapshot() []multiaddr.Multiaddr {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return slices.Clone(a.addrs)
}

func (a *advertisedRelayAddrs) watch(ctx context.Context, bus event.Bus) error {
	subscription, err := bus.Subscribe(new(event.EvtAutoRelayAddrsUpdated))
	if err != nil {
		return err
	}
	go func() {
		defer subscription.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-subscription.Out():
				if !ok {
					return
				}
				update, isUpdate := raw.(event.EvtAutoRelayAddrsUpdated)
				if !isUpdate {
					continue
				}
				a.mu.Lock()
				a.addrs = slices.Clone(update.RelayAddrs)
				a.mu.Unlock()
			}
		}
	}()
	return nil
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

type relayReservationUpdate struct {
	peer      peer.ID
	expires   time.Time
	addresses []multiaddr.Multiaddr
	remove    bool
}

func (m *RelayReservationManager) Run(ctx context.Context) error {
	if m == nil || m.Host == nil || len(m.Relays) == 0 {
		return errors.New("libp2p: relay reservation manager is not configured")
	}
	emitter, err := m.Host.EventBus().Emitter(new(event.EvtAutoRelayAddrsUpdated), eventbus.Stateful)
	if err != nil {
		return err
	}
	relays := make(map[peer.ID]peer.AddrInfo, len(m.Relays))
	order := make([]peer.ID, 0, len(m.Relays))
	wake := make(map[peer.ID]chan struct{}, len(m.Relays))
	for _, relay := range m.Relays {
		current, exists := relays[relay.ID]
		if !exists {
			order = append(order, relay.ID)
			wake[relay.ID] = make(chan struct{}, 1)
		}
		current.ID = relay.ID
		current.Addrs = append(current.Addrs, relay.Addrs...)
		relays[relay.ID] = current
	}
	updates := make(chan relayReservationUpdate, len(order))
	notifiee := &network.NotifyBundle{DisconnectedF: func(_ network.Network, connection network.Conn) {
		if signal, ok := wake[connection.RemotePeer()]; ok {
			select {
			case signal <- struct{}{}:
			default:
			}
		}
	}}
	m.Host.Network().Notify(notifiee)
	go func() {
		defer emitter.Close()
		defer m.Host.Network().StopNotify(notifiee)
		defer func() {
			for _, id := range order {
				m.Host.ConnManager().Unprotect(id, "entmoot-relay")
			}
		}()
		active := make(map[peer.ID]relayReservationUpdate, len(order))
		for {
			var update relayReservationUpdate
			select {
			case <-ctx.Done():
				return
			case update = <-updates:
			}
			previous, exists := active[update.peer]
			if update.remove {
				// An old expiry timer must not retire a refreshed reservation.
				if !exists || (!update.expires.IsZero() && !previous.expires.Equal(update.expires)) {
					continue
				}
				delete(active, update.peer)
				m.Host.ConnManager().Unprotect(update.peer, "entmoot-relay")
			} else {
				if m.Host.Network().Connectedness(update.peer) != network.Connected {
					continue
				}
				active[update.peer] = update
				m.Host.ConnManager().Protect(update.peer, "entmoot-relay")
				if exists && slices.EqualFunc(previous.addresses, update.addresses, multiaddr.Multiaddr.Equal) {
					continue
				}
			}
			addresses := make([]multiaddr.Multiaddr, 0, len(active))
			for _, id := range order {
				addresses = append(addresses, active[id].addresses...)
			}
			_ = emitter.Emit(event.EvtAutoRelayAddrsUpdated{RelayAddrs: addresses})
		}
	}()
	for _, id := range order {
		relay := relays[id]
		m.Host.Peerstore().AddAddrs(id, relay.Addrs, peerstore.PermanentAddrTTL)
		go m.renew(ctx, relay, wake[id], updates)
	}
	return nil
}

func sendRelayUpdate(ctx context.Context, updates chan<- relayReservationUpdate, update relayReservationUpdate) bool {
	select {
	case <-ctx.Done():
		return false
	case updates <- update:
		return true
	}
}

func (m *RelayReservationManager) renew(ctx context.Context, relay peer.AddrInfo, wake <-chan struct{}, updates chan<- relayReservationUpdate) {
	backoff := time.Second
	var expiryTimer *time.Timer
	defer func() {
		if expiryTimer != nil {
			expiryTimer.Stop()
		}
	}()
	for ctx.Err() == nil {
		if m.Host.Network().Connectedness(relay.ID) != network.Connected {
			if !sendRelayUpdate(ctx, updates, relayReservationUpdate{peer: relay.ID, remove: true}) {
				return
			}
		}
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		reservation, err := relayclient.Reserve(attempt, m.Host, peer.AddrInfo{ID: relay.ID})
		cancel()
		delay := backoff
		if err != nil {
			if m.OnError != nil {
				m.OnError(relay.ID, err)
			}
			backoff = min(2*backoff, 30*time.Second)
		} else {
			backoff = time.Second
			update := relayReservationUpdate{
				peer: relay.ID, expires: reservation.Expiration,
				addresses: reservationCircuitAddresses(relay, reservation),
			}
			if !sendRelayUpdate(ctx, updates, update) {
				return
			}
			if expiryTimer != nil {
				expiryTimer.Stop()
			}
			expires := reservation.Expiration
			expiryTimer = time.AfterFunc(time.Until(expires), func() {
				sendRelayUpdate(ctx, updates, relayReservationUpdate{peer: relay.ID, expires: expires, remove: true})
			})
			delay = time.Until(expires) / 2
			if delay <= 0 {
				delay = time.Second
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func reservationCircuitAddresses(relay peer.AddrInfo, reservation *relayclient.Reservation) []multiaddr.Multiaddr {
	suffix := multiaddr.StringCast("/p2p/" + relay.ID.String() + "/p2p-circuit")
	addresses := make([]multiaddr.Multiaddr, 0, len(reservation.Addrs))
	for _, address := range reservation.Addrs {
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil || info.ID != relay.ID {
			continue
		}
		for _, base := range info.Addrs {
			addresses = append(addresses, base.Encapsulate(suffix))
		}
	}
	if len(addresses) == 0 {
		// Explicitly configured private relays need not advertise public addrs.
		for _, address := range relay.Addrs {
			addresses = append(addresses, address.Encapsulate(suffix))
		}
	}
	return multiaddr.Unique(addresses)
}
