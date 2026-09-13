package libp2ptransport

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot/keystore"
)

// RelayServerConfig defines one bounded, allowlisted Circuit Relay v2 service.
// The relay identity must be distinct from every Entmoot application identity.
type RelayServerConfig struct {
	ListenAddrs           []string
	AnnounceAddrs         []string
	AllowedPeers          []peer.ID
	ReservationTTL        time.Duration
	CircuitDuration       time.Duration
	CircuitBytes          int64
	MaxReservations       int
	MaxCircuitsPerPeer    int // relayv2 enforces this independently for each source and destination peer.
	MaxReservationsPerIP  int
	MaxReservationsPerASN int
}

// NewRelayServer starts an allowlisted Circuit Relay v2 service on a dedicated
// libp2p host. Empty allowlists and unbounded resource values are rejected.
func NewRelayServer(ctx context.Context, identity *keystore.Identity, cfg RelayServerConfig) (host.Host, Binding, error) {
	if len(cfg.ListenAddrs) == 0 {
		return nil, Binding{}, errors.New("libp2p relay: at least one listen address is required")
	}
	if len(cfg.AllowedPeers) == 0 {
		return nil, Binding{}, errors.New("libp2p relay: at least one allowed peer is required")
	}
	if cfg.ReservationTTL <= 0 || cfg.CircuitDuration <= 0 || cfg.CircuitBytes <= 0 ||
		cfg.MaxReservations <= 0 || cfg.MaxCircuitsPerPeer <= 0 ||
		cfg.MaxReservationsPerIP <= 0 || cfg.MaxReservationsPerASN <= 0 {
		return nil, Binding{}, errors.New("libp2p relay: every resource limit must be positive")
	}

	allowed := make(map[peer.ID]struct{}, len(cfg.AllowedPeers))
	for _, id := range cfg.AllowedPeers {
		if id == "" {
			return nil, Binding{}, errors.New("libp2p relay: allowed peer id is empty")
		}
		allowed[id] = struct{}{}
	}

	listenAddrs, err := parseRelayServerAddresses("listen", cfg.ListenAddrs)
	if err != nil {
		return nil, Binding{}, err
	}
	announceAddrs, err := parseRelayServerAddresses("announce", cfg.AnnounceAddrs)
	if err != nil {
		return nil, Binding{}, err
	}
	options := []libp2p.Option{
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.ForceReachabilityPublic(),
		libp2p.EnableRelayService(
			relayv2.WithResources(relayv2.Resources{
				Limit: &relayv2.RelayLimit{
					Duration: cfg.CircuitDuration,
					Data:     cfg.CircuitBytes,
				},
				ReservationTTL:         cfg.ReservationTTL,
				MaxReservations:        cfg.MaxReservations,
				MaxCircuits:            cfg.MaxCircuitsPerPeer,
				BufferSize:             relayv2.DefaultResources().BufferSize,
				MaxReservationsPerPeer: 1,
				MaxReservationsPerIP:   cfg.MaxReservationsPerIP,
				MaxReservationsPerASN:  cfg.MaxReservationsPerASN,
			}),
			relayv2.WithACL(relayPeerAllowlist(allowed)),
		),
	}
	if len(announceAddrs) > 0 {
		options = append(options, libp2p.AddrsFactory(func([]multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return slices.Clone(announceAddrs)
		}))
	}
	return NewHost(ctx, identity, options...)
}

type relayPeerAllowlist map[peer.ID]struct{}

func (a relayPeerAllowlist) AllowReserve(id peer.ID, _ multiaddr.Multiaddr) bool {
	_, ok := a[id]
	return ok
}

func (a relayPeerAllowlist) AllowConnect(source peer.ID, _ multiaddr.Multiaddr, destination peer.ID) bool {
	_, sourceAllowed := a[source]
	_, destinationAllowed := a[destination]
	return sourceAllowed && destinationAllowed
}

func parseRelayServerAddresses(kind string, values []string) ([]multiaddr.Multiaddr, error) {
	addresses := make([]multiaddr.Multiaddr, 0, len(values))
	for _, raw := range values {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			return nil, fmt.Errorf("libp2p relay: %s address %q: %w", kind, raw, err)
		}
		hasRoutingComponent := false
		multiaddr.ForEach(address, func(component multiaddr.Component) bool {
			switch component.Protocol().Code {
			case multiaddr.P_P2P, multiaddr.P_CIRCUIT:
				hasRoutingComponent = true
				return false
			default:
				return true
			}
		})
		if hasRoutingComponent {
			return nil, fmt.Errorf("libp2p relay: %s address %q must not contain /p2p or /p2p-circuit", kind, raw)
		}
		addresses = append(addresses, address)
	}
	return addresses, nil
}
