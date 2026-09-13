package libp2ptransport

import (
	"context"
	"errors"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/record"
	pstoremem "github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	multiaddr "github.com/multiformats/go-multiaddr"
)

const maxPeerAddressAge = 30 * time.Minute

// relayOnlyPeerstore enforces privacy at ingestion, including identify and
// signed peer records. Filtering a diagnostic view leaves unsafe dial hints
// and address-stream notifications in the underlying store.
type relayOnlyPeerstore struct {
	peerstore.Peerstore
	certified peerstore.CertifiedAddrBook
	relays    map[peer.ID]struct{}
}

func newRelayOnlyPeerstore(relays map[peer.ID]struct{}) (*relayOnlyPeerstore, error) {
	memory, err := pstoremem.NewPeerstore()
	if err != nil {
		return nil, err
	}
	return &relayOnlyPeerstore{Peerstore: memory, certified: memory, relays: relays}, nil
}

func (p *relayOnlyPeerstore) allowed(id peer.ID, address multiaddr.Multiaddr) bool {
	_, relay := p.relays[id]
	return relay || circuitUsesControlledRelay(address, p.relays)
}

func (p *relayOnlyPeerstore) filter(id peer.ID, addresses []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	for i, address := range addresses {
		if p.allowed(id, address) {
			continue
		}
		filtered := make([]multiaddr.Multiaddr, i, len(addresses)-1)
		copy(filtered, addresses[:i])
		for _, remaining := range addresses[i+1:] {
			if p.allowed(id, remaining) {
				filtered = append(filtered, remaining)
			}
		}
		return filtered
	}
	return addresses
}

func (p *relayOnlyPeerstore) addressTTL(id peer.ID, ttl time.Duration) time.Duration {
	if _, relay := p.relays[id]; relay || p.Peerstore.PrivKey(id) != nil {
		return ttl
	}
	if ttl > maxPeerAddressAge {
		return maxPeerAddressAge
	}
	return ttl
}

func (p *relayOnlyPeerstore) AddAddr(id peer.ID, address multiaddr.Multiaddr, ttl time.Duration) {
	if p.allowed(id, address) {
		p.Peerstore.AddAddr(id, address, p.addressTTL(id, ttl))
	}
}

func (p *relayOnlyPeerstore) AddAddrs(id peer.ID, addresses []multiaddr.Multiaddr, ttl time.Duration) {
	p.Peerstore.AddAddrs(id, p.filter(id, addresses), p.addressTTL(id, ttl))
}

func (p *relayOnlyPeerstore) SetAddr(id peer.ID, address multiaddr.Multiaddr, ttl time.Duration) {
	if p.allowed(id, address) {
		p.Peerstore.SetAddr(id, address, p.addressTTL(id, ttl))
	}
}

func (p *relayOnlyPeerstore) SetAddrs(id peer.ID, addresses []multiaddr.Multiaddr, ttl time.Duration) {
	p.Peerstore.SetAddrs(id, p.filter(id, addresses), p.addressTTL(id, ttl))
}

func (p *relayOnlyPeerstore) UpdateAddrs(id peer.ID, oldTTL, newTTL time.Duration) {
	p.Peerstore.UpdateAddrs(id, oldTTL, p.addressTTL(id, newTTL))
}

func (p *relayOnlyPeerstore) ConsumePeerRecord(envelope *record.Envelope, ttl time.Duration) (bool, error) {
	value, err := envelope.Record()
	if err != nil {
		return false, err
	}
	info, ok := value.(*peer.PeerRecord)
	if !ok {
		return false, errors.New("libp2p: expected a signed peer address record")
	}
	for _, address := range info.Addrs {
		if !p.allowed(info.PeerID, address) {
			// A foreign signature cannot be preserved while removing an address.
			return false, nil
		}
	}
	return p.certified.ConsumePeerRecord(envelope, p.addressTTL(info.PeerID, ttl))
}

func (p *relayOnlyPeerstore) GetPeerRecord(id peer.ID) *record.Envelope {
	return p.certified.GetPeerRecord(id)
}

type relayOnlyHost struct {
	host.Host
	addresses *relayOnlyPeerstore
	cancel    context.CancelFunc
}

func (h *relayOnlyHost) Connect(ctx context.Context, remote peer.AddrInfo) error {
	remote.Addrs = h.addresses.filter(remote.ID, remote.Addrs)
	// Keep approved input hints distinct from identify's temporary replacement
	// set. A direct-only identify update must not erase a usable circuit hint.
	h.addresses.AddAddrs(remote.ID, remote.Addrs, maxPeerAddressAge)
	return h.Host.Connect(ctx, remote)
}

func (h *relayOnlyHost) Close() error {
	h.cancel()
	return h.Host.Close()
}
