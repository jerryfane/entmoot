package libp2ptransport

import (
	"slices"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
)

// PeerRecordCache holds the newest verified signed peer record per member so a
// node can forward them. libp2p identify keeps a signed record only for the
// local host, so without this cache a member could serve nothing but its own
// address and a peer behind NAT would stay unreachable to every member that
// cannot already dial it.
type PeerRecordCache struct {
	mu      sync.RWMutex
	records map[peer.ID]cachedPeerRecord
}

type cachedPeerRecord struct {
	payload []byte
	seq     uint64
	// installed records which addresses this cache put in the peerstore, so a
	// replacement retires exactly those and leaves hints from invites, mDNS or
	// identify alone.
	installed []multiaddr.Multiaddr
}

func NewPeerRecordCache() *PeerRecordCache {
	return &PeerRecordCache{records: make(map[peer.ID]cachedPeerRecord)}
}

func (c *PeerRecordCache) current(id peer.ID) (cachedPeerRecord, bool) {
	if c == nil {
		return cachedPeerRecord{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	existing, ok := c.records[id]
	return existing, ok
}

// accept stores a verified record. An older record never replaces a newer one,
// so a stale forward cannot undo a NAT change.
func (c *PeerRecordCache) accept(id peer.ID, payload []byte, seq uint64, installed []multiaddr.Multiaddr) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.records == nil {
		c.records = make(map[peer.ID]cachedPeerRecord)
	}
	if existing, ok := c.records[id]; ok && existing.seq > seq {
		return
	}
	c.records[id] = cachedPeerRecord{
		payload:   slices.Clone(payload),
		seq:       seq,
		installed: slices.Clone(installed),
	}
}

// retain drops records for peers that are no longer members, so a removed
// member is neither forwarded nor counted against the response budget.
func (c *PeerRecordCache) retain(members map[peer.ID]entmoot.NodeInfo) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.records {
		if _, ok := members[id]; !ok {
			delete(c.records, id)
		}
	}
}

// Snapshot returns the cached records except the excluded peer's own record,
// which the requester already knows.
func (c *PeerRecordCache) Snapshot(exclude peer.ID) [][]byte {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([][]byte, 0, len(c.records))
	for id, existing := range c.records {
		if id == exclude {
			continue
		}
		out = append(out, slices.Clone(existing.payload))
	}
	return out
}

// Len reports how many members this node can forward addresses for.
func (c *PeerRecordCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.records)
}

// InstallPeerRecords verifies forwarded libp2p signed peer records and installs
// the surviving addresses for current roster members. A newer record retires
// the addresses its predecessor installed so a NAT change cannot leave stale
// hints behind, and an older record is ignored. A newer record whose addresses
// the local profile rejects still advances the freshness floor but changes no
// address, because losing a working address to an unusable update would make a
// reachable member unreachable. Accepted records are cached for forwarding.
// Returns the number of records that installed an address.
func InstallPeerRecords(h host.Host, r *membership.Group, records [][]byte, mode ConnectivityMode, controlledRelays []peer.AddrInfo, cache *PeerRecordCache) int {
	if h == nil || r == nil {
		return 0
	}
	members := make(map[peer.ID]entmoot.NodeInfo)
	for _, memberID := range r.MemberIDs() {
		info, known := r.MemberInfoByID(memberID)
		if !known {
			continue
		}
		binding, err := BindingFromPublicKey(info.EntmootPubKey)
		if err != nil {
			continue
		}
		members[binding.PeerID] = info
	}
	cache.retain(members)
	installed := 0
	for _, payload := range records {
		envelope, value, err := record.ConsumeEnvelope(payload, peer.PeerRecordEnvelopeDomain)
		if err != nil {
			continue
		}
		peerRecord, ok := value.(*peer.PeerRecord)
		if !ok {
			continue
		}
		signer, err := peer.IDFromPublicKey(envelope.PublicKey)
		if err != nil || signer != peerRecord.PeerID || peerRecord.PeerID == h.ID() {
			continue
		}
		member, isMember := members[peerRecord.PeerID]
		if !isMember {
			continue
		}
		existing, seen := cache.current(peerRecord.PeerID)
		if seen && peerRecord.Seq < existing.seq {
			continue
		}
		surviving := profileAddresses(peerRecord.PeerID, peerRecord.Addrs, mode, controlledRelays)
		if len(surviving) == 0 {
			// Authentic and current, but nothing here is dialable under this
			// profile. Record the sequence so an older forward cannot win.
			cache.accept(peerRecord.PeerID, payload, peerRecord.Seq, nil)
			continue
		}
		retireReplacedAddresses(h, peerRecord.PeerID, existing.installed, surviving)
		if err := InstallVerifiedPeer(h, r, member, peerRecord.PeerID, surviving,
			maxPeerAddressAge, mode, controlledRelays); err != nil {
			continue
		}
		cache.accept(peerRecord.PeerID, payload, peerRecord.Seq, surviving)
		installed++
	}
	return installed
}

// retireReplacedAddresses expires the addresses a previous record installed and
// the new one dropped. Expiring by TTL keeps every address this node learned
// from another source.
func retireReplacedAddresses(h host.Host, peerID peer.ID, previous, current []multiaddr.Multiaddr) {
	if len(previous) == 0 {
		return
	}
	keep := make(map[string]struct{}, len(current))
	for _, address := range current {
		keep[address.String()] = struct{}{}
	}
	stale := make([]multiaddr.Multiaddr, 0, len(previous))
	for _, address := range previous {
		if _, ok := keep[address.String()]; !ok {
			stale = append(stale, address)
		}
	}
	if len(stale) == 0 {
		return
	}
	h.Peerstore().SetAddrs(peerID, stale, 0)
}
