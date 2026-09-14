package libp2ptransport

import (
	"slices"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/roster"
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
}

func NewPeerRecordCache() *PeerRecordCache {
	return &PeerRecordCache{records: make(map[peer.ID]cachedPeerRecord)}
}

// sequence reports the newest accepted sequence for a peer.
func (c *PeerRecordCache) sequence(id peer.ID) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	current, ok := c.records[id]
	return current.seq, ok
}

// put keeps the highest sequence seen for a peer. An older record never
// replaces a newer one, so a stale forward cannot undo a NAT change.
func (c *PeerRecordCache) put(id peer.ID, payload []byte, seq uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.records == nil {
		c.records = make(map[peer.ID]cachedPeerRecord)
	}
	if current, ok := c.records[id]; ok && current.seq > seq {
		return
	}
	c.records[id] = cachedPeerRecord{payload: slices.Clone(payload), seq: seq}
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
	for id, current := range c.records {
		if id == exclude {
			continue
		}
		out = append(out, slices.Clone(current.payload))
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

// Forget drops a peer's cached record, used when membership ends.
func (c *PeerRecordCache) Forget(id peer.ID) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.records, id)
}

// InstallPeerRecords verifies forwarded libp2p signed peer records and installs
// the surviving addresses for current roster members. A newer record replaces
// every previously known address for that peer so a NAT change cannot
// resurrect stale hints; an older one is ignored. Accepted records are cached
// for forwarding. Returns the number of records that installed an address.
func InstallPeerRecords(h host.Host, r *roster.RosterLog, records [][]byte, mode ConnectivityMode, controlledRelays []peer.AddrInfo, cache *PeerRecordCache) int {
	if h == nil || r == nil || len(records) == 0 {
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
		last, seen := cache.sequence(peerRecord.PeerID)
		if seen && peerRecord.Seq < last {
			continue
		}
		if !seen || peerRecord.Seq > last {
			h.Peerstore().ClearAddrs(peerRecord.PeerID)
		}
		if err := InstallVerifiedPeer(h, r, member, peerRecord.PeerID, slices.Clone(peerRecord.Addrs),
			maxPeerAddressAge, mode, controlledRelays); err != nil {
			continue
		}
		cache.put(peerRecord.PeerID, payload, peerRecord.Seq)
		installed++
	}
	return installed
}
