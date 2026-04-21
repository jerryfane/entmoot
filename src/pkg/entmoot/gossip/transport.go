// Package gossip implements Entmoot v0's push-only epidemic broadcast plus the
// bootstrap (join) flow described in ARCHITECTURE.md §5 and §7.
//
// v0 scope (tracked in the plan's "Deferred from v0" section):
//
//   - Push-only gossip. No push-pull / anti-entropy sweep. Each local Publish
//     fans out one Gossip frame to min(Fanout, |members-1|) random peers; the
//     receivers pull bodies via FetchReq.
//   - Random-only peer sampling. No near-peer (interest-overlap) pool.
//   - Hard-disconnect on any wire-level error. No cooldown / backoff.
//   - No announce_group. Group discovery piggybacks on invites.
//   - Positive-inclusion Merkle only. Canary diffs roots; absent subtrees are
//     resolved by FetchReq iterations, not by absence proofs.
//
// The package never imports the parallel replay sub-layer (Phase B2); Phase E
// wires them together at the main-binary seam. Within this file, Transport
// exists so tests can drive the full Gossiper accept/publish loop without a
// running Pilot daemon.
package gossip

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"entmoot/pkg/entmoot"
)

// Transport abstracts Pilot's dial / listen / trust surface. Production wires
// it to driver.DialAddr / driver.Listen / driver.TrustedPeers in Phase E; tests
// use the in-memory pair returned by NewMemTransports.
//
// Implementations MUST authenticate the remote peer before returning it from
// Accept: v0 skips the Hello handshake (the plan's v0 simplification) and
// relies entirely on the underlying trust layer to attest the remote NodeID.
// For Pilot that attestation is implicit in driver.Listen — the tunnel is only
// established after Pilot's pairwise trust succeeds, so the remote is known at
// accept time.
type Transport interface {
	// Dial opens a bidirectional stream to peer. Returns an error if peer is
	// unreachable or the context is cancelled. The returned net.Conn is the
	// caller's to close.
	Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error)

	// Accept blocks until a new inbound connection arrives. Returns the conn
	// plus the remote NodeID (resolved by the underlying trust layer). Returns
	// ctx.Err() on cancellation and a non-nil error once the transport is
	// closed.
	Accept(ctx context.Context) (conn net.Conn, remote entmoot.NodeID, err error)

	// TrustedPeers returns NodeIDs for which we have a Pilot trust edge. Used
	// by the bootstrap fallback (ARCHITECTURE §5.2).
	TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error)

	// SetPeerEndpoints installs externally-sourced transport endpoints for a
	// peer into the underlying transport's state (e.g., Pilot's peerTCP
	// map). Called by the gossiper when a verified TransportAd arrives and
	// when invite-embedded endpoints are parsed at Join time.
	// Implementations that don't support per-peer endpoint injection (the
	// in-memory test transport) may no-op. (v1.2.0)
	SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error

	// Close releases resources. Pending Accepts return an error.
	Close() error
}

// memTransport is the in-memory Transport implementation. One memTransport per
// participating NodeID; all of them share a single memHub which routes dials
// into the target's accept queue.
type memTransport struct {
	local entmoot.NodeID
	hub   *memHub

	acceptCh chan memAcceptItem

	closeOnce sync.Once
	closed    chan struct{}

	// endpoints records SetPeerEndpoints calls so tests can introspect
	// "was this peer's set called, and with what endpoints?" via
	// EndpointsFor. Not consulted by Dial (the hub routes by NodeID
	// alone); purely observational. (v1.2.0)
	epMu      sync.Mutex
	endpoints map[entmoot.NodeID][]entmoot.NodeEndpoint
}

// EndpointsFor returns the last endpoints slice SetPeerEndpoints was
// called with for peer, or nil if no such call was made. Test-only
// helper: production gossip code never reads this.
func (t *memTransport) EndpointsFor(peer entmoot.NodeID) []entmoot.NodeEndpoint {
	t.epMu.Lock()
	defer t.epMu.Unlock()
	out := t.endpoints[peer]
	if out == nil {
		return nil
	}
	return append([]entmoot.NodeEndpoint(nil), out...)
}

// memAcceptItem is what memTransport.acceptCh delivers: the server-side end of
// a net.Pipe plus the dialing NodeID. It is the shape Accept returns.
type memAcceptItem struct {
	conn   net.Conn
	remote entmoot.NodeID
}

// memHub wires a set of memTransports together. It holds the per-node accept
// channels so Dial can locate the target and deliver the server-side of a
// net.Pipe. All participants share their view of TrustedPeers through the hub
// (everyone trusts everyone else in the mock).
type memHub struct {
	mu     sync.RWMutex
	nodes  map[entmoot.NodeID]*memTransport
	closed bool
}

// NewMemTransports returns a Transport per input NodeID. Each transport can
// Dial any other participant and vice versa; closed transports drop further
// dials with net.ErrClosed. All nodes report every other node in the set from
// TrustedPeers — the mock has no partial trust graph.
//
// Call Close on each returned Transport (or stop reusing them) to tear down
// the hub. Closing one does not close the others; tests usually defer Close
// on each.
func NewMemTransports(nodes []entmoot.NodeID) map[entmoot.NodeID]Transport {
	hub := &memHub{nodes: make(map[entmoot.NodeID]*memTransport, len(nodes))}
	out := make(map[entmoot.NodeID]Transport, len(nodes))
	for _, n := range nodes {
		tr := &memTransport{
			local:    n,
			hub:      hub,
			acceptCh: make(chan memAcceptItem, 32),
			closed:   make(chan struct{}),
		}
		hub.nodes[n] = tr
		out[n] = tr
	}
	return out
}

// Dial implements Transport. It builds a net.Pipe and hands the server end
// plus the local NodeID to the target's Accept queue. Returns an error if
// the target is unknown to the hub, if the target is closed, or if ctx is
// cancelled before the server end is accepted by a reader.
func (t *memTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	select {
	case <-t.closed:
		return nil, net.ErrClosed
	default:
	}

	t.hub.mu.RLock()
	target, ok := t.hub.nodes[peer]
	closed := t.hub.closed
	t.hub.mu.RUnlock()
	if closed {
		return nil, net.ErrClosed
	}
	if !ok {
		return nil, fmt.Errorf("memTransport: unknown peer %d", peer)
	}
	select {
	case <-target.closed:
		return nil, net.ErrClosed
	default:
	}

	client, server := net.Pipe()
	select {
	case target.acceptCh <- memAcceptItem{conn: server, remote: t.local}:
		return client, nil
	case <-ctx.Done():
		_ = client.Close()
		_ = server.Close()
		return nil, ctx.Err()
	case <-target.closed:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	case <-t.closed:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	}
}

// Accept implements Transport. It blocks on the per-node accept queue until a
// Dial delivers an item or the transport is closed / ctx cancelled.
func (t *memTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	select {
	case item, ok := <-t.acceptCh:
		if !ok {
			return nil, 0, net.ErrClosed
		}
		return item.conn, item.remote, nil
	case <-t.closed:
		return nil, 0, net.ErrClosed
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

// SetPeerEndpoints implements Transport. The in-memory mock has no
// equivalent of Pilot's peerTCP map (dials route through the hub on
// NodeID alone), so the recorded call is kept on t.endpoints for tests
// that care to introspect, but delivery semantics are unaffected.
// (v1.2.0)
func (t *memTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	select {
	case <-t.closed:
		return net.ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.epMu.Lock()
	if t.endpoints == nil {
		t.endpoints = make(map[entmoot.NodeID][]entmoot.NodeEndpoint)
	}
	t.endpoints[peer] = append(t.endpoints[peer][:0:0], endpoints...)
	t.epMu.Unlock()
	return nil
}

// TrustedPeers implements Transport. The mock treats every node in the hub
// (excluding the local one) as trusted.
func (t *memTransport) TrustedPeers(_ context.Context) ([]entmoot.NodeID, error) {
	t.hub.mu.RLock()
	defer t.hub.mu.RUnlock()
	if t.hub.closed {
		return nil, net.ErrClosed
	}
	out := make([]entmoot.NodeID, 0, len(t.hub.nodes))
	for n := range t.hub.nodes {
		if n == t.local {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

// Close implements Transport. Marks the transport as closed by closing
// t.closed; every Dial/Accept path selects on t.closed so in-flight callers
// observe the close promptly. Intentionally does NOT close(t.acceptCh) —
// that would race with concurrent Dial goroutines that have already entered
// their select and chosen the send branch (Go's race detector flags the
// send-vs-close even though close-notice usually wins). Leaving acceptCh
// open lets any straggler send complete harmlessly into the buffered
// channel, where the item is then garbage-collected with the transport.
// Safe to call multiple times.
func (t *memTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
	})
	return nil
}

// ErrTransportClosed is a convenience sentinel surfaced when a Transport has
// been closed. Returned via net.ErrClosed from the mock; production
// implementations may choose to wrap a different underlying error.
var ErrTransportClosed = errors.New("gossip: transport closed")
