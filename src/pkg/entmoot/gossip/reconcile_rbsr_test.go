package gossip

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/wire"
)

// dialCountingTransport wraps a Transport and counts Dial / SetOnTunnelUp
// invocations so reconcile-RBSR tests can assert on behaviour that's
// otherwise invisible from the message-store surface. Every method
// delegates to the inner transport (so hub routing remains intact) while
// bumping local counters under a mutex.
type dialCountingTransport struct {
	inner Transport

	mu             sync.Mutex
	fetchReqCount  int
	reconcileCount int
	tunnelUpCB     func(entmoot.NodeID)
}

func newDialCountingTransport(inner Transport) *dialCountingTransport {
	return &dialCountingTransport{inner: inner}
}

func (t *dialCountingTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	return t.inner.Dial(ctx, peer)
}

func (t *dialCountingTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	return t.inner.Accept(ctx)
}

func (t *dialCountingTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return t.inner.TrustedPeers(ctx)
}

func (t *dialCountingTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	return t.inner.SetPeerEndpoints(ctx, peer, endpoints)
}

func (t *dialCountingTransport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {
	t.mu.Lock()
	t.tunnelUpCB = cb
	t.mu.Unlock()
	t.inner.SetOnTunnelUp(cb)
}

func (t *dialCountingTransport) Close() error {
	return t.inner.Close()
}

// countingStore wraps a store.MessageStore and records how many times
// Has, Get, Put, MerkleRoot, and IterMessageIDsInIDRange are called.
// Used by reconcile-RBSR tests to prove anti-entropy produced zero
// fetch round-trips in the no-diff path.
type fetchCounter struct {
	mu    sync.Mutex
	count int
}

func (c *fetchCounter) bump() {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
}

func (c *fetchCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// observeFetchReqs installs a middleware transport on every node in f
// that bumps `counter` each time a FetchReq frame is written. Returns
// the set of wrappers so callers can introspect per-node counts. We
// hook at the connection-write layer by wrapping net.Conn; the simpler
// alternative (counting onFetchReq handler invocations) would miss
// dropped-before-accept attempts.
//
// Implementation: we replace each node's transport with a wrapper that
// decodes the outbound frame type cheaply on Dial. Because the wire
// codec writes a length-prefixed JSON body, we sniff the first byte of
// the body (the JSON always starts with '{') preceded by the length +
// type header — too brittle. Simpler: wrap the net.Conn returned from
// Dial and inspect the first ReadFrame / WriteFrame on it. For
// this test suite we use a coarser metric: count calls to
// handleConn dispatch on MsgFetchReq at the receiver. That's done
// via a test-only `onFetchReqObserver` field.
//
// We actually don't need byte-level sniffing for the test cases here;
// the simpler path is to hook a shim net.Conn that snoops the outbound
// type byte. See fetchCountingConn below.
type fetchCountingConn struct {
	net.Conn
	counter *fetchCounter
	seen    atomic.Bool // only count once per conn
}

func (c *fetchCountingConn) Write(p []byte) (int, error) {
	// Wire frame layout: uint8 type + uint32 length + body. The first
	// byte written on a newly-dialed conn is the type tag. Count
	// FetchReq (0x05) outgoing.
	if !c.seen.Load() && len(p) > 0 {
		if wire.MsgType(p[0]) == wire.MsgFetchReq {
			c.counter.bump()
		}
		c.seen.Store(true)
	}
	return c.Conn.Write(p)
}

// fetchCountingTransport wraps Dial so every outbound conn sniffs its
// first-written frame type and bumps the fetch counter on FetchReq.
type fetchCountingTransport struct {
	Transport
	counter *fetchCounter
}

func (t *fetchCountingTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	c, err := t.Transport.Dial(ctx, peer)
	if err != nil {
		return nil, err
	}
	return &fetchCountingConn{Conn: c, counter: t.counter}, nil
}

// --- Tests ---------------------------------------------------------------

// TestReconcileViaRBSR_NoDiff asserts the happy-path short-circuit:
// two peers with identical stores should observe zero FetchReqs and
// converge after one root-equality round-trip.
func TestReconcileViaRBSR_NoDiff(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	// Wrap A's transport so we can count FetchReq Dials leaving A.
	counter := &fetchCounter{}
	aWrap := &fetchCountingTransport{Transport: f.transports[10], counter: counter}
	f.nodes[10].gossip.cfg.Transport = aWrap

	// Seed both nodes with 20 identical messages. The IDs are all
	// derived from canonical hashes, so both stores end up with
	// identical contents → identical Merkle roots.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 20; i++ {
		msg := f.buildMessage(10, fmt.Sprintf("msg-%d", i), int64(2_000+i))
		if err := f.nodes[10].storeM.Put(ctx, msg); err != nil {
			t.Fatalf("seed A: %v", err)
		}
		if err := f.nodes[20].storeM.Put(ctx, msg); err != nil {
			t.Fatalf("seed B: %v", err)
		}
	}

	f.startAll(ctx)
	// Drain the OnTunnelUp-triggered reconciles from startup so we
	// measure only the one we explicitly drive below. A short sleep
	// lets any in-flight async maybeReconcile goroutine settle; then
	// we clear the fetch counter.
	time.Sleep(200 * time.Millisecond)

	// Clear the counter — any startup reconciles have now landed and
	// we want to observe only the explicit call below.
	counter.mu.Lock()
	counter.count = 0
	counter.mu.Unlock()

	// Force the per-peer cooldown to an expired state so maybeReconcile
	// actually fires. A's prior reconciles (from OnTunnelUp) may have
	// populated lastReconciled.
	aG := f.nodes[10].gossip
	aG.pendMu.Lock()
	delete(aG.lastReconciled, 20)
	aG.pendMu.Unlock()

	// Drive a reconcile on A against B.
	aG.maybeReconcile(ctx, 20)

	// Wait for the async session to complete. Root-equality short-
	// circuit is one MerkleReq round-trip.
	waitUntil(t, 3*time.Second, "A caches B's root after no-diff reconcile", func() bool {
		_, ok := aG.readLastKnownPeerRoot(20)
		return ok
	})

	// Both roots must still match.
	rootA, err := f.nodes[10].storeM.MerkleRoot(ctx, f.groupID)
	if err != nil {
		t.Fatalf("A MerkleRoot: %v", err)
	}
	rootB, err := f.nodes[20].storeM.MerkleRoot(ctx, f.groupID)
	if err != nil {
		t.Fatalf("B MerkleRoot: %v", err)
	}
	if !bytes.Equal(rootA[:], rootB[:]) {
		t.Fatalf("no-diff reconcile should preserve equal roots: A=%x B=%x", rootA, rootB)
	}

	// Load-bearing assertion: no FetchReq left A during the
	// reconcile. Merkle equality should short-circuit before any
	// body-fetch happens.
	if n := counter.get(); n != 0 {
		t.Fatalf("no-diff reconcile issued %d FetchReqs; want 0", n)
	}
}

// TestReconcileViaRBSR_GapInMiddle is the today's-bug repro: A has
// {m1..m10}, B has {m1..m10, m11, m12} where the extra ids sort into
// the MIDDLE of byte-order space (not just "at the tail"), and the
// extras have timestamps BEFORE A's latest-known timestamp so the old
// "since = latestLocalTimestamp" path would have skipped them.
//
// The RBSR path walks byte-order ranges, so middle-of-space gaps are
// found deterministically in O(log) rounds.
func TestReconcileViaRBSR_GapInMiddle(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We need A to have 10 messages and B to have those same 10 + 2
	// extras. The extras must satisfy two constraints:
	//   (a) their MessageIDs sort in the middle of the combined set's
	//       byte-order (not at head or tail), and
	//   (b) their Timestamps are EARLIER than A's latest timestamp
	//       (so the old fetchPeerRange "since latestLocalTimestamp"
	//       would have missed them).
	//
	// Strategy: generate many candidate messages, pick 10 "base"
	// messages for A's set at timestamps 3000..3009, then search the
	// remaining candidates for 2 extras at timestamps 2500..2999 (all
	// strictly before A's earliest) whose IDs land in the middle of
	// the base set's byte-range.
	base := make([]entmoot.Message, 0, 10)
	for i := 0; i < 10; i++ {
		base = append(base, f.buildMessage(10, fmt.Sprintf("base-%d", i), int64(3_000+i)))
	}
	// Compute the min and max IDs in the base set for the "middle"
	// constraint. Messages are byte-ordered over their 32-byte IDs.
	var minID, maxID entmoot.MessageID
	copy(minID[:], base[0].ID[:])
	copy(maxID[:], base[0].ID[:])
	for _, m := range base[1:] {
		if bytes.Compare(m.ID[:], minID[:]) < 0 {
			copy(minID[:], m.ID[:])
		}
		if bytes.Compare(m.ID[:], maxID[:]) > 0 {
			copy(maxID[:], m.ID[:])
		}
	}

	// Scan candidates whose IDs fall strictly between minID and maxID.
	// Timestamps below 3000 ensure they're "before A's latest".
	var extras []entmoot.Message
	for i := 0; len(extras) < 2 && i < 10_000; i++ {
		cand := f.buildMessage(10, fmt.Sprintf("extra-%d", i), int64(2_500+(i%500)))
		if bytes.Compare(cand.ID[:], minID[:]) > 0 && bytes.Compare(cand.ID[:], maxID[:]) < 0 {
			extras = append(extras, cand)
		}
	}
	if len(extras) < 2 {
		t.Fatalf("could not find 2 extras in the middle of the base ID range")
	}

	// Seed A's store with the base set.
	for _, m := range base {
		if err := f.nodes[10].storeM.Put(ctx, m); err != nil {
			t.Fatalf("seed A: %v", err)
		}
	}
	// Seed B's store with base + extras.
	for _, m := range base {
		if err := f.nodes[20].storeM.Put(ctx, m); err != nil {
			t.Fatalf("seed B base: %v", err)
		}
	}
	for _, m := range extras {
		if err := f.nodes[20].storeM.Put(ctx, m); err != nil {
			t.Fatalf("seed B extra: %v", err)
		}
	}

	// Sanity: roots differ.
	rootA, _ := f.nodes[10].storeM.MerkleRoot(ctx, f.groupID)
	rootB, _ := f.nodes[20].storeM.MerkleRoot(ctx, f.groupID)
	if bytes.Equal(rootA[:], rootB[:]) {
		t.Fatalf("pre-reconcile roots should differ; test seeding is broken")
	}

	f.startAll(ctx)

	// Clear any startup-reconcile state so maybeReconcile definitely
	// fires.
	aG := f.nodes[10].gossip
	time.Sleep(100 * time.Millisecond)
	aG.pendMu.Lock()
	delete(aG.lastReconciled, 20)
	aG.pendMu.Unlock()

	// Drive the reconcile.
	aG.maybeReconcile(ctx, 20)

	// A must eventually acquire both extras. RBSR O(log) rounds +
	// body-fetch round-trip per extra. Give 5 s.
	for _, extra := range extras {
		extra := extra
		waitUntil(t, 5*time.Second, fmt.Sprintf("A fetches extra %x", extra.ID[:4]), func() bool {
			has, _ := f.nodes[10].storeM.Has(ctx, f.groupID, extra.ID)
			return has
		})
	}

	// Merkle roots must now converge.
	waitUntil(t, 2*time.Second, "A and B Merkle roots converge", func() bool {
		ra, _ := f.nodes[10].storeM.MerkleRoot(ctx, f.groupID)
		rb, _ := f.nodes[20].storeM.MerkleRoot(ctx, f.groupID)
		return bytes.Equal(ra[:], rb[:])
	})
}

// silentReconcileTransport wraps memTransport and rejects inbound
// MsgReconcile frames by closing the connection immediately — the
// wire-compat regression test for "peer that doesn't understand
// MsgReconcile". The rejection is done at the accept layer by
// wrapping the accepted conn with a read-sniffer that closes on
// MsgReconcile.
type droppingReconcileTransport struct {
	Transport
}

type droppingReconcileConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *droppingReconcileConn) Read(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	// Sniff the first byte (the MsgType) and drop if Reconcile.
	// We only do this on the first read per conn; subsequent reads
	// pass through unchanged.
	n, err := c.Conn.Read(p)
	if n > 0 && !c.closed.Load() && wire.MsgType(p[0]) == wire.MsgReconcile {
		c.closed.Store(true)
		_ = c.Conn.Close()
		// Return what we got but immediately error on subsequent reads
		// so the handler unwinds. The caller will observe EOF-like
		// behaviour.
		return n, err
	}
	return n, err
}

func (t *droppingReconcileTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	c, remote, err := t.Transport.Accept(ctx)
	if err != nil {
		return nil, 0, err
	}
	return &droppingReconcileConn{Conn: c}, remote, nil
}

// TestReconcileViaRBSR_PeerReturnsUnknownOpcode exercises wire-compat
// when the responder doesn't understand MsgReconcile. We simulate a
// v1.2.0-era peer by wrapping B's transport so its accept side drops
// any connection whose first frame is MsgReconcile (as a v1.2.0
// handleConn would: the switch falls through to the default branch
// and the caller closes the conn). A must observe read error / EOF,
// log the "session ended early" Debug, and return without corrupting
// its own state.
func TestReconcileViaRBSR_PeerReturnsUnknownOpcode(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	// Wrap B's transport so MsgReconcile frames get dropped at accept.
	// A still sends MerkleReq successfully (that's a different opcode);
	// only the follow-up MsgReconcile frame triggers the drop.
	bWrap := &droppingReconcileTransport{Transport: f.transports[20]}
	f.nodes[20].gossip.cfg.Transport = bWrap

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Seed A with 5 messages and B with a disjoint set of 5 so roots
	// differ and A actually tries to drive the reconcile past the
	// MerkleRoot short-circuit.
	for i := 0; i < 5; i++ {
		mA := f.buildMessage(10, fmt.Sprintf("A-%d", i), int64(2_000+i))
		if err := f.nodes[10].storeM.Put(ctx, mA); err != nil {
			t.Fatalf("seed A: %v", err)
		}
		mB := f.buildMessage(10, fmt.Sprintf("B-%d", i), int64(3_000+i))
		if err := f.nodes[20].storeM.Put(ctx, mB); err != nil {
			t.Fatalf("seed B: %v", err)
		}
	}

	// Record A's pre-reconcile root so we can assert non-corruption.
	rootABefore, err := f.nodes[10].storeM.MerkleRoot(ctx, f.groupID)
	if err != nil {
		t.Fatalf("A MerkleRoot: %v", err)
	}

	f.startAll(ctx)

	// Clear any startup-reconcile cooldown so the forced call below
	// actually fires.
	aG := f.nodes[10].gossip
	time.Sleep(100 * time.Millisecond)
	aG.pendMu.Lock()
	delete(aG.lastReconciled, 20)
	aG.pendMu.Unlock()

	// Drive the reconcile. Expected: A sends MerkleReq (B answers
	// fine), A detects root mismatch, opens fresh conn, sends
	// MsgReconcile frame → dropping-responder closes the conn → A
	// reads EOF → logs Debug and returns. A's store is unchanged.
	aG.maybeReconcile(ctx, 20)

	// Give the async reconcile ample time to fail and return.
	time.Sleep(500 * time.Millisecond)

	rootAAfter, err := f.nodes[10].storeM.MerkleRoot(ctx, f.groupID)
	if err != nil {
		t.Fatalf("A MerkleRoot post: %v", err)
	}
	if !bytes.Equal(rootABefore[:], rootAAfter[:]) {
		t.Fatalf("A's Merkle root changed after failed reconcile: before=%x after=%x",
			rootABefore, rootAAfter)
	}
}

// TestReconcileTunnelUpTrigger asserts that a fresh memTransport Dial
// fires the OnTunnelUp callback exactly once, and that the callback
// routes into maybeReconcile (visible as an entry appearing in
// lastReconciled). We use a fresh fixture with no prior state, dial
// once, and confirm lastReconciled[peer] got populated.
func TestReconcileTunnelUpTrigger(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	// Clear any pre-existing lastReconciled entries on A so we can
	// observe the fresh dial's effect.
	aG := f.nodes[10].gossip
	aG.pendMu.Lock()
	delete(aG.lastReconciled, 20)
	aG.pendMu.Unlock()

	// Fresh Dial from A → B. memTransport.Dial fires OnTunnelUp on
	// the caller side; Accept fires it on the server side.
	conn, err := f.transports[10].Dial(ctx, 20)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Expect A's lastReconciled[20] to appear within a short window.
	// The callback runs in a goroutine and maybeReconcile is async —
	// give scheduler + cooldown-record plenty of slack.
	waitUntil(t, 2*time.Second, "A.lastReconciled[20] populated by tunnel-up callback", func() bool {
		aG.pendMu.Lock()
		defer aG.pendMu.Unlock()
		_, ok := aG.lastReconciled[20]
		return ok
	})

	// Record the firing time so we can detect whether a second
	// maybeReconcile would fire on the cooldown-suppression test below.
	aG.pendMu.Lock()
	firstAt := aG.lastReconciled[20].at
	aG.pendMu.Unlock()

	// Dial again immediately. OnTunnelUp fires again, but
	// maybeReconcile's cooldown gate MUST suppress the second
	// reconcile — Part C's contract is "callback on every new tunnel,
	// dedupe in the gossiper."
	conn2, err := f.transports[10].Dial(ctx, 20)
	if err != nil {
		t.Fatalf("Dial 2: %v", err)
	}
	defer conn2.Close()

	// Wait a little longer than the callback would need; then assert
	// lastReconciled[20].at is unchanged — the cooldown gate
	// prevented a second reconcile from updating the timestamp.
	time.Sleep(300 * time.Millisecond)
	aG.pendMu.Lock()
	secondAt := aG.lastReconciled[20].at
	aG.pendMu.Unlock()
	if !secondAt.Equal(firstAt) {
		t.Fatalf("cooldown should have suppressed second reconcile: firstAt=%v secondAt=%v",
			firstAt, secondAt)
	}
}
