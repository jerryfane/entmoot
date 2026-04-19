package gossip

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/wire"
)

// filteringTransport wraps a Transport and drops Dial calls whose
// (local → peer) pair is not in the allowed set. Used by Plumtree tests
// to construct partial-connectivity graphs — e.g. "A ↔ B, B ↔ C, A ↛ C"
// — so we can prove that receive-side re-fanout (not direct dial) is
// what delivers a message to a peer the originator cannot reach.
//
// Accept, TrustedPeers, and Close pass through unchanged so the inner
// transport's hub still delivers frames that arrived through permitted
// dial paths.
type filteringTransport struct {
	local   entmoot.NodeID
	inner   Transport
	mu      sync.RWMutex
	allowed map[entmoot.NodeID]bool
}

func newFilteringTransport(local entmoot.NodeID, inner Transport, allowed []entmoot.NodeID) *filteringTransport {
	allowedSet := make(map[entmoot.NodeID]bool, len(allowed))
	for _, p := range allowed {
		allowedSet[p] = true
	}
	return &filteringTransport{local: local, inner: inner, allowed: allowedSet}
}

func (t *filteringTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	t.mu.RLock()
	ok := t.allowed[peer]
	t.mu.RUnlock()
	if !ok {
		return nil, net.ErrClosed
	}
	return t.inner.Dial(ctx, peer)
}

func (t *filteringTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	return t.inner.Accept(ctx)
}

func (t *filteringTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return t.inner.TrustedPeers(ctx)
}

func (t *filteringTransport) Close() error { return t.inner.Close() }

// wrapFixtureWithFilter replaces each node's transport in the fixture
// with a filteringTransport. `graph` is an adjacency list: graph[A]
// lists every peer that A is allowed to Dial. (The graph is directed
// at the Dial level, but since both sides need a Dial edge to reach
// each other, callers typically specify symmetric pairs.)
func wrapFixtureWithFilter(t *testing.T, f *fixture, graph map[entmoot.NodeID][]entmoot.NodeID) {
	t.Helper()
	for id, ns := range f.nodes {
		allowed := graph[id]
		ns.gossip.cfg.Transport = newFilteringTransport(id, f.transports[id], allowed)
	}
}

// TestPlumtreeReFanoutOnReceive exercises the core Plumtree property:
// a message A publishes reaches C even though A has no edge to C.
// Topology: A ↔ B, B ↔ C, A ↛ C (and C ↛ A). Without re-fanout, C
// never sees the message. With Plumtree, B stores on receive and
// forwards to C.
func TestPlumtreeReFanoutOnReceive(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30}) // A=10, B=20, C=30
	defer f.closeTransports()

	wrapFixtureWithFilter(t, f, map[entmoot.NodeID][]entmoot.NodeID{
		10: {20},     // A ↔ B only
		20: {10, 30}, // B ↔ everyone (the pivot)
		30: {20},     // C ↔ B only
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	msg := f.buildMessage(10, "from A to everyone", 2_000)
	if err := f.nodes[10].gossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}

	// Assert C eventually stores the message. Convergence time is
	// one A→B push + one B→C refanout push ≈ milliseconds over
	// net.Pipe; give 2 s for safety.
	waitUntil(t, 2*time.Second, "C stores A's message (via B re-fanout)", func() bool {
		has, _ := f.nodes[30].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})
	waitUntil(t, 2*time.Second, "B stores A's message", func() bool {
		has, _ := f.nodes[20].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})
}

// TestPlumtreePruneOnDuplicate verifies the tree-pruning half of the
// protocol: when a node receives a duplicate Gossip for the same id,
// it PRUNEs the sender (demotes them to lazyPushPeers) so subsequent
// messages from that peer arrive as IHave instead of full-body pushes.
//
// Topology: fully connected 3-node mesh. A publishes; B receives
// via A direct AND via C (after C re-fanouts). The second arrival
// triggers PRUNE. Assert C is in B's lazyPushPeers after convergence.
func TestPlumtreePruneOnDuplicate(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	msg := f.buildMessage(10, "duplicate-burst test", 2_000)
	if err := f.nodes[10].gossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}

	// Wait until everyone has the message before inspecting the
	// tree state. Both B and C should have received via A's direct
	// fanout; the race is whose re-fanout delivers a duplicate to
	// the other. Whoever sees a duplicate will PRUNE the duplicate
	// source.
	waitUntil(t, 2*time.Second, "B + C both have A's message", func() bool {
		hb, _ := f.nodes[20].storeM.Has(ctx, f.groupID, msg.ID)
		hc, _ := f.nodes[30].storeM.Has(ctx, f.groupID, msg.ID)
		return hb && hc
	})

	// Allow a short settling period for any in-flight PRUNE to
	// land. Both B and C should have demoted exactly one peer to
	// lazy; which peer depends on the order of arrivals, so we only
	// assert "at least one side has a non-empty lazy set" — that's
	// the signature of the PRUNE having fired.
	deadline := time.Now().Add(1 * time.Second)
	var bLazy, cLazy int
	for time.Now().Before(deadline) {
		f.nodes[20].gossip.plumMu.Lock()
		bLazy = len(f.nodes[20].gossip.lazyPushPeers)
		f.nodes[20].gossip.plumMu.Unlock()
		f.nodes[30].gossip.plumMu.Lock()
		cLazy = len(f.nodes[30].gossip.lazyPushPeers)
		f.nodes[30].gossip.plumMu.Unlock()
		if bLazy > 0 || cLazy > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if bLazy == 0 && cLazy == 0 {
		t.Fatalf("expected a PRUNE to have fired on at least one of B/C, but both have empty lazyPushPeers")
	}
}

// TestPlumtreeGraftOnMissing verifies the tree-repair half: when a
// node receives IHave for an id it doesn't have AND no eager push
// arrives within graftTimeout, it sends GRAFT to pull the body AND
// promotes the IHave sender into its eagerPushPeers.
//
// To force the GRAFT timer to fire we need a topology where the
// IHave sender is the ONLY path to the body. Simplest: A ↔ B, B has
// the body, B sends IHave to C directly (without A re-fanout); C
// needs to graft B.
//
// The trick: we force B into C's lazy set before the test. Then B's
// re-fanout on receive from A goes as IHave, and C must graft to get
// the body.
func TestPlumtreeGraftOnMissing(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30}) // A=10, B=20, C=30
	defer f.closeTransports()

	// Force a short GRAFT timer for the test. Reverting to default
	// would need 3 s per test which is slow; override for test
	// duration only on node C (the grafter).
	// The package-level `graftTimeout` is a const, so instead we
	// exploit that our test already waits with waitUntil; 3 s is
	// within budget.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	// Pre-seed C's plumtree state: put B in C's lazyPushPeers and
	// remove B from C's eagerPushPeers. The seeding is idempotent
	// on first use, so we call seedPlumtreeLocked first to populate,
	// then reassign.
	cGossip := f.nodes[30].gossip
	cGossip.plumMu.Lock()
	cGossip.seedPlumtreeLocked()
	delete(cGossip.eagerPushPeers, 20)
	cGossip.lazyPushPeers[20] = struct{}{}
	cGossip.plumMu.Unlock()
	// Also pre-prune B in A's view so A doesn't directly eager-push
	// to C — force the message through B's lazy-push path.
	aGossip := f.nodes[10].gossip
	aGossip.plumMu.Lock()
	aGossip.seedPlumtreeLocked()
	delete(aGossip.eagerPushPeers, 30)
	aGossip.lazyPushPeers[30] = struct{}{}
	aGossip.plumMu.Unlock()

	msg := f.buildMessage(10, "graft-me test", 2_000)
	if err := aGossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}

	// The delivery chain:
	//   1. A eager→B (B in A's eager)
	//   2. A lazy→C (IHave only; C starts GRAFT timer)
	//   3. B stores, re-fanouts: lazy→C (another IHave arrives; C's
	//      timer resets or adds another keyed entry)
	//   4. After graftTimeout (3 s) C's timer fires → GRAFT to B or A
	//   5. Recipient of GRAFT responds with full Gossip → C fetches body
	//
	// Budget: graftTimeout (3 s) + message round-trips (~100 ms).
	// Give 5 s.
	waitUntil(t, 5*time.Second, "C stores A's message via GRAFT", func() bool {
		has, _ := f.nodes[30].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})
}

// TestPlumtreeInitialEagerPopulation verifies that on first use of
// the plumtree state, every non-self roster member is in
// eagerPushPeers per the paper's convention. Paper quote: "initially
// all known peers are in eagerPushPeers".
func TestPlumtreeInitialEagerPopulation(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30})
	defer f.closeTransports()

	// seedPlumtreeLocked is called on first plumMu-held access; call
	// plumEagerExcept with a sentinel to trigger it.
	got := f.nodes[10].gossip.plumEagerExcept(0)

	// A=10; expect 20 and 30 in eager, nothing in lazy.
	if len(got) != 2 {
		t.Fatalf("eagerPushPeers count = %d, want 2: %v", len(got), got)
	}
	f.nodes[10].gossip.plumMu.Lock()
	lazyLen := len(f.nodes[10].gossip.lazyPushPeers)
	f.nodes[10].gossip.plumMu.Unlock()
	if lazyLen != 0 {
		t.Fatalf("lazyPushPeers should be empty on startup, got %d", lazyLen)
	}
}

// TestPlumtreeOnPruneDemotesSender is a focused unit test on the
// Prune handler: a Prune from peer X must move X from eager to lazy.
func TestPlumtreeOnPruneDemotesSender(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	g := f.nodes[10].gossip
	// Seed and verify B=20 is in eager.
	_ = g.plumEagerExcept(0)
	g.plumMu.Lock()
	if _, ok := g.eagerPushPeers[20]; !ok {
		g.plumMu.Unlock()
		t.Fatalf("expected 20 in eagerPushPeers after seeding")
	}
	g.plumMu.Unlock()

	// Deliver a valid Prune frame from B through the handler.
	g.onPrune(20, &wire.Prune{GroupID: f.groupID})

	g.plumMu.Lock()
	_, inEager := g.eagerPushPeers[20]
	_, inLazy := g.lazyPushPeers[20]
	g.plumMu.Unlock()
	if inEager {
		t.Fatalf("expected 20 to be demoted out of eagerPushPeers after Prune")
	}
	if !inLazy {
		t.Fatalf("expected 20 to be in lazyPushPeers after Prune")
	}
}
