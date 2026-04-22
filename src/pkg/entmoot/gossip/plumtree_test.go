package gossip

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/wire"
)

// filteringTransport wraps a Transport and drops Dial calls whose
// (local → peer) pair is not in the allowed set. Used by Plumtree tests
// to construct partial-connectivity graphs — e.g. "A ↔ B, B ↔ C, A ↛ C"
// — so we can prove that receive-side re-fanout (not direct dial) is
// what delivers a message to a peer the originator cannot reach.
//
// Accept and Close pass through unchanged so the inner transport's hub
// still delivers frames that arrived through permitted dial paths.
// TrustedPeers is filtered by `allowed` as well (v1.0.6): the Fix A
// trust oracle reads Transport.TrustedPeers, so tests that want to
// simulate "no trust pair between A and C" need both the Dial gate
// AND the trust-list gate to agree.
//
// slowDial (v1.0.6 Fix B) lets a test inject a per-peer Dial delay so
// it can simulate Pilot's ~32 s SYN-retry stall without actually
// consuming wallclock minutes. When slowDial[peer] > 0, Dial sleeps
// that long (respecting ctx cancellation) before returning — so a
// fanoutPerPeerTimeout-wrapped context will cancel the dial early
// exactly like a real stalled peer.
type filteringTransport struct {
	local     entmoot.NodeID
	inner     Transport
	mu        sync.RWMutex
	allowed   map[entmoot.NodeID]bool
	dialCount map[entmoot.NodeID]int
	slowDial  map[entmoot.NodeID]time.Duration
}

func newFilteringTransport(local entmoot.NodeID, inner Transport, allowed []entmoot.NodeID) *filteringTransport {
	allowedSet := make(map[entmoot.NodeID]bool, len(allowed))
	for _, p := range allowed {
		allowedSet[p] = true
	}
	return &filteringTransport{
		local:     local,
		inner:     inner,
		allowed:   allowedSet,
		dialCount: make(map[entmoot.NodeID]int),
		slowDial:  make(map[entmoot.NodeID]time.Duration),
	}
}

// setSlowDial arranges for subsequent Dial(peer) calls to block for the
// given duration before resolving. ctx cancellation cuts the wait early,
// mirroring real Pilot behaviour when a dial context deadline fires.
func (t *filteringTransport) setSlowDial(peer entmoot.NodeID, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.slowDial[peer] = d
}

func (t *filteringTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	t.mu.Lock()
	ok := t.allowed[peer]
	t.dialCount[peer]++
	delay := t.slowDial[peer]
	t.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if !ok {
		return nil, net.ErrClosed
	}
	return t.inner.Dial(ctx, peer)
}

func (t *filteringTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	return t.inner.Accept(ctx)
}

func (t *filteringTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	inner, err := t.inner.TrustedPeers(ctx)
	if err != nil {
		return nil, err
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]entmoot.NodeID, 0, len(inner))
	for _, p := range inner {
		if t.allowed[p] {
			out = append(out, p)
		}
	}
	return out, nil
}

func (t *filteringTransport) Close() error { return t.inner.Close() }

// SetPeerEndpoints implements Transport. Delegates to the inner
// transport so tests that wire a filter on top of memTransport still
// observe SetPeerEndpoints calls via memTransport.EndpointsFor.
// (v1.2.0)
func (t *filteringTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	return t.inner.SetPeerEndpoints(ctx, peer, endpoints)
}

// SetOnTunnelUp implements Transport. Delegates to the inner transport;
// Plumtree tests don't exercise the callback directly, but the filter
// still needs to satisfy the interface. (v1.2.1)
func (t *filteringTransport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {
	t.inner.SetOnTunnelUp(cb)
}

// dialsTo returns how many times Dial has been invoked for `peer`,
// regardless of whether the dial was allowed. Used by Fix A tests to
// prove the trust oracle short-circuited a would-be dial before the
// transport ever saw it.
func (t *filteringTransport) dialsTo(peer entmoot.NodeID) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.dialCount[peer]
}

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
	got := f.nodes[10].gossip.plumEagerExcept(context.Background(), 0)

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
	_ = g.plumEagerExcept(context.Background(), 0)
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

// TestPlumtreeRefanoutOnFetchFrom is the v1.0.5 regression guard: any
// acquisition path that flows through fetchFrom (gossip-push, retry,
// reconcile) must trigger Plumtree re-fanout to our other peers.
// Before v1.0.5 only onGossip refanned out after a successful fetch;
// retry-fetch and reconcile-fetch silently swallowed messages, breaking
// propagation when a hub acquired an edge's message via reconcile.
//
// We invoke fetchFrom directly (as reconcileWith and executeRetry both
// do) and assert the re-fanout arrived at the third node.
//
// Topology: fully connected 3-node mesh. A=10, B=20, C=30. We seed B's
// store with a message authored by B, then call A.fetchFrom(B, id).
// After fetchFrom returns, refanout must push to C, which should store
// the message.
func TestPlumtreeRefanoutOnFetchFrom(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	// Put a B-authored message directly in B's store without
	// publishing. This simulates the state after reconcileWith has
	// located the id on peer B via RangeReq: A about to fetchFrom B.
	msg := f.buildMessage(20, "B-authored, never gossiped", 2_000)
	if err := f.nodes[20].storeM.Put(ctx, msg); err != nil {
		t.Fatalf("seed B store: %v", err)
	}

	// Drive A's fetchFrom directly — mirrors what reconcileWith or
	// executeRetry(opFetch) would do on the post-1.0.5 code path.
	if err := f.nodes[10].gossip.fetchFrom(ctx, 20, msg.ID); err != nil {
		t.Fatalf("fetchFrom on A: %v", err)
	}

	// A must now have the message locally (Put side-effect).
	has, _ := f.nodes[10].storeM.Has(ctx, f.groupID, msg.ID)
	if !has {
		t.Fatalf("A did not store message after fetchFrom")
	}

	// And A's refanout must have propagated to C. One A→C eager push
	// over net.Pipe is sub-millisecond; budget 2 s for scheduler
	// slack.
	waitUntil(t, 2*time.Second, "C stores message via A's post-fetch refanout", func() bool {
		has, _ := f.nodes[30].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})
}

// TestPlumtreeSkipsUntrusted is the v1.0.6 Fix A guard: the Plumtree
// fanout helpers must not return peers that Pilot reports as
// untrusted, so the originator never wastes a dial on a peer with no
// trust pair. The message must still reach the untrusted peer via a
// trusted hub's refanout — Plumtree's safety property is preserved as
// long as at least one forwarder has the missing edge.
//
// Topology: A=10, B=20, C=30; all three are roster members. A trusts
// only B (filteringTransport narrows both the Dial gate AND the
// TrustedPeers result). C is filtered symmetrically — C trusts only
// B — mirroring the live-mesh laptop↔Phobos scenario where neither
// edge has a Pilot pair with the other. B is the unfiltered hub so
// its refanout reaches both. Assert:
//
//  1. B receives the message via A's direct eager push.
//  2. A never invokes Transport.Dial(C) — the trust oracle filters C
//     out of the eager set before the fanout loop runs, and because
//     C also cannot dial A, no inbound-connection path wakes up A's
//     maybeReconcile to dial C either.
//  3. No retry entry for C is ever enqueued on A.
//  4. C still receives the message, via B's refanout.
func TestPlumtreeSkipsUntrusted(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30}) // A=10, B=20, C=30
	defer f.closeTransports()

	// A trusts only B. C trusts only B. B keeps the default unfiltered
	// memTransport so its refanout reaches everyone. This mirrors the
	// live-mesh topology where the two edge nodes lack a Pilot trust
	// pair and only the hub can bridge them.
	aFilter := newFilteringTransport(10, f.transports[10], []entmoot.NodeID{20})
	cFilter := newFilteringTransport(30, f.transports[30], []entmoot.NodeID{20})
	f.nodes[10].gossip.cfg.Transport = aFilter
	f.nodes[30].gossip.cfg.Transport = cFilter

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	msg := f.buildMessage(10, "trust-aware fanout", 2_000)
	if err := f.nodes[10].gossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}

	// B receives via A's direct eager push.
	waitUntil(t, 2*time.Second, "B stores A's message (trusted direct push)", func() bool {
		has, _ := f.nodes[20].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})

	// C still reaches consistency via B's refanout.
	waitUntil(t, 2*time.Second, "C stores A's message via B refanout", func() bool {
		has, _ := f.nodes[30].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})

	// Settle: let the retry loop tick once more than its interval so
	// any latent dial-to-C attempt would have surfaced by now.
	time.Sleep(retryTickInterval + 100*time.Millisecond)

	// Core assertion: A never even tried to dial C. Without Fix A, A
	// would have dialed C once in the initial fanout and ~10 more
	// times through the retry schedule before giving up.
	if n := aFilter.dialsTo(30); n != 0 {
		t.Fatalf("A dialed untrusted peer C %d times; want 0 (trust oracle should have filtered it)", n)
	}

	// And no retry entry for C should ever have been enqueued on A —
	// the trust oracle short-circuited before fanoutPush would have
	// called enqueueRetry.
	f.nodes[10].gossip.pendMu.Lock()
	for k := range f.nodes[10].gossip.pending {
		if k.peer == 30 {
			f.nodes[10].gossip.pendMu.Unlock()
			t.Fatalf("A has a pending retry entry for untrusted peer C: key=%+v", k)
		}
	}
	f.nodes[10].gossip.pendMu.Unlock()
}

// TestPlumtreeParallelFanoutWithSlowPeer is the v1.0.6 Fix B guard: one
// stalled peer must not head-of-line-block fanout to healthy peers, and
// every attempt must be bounded by fanoutPerPeerTimeout so a dead peer
// falls fast into the retry scheduler instead of consuming the full
// ~32 s Pilot SYN-retry budget.
//
// Topology: A=10, B=20, C=30; fully-trusted 3-node mesh. A's transport
// is wrapped in a filter that allows all dials but makes Dial(C) block
// for 30 s — far longer than the 5 s per-peer budget.
//
// With sequential pre-v1.0.6 fanout, a publish on A would stall for
// 30 s before B ever saw the message. With parallel fanout + per-peer
// timeout we assert:
//
//  1. B receives the message within ~1 s (its dial runs concurrently
//     with the stalled C dial, so B is unaffected by A's slow C dial).
//  2. A's pending map gains a retryKey{peer: 30, id: msg.ID, op: opPush}
//     entry well before the 30 s injected delay would have elapsed —
//     proving A's Dial(C) was cancelled by fanoutPerPeerTimeout (5 s)
//     and the failure was enqueued for retry rather than silently
//     dropped.
func TestPlumtreeParallelFanoutWithSlowPeer(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30}) // A=10, B=20, C=30
	defer f.closeTransports()

	// A's transport is a filter that allows all dials but makes Dial(C)
	// artificially slow. B and C keep the unfiltered memTransport so
	// their accept paths and any refanout they do proceed normally.
	aFilter := newFilteringTransport(10, f.transports[10], []entmoot.NodeID{20, 30})
	aFilter.setSlowDial(30, 30*time.Second) // well beyond fanoutPerPeerTimeout (5 s)
	f.nodes[10].gossip.cfg.Transport = aFilter

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	msg := f.buildMessage(10, "parallel fanout with slow C", 2_000)
	publishStart := time.Now()
	if err := f.nodes[10].gossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}

	// B must receive via A's direct eager push without being blocked by
	// the stalled Dial(C). Budget 1 s — eager push over net.Pipe is
	// sub-millisecond; anything beyond that is scheduler slack. Crucial
	// that this is < fanoutPerPeerTimeout so a regression to sequential
	// fanout (where Dial(C)'s 30 s stall blocks Dial(B)) fails loudly.
	waitUntil(t, 1*time.Second, "B stores A's message despite slow C", func() bool {
		has, _ := f.nodes[20].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})
	if elapsed := time.Since(publishStart); elapsed > 2*time.Second {
		t.Fatalf("B delivery took %v; parallel fanout should have delivered in <1s", elapsed)
	}

	// Wait for the per-peer timeout to expire (5 s) plus slack so the
	// pushGossip(C) goroutine has definitely returned and bumped the
	// retry slot. The 6.5 s cap is well under the 30 s injected delay,
	// so this also implicitly proves the timeout fired — if it hadn't,
	// the publish fanout would still be in flight and the retry slot
	// would not yet exist. The retry-slot appearance is the load-
	// bearing assertion for Fix B: it proves both that Dial(C) was
	// cancelled at 5 s (not 30 s) AND that the failure fell into the
	// retry scheduler instead of silently dropping the message.
	waitUntil(t, 6500*time.Millisecond, "A enqueues retry for slow C after per-peer timeout", func() bool {
		f.nodes[10].gossip.pendMu.Lock()
		defer f.nodes[10].gossip.pendMu.Unlock()
		_, ok := f.nodes[10].gossip.pending[retryKey{peer: 30, id: msg.ID, op: opPush}]
		return ok
	})
}

// TestPlumtreePerPeerDialBackoff is the v1.0.6 Fix C guard: once a
// dial to peer P fails, subsequent send attempts to P are short-
// circuited at the gossiper layer (no Transport.Dial invocation)
// until the exponential backoff window elapses. Prevents the retry
// scheduler's 10-step exponential schedule from multiplying a single
// dead peer's ~32 s Pilot dial tax across every pending message.
//
// Topology: A=10, B=20, C=30. A's transport is a filteringTransport
// that allows B only, so every Dial(C) returns net.ErrClosed.
//
// The test covers three layers:
//  1. Direct helper contract — canDial flips false after
//     recordDialFailure and true after recordDialSuccess.
//  2. Exponential growth — consecutive failures saturate at
//     dialBackoffCap and the window doubles as expected.
//  3. End-to-end — with the fake clock held fixed in the "backoff
//     window open" state, repeated pushGossip(C) attempts return
//     the synthetic "in dial-backoff" error without invoking
//     Transport.Dial. The dial count must stay at exactly 1
//     (the initial failing attempt) regardless of how many retries
//     fire.
func TestPlumtreePerPeerDialBackoff(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30})
	defer f.closeTransports()

	// A can dial B but not C. The filter's TrustedPeers already
	// intersects with `allowed`, so Fix A's trust oracle will filter C
	// out of A's eager fanout list — which means A's Publish() won't
	// attempt C directly. For this test we bypass the publish path
	// entirely and drive pushGossip / the helpers directly, which is
	// exactly the scenario the retry scheduler exercises when a
	// transient failure has already enqueued a slot that Fix A
	// couldn't pre-empt.
	aFilter := newFilteringTransport(10, f.transports[10], []entmoot.NodeID{20})
	f.nodes[10].gossip.cfg.Transport = aFilter

	aGossip := f.nodes[10].gossip
	fakeClk, ok := aGossip.clk.(*clock.Fake)
	if !ok {
		t.Fatalf("expected fixture to use clock.Fake, got %T", aGossip.clk)
	}
	ctx := context.Background()

	// Layer 1: direct helper contract.
	if !aGossip.canDial(30) {
		t.Fatalf("canDial(C) should be true before any failure")
	}
	aGossip.recordDialFailure(30)
	if aGossip.canDial(30) {
		t.Fatalf("canDial(C) should be false immediately after a dial failure")
	}
	aGossip.recordDialSuccess(30)
	if !aGossip.canDial(30) {
		t.Fatalf("canDial(C) should be true after a dial success clears backoff")
	}

	// Layer 2: exponential growth. First failure sets a window of
	// dialBackoffBase; advance by (base - 1 ms) and canDial is still
	// false, then by another 2 ms and it flips true.
	aGossip.recordDialFailure(30) // window = base
	fakeClk.Advance(dialBackoffBase - time.Millisecond)
	if aGossip.canDial(30) {
		t.Fatalf("canDial(C) should still be false just before the base window expires")
	}
	fakeClk.Advance(2 * time.Millisecond)
	if !aGossip.canDial(30) {
		t.Fatalf("canDial(C) should be true once the base window elapses")
	}
	// Time passing does not reset consecutiveFailures — only
	// recordDialSuccess does — so two more failures push
	// consecutiveFailures to 3 and the window to base*4. Advance by
	// base*3; still inside the window.
	aGossip.recordDialFailure(30) // consecutiveFailures=2, window=base*2
	aGossip.recordDialFailure(30) // consecutiveFailures=3, window=base*4
	fakeClk.Advance(3 * dialBackoffBase)
	if aGossip.canDial(30) {
		t.Fatalf("canDial(C) should still be false within the growing 4*base window")
	}
	aGossip.recordDialSuccess(30) // reset for layer 3

	// Layer 3: prove Transport.Dial is NOT invoked while the backoff
	// window is open. Drive a single direct pushGossip(C) so we see the
	// real dial path failure, then verify subsequent calls short-circuit.

	// A minimal Gossip frame suffices: pushGossip's canDial gate fires
	// before any frame encoding, and the underlying Dial is rejected
	// before EncodeAndWrite ever runs. Contents irrelevant.
	frame := &wire.Gossip{GroupID: f.groupID}

	// First attempt: hits the real dial path, records failure.
	if err := aGossip.pushGossip(ctx, 30, frame); err == nil {
		t.Fatalf("pushGossip(C) should fail; C is disallowed")
	}
	if n := aFilter.dialsTo(30); n != 1 {
		t.Fatalf("after first pushGossip, dialsTo(C) = %d; want 1", n)
	}
	// Window is now open (base = 1s); fake clock hasn't moved during
	// this layer, so subsequent attempts must short-circuit.
	for i := 0; i < 10; i++ {
		err := aGossip.pushGossip(ctx, 30, frame)
		if err == nil {
			t.Fatalf("pushGossip(C) attempt %d should fail (backoff)", i+2)
		}
		// Short-circuit error must NOT be wrapped as "dial: <underlying>";
		// it must be the synthetic "peer ... in dial-backoff" form. Check
		// the exact prefix to catch regressions where canDial returns true
		// but the underlying dial fails again.
		if got := err.Error(); !containsDialBackoff(got) {
			t.Fatalf("pushGossip(C) attempt %d error = %q; want dial-backoff sentinel", i+2, got)
		}
	}
	// Critical invariant: dial count must still be 1. All 10 follow-up
	// pushGossip calls short-circuited without reaching Transport.Dial.
	if n := aFilter.dialsTo(30); n != 1 {
		t.Fatalf("dialsTo(C) = %d after 10 short-circuit attempts; want 1", n)
	}

	// Advance past the backoff window and verify one more attempt DOES
	// reach Transport.Dial (proving the gate eventually reopens).
	fakeClk.Advance(2 * dialBackoffCap) // well past any window
	if err := aGossip.pushGossip(ctx, 30, frame); err == nil {
		t.Fatalf("pushGossip(C) post-window should still fail via Transport.Dial")
	}
	if n := aFilter.dialsTo(30); n != 2 {
		t.Fatalf("dialsTo(C) = %d after post-window attempt; want 2", n)
	}
}

// containsDialBackoff reports whether s contains the synthetic
// short-circuit marker produced by every Transport.Dial call-site
// when canDial returns false. Kept as a helper so a rename of the
// marker only breaks one spot.
func containsDialBackoff(s string) bool {
	const marker = "in dial-backoff"
	for i := 0; i+len(marker) <= len(s); i++ {
		if s[i:i+len(marker)] == marker {
			return true
		}
	}
	return false
}

// TestGossipInlineBodySkipsFetch is the v1.0.7 Fix A happy-path guard:
// when a Gossip frame carries an inline Body, the receiver stores the
// message without having to dial the sender back for a FetchReq
// round-trip. We drive onGossip directly with a Publish-shaped frame
// (A.Publish inlines automatically for small bodies) and assert that
// the full Publish → onGossip pipeline produces both (a) an inlined
// Body on the outbound frame and (b) a successful Store.Put on the
// receiver, all without any B→A dial.
//
// Topology: A=10, B=20 (two-node). B's transport is wrapped with an
// empty allow-set so every B→A Dial is refused with net.ErrClosed
// — any reliance on fetchFrom would leave B.Has == false forever.
// The test bypasses B's accept loop (no inbound connection ⇒ no
// maybeReconcile spurious dial) by feeding the frame straight into
// onGossip. That isolates the inline-body logic from unrelated
// reconcile dials.
func TestGossipInlineBodySkipsFetch(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20}) // A=10, B=20
	defer f.closeTransports()

	// B can't dial anyone: the allow-set is empty so every Dial
	// returns net.ErrClosed. This eliminates the v1.0.6 fetch path
	// entirely — only the inline-body path can deliver.
	bFilter := newFilteringTransport(20, f.transports[20], nil)
	f.nodes[20].gossip.cfg.Transport = bFilter

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Suppress the async maybeReconcile that fires after a successful
	// onGossip body store: seed B's lastReconciled[10] to "just now"
	// so the cooldown gate short-circuits the spawn before it can
	// dial A. This isolates the dial-count assertion to purely the
	// fetch-vs-inline question the test is actually about.
	bG := f.nodes[20].gossip
	bG.pendMu.Lock()
	// v1.0.7: lastReconciled now carries (at, cooldown). Seed a long
	// cooldown (60 s) so the maybeReconcile gate short-circuits for
	// the entire duration of the test window regardless of the jitter
	// value currently drawn by jitteredReconcileCooldown.
	bG.lastReconciled[10] = reconcileState{at: bG.clk.Now(), cooldown: 60 * time.Second}
	bG.pendMu.Unlock()

	// Build a small message and the Gossip frame A.Publish would
	// produce. maybeInlineBody attaches the body because the
	// canonical encoding is well under inlineBodyThreshold (4 KiB).
	msg := f.buildMessage(10, "inline me", 2_000)
	if err := f.nodes[10].storeM.Put(ctx, msg); err != nil {
		t.Fatalf("seed A store: %v", err)
	}
	frame := &wire.Gossip{
		GroupID:   f.groupID,
		IDs:       []entmoot.MessageID{msg.ID},
		Timestamp: 3_000,
	}
	maybeInlineBody(frame, msg)
	if frame.Body == nil {
		t.Fatalf("maybeInlineBody did not inline a small message (body too large?)")
	}
	if err := signGossip(frame, f.nodes[10].id); err != nil {
		t.Fatalf("signGossip: %v", err)
	}

	// Deliver directly into B's onGossip (same path handleConn would
	// invoke after a real Accept). Inline branch should Store.Put and
	// return; no FetchReq round-trip required.
	f.nodes[20].gossip.onGossip(ctx, 10, frame)

	has, _ := f.nodes[20].storeM.Has(ctx, f.groupID, msg.ID)
	if !has {
		t.Fatalf("B did not store msg after onGossip with inline body")
	}

	// Load-bearing assertion: B never dialed A. With Fix A the inline
	// branch accepts the message without touching Transport; without
	// Fix A, B would have to fetchFrom(A) which in this fixture is
	// blocked by bFilter but would still bump dialsTo(10).
	if n := bFilter.dialsTo(10); n != 0 {
		t.Fatalf("B dialed A %d times; want 0 (inline body should skip fetchFrom)", n)
	}
}

// TestGossipInlineBodyHashMismatchRejected is the v1.0.7 Fix A
// integrity guard: a Gossip frame whose inline Body has an id that
// matches the advertised id but whose canonical-hashed content
// hashes to a different value must be rejected on the hash-mismatch
// branch, not stored. We forge such a frame by constructing a
// legitimate-looking Body, overwriting Body.ID to point at a
// different message's id, and verifying the receiver's store stays
// empty. Using onGossip directly avoids dragging the accept /
// Transport layer into the integrity-logic assertion.
//
// B's transport is filtered with an empty allow-set so a bug where
// the hash-mismatch path falls through to fetchFrom (instead of
// `continue`) would fail loudly — B can't dial A — rather than
// silently pulling the "real" body and masking the regression.
func TestGossipInlineBodyHashMismatchRejected(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20}) // A=10 sender, B=20 receiver
	defer f.closeTransports()

	bFilter := newFilteringTransport(20, f.transports[20], nil)
	f.nodes[20].gossip.cfg.Transport = bFilter

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two legitimate A-authored messages. We'll advertise real1.ID
	// and craft a forged Body whose ID field has been overwritten to
	// real1.ID — so the onGossip precondition `Body.ID == id` holds
	// — but whose canonical-hashed content still produces real2.ID.
	real1 := f.buildMessage(10, "real message 1", 2_000)
	real2 := f.buildMessage(10, "real message 2 — forged body", 3_000)
	forged := real2
	forged.ID = real1.ID // lie: claim to be real1, content is real2
	// Sanity: the forged body's claimed ID should NOT match its
	// canonical hash, which is exactly the condition the hash-mismatch
	// branch catches.
	if canonical.MessageID(forged) == forged.ID {
		t.Fatalf("test setup bug: forged.ID happens to match canonical hash")
	}

	frame := &wire.Gossip{
		GroupID:   f.groupID,
		IDs:       []entmoot.MessageID{real1.ID},
		Timestamp: 4_000,
		Body:      &forged,
	}
	if err := signGossip(frame, f.nodes[10].id); err != nil {
		t.Fatalf("signGossip: %v", err)
	}

	f.nodes[20].gossip.onGossip(ctx, 10, frame)

	// Neither id should be stored: the advertised id's body was
	// rejected on hash mismatch and the loop `continue`d, so we did
	// not fall through to fetchFrom. real2.ID was never advertised,
	// so it never entered the loop either. No successful B→A dial.
	if has, _ := f.nodes[20].storeM.Has(ctx, f.groupID, real1.ID); has {
		t.Fatalf("B stored real1.ID despite inline body hash mismatch")
	}
	if has, _ := f.nodes[20].storeM.Has(ctx, f.groupID, real2.ID); has {
		t.Fatalf("B stored real2.ID from a frame that didn't advertise it")
	}
	if n := bFilter.dialsTo(10); n != 0 {
		t.Fatalf("B dialed A %d times; want 0 (hash mismatch should `continue`, not fall through to fetchFrom)", n)
	}
}

// TestGossipInlineBodyBackwardCompat is the v1.0.7 Fix A mixed-version
// guard: a Gossip frame shaped exactly like v1.0.6 (Body==nil) must
// still propagate via the fetchFrom path so a v1.0.7 receiver remains
// wire-compatible with v1.0.6 senders. We simulate an old-style sender
// by constructing the frame directly with no Body (bypassing Publish
// and its maybeInlineBody call), then feeding it into B's onGossip.
// B must then dial A for a FetchReq and store the message.
func TestGossipInlineBodyBackwardCompat(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20}) // A=10, B=20
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	// Seed A's store so a subsequent B→A FetchReq can actually
	// resolve the body. Published via A.Publish would also inline; we
	// want to bypass inlining entirely and prove the v1.0.6 shape
	// still works, so Put directly.
	msg := f.buildMessage(10, "no inline — v1.0.6 shape", 2_000)
	if err := f.nodes[10].storeM.Put(ctx, msg); err != nil {
		t.Fatalf("seed A store: %v", err)
	}

	// Hand-craft a Gossip frame with Body==nil — the exact shape a
	// v1.0.6 sender would produce. signGossip's v1.0.7 scope matches
	// the v1.0.6 scope byte-for-byte when Body is nil, so the
	// signature an old sender produced would verify identically here.
	frame := &wire.Gossip{
		GroupID:   f.groupID,
		IDs:       []entmoot.MessageID{msg.ID},
		Timestamp: 3_000,
	}
	if err := signGossip(frame, f.nodes[10].id); err != nil {
		t.Fatalf("signGossip: %v", err)
	}

	f.nodes[20].gossip.onGossip(ctx, 10, frame)

	// B must have fetched the body via the v1.0.6 path and stored it.
	waitUntil(t, 2*time.Second, "B stores message via fetchFrom fallback (v1.0.6 shape)", func() bool {
		has, _ := f.nodes[20].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})
}

// TestRetryBackoffDecorrelatedJitter drives nextBackoff many times and
// asserts the decorrelated-jitter envelope: every sample stays within
// [retryBackoffBase, retryBackoffCap], the distribution is not
// collapsed to a single value, and successive draws from the same
// prev diverge — proving the schedule is randomised rather than
// deterministic (v1.0.7 Fix B).
func TestRetryBackoffDecorrelatedJitter(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	g := f.nodes[10].gossip

	const samples = 1000
	distinctFirst := make(map[time.Duration]struct{}, 8)
	for i := 0; i < samples; i++ {
		// prev==0 exercises the first-failure path. Per the formula,
		// hi=base+1 so next == base exactly; record that it stays
		// within the envelope regardless.
		d := g.nextBackoff(0)
		if d < retryBackoffBase || d > retryBackoffCap {
			t.Fatalf("nextBackoff(0) out of envelope: %v", d)
		}
		distinctFirst[d] = struct{}{}
	}

	// Drive a growing prev through the cap to sample the "real"
	// jittered window. prev=20s produces hi=60s, so the draw is
	// uniform over [1s, 60s).
	prev := 20 * time.Second
	distinct := make(map[time.Duration]struct{}, 64)
	for i := 0; i < samples; i++ {
		d := g.nextBackoff(prev)
		if d < retryBackoffBase || d > retryBackoffCap {
			t.Fatalf("nextBackoff(%v) out of envelope: %v", prev, d)
		}
		distinct[d] = struct{}{}
	}
	if len(distinct) < 50 {
		t.Fatalf("expected >=50 distinct values across %d samples, got %d",
			samples, len(distinct))
	}

	// Two successive draws from the same prev must differ with
	// overwhelming probability under a uniform distribution.
	a := g.nextBackoff(prev)
	b := g.nextBackoff(prev)
	if a == b {
		// Exceedingly unlikely; retry once before failing.
		a = g.nextBackoff(prev)
		b = g.nextBackoff(prev)
		if a == b {
			t.Fatalf("two successive draws from prev=%v were identical: %v", prev, a)
		}
	}
}

// TestReconcileCooldownJittered confirms jitteredReconcileCooldown
// produces samples in [Base-Jitter, Base+Jitter) and that the
// distribution is not collapsed to a single value (v1.0.7 Fix B).
func TestReconcileCooldownJittered(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	g := f.nodes[10].gossip

	const samples = 1000
	lo := reconcileCooldownBase - reconcileCooldownJitter
	hi := reconcileCooldownBase + reconcileCooldownJitter
	distinct := make(map[time.Duration]struct{}, 64)
	for i := 0; i < samples; i++ {
		d := g.jitteredReconcileCooldown()
		if d < lo || d >= hi {
			t.Fatalf("jitteredReconcileCooldown out of [%v,%v): %v", lo, hi, d)
		}
		distinct[d] = struct{}{}
	}
	if len(distinct) < 50 {
		t.Fatalf("expected >=50 distinct cooldown values across %d samples, got %d",
			samples, len(distinct))
	}
}
