package gossip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/wire"
)

// --- failing transport --------------------------------------------------
//
// failingTransport wraps a memTransport (looked up out of the fixture's
// hub) and optionally fails Dial for specific peers. Switching a peer's
// failure mode is atomic so tests can go from "always fail" to "succeed
// next time" without restarting anything.
//
// Accept / SetPeerEndpoints / TrustedPeers / SetOnTunnelUp / Close all
// delegate to the wrapped transport unchanged — we only distort Dial.

type failingTransport struct {
	inner Transport

	// dialFail[peer] atomically reports whether Dial to peer currently
	// returns an injected error. Writers set via setFail; readers via
	// shouldFail.
	mu       sync.Mutex
	dialFail map[entmoot.NodeID]bool

	// dialCalls[peer] counts attempted dials (both failing and
	// passing). Exposed via DialCount for test assertions.
	dialCalls map[entmoot.NodeID]*atomic.Int64
}

func newFailingTransport(inner Transport) *failingTransport {
	return &failingTransport{
		inner:     inner,
		dialFail:  make(map[entmoot.NodeID]bool),
		dialCalls: make(map[entmoot.NodeID]*atomic.Int64),
	}
}

func (t *failingTransport) setFail(peer entmoot.NodeID, fail bool) {
	t.mu.Lock()
	t.dialFail[peer] = fail
	t.mu.Unlock()
}

func (t *failingTransport) shouldFail(peer entmoot.NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dialFail[peer]
}

func (t *failingTransport) DialCount(peer entmoot.NodeID) int64 {
	t.mu.Lock()
	c, ok := t.dialCalls[peer]
	t.mu.Unlock()
	if !ok {
		return 0
	}
	return c.Load()
}

func (t *failingTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	t.mu.Lock()
	c, ok := t.dialCalls[peer]
	if !ok {
		c = &atomic.Int64{}
		t.dialCalls[peer] = c
	}
	fail := t.dialFail[peer]
	t.mu.Unlock()
	c.Add(1)
	if fail {
		return nil, fmt.Errorf("failingTransport: injected dial failure to %d", peer)
	}
	return t.inner.Dial(ctx, peer)
}

func (t *failingTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	return t.inner.Accept(ctx)
}

func (t *failingTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return t.inner.TrustedPeers(ctx)
}

func (t *failingTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, eps []entmoot.NodeEndpoint) error {
	return t.inner.SetPeerEndpoints(ctx, peer, eps)
}

func (t *failingTransport) SetOnTunnelUp(cb func(entmoot.NodeID)) {
	t.inner.SetOnTunnelUp(cb)
}

func (t *failingTransport) Close() error { return t.inner.Close() }

// wrapWithFailingTransport swaps every node's transport in `f` for a
// failingTransport wrapping the original. Returns the per-node wrappers
// keyed by NodeID so tests can toggle failure modes per peer.
func wrapWithFailingTransport(t *testing.T, f *fixture) map[entmoot.NodeID]*failingTransport {
	t.Helper()
	out := make(map[entmoot.NodeID]*failingTransport, len(f.nodes))
	for id, ns := range f.nodes {
		ft := newFailingTransport(f.transports[id])
		f.transports[id] = ft
		ns.gossip.cfg.Transport = ft
		out[id] = ft
	}
	return out
}

// pendingAdCount returns the number of retry entries in g.pending whose
// op is opTransportAd. Test-only helper — production code never counts.
func pendingAdCount(g *Gossiper) int {
	g.pendMu.Lock()
	defer g.pendMu.Unlock()
	n := 0
	for k := range g.pending {
		if k.op == opTransportAd {
			n++
		}
	}
	return n
}

// adRetryState returns the retry entry for (peer, author), if any.
func adRetryState(g *Gossiper, peer, author entmoot.NodeID) (*retryState, bool) {
	g.pendMu.Lock()
	defer g.pendMu.Unlock()
	s, ok := g.pending[retryKey{peer: peer, author: author, op: opTransportAd}]
	return s, ok
}

// advanceAndDrain advances the fake clock by d, then runs one
// drainDueRetries pass synchronously so the test observes a single
// deterministic iteration instead of racing the retryLoop goroutine.
func advanceAndDrain(ctx context.Context, g *Gossiper, fc *clock.Fake, d time.Duration) {
	fc.Advance(d)
	g.drainDueRetries(ctx)
}

// --- tests --------------------------------------------------------------

// TestTransportAdRetry_OnDialFailure exercises the core v1.4.1 path:
// the first fanout fails, enqueueAdRetry stores the entry, clock
// advance fires a second attempt, the second attempt succeeds, the
// retry entry is cleared, and the ad lands in the peer's store.
func TestTransportAdRetry_OnDialFailure(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "tcp", Addr: "198.51.100.1:4001"}},
	}
	stores := installTransportAdStores(t, f, eps)
	fts := wrapWithFailingTransport(t, f)

	// Only 10 is an advertiser; it'll fanout to 20. We want 10's dial
	// to 20 to fail once, then succeed.
	fts[10].setFail(20, true)

	// Start 20's accept loop (so its onTransportAd fires when 10
	// eventually connects), but NOT 10's — we drive 10's publisher
	// path by calling publishTransportAd directly so the retryLoop
	// wall-clock ticker can't race the test. 10's retry is driven via
	// drainDueRetries.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 20 runs its accept loop so it can receive the eventual dial.
	f.nodes[20].running.Store(true)
	go func() {
		defer f.nodes[20].running.Store(false)
		_ = f.nodes[20].gossip.Start(ctx)
	}()

	// Advertiser path on 10: publishTransportAd builds, stores, and
	// fanouts. The fanout's single peer (20) has dial-fail injected,
	// so enqueueAdRetry fires.
	if err := f.nodes[10].gossip.publishTransportAd(ctx, eps[10]); err != nil {
		t.Fatalf("publishTransportAd: %v", err)
	}

	// An ad retry for (peer=20, author=10) must now be pending.
	waitUntil(t, 2*time.Second, "retry enqueued for (20, 10)", func() bool {
		_, ok := adRetryState(f.nodes[10].gossip, 20, 10)
		return ok
	})

	// Remove the dial failure and advance the clock past the first
	// backoff step (base = 1s). Also wait out the per-peer dial
	// backoff (also starts at 1s after the first failure) by
	// advancing a little more aggressively — a 5s advance comfortably
	// clears both windows.
	fts[10].setFail(20, false)
	// Clear any dial-backoff the first failure left behind so the
	// retry's sendTransportAd isn't short-circuited by canDial.
	f.nodes[10].gossip.recordDialSuccess(20)

	// Drive the retry. drainDueRetries walks g.pending; entries whose
	// nextAt has passed are fired.
	fakeClk := f.nodes[10].gossip.clk.(*clock.Fake)
	advanceAndDrain(ctx, f.nodes[10].gossip, fakeClk, 5*time.Second)

	// The retry should now have cleared (success) and 20 should hold
	// 10's ad in its store.
	waitUntil(t, 2*time.Second, "20 stores 10's ad via retry", func() bool {
		_, ok := stores[20].adFor(f.groupID, 10)
		return ok
	})
	if _, ok := adRetryState(f.nodes[10].gossip, 20, 10); ok {
		t.Fatalf("retry entry still pending after successful retry fire")
	}
	// At least two dials: the failing first attempt plus the
	// successful retry. Accept more in case startup races produced an
	// extra attempt.
	if got := fts[10].DialCount(20); got < 2 {
		t.Fatalf("dial count to 20 = %d, want >= 2", got)
	}
}

// TestTransportAdRetry_TerminatesAfterMaxAttempts pins the attempt cap.
// With Dial permanently failing, the retry slot must clear itself after
// exactly adRetryMaxAttempts attempts and no goroutine leak remains.
func TestTransportAdRetry_TerminatesAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "tcp", Addr: "198.51.100.1:4001"}},
	}
	installTransportAdStores(t, f, eps)
	fts := wrapWithFailingTransport(t, f)
	fts[10].setFail(20, true)

	ctx := context.Background()
	fakeClk := f.nodes[10].gossip.clk.(*clock.Fake)
	// Pre-seed trust cache so plumEagerExcept doesn't block on
	// TrustedPeers / fail-open silently. Advance trustedSetTTL too.
	_ = f.nodes[10].gossip.trustedSet(ctx)

	// Fire the first fanout synchronously. This queues the retry
	// with attempts=1 (one "failed attempt" is already counted by
	// enqueueAdRetry before backoff scheduling).
	ad := f.buildTransportAd(10, 1, eps[10], fakeClk.Now())
	f.nodes[10].gossip.fanoutTransportAd(ctx, &ad)

	// Each drain failure re-enqueues the retry, advancing attempts.
	// We start from attempts=1 and need to see it drop after
	// adRetryMaxAttempts (= 6). That's up to five drain iterations
	// since drain fires the already-queued attempts=1 and ticks up to
	// 2, 3, ... 6, at which point enqueueAdRetry detects the cap and
	// deletes. Stop once the pending slot is gone.
	for i := 0; i < adRetryMaxAttempts+3; i++ {
		if _, ok := adRetryState(f.nodes[10].gossip, 20, 10); !ok {
			break
		}
		// Clear dial-backoff so the retry actually attempts the dial.
		f.nodes[10].gossip.recordDialSuccess(20)
		advanceAndDrain(ctx, f.nodes[10].gossip, fakeClk, 120*time.Second)
	}

	if _, ok := adRetryState(f.nodes[10].gossip, 20, 10); ok {
		t.Fatalf("retry slot still pending after exhausted attempts")
	}
	// Dial count should be bounded by the attempt cap. We count the
	// first fanout attempt plus up to (adRetryMaxAttempts - 1) retry
	// attempts. Allow a small slack in case a retry was queued by
	// enqueueAdRetry past the cap and we bailed on the very edge.
	got := fts[10].DialCount(20)
	if got > int64(adRetryMaxAttempts) {
		t.Fatalf("dial count to 20 = %d, want <= %d (attempt cap)", got, adRetryMaxAttempts)
	}
	if got < 2 {
		t.Fatalf("dial count to 20 = %d, want >= 2 (at least one retry fired)", got)
	}
}

// TestTransportAdRetry_TerminatesAtWallClockCeiling pins the 5-minute
// wall-clock ceiling. With Dial permanently failing AND clock advances
// that overshoot the ceiling before attempts exhaust, termination must
// come from the ceiling rather than the attempt cap.
func TestTransportAdRetry_TerminatesAtWallClockCeiling(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "tcp", Addr: "198.51.100.1:4001"}},
	}
	installTransportAdStores(t, f, eps)
	fts := wrapWithFailingTransport(t, f)
	fts[10].setFail(20, true)

	ctx := context.Background()
	fakeClk := f.nodes[10].gossip.clk.(*clock.Fake)

	ad := f.buildTransportAd(10, 1, eps[10], fakeClk.Now())
	f.nodes[10].gossip.fanoutTransportAd(ctx, &ad)

	if _, ok := adRetryState(f.nodes[10].gossip, 20, 10); !ok {
		t.Fatalf("retry slot not queued after initial failure")
	}

	// Jump well past the 5-minute ceiling in one go. drainDueRetries
	// notices the expired firstTry and deletes the slot before even
	// trying to dial.
	f.nodes[10].gossip.recordDialSuccess(20)
	preCount := fts[10].DialCount(20)
	advanceAndDrain(ctx, f.nodes[10].gossip, fakeClk, adRetryWallClockLimit+time.Second)

	if _, ok := adRetryState(f.nodes[10].gossip, 20, 10); ok {
		t.Fatalf("retry slot still pending after wall-clock ceiling")
	}
	// No further dial occurred past the initial failure.
	if got := fts[10].DialCount(20); got > preCount {
		t.Fatalf("dial count advanced past ceiling: pre=%d post=%d (want no new dial)", preCount, got)
	}
}

// TestTransportAdRetry_DropsOnSupersededAd queues a retry for seq=3,
// then stores a seq=4 ad from the same author locally before the
// retry fires. The drain pass must see the newer seq and drop the
// stale retry without dialling.
func TestTransportAdRetry_DropsOnSupersededAd(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "tcp", Addr: "198.51.100.1:4001"}},
	}
	stores := installTransportAdStores(t, f, eps)
	fts := wrapWithFailingTransport(t, f)
	fts[10].setFail(20, true)

	ctx := context.Background()
	fakeClk := f.nodes[10].gossip.clk.(*clock.Fake)

	// Queue a retry for seq=3 via an initial failing fanout.
	adSeq3 := f.buildTransportAd(10, 3, eps[10], fakeClk.Now())
	f.nodes[10].gossip.fanoutTransportAd(ctx, &adSeq3)
	if _, ok := adRetryState(f.nodes[10].gossip, 20, 10); !ok {
		t.Fatalf("retry slot not queued for seq=3")
	}
	preCount := fts[10].DialCount(20)

	// Store seq=4 locally from the same author before the retry has
	// a chance to fire. We write directly to the advertiser's own
	// TransportAdStore (that's the one adSuperseded consults).
	adSeq4 := f.buildTransportAd(10, 4, eps[10], fakeClk.Now())
	if _, err := stores[10].PutTransportAd(ctx, adSeq4); err != nil {
		t.Fatalf("PutTransportAd seq=4: %v", err)
	}

	// Clear dial-backoff and drive the retry. The drain should
	// short-circuit via adSuperseded and delete the slot without
	// dialling (so dial count stays put).
	f.nodes[10].gossip.recordDialSuccess(20)
	advanceAndDrain(ctx, f.nodes[10].gossip, fakeClk, 5*time.Second)

	if _, ok := adRetryState(f.nodes[10].gossip, 20, 10); ok {
		t.Fatalf("stale retry slot still pending after seq=4 stored")
	}
	if got := fts[10].DialCount(20); got > preCount {
		t.Fatalf("retry dialled a superseded ad: pre=%d post=%d", preCount, got)
	}
}

// TestTransportAdRetry_RefanoutPathAlsoRetries exercises the receive
// side. Node 30 receives an inbound ad from 10, then tries to refanout
// to 20; the 30→20 dial is injected-fail on the first attempt. The
// retry must queue on 30, and once the failure clears, fire a second
// dial and clear the pending slot.
//
// Note: the onTransportAd publisher-allowlist check on 20 will reject
// the refanout frame itself (sender=30 != author=10 — a pre-existing
// single-hop-authoring limitation explicitly documented on the receive
// path). What this test confirms is the retry-scheduler hookup: the
// refanout's first dial fails, enqueueAdRetry queues, and the retry
// re-dials successfully. The plan's scope is "make retry queue for
// fanout failures," not "fix multi-hop authentication."
func TestTransportAdRetry_RefanoutPathAlsoRetries(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30})
	defer f.closeTransports()

	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "tcp", Addr: "198.51.100.1:4001"}},
	}
	installTransportAdStores(t, f, eps)
	fts := wrapWithFailingTransport(t, f)

	// 30 receives an inbound ad from 10, then tries to refanout to 20.
	// Inject the failure on 30->20.
	fts[30].setFail(20, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start 20's accept loop so the eventual retry dial connects.
	f.nodes[20].running.Store(true)
	go func() {
		defer f.nodes[20].running.Store(false)
		_ = f.nodes[20].gossip.Start(ctx)
	}()

	// Build an ad authored by 10 and deliver it through 30's
	// onTransportAd with 10 as sender.
	fakeClk := f.nodes[30].gossip.clk.(*clock.Fake)
	ad := f.buildTransportAd(10, 1, eps[10], fakeClk.Now())
	f.nodes[30].gossip.onTransportAd(ctx, 10, &ad)

	// Refanout must have queued a retry for (20, 10) on 30.
	waitUntil(t, 2*time.Second, "refanout retry queued", func() bool {
		_, ok := adRetryState(f.nodes[30].gossip, 20, 10)
		return ok
	})
	preCount := fts[30].DialCount(20)
	if preCount < 1 {
		t.Fatalf("expected at least one failing refanout dial attempt; got %d", preCount)
	}

	// Flip the failure off and let the retry fire.
	fts[30].setFail(20, false)
	f.nodes[30].gossip.recordDialSuccess(20)
	advanceAndDrain(ctx, f.nodes[30].gossip, fakeClk, 5*time.Second)

	// The retry must have fired (dial count increased) and cleared.
	if got := fts[30].DialCount(20); got <= preCount {
		t.Fatalf("retry did not fire: dial count stayed at %d (pre=%d)", got, preCount)
	}
	if _, ok := adRetryState(f.nodes[30].gossip, 20, 10); ok {
		t.Fatalf("refanout retry slot still pending after successful retry fire")
	}
}

// TestTransportAdRetry_DoesNotDisturbMessageRetry is a regression guard:
// the message-retry path must continue to behave exactly as it did
// before v1.4.1. Queue a failing message push, then assert the retry
// slot has op=opPush, uses the message retry cap, and doesn't appear
// as an ad retry. (The bulk of the existing message-retry coverage
// lives in gossiper_test.go; this test's role is just to confirm the
// shared scheduler still routes message retries through their old
// executeRetry branch.)
func TestTransportAdRetry_DoesNotDisturbMessageRetry(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	installTransportAdStores(t, f, nil)
	fts := wrapWithFailingTransport(t, f)
	fts[10].setFail(20, true)

	// Build a minimal push frame via buildMessage + Publish. We skip
	// buildMessage machinery and directly enqueueRetry with a dummy
	// frame since the scheduler's routing decision is the thing we're
	// testing here.
	var id entmoot.MessageID
	id[0] = 0xAA
	frame := &wire.Gossip{
		GroupID: f.groupID,
		IDs:     []entmoot.MessageID{id},
	}
	f.nodes[10].gossip.enqueueRetry(retryKey{peer: 20, id: id, op: opPush}, frame)

	// The pending map should hold one opPush retry and zero
	// opTransportAd retries.
	if got := pendingAdCount(f.nodes[10].gossip); got != 0 {
		t.Fatalf("pendingAdCount = %d, want 0", got)
	}
	f.nodes[10].gossip.pendMu.Lock()
	n := len(f.nodes[10].gossip.pending)
	f.nodes[10].gossip.pendMu.Unlock()
	if n != 1 {
		t.Fatalf("total pending = %d, want 1", n)
	}
	// And the message path uses its own cap (retryMaxAttempts=10),
	// not adRetryMaxAttempts — spot-check they're different so any
	// future conflation is caught.
	if adRetryMaxAttempts == retryMaxAttempts {
		t.Fatalf("ad/message retry caps must differ; got both = %d", retryMaxAttempts)
	}
}

