package gossip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"
)

// Retry scheduler constants (patch 6). The schedule is ~5 minutes of total
// effort per (peer, message, op), long enough to cover the typical
// consumer-NAT flap window while bounded so a permanently-unreachable peer
// doesn't accumulate unbounded pending state.
var retryBackoff = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	30 * time.Second,
	60 * time.Second,
	60 * time.Second,
	60 * time.Second,
	60 * time.Second,
}

// retryTickInterval is how often retryLoop wakes to check for due retries.
// 500ms keeps the worst-case scheduling jitter well under the smallest
// backoff step (1s).
const retryTickInterval = 500 * time.Millisecond

// trustedSetTTL bounds how stale the trust oracle may be. Short
// enough that a just-completed handshake is visible within one
// retryTickInterval; long enough that a bursty fanout doesn't
// hammer Pilot IPC per peer per message.
const trustedSetTTL = 1 * time.Second

// fanoutPerPeerTimeout is the wallclock ceiling for a single
// push / IHave / retry attempt. Entmoot calls Transport.Dial +
// EncodeAndWrite within this budget; if Pilot's internal ~32 s
// SYN-retry cycle hasn't produced a tunnel by this deadline, cut
// the attempt and let the retry scheduler re-queue. Value covers
// relay-over-beacon p99 (~600 ms) plus slack. Much shorter than
// Pilot's internal dial ceiling so a dead peer fails fast into
// the v1.0.6 per-peer dial-backoff cache (Fix C) instead of
// head-of-line-blocking healthy peers. (v1.0.6)
const fanoutPerPeerTimeout = 5 * time.Second

// fanoutMaxConcurrency caps the goroutines we spawn per fanout
// burst. Irrelevant at N=3 but a cheap guard against accidentally
// launching thousands of in-flight dials if the roster grows. Mirrors
// typical libp2p SetLimit values. (v1.0.6)
const fanoutMaxConcurrency = 32

// dialBackoffBase / dialBackoffCap bound the per-peer exponential
// dial-backoff cache (v1.0.6 Fix C). Once a dial to peer P fails,
// subsequent send attempts to P are short-circuited with a synthetic
// error (no Transport.Dial invocation) until nextAllowed. Successful
// dial clears the window. Prevents 10 concurrent retry slots from
// all independently paying Pilot's ~32 s dial tax on a dead peer.
// Formula: backoff = min(dialBackoffBase * 2^consecutiveFailures, dialBackoffCap).
// Matches libp2p swarm.DialBackoff (5-minute cap is their default).
const (
	dialBackoffBase = 1 * time.Second
	dialBackoffCap  = 5 * time.Minute
)

// Reconciliation cooldown (patch 7). Limits how often we fire a full
// MerkleReq round-trip against any single peer so a chatty connection
// pattern doesn't turn into a reconciliation storm.
const reconcileCooldown = 60 * time.Second

// Plumtree GRAFT timeout (v1.0.4). After receiving an IHave advertising
// an unknown id, wait this long for the body to arrive via eager push
// (possibly from a different peer) before graft-requesting it. Matches
// libp2p GossipSub's IWantFollowupTime default (3 s) — short enough that
// missing messages recover within a human-perceptible window, long
// enough that transient latency doesn't thrash the spanning tree.
const graftTimeout = 3 * time.Second

// retryOp distinguishes push and fetch in the pending map key so the same
// (peer, id) pair can have independent retries in both directions.
type retryOp uint8

const (
	opPush  retryOp = 1
	opFetch retryOp = 2
	// Plumtree control-frame retry ops (v1.0.4). Each follows the same
	// exponential-backoff schedule as opPush; the executeRetry switch
	// knows how to rebuild the outbound frame from the retry state.
	opIHave retryOp = 3
	opGraft retryOp = 4
	opPrune retryOp = 5
)

func (o retryOp) String() string {
	switch o {
	case opPush:
		return "push"
	case opFetch:
		return "fetch"
	case opIHave:
		return "ihave"
	case opGraft:
		return "graft"
	case opPrune:
		return "prune"
	default:
		return "unknown"
	}
}

// retryKey identifies a single pending retry slot. One slot per
// (peer, message, direction).
type retryKey struct {
	peer entmoot.NodeID
	id   entmoot.MessageID
	op   retryOp
}

// retryState tracks how many attempts have run and when the next is due.
// frame is populated only for pushes — fetches re-issue FetchReq from id.
type retryState struct {
	attempts int
	nextAt   time.Time
	frame    *wire.Gossip
}

// defaultFanout is the number of random peers a Publish pushes a new message
// id to when Config.Fanout is zero. Chosen for the canary (3-node group); the
// real gossip math is O(log N + k) and we'll revisit for v1.
const defaultFanout = 3

// Config parameterizes a Gossiper. Every field except Fanout, Clock, and
// Logger is required; New returns an error if a required field is nil or
// zero. Construct one Gossiper per group per process.
type Config struct {
	// LocalNode is the Pilot node id of this peer. Used to exclude self from
	// peer sampling, to label outbound Gossip frames, and to sanity-check
	// inbound FetchResp signatures.
	LocalNode entmoot.NodeID
	// Identity signs outbound Gossip frames. Must be the same Ed25519 key that
	// the roster pairs with LocalNode.
	Identity *keystore.Identity
	// Roster is the group membership log. Required for signature verification
	// (inbound messages, inbound FetchResp bodies) and for peer sampling.
	Roster *roster.RosterLog
	// Store is where incoming message bodies and locally-published bodies are
	// persisted.
	Store store.MessageStore
	// Transport is the dial / listen / trust surface. In tests this is the
	// in-memory mock from NewMemTransports; in production Phase E wires it
	// to Pilot's driver.
	Transport Transport
	// GroupID identifies the single group this Gossiper serves.
	GroupID entmoot.GroupID
	// Fanout is the number of random peers to push each Publish to. Zero
	// selects defaultFanout.
	Fanout int
	// Clock supplies wall-clock time for Gossip frame timestamps. Nil
	// selects clock.System.
	Clock clock.Clock
	// Logger is used for slog.Warn / slog.Error surfaces. Nil selects
	// slog.Default().
	Logger *slog.Logger
}

// Gossiper runs the accept loop, publishes local messages, and fetches
// unseen ids announced by peers. Every Gossiper instance is scoped to a
// single group (Config.GroupID); v0 makes no attempt to multiplex.
//
// Concurrency: Publish is safe to call concurrently from any goroutine after
// Start has been invoked. The accept loop runs in the goroutine that calls
// Start and spawns one short-lived goroutine per inbound connection. Every
// goroutine respects the context passed to Start.
type Gossiper struct {
	cfg    Config
	logger *slog.Logger
	clk    clock.Clock
	fanout int

	// picker is rebuilt lazily on first Sample so a freshly-Joined Gossiper
	// (whose roster may grow between New and Start) always samples from the
	// latest membership view.
	pickerOnce sync.Once
	picker     *PeerPicker

	// wg tracks in-flight connection handlers so Start can wait for clean
	// shutdown when its context is cancelled.
	wg sync.WaitGroup

	// lifeCtx is the gossiper's lifetime context (set by Start). Async
	// work that outlives a per-request handler — notably publish fanout
	// now that Publish returns on local-durable accept rather than
	// blocking on peer delivery — uses this so shutdown cancels it,
	// but IPC handler teardown does not. nil before Start runs;
	// publishers that race Start into a pre-started gossiper fall back
	// to inline fanout in that case. Guarded by lifeMu since tests
	// may call Start and Publish from concurrent goroutines.
	lifeMu  sync.Mutex
	lifeCtx context.Context

	// Retry scheduler (patch 6). pending holds (peer, id, op) slots that
	// failed their initial attempt and are waiting on an exponential-backoff
	// re-try cycle. retryLoop drains it. Both pending and lastReconciled are
	// guarded by pendMu to keep the allocation story simple; the maps are
	// accessed far less often than the accept loop, so a single mutex is
	// fine.
	pendMu          sync.Mutex
	pending         map[retryKey]*retryState
	lastReconciled  map[entmoot.NodeID]time.Time

	// Plumtree state (v1.0.4). The broadcast spanning tree is
	// represented as a partition of Roster.Members() \ {self} into
	// eagerPushPeers (receive full Gossip frames) and lazyPushPeers
	// (receive IHave advertisements only). The two sets together cover
	// every current member; each peer sits in exactly one at a time.
	//
	// On Publish, the originator sends full Gossip to eager and IHave
	// to lazy. On receiving a duplicate Gossip, the receiver sends
	// Prune to the sender and moves it from eager to lazy; on
	// receiving IHave for an unknown id, it starts a GraftTimeout
	// timer and on expiry sends Graft + promotes the IHave sender to
	// eager. This is the Leitão/Pereira/Rodrigues SRDS 2007 protocol.
	//
	// pendingGraft maps (id, sender) → timer so we can cancel the
	// timer if the body arrives via eager push before GraftTimeout.
	// Guarded by plumMu. Separate mutex from pendMu to avoid
	// contention with the retry scheduler (which fires every 500 ms
	// and iterates pending under pendMu).
	plumMu          sync.Mutex
	eagerPushPeers  map[entmoot.NodeID]struct{}
	lazyPushPeers   map[entmoot.NodeID]struct{}
	pendingGraft    map[pendingGraftKey]*time.Timer

	// Trust-aware reachability cache (v1.0.6). Plumtree peer-set helpers
	// intersect eager/lazy lists with this set before returning, so the
	// fanout never wastes a 32-s Pilot dial on a peer we have no trust
	// pair with. Short TTL trades bursty IPC against reacting to newly-
	// established handshakes quickly.
	trustMu        sync.Mutex
	trustedSnap    map[entmoot.NodeID]struct{}
	trustedExpires time.Time

	// Dial-backoff cache (v1.0.6 Fix C). canDial consults this before
	// every Transport.Dial; recordDialFailure / recordDialSuccess
	// update it around each attempt. Separate mutex from plumMu/pendMu
	// so the gate is cheap on the hot fanout path even when the retry
	// scheduler is holding pendMu.
	dialMu       sync.Mutex
	dialBackoffs map[entmoot.NodeID]*peerDialState
}

// peerDialState tracks the per-peer dial-backoff state. nextAllowed
// gates send attempts; consecutiveFailures drives exponential
// growth on each fresh failure. Reset entirely on the first
// successful dial.
type peerDialState struct {
	nextAllowed         time.Time
	consecutiveFailures int
}

// pendingGraftKey keys the outstanding GRAFT-timer map: one timer per
// (message id, IHave sender) pair. Multiple peers can advertise the
// same id via IHave; we keep a timer per sender so a body arriving
// from any source cancels all pending timers for that id.
type pendingGraftKey struct {
	id     entmoot.MessageID
	sender entmoot.NodeID
}

// New constructs a Gossiper. Does not start the accept loop; callers must
// invoke Start (typically after a successful Join). Returns an error if the
// Config is missing a required field.
func New(cfg Config) (*Gossiper, error) {
	if cfg.LocalNode == 0 {
		return nil, errors.New("gossip: Config.LocalNode is required")
	}
	if cfg.Identity == nil {
		return nil, errors.New("gossip: Config.Identity is required")
	}
	if cfg.Roster == nil {
		return nil, errors.New("gossip: Config.Roster is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("gossip: Config.Store is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("gossip: Config.Transport is required")
	}
	var zeroGroup entmoot.GroupID
	if cfg.GroupID == zeroGroup {
		return nil, errors.New("gossip: Config.GroupID is required")
	}
	fanout := cfg.Fanout
	if fanout == 0 {
		fanout = defaultFanout
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.System{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Gossiper{
		cfg:            cfg,
		logger:         logger,
		clk:            clk,
		fanout:         fanout,
		pending:        make(map[retryKey]*retryState),
		lastReconciled: make(map[entmoot.NodeID]time.Time),
		// Plumtree peer-set maps and pending-graft timer map are
		// lazily populated. Per paper convention, eagerPushPeers
		// starts with every current non-self roster member at first
		// use; duplicates arriving via multiple eager paths self-prune
		// the tree to the spanning subset.
		eagerPushPeers: make(map[entmoot.NodeID]struct{}),
		lazyPushPeers:  make(map[entmoot.NodeID]struct{}),
		pendingGraft:   make(map[pendingGraftKey]*time.Timer),
		// Per-peer dial-backoff cache (v1.0.6 Fix C). Populated lazily
		// by recordDialFailure on the first failed dial to a given
		// peer; wiped by recordDialSuccess after any successful dial
		// clears reachability.
		dialBackoffs: make(map[entmoot.NodeID]*peerDialState),
	}, nil
}

// seedPlumtreeLocked populates eagerPushPeers with every current roster
// member (excluding self) the first time any Plumtree code path runs.
// Idempotent thereafter: existing assignments stay put, any newly-added
// roster members default to eager. Caller must hold plumMu.
//
// Kept lazy rather than wired into New so the Gossiper can be
// constructed before the founder's initial genesis/add entries have been
// Applied to the roster (the tests and the real daemon both rely on
// this ordering).
func (g *Gossiper) seedPlumtreeLocked() {
	for _, m := range g.cfg.Roster.Members() {
		if m == g.cfg.LocalNode {
			continue
		}
		_, eager := g.eagerPushPeers[m]
		_, lazy := g.lazyPushPeers[m]
		if !eager && !lazy {
			g.eagerPushPeers[m] = struct{}{}
		}
	}
}

// trustedSet returns a snapshot of Pilot-trusted peer IDs, cached
// for trustedSetTTL. On IPC error, returns the most recent
// successful snapshot; if no snapshot has ever been taken, returns
// nil to signal "unknown — treat every peer as trusted" so behaviour
// degrades gracefully to v1.0.5.
func (g *Gossiper) trustedSet(ctx context.Context) map[entmoot.NodeID]struct{} {
	g.trustMu.Lock()
	defer g.trustMu.Unlock()
	if g.trustedSnap != nil && g.clk.Now().Before(g.trustedExpires) {
		return g.trustedSnap
	}
	peers, err := g.cfg.Transport.TrustedPeers(ctx)
	if err != nil {
		g.logger.Debug("gossip: trusted peers query failed",
			slog.String("err", err.Error()))
		return g.trustedSnap // may be nil on cold start; callers handle it
	}
	fresh := make(map[entmoot.NodeID]struct{}, len(peers))
	for _, p := range peers {
		fresh[p] = struct{}{}
	}
	g.trustedSnap = fresh
	g.trustedExpires = g.clk.Now().Add(trustedSetTTL)
	return fresh
}

// plumEagerExcept returns a copy of eagerPushPeers with `except`
// removed, suitable for re-fanout on receive (we never push back to
// the source). Also seeds the peer maps on first use. Caller must NOT
// hold plumMu.
//
// v1.0.6: the result is intersected with the Pilot-trusted peer set
// so fanout never wastes a 32-s dial on a peer we have no trust pair
// with. If the trust oracle has no cached snapshot yet (cold-start
// IPC error), the intersection is skipped — behaviour degrades to
// v1.0.5 (fail-open).
func (g *Gossiper) plumEagerExcept(ctx context.Context, except entmoot.NodeID) []entmoot.NodeID {
	trusted := g.trustedSet(ctx)
	g.plumMu.Lock()
	defer g.plumMu.Unlock()
	g.seedPlumtreeLocked()
	out := make([]entmoot.NodeID, 0, len(g.eagerPushPeers))
	for p := range g.eagerPushPeers {
		if p == except {
			continue
		}
		if trusted != nil {
			if _, ok := trusted[p]; !ok {
				continue // skip untrusted peer; would dial-timeout anyway
			}
		}
		out = append(out, p)
	}
	return out
}

// plumLazyExcept mirrors plumEagerExcept for the lazy set.
func (g *Gossiper) plumLazyExcept(ctx context.Context, except entmoot.NodeID) []entmoot.NodeID {
	trusted := g.trustedSet(ctx)
	g.plumMu.Lock()
	defer g.plumMu.Unlock()
	g.seedPlumtreeLocked()
	out := make([]entmoot.NodeID, 0, len(g.lazyPushPeers))
	for p := range g.lazyPushPeers {
		if p == except {
			continue
		}
		if trusted != nil {
			if _, ok := trusted[p]; !ok {
				continue // skip untrusted peer; would dial-timeout anyway
			}
		}
		out = append(out, p)
	}
	return out
}

// plumDemoteToLazy moves `peer` from eagerPushPeers to lazyPushPeers.
// Called when we receive a duplicate Gossip (PRUNE the sender) or
// when we receive an explicit Prune. No-op if already lazy.
func (g *Gossiper) plumDemoteToLazy(peer entmoot.NodeID) {
	g.plumMu.Lock()
	defer g.plumMu.Unlock()
	g.seedPlumtreeLocked()
	if _, ok := g.eagerPushPeers[peer]; ok {
		delete(g.eagerPushPeers, peer)
		g.lazyPushPeers[peer] = struct{}{}
	}
}

// plumPromoteToEager moves `peer` from lazyPushPeers to eagerPushPeers.
// Called when we receive a Graft (they want full pushes) or when a
// GRAFT timer we scheduled fires (we had to ask for a body, so the
// sender is a viable eager source). No-op if already eager.
func (g *Gossiper) plumPromoteToEager(peer entmoot.NodeID) {
	g.plumMu.Lock()
	defer g.plumMu.Unlock()
	g.seedPlumtreeLocked()
	if _, ok := g.lazyPushPeers[peer]; ok {
		delete(g.lazyPushPeers, peer)
		g.eagerPushPeers[peer] = struct{}{}
	} else if _, ok := g.eagerPushPeers[peer]; !ok {
		// Unknown peer (e.g. just added to roster but not seeded
		// yet) — add directly to eager.
		g.eagerPushPeers[peer] = struct{}{}
	}
}

// plumCancelGraftsFor cancels every outstanding GRAFT timer for the
// given message id, regardless of which peer's IHave triggered it.
// Called after we've stored the body via any path (eager push, graft
// response, anti-entropy) — the message is no longer missing so we
// don't need to ask any other IHave-sender for it.
func (g *Gossiper) plumCancelGraftsFor(id entmoot.MessageID) {
	g.plumMu.Lock()
	defer g.plumMu.Unlock()
	for k, t := range g.pendingGraft {
		if k.id == id {
			t.Stop()
			delete(g.pendingGraft, k)
		}
	}
}

// canDial reports whether peer P is currently outside its
// dial-backoff window (v1.0.6 Fix C). Called by every send-path
// before Transport.Dial; suppressed sends return cheaply without
// touching the network.
func (g *Gossiper) canDial(peer entmoot.NodeID) bool {
	g.dialMu.Lock()
	defer g.dialMu.Unlock()
	s, ok := g.dialBackoffs[peer]
	if !ok {
		return true
	}
	return !g.clk.Now().Before(s.nextAllowed)
}

// recordDialFailure extends the per-peer backoff window by one
// exponential step (capped at dialBackoffCap). Call immediately
// after Transport.Dial returns a non-nil error.
func (g *Gossiper) recordDialFailure(peer entmoot.NodeID) {
	g.dialMu.Lock()
	defer g.dialMu.Unlock()
	s, ok := g.dialBackoffs[peer]
	if !ok {
		s = &peerDialState{}
		g.dialBackoffs[peer] = s
	}
	s.consecutiveFailures++
	// 2^n growth with overflow guard for n > 30. The saturation
	// at dialBackoffCap handles the practical case long before
	// bit overflow matters, but belt-and-braces keeps the math
	// readable under fuzz tests.
	shift := s.consecutiveFailures - 1
	if shift > 30 {
		shift = 30
	}
	window := dialBackoffBase * (1 << shift)
	if window > dialBackoffCap || window <= 0 {
		window = dialBackoffCap
	}
	s.nextAllowed = g.clk.Now().Add(window)
}

// recordDialSuccess clears any backoff state for peer P. Called
// immediately after a successful Transport.Dial (even if the
// subsequent stream write fails — dial reachability and protocol
// health are independent, per libp2p's model).
func (g *Gossiper) recordDialSuccess(peer entmoot.NodeID) {
	g.dialMu.Lock()
	defer g.dialMu.Unlock()
	delete(g.dialBackoffs, peer)
}

// Start runs the accept loop. It blocks until ctx is cancelled or the
// transport is closed. Any inbound connection is handled in its own
// goroutine; Start waits for those goroutines to return before it itself
// returns. The returned error is nil on clean shutdown (ctx cancelled or
// transport closed); it is a wrapped Accept error otherwise.
func (g *Gossiper) Start(ctx context.Context) error {
	// Stash the long-lived context so async workers spawned from
	// per-request entry points (Publish's fanout goroutine) can use
	// it instead of the request-scoped ctx that dies when the IPC
	// caller disconnects.
	g.lifeMu.Lock()
	g.lifeCtx = ctx
	g.lifeMu.Unlock()

	// Background workers (patch 6, patch 7). retryLoop drains the
	// exponential-backoff queue for push/fetch attempts that failed their
	// initial try. Both live under g.wg so Start's drain on shutdown also
	// covers them.
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.retryLoop(ctx)
	}()

	for {
		conn, remote, err := g.cfg.Transport.Accept(ctx)
		if err != nil {
			// Accept returning is the signal to shut down: wait for any
			// in-flight handlers before surfacing.
			g.wg.Wait()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("gossip: accept: %w", err)
		}
		// Server-side membership gate (replaces the v0-dropped Hello).
		if !g.cfg.Roster.IsMember(remote) {
			g.logger.Warn("gossip: reject non-member",
				slog.Uint64("remote", uint64(remote)),
				slog.String("group_id", g.cfg.GroupID.String()))
			_ = conn.Close()
			continue
		}
		// Patch 7: an inbound connection from a roster-verified peer is the
		// most reliable signal we have that the peer is reachable again. Fire
		// an async reconciliation (rate-limited per peer) so any state they
		// accumulated while we were partitioned gets pulled in. This runs
		// alongside the handleConn goroutine so the reconcile dial doesn't
		// block the incoming frame.
		g.maybeReconcile(ctx, remote)

		g.wg.Add(1)
		go func(c net.Conn, r entmoot.NodeID) {
			defer g.wg.Done()
			defer c.Close()
			g.handleConn(ctx, c, r)
		}(conn, remote)
	}
}

// handleConn reads one frame from c and dispatches on type. v0 is stateless
// per connection: exactly one request-response, then close. Errors are
// logged and the connection is dropped (hard-disconnect per the plan).
func (g *Gossiper) handleConn(ctx context.Context, c net.Conn, remote entmoot.NodeID) {
	t, payload, err := wire.ReadAndDecode(c)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return
		}
		g.logger.Warn("gossip: read frame",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	switch v := payload.(type) {
	case *wire.RosterReq:
		g.onRosterReq(c, remote, v)
	case *wire.FetchReq:
		g.onFetchReq(ctx, c, remote, v)
	case *wire.MerkleReq:
		g.onMerkleReq(ctx, c, remote, v)
	case *wire.RangeReq:
		g.onRangeReq(ctx, c, remote, v)
	case *wire.Gossip:
		g.onGossip(ctx, remote, v)
	case *wire.IHave:
		g.onIHave(ctx, remote, v)
	case *wire.Graft:
		g.onGraft(ctx, remote, v)
	case *wire.Prune:
		g.onPrune(remote, v)
	case *wire.Hello:
		// v0 drops Hello: Pilot's tunnel already authenticates the remote.
		// The codec still recognizes the type (kept for future use), so we
		// simply log and move on.
		g.logger.Debug("gossip: ignoring hello frame (v0 skips handshake)",
			slog.Uint64("remote", uint64(remote)))
	default:
		g.logger.Warn("gossip: unexpected frame type",
			slog.Uint64("remote", uint64(remote)),
			slog.String("type", t.String()))
	}
}

// onRosterReq responds with the full roster entries. SinceHead is accepted
// but ignored (v0 responders always return the full log).
func (g *Gossiper) onRosterReq(c net.Conn, remote entmoot.NodeID, req *wire.RosterReq) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: roster_req for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}
	resp := &wire.RosterResp{
		GroupID: g.cfg.GroupID,
		Entries: g.cfg.Roster.Entries(),
	}
	if err := wire.EncodeAndWrite(c, resp); err != nil {
		g.logger.Warn("gossip: write roster_resp",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
	}
}

// onFetchReq looks up the requested id in the local store and responds with
// either the message body or NotFound=true.
func (g *Gossiper) onFetchReq(ctx context.Context, c net.Conn, remote entmoot.NodeID, req *wire.FetchReq) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: fetch_req for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}
	resp := &wire.FetchResp{GroupID: g.cfg.GroupID, ID: req.ID}
	msg, err := g.cfg.Store.Get(ctx, req.GroupID, req.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			resp.NotFound = true
		} else {
			g.logger.Warn("gossip: store.Get",
				slog.Uint64("remote", uint64(remote)),
				slog.String("err", err.Error()))
			resp.NotFound = true
		}
	} else {
		m := msg
		resp.Message = &m
	}
	if err := wire.EncodeAndWrite(c, resp); err != nil {
		g.logger.Warn("gossip: write fetch_resp",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
	}
}

// onMerkleReq responds with the current Merkle root plus the number of
// messages in the local store for this group.
func (g *Gossiper) onMerkleReq(ctx context.Context, c net.Conn, remote entmoot.NodeID, req *wire.MerkleReq) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: merkle_req for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}
	root, err := g.cfg.Store.MerkleRoot(ctx, req.GroupID)
	if err != nil {
		g.logger.Warn("gossip: merkle root",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	// Count via Range with the widest possible window. v0's Range uses
	// [sinceMillis, untilMillis) with untilMillis=0 meaning no upper bound.
	msgs, err := g.cfg.Store.Range(ctx, req.GroupID, 0, 0)
	if err != nil {
		g.logger.Warn("gossip: range for count",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	resp := &wire.MerkleResp{
		GroupID:      g.cfg.GroupID,
		Root:         wire.MerkleRoot(root),
		MessageCount: len(msgs),
	}
	if err := wire.EncodeAndWrite(c, resp); err != nil {
		g.logger.Warn("gossip: write merkle_resp",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
	}
}

// onRangeReq responds with the message ids we hold in this group whose
// Timestamp falls at or after SinceMillis. Bodies are pulled separately by
// the caller via FetchReq; this endpoint intentionally only carries ids so
// a bulk reconciliation never has to transmit a large signed payload in a
// single frame.
func (g *Gossiper) onRangeReq(ctx context.Context, c net.Conn, remote entmoot.NodeID, req *wire.RangeReq) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: range_req for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}
	msgs, err := g.cfg.Store.Range(ctx, req.GroupID, req.SinceMillis, 0)
	if err != nil {
		g.logger.Warn("gossip: store.Range",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	ids := make([]entmoot.MessageID, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.ID)
	}
	resp := &wire.RangeResp{GroupID: g.cfg.GroupID, IDs: ids}
	if err := wire.EncodeAndWrite(c, resp); err != nil {
		g.logger.Warn("gossip: write range_resp",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
	}
}

// onGossip validates the sender's signature, then for each advertised id
// that we have not already seen, dials the sender back on a fresh connection
// to fetch the body. Bodies are verified against the author's roster pubkey
// before being stored.
//
// Signature-vs-roster race: if the sender is in our roster but we have not
// yet learned about the author's membership (e.g. a roster_sync is still in
// flight), FetchResp verification below will fail and the message is
// dropped. That is the desired safe behavior — we prefer false negatives
// (re-delivery on the next gossip push) over accepting an unverifiable body.
// A Phase-E integration that wants to bridge this gap would gate Start on a
// completed Join rather than running both in parallel.
func (g *Gossiper) onGossip(ctx context.Context, remote entmoot.NodeID, gos *wire.Gossip) {
	if gos.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: gossip for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", gos.GroupID.String()))
		return
	}
	senderInfo, ok := g.cfg.Roster.MemberInfo(remote)
	if !ok {
		g.logger.Warn("gossip: gossip from non-member",
			slog.Uint64("remote", uint64(remote)))
		return
	}
	if !verifyGossipSig(gos, senderInfo.EntmootPubKey) {
		g.logger.Warn("gossip: gossip signature invalid",
			slog.Uint64("remote", uint64(remote)))
		return
	}

	pruned := false
	for _, id := range gos.IDs {
		has, err := g.cfg.Store.Has(ctx, g.cfg.GroupID, id)
		if err != nil {
			g.logger.Warn("gossip: store.Has",
				slog.String("err", err.Error()))
			continue
		}
		if has {
			// Plumtree (v1.0.4): duplicate eager push from `remote`
			// means there is a shorter path to us for this message.
			// Demote `remote` to lazyPushPeers and send exactly one
			// Prune per duplicate burst so subsequent messages from
			// that peer arrive as IHave, not full Gossip. The
			// dedupe is "once per frame" (the pruned flag) because a
			// single Gossip frame is already one round-trip of
			// waste; further ids in the same frame don't warrant
			// additional prunes.
			if !pruned {
				g.plumDemoteToLazy(remote)
				if err := g.sendPrune(ctx, remote); err != nil {
					g.logger.Debug("gossip: prune",
						slog.Uint64("peer", uint64(remote)),
						slog.String("err", err.Error()))
					g.enqueueRetry(retryKey{peer: remote, id: id, op: opPrune}, nil)
				}
				pruned = true
			}
			continue
		}
		if err := g.fetchFrom(ctx, remote, id); err != nil {
			// Patch 6: queue a retry instead of dropping. Transient Pilot
			// errors (dial timeout, stream EOF, NAT flap) would otherwise
			// permanently lose the message — the retry loop gives ~5 min of
			// exponential-backoff attempts before giving up, and patch 7's
			// reconciliation-on-reconnect catches anything that still slips
			// through. When the retry later succeeds, fetchFrom itself
			// drives the Plumtree re-fanout (v1.0.5).
			g.logger.Warn("gossip: fetch",
				slog.Uint64("remote", uint64(remote)),
				slog.String("id", id.String()),
				slog.Int("attempt", 1),
				slog.String("err", err.Error()))
			g.enqueueRetry(retryKey{peer: remote, id: id, op: opFetch}, nil)
			continue
		}
		// Plumtree re-fanout and graft-timer cancellation happen inside
		// fetchFrom on successful Put (v1.0.5). Centralizing the hook
		// there means retry-fetch and reconcile-fetch acquisitions also
		// propagate; prior to v1.0.5 only this gossip-push path did.
	}
}

// refanout forwards a just-received message to this node's other
// peers per the Plumtree spanning tree: full Gossip to
// eagerPushPeers \ {except}, IHave to lazyPushPeers \ {except}.
// Every send goes through the same retry machinery as the originator's
// fanout so transient dial failures are re-tried and eventually fall
// through to anti-entropy reconciliation.
func (g *Gossiper) refanout(ctx context.Context, except entmoot.NodeID, id entmoot.MessageID) {
	eager := g.plumEagerExcept(ctx, except)
	lazy := g.plumLazyExcept(ctx, except)
	if len(eager) == 0 && len(lazy) == 0 {
		return
	}
	// Re-sign the Gossip frame with our own identity so recipients
	// verify against our roster entry, not the originator's. Plumtree
	// treats the forwarder as the immediate-sender; the message's
	// internal signature (verified during fetchFrom → store.Put)
	// attributes authorship separately.
	frame := &wire.Gossip{
		GroupID:   g.cfg.GroupID,
		IDs:       []entmoot.MessageID{id},
		Timestamp: g.clk.Now().UnixMilli(),
	}
	if err := signGossip(frame, g.cfg.Identity); err != nil {
		g.logger.Warn("gossip: refanout sign",
			slog.String("id", id.String()),
			slog.String("err", err.Error()))
		return
	}
	ihave := &wire.IHave{
		GroupID: g.cfg.GroupID,
		IDs:     []entmoot.MessageID{id},
	}
	g.fanoutPush(ctx, eager, frame, id)
	g.fanoutIHave(ctx, lazy, ihave, id)
}

// sendPrune dials peer and writes a single Prune frame.
func (g *Gossiper) sendPrune(ctx context.Context, peer entmoot.NodeID) error {
	// v1.0.6 Fix C: short-circuit if peer is in its dial-backoff
	// window. Synthetic error keeps the retry scheduler's error
	// shape unchanged.
	if !g.canDial(peer) {
		return fmt.Errorf("peer %d in dial-backoff", peer)
	}
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		g.recordDialFailure(peer)
		return fmt.Errorf("dial: %w", err)
	}
	g.recordDialSuccess(peer)
	defer conn.Close()
	frame := &wire.Prune{GroupID: g.cfg.GroupID}
	if err := wire.EncodeAndWrite(conn, frame); err != nil {
		return fmt.Errorf("write prune: %w", err)
	}
	return nil
}

// sendGraft dials peer and writes a Graft frame requesting the bodies
// for the given ids AND promoting us in the recipient's eagerPushPeers.
func (g *Gossiper) sendGraft(ctx context.Context, peer entmoot.NodeID, ids []entmoot.MessageID) error {
	// v1.0.6 Fix C: short-circuit if peer is in its dial-backoff window.
	if !g.canDial(peer) {
		return fmt.Errorf("peer %d in dial-backoff", peer)
	}
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		g.recordDialFailure(peer)
		return fmt.Errorf("dial: %w", err)
	}
	g.recordDialSuccess(peer)
	defer conn.Close()
	frame := &wire.Graft{GroupID: g.cfg.GroupID, IDs: ids}
	if err := wire.EncodeAndWrite(conn, frame); err != nil {
		return fmt.Errorf("write graft: %w", err)
	}
	return nil
}

// onIHave handles inbound IHave frames: for each advertised id we do
// not already have, schedule a GRAFT timer against the IHave sender.
// If the body arrives via eager push before graftTimeout (from any
// peer — refanout + multiple originators can race), the timer is
// cancelled. On expiry, we send a Graft to the sender and promote
// them into our eagerPushPeers — healing a gap in our tree.
func (g *Gossiper) onIHave(ctx context.Context, remote entmoot.NodeID, ih *wire.IHave) {
	if ih.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: ihave for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", ih.GroupID.String()))
		return
	}
	for _, id := range ih.IDs {
		has, err := g.cfg.Store.Has(ctx, g.cfg.GroupID, id)
		if err != nil {
			g.logger.Debug("gossip: ihave store.Has", slog.String("err", err.Error()))
			continue
		}
		if has {
			continue
		}
		g.scheduleGraft(id, remote)
	}
}

// scheduleGraft arms a timer that fires graftTimeout from now and, if
// the body is still missing at expiry, sends a Graft to `sender`.
// Idempotent per (id, sender): a second IHave for the same pair
// resets the timer.
func (g *Gossiper) scheduleGraft(id entmoot.MessageID, sender entmoot.NodeID) {
	g.plumMu.Lock()
	key := pendingGraftKey{id: id, sender: sender}
	if existing, ok := g.pendingGraft[key]; ok {
		existing.Stop()
	}
	// Cannot run timer logic under plumMu; arm with a callback that
	// re-acquires the lock briefly to remove itself before the real
	// work.
	t := time.AfterFunc(graftTimeout, func() {
		g.onGraftTimer(id, sender)
	})
	g.pendingGraft[key] = t
	g.plumMu.Unlock()
}

// onGraftTimer fires graftTimeout after we received an IHave for a
// missing id. If the body still has not arrived, send a Graft and
// promote the IHave sender into eagerPushPeers.
func (g *Gossiper) onGraftTimer(id entmoot.MessageID, sender entmoot.NodeID) {
	g.plumMu.Lock()
	delete(g.pendingGraft, pendingGraftKey{id: id, sender: sender})
	g.plumMu.Unlock()

	// The context to use for the send. Prefer lifeCtx (set by Start);
	// fall back to Background() so the timer is safe to fire even if
	// Start has not yet been called.
	g.lifeMu.Lock()
	ctx := g.lifeCtx
	g.lifeMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}

	// Re-check that we still lack the body; a late arrival via eager
	// push could have filled it after the timer fired but before we
	// got here.
	has, err := g.cfg.Store.Has(ctx, g.cfg.GroupID, id)
	if err == nil && has {
		return
	}

	g.plumPromoteToEager(sender)
	if err := g.sendGraft(ctx, sender, []entmoot.MessageID{id}); err != nil {
		g.logger.Debug("gossip: graft",
			slog.Uint64("peer", uint64(sender)),
			slog.String("id", id.String()),
			slog.Int("attempt", 1),
			slog.String("err", err.Error()))
		g.enqueueRetry(retryKey{peer: sender, id: id, op: opGraft}, nil)
	}
}

// onGraft handles inbound Graft frames: promote the sender into
// eagerPushPeers (they want full pushes) and, for each requested id
// we have locally, push the full Gossip frame back through the
// standard eager path. If we don't have an id (it was evicted or
// never stored), silently drop — the requester will re-discover via
// a later IHave or anti-entropy reconciliation.
func (g *Gossiper) onGraft(ctx context.Context, remote entmoot.NodeID, gr *wire.Graft) {
	if gr.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: graft for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", gr.GroupID.String()))
		return
	}
	g.plumPromoteToEager(remote)

	for _, id := range gr.IDs {
		has, err := g.cfg.Store.Has(ctx, g.cfg.GroupID, id)
		if err != nil || !has {
			continue
		}
		frame := &wire.Gossip{
			GroupID:   g.cfg.GroupID,
			IDs:       []entmoot.MessageID{id},
			Timestamp: g.clk.Now().UnixMilli(),
		}
		if err := signGossip(frame, g.cfg.Identity); err != nil {
			g.logger.Warn("gossip: graft sign", slog.String("err", err.Error()))
			continue
		}
		if err := g.pushGossip(ctx, remote, frame); err != nil {
			g.enqueueRetry(retryKey{peer: remote, id: id, op: opPush}, frame)
		}
	}
}

// onPrune handles inbound Prune frames: demote the sender to
// lazyPushPeers so future messages go as IHave rather than full
// Gossip. No-op if the sender is already in lazy.
func (g *Gossiper) onPrune(remote entmoot.NodeID, pr *wire.Prune) {
	if pr.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: prune for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", pr.GroupID.String()))
		return
	}
	g.plumDemoteToLazy(remote)
}

// fetchFrom opens a fresh connection to peer, sends FetchReq for id, and
// stores the response body after verifying its signature against the
// author's pubkey from the local roster. Returns an error on dial failure,
// codec failure, NotFound, or signature verification failure.
func (g *Gossiper) fetchFrom(ctx context.Context, peer entmoot.NodeID, id entmoot.MessageID) error {
	// v1.0.6 Fix C: short-circuit if peer is in its dial-backoff window.
	if !g.canDial(peer) {
		return fmt.Errorf("peer %d in dial-backoff", peer)
	}
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		g.recordDialFailure(peer)
		return fmt.Errorf("dial %d: %w", peer, err)
	}
	g.recordDialSuccess(peer)
	defer conn.Close()

	req := &wire.FetchReq{GroupID: g.cfg.GroupID, ID: id}
	if err := wire.EncodeAndWrite(conn, req); err != nil {
		return fmt.Errorf("write fetch_req: %w", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		return fmt.Errorf("read fetch_resp: %w", err)
	}
	resp, ok := payload.(*wire.FetchResp)
	if !ok {
		return fmt.Errorf("fetch: unexpected response type")
	}
	if resp.NotFound || resp.Message == nil {
		return fmt.Errorf("fetch: peer %d reports not-found for %s", peer, id)
	}
	if resp.Message.ID != id {
		return fmt.Errorf("fetch: response id mismatch")
	}
	if err := g.verifyMessage(*resp.Message); err != nil {
		return err
	}
	if err := g.cfg.Store.Put(ctx, *resp.Message); err != nil {
		return fmt.Errorf("store put: %w", err)
	}
	// Plumtree "first-seen → forward" rule (v1.0.5). A message we just
	// acquired is new to our store by construction: every caller of
	// fetchFrom gates on Store.Has first. Forward it through the spanning
	// tree regardless of which acquisition path led us here — gossip push
	// (onGossip), queued retry (executeRetry/opFetch), or anti-entropy
	// pull (reconcileWith). Previously only onGossip called refanout
	// afterwards, so messages acquired via retry or reconcile silently
	// terminated at this node — breaking propagation when, e.g., a hub
	// acquires an edge-node's message via reconcile (because the edge's
	// direct push was flaky) and the hub's other edge never hears. This
	// matches GossipSub v1.0's forwarding rule ("not seen before →
	// forward to mesh, irrespective of arrival method").
	//
	// plumCancelGraftsFor clears any outstanding GRAFT timers for this id
	// (the body arrived, nothing to graft-request anymore). refanout pushes
	// to eagerPushPeers \ {peer} and IHave-advertises to lazyPushPeers \
	// {peer}.
	g.plumCancelGraftsFor(id)
	g.refanout(ctx, peer, id)
	// Successful fetch also implies the peer is reachable; trigger a
	// cooldown-gated reconcile so anti-entropy catches anything else they
	// have that we don't (patch 7).
	g.maybeReconcile(ctx, peer)
	return nil
}

// Publish validates, stores, and gossips a new local message. The message
// must already be signed by Config.Identity (Publish re-verifies before
// storing). Fan-out errors are logged but do not fail Publish — a failed
// push is just less fan-out; the next peer that gossips us this id will
// pull the body back.
func (g *Gossiper) Publish(ctx context.Context, msg entmoot.Message) error {
	if msg.GroupID != g.cfg.GroupID {
		return fmt.Errorf("gossip: publish for wrong group %s", msg.GroupID.String())
	}
	if err := g.verifyMessage(msg); err != nil {
		return err
	}
	if err := g.cfg.Store.Put(ctx, msg); err != nil {
		return fmt.Errorf("gossip: store put: %w", err)
	}

	// Plumtree dissemination (v1.0.4). The full Gossip frame goes to
	// every peer in eagerPushPeers; lazy peers receive only an IHave
	// advertisement. The split self-heals over time: duplicate
	// arrivals prune redundant eager edges, IHave-to-lazy pulls graft
	// peers back to eager when the tree is incomplete. Before v1.0.4
	// this was a simple random Sample(fanout) push; the new behavior
	// is a strict superset: if no peers have been pruned yet, the
	// entire roster sits in eager and everyone gets the full push,
	// which matches previous behavior when fanout ≥ |members-1|.
	eager := g.plumEagerExcept(ctx, 0)
	lazy := g.plumLazyExcept(ctx, 0)
	if len(eager) == 0 && len(lazy) == 0 {
		return nil
	}

	// Build a signed Gossip frame once; reuse across fan-out targets.
	frame := &wire.Gossip{
		GroupID:   g.cfg.GroupID,
		IDs:       []entmoot.MessageID{msg.ID},
		Timestamp: g.clk.Now().UnixMilli(),
	}
	if err := signGossip(frame, g.cfg.Identity); err != nil {
		return fmt.Errorf("gossip: sign: %w", err)
	}

	ihave := &wire.IHave{
		GroupID: g.cfg.GroupID,
		IDs:     []entmoot.MessageID{msg.ID},
	}

	// v1.0.3 publish semantics: local-durable accept is the contract.
	// Run the fanout asynchronously so the IPC handler returns in
	// milliseconds instead of blocking on potentially-slow per-peer
	// dials. Failed pushes still feed the existing retry scheduler
	// (patch 6). This matches the standard pub/sub convention (Kafka
	// durable-log commit, NATS fire-and-forget, RabbitMQ publisher
	// confirms) where publish = "accepted for delivery", not
	// "delivered to subscribers".
	//
	// The async fanout runs under g.lifeCtx so request teardown
	// doesn't cancel it; g.wg ensures Start's shutdown drain waits
	// for in-flight fanouts. When lifeCtx is nil (publishing before
	// Start — rare, mostly tests), fall back to inline fanout so we
	// don't drop messages on the floor.
	g.lifeMu.Lock()
	lifeCtx := g.lifeCtx
	g.lifeMu.Unlock()
	if lifeCtx == nil {
		g.fanoutPush(ctx, eager, frame, msg.ID)
		g.fanoutIHave(ctx, lazy, ihave, msg.ID)
		return nil
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.fanoutPush(lifeCtx, eager, frame, msg.ID)
		g.fanoutIHave(lifeCtx, lazy, ihave, msg.ID)
	}()
	return nil
}

// fanoutPush attempts the per-peer push, queuing failures for the
// retry scheduler. Shared between the synchronous pre-Start fallback
// path and the async post-Start goroutine.
//
// v1.0.6: runs each peer's dial+write in its own goroutine under an
// errgroup with SetLimit(fanoutMaxConcurrency), and wraps every
// attempt in a fanoutPerPeerTimeout-bounded context. One stalled peer
// can no longer head-of-line-block healthy peers, and a dead peer
// fails fast into the retry scheduler rather than consuming the full
// Pilot-internal ~32 s dial budget. Gossip is best-effort, so
// individual failures are logged + queued for retry but never fail
// the group.
func (g *Gossiper) fanoutPush(ctx context.Context, peers []entmoot.NodeID, frame *wire.Gossip, id entmoot.MessageID) {
	if len(peers) == 0 {
		return
	}
	var eg errgroup.Group
	eg.SetLimit(fanoutMaxConcurrency)
	for _, p := range peers {
		p := p // capture
		eg.Go(func() error {
			dctx, cancel := context.WithTimeout(ctx, fanoutPerPeerTimeout)
			defer cancel()
			if err := g.pushGossip(dctx, p, frame); err != nil {
				g.logger.Warn("gossip: push",
					slog.Uint64("peer", uint64(p)),
					slog.String("id", id.String()),
					slog.Int("attempt", 1),
					slog.String("err", err.Error()))
				g.enqueueRetry(retryKey{peer: p, id: id, op: opPush}, frame)
			}
			return nil // gossip is best-effort; never fail the group
		})
	}
	_ = eg.Wait()
}

// fanoutIHave advertises a message id to a set of lazy peers via
// Plumtree's IHave frame. Failures go through the same retry scheduler
// as eager pushes. The frame is a plain unsigned id list — peers that
// want the body issue a follow-up Graft.
//
// v1.0.6: identical parallelism / timeout treatment as fanoutPush — one
// stalled peer cannot block the rest of the lazy set, and each attempt
// is bounded by fanoutPerPeerTimeout before dropping into the retry
// scheduler.
func (g *Gossiper) fanoutIHave(ctx context.Context, peers []entmoot.NodeID, frame *wire.IHave, id entmoot.MessageID) {
	if len(peers) == 0 {
		return
	}
	var eg errgroup.Group
	eg.SetLimit(fanoutMaxConcurrency)
	for _, p := range peers {
		p := p // capture
		eg.Go(func() error {
			dctx, cancel := context.WithTimeout(ctx, fanoutPerPeerTimeout)
			defer cancel()
			if err := g.sendIHave(dctx, p, frame); err != nil {
				g.logger.Debug("gossip: ihave",
					slog.Uint64("peer", uint64(p)),
					slog.String("id", id.String()),
					slog.Int("attempt", 1),
					slog.String("err", err.Error()))
				g.enqueueRetry(retryKey{peer: p, id: id, op: opIHave}, nil)
			}
			return nil // IHave advertisement is best-effort; never fail the group
		})
	}
	_ = eg.Wait()
}

// sendIHave dials the peer and writes a single IHave frame. Fire-and-
// forget: Plumtree IHave is an advertisement, not a query, so we do not
// wait for or interpret any response.
func (g *Gossiper) sendIHave(ctx context.Context, peer entmoot.NodeID, frame *wire.IHave) error {
	// v1.0.6 Fix C: short-circuit if peer is in its dial-backoff window.
	if !g.canDial(peer) {
		return fmt.Errorf("peer %d in dial-backoff", peer)
	}
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		g.recordDialFailure(peer)
		return fmt.Errorf("dial: %w", err)
	}
	g.recordDialSuccess(peer)
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, frame); err != nil {
		return fmt.Errorf("write ihave: %w", err)
	}
	return nil
}

// pushGossip opens a connection to peer, writes the frame, and closes. v0 is
// stateless: we never expect a response to a gossip push.
//
// A successful dial also fires maybeReconcile (patch 7): a peer we can talk
// to via outbound is a peer that may have state we missed during a
// partition. The cooldown gate inside maybeReconcile deduplicates repeat
// dials so this costs at most one MerkleReq per cooldown window per peer.
func (g *Gossiper) pushGossip(ctx context.Context, peer entmoot.NodeID, frame *wire.Gossip) error {
	// v1.0.6 Fix C: short-circuit if peer is in its dial-backoff window.
	if !g.canDial(peer) {
		return fmt.Errorf("peer %d in dial-backoff", peer)
	}
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		g.recordDialFailure(peer)
		return fmt.Errorf("dial: %w", err)
	}
	g.recordDialSuccess(peer)
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, frame); err != nil {
		return fmt.Errorf("write gossip: %w", err)
	}
	g.maybeReconcile(ctx, peer)
	return nil
}

// getPicker returns a cached PeerPicker, constructing it on first use. The
// picker shares the Gossiper's rand source; the source is seeded off the
// current time when Config.Clock is nil (production) and off a fixed seed
// when Fake clocks are injected (tests) so deterministic coverage stays
// possible.
func (g *Gossiper) getPicker() *PeerPicker {
	g.pickerOnce.Do(func() {
		// Seed off the clock. Fake clocks produce a stable pair for tests
		// that want determinism; the System clock produces a moving seed
		// across restarts, which is fine for gossip fan-out.
		now := g.clk.Now().UnixNano()
		src := rand.New(rand.NewPCG(uint64(now), uint64(now>>1)^0x9E3779B97F4A7C15))
		g.picker = NewPicker(g.cfg.Roster, g.cfg.LocalNode, src)
	})
	return g.picker
}

// verifyMessage checks that msg.Author is a current roster member, that
// msg.Signature verifies against the author's roster pubkey, and that
// msg.ID matches canonical.MessageID of msg (id/sig zeroed). Returns
// entmoot.ErrNotMember or entmoot.ErrSigInvalid on failure.
func (g *Gossiper) verifyMessage(msg entmoot.Message) error {
	author, ok := g.cfg.Roster.MemberInfo(msg.Author.PilotNodeID)
	if !ok {
		return fmt.Errorf("%w: author %d", entmoot.ErrNotMember, msg.Author.PilotNodeID)
	}
	// The roster's stored pubkey wins over whatever the message carries in
	// Author.EntmootPubKey: a forged message could put a valid-looking key
	// in the author slot and sign with its matching private key.
	signing := msg
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return fmt.Errorf("gossip: canonical encode: %w", err)
	}
	if !keystore.Verify(author.EntmootPubKey, sigInput, msg.Signature) {
		return fmt.Errorf("%w: message %s", entmoot.ErrSigInvalid, msg.ID)
	}
	if canonical.MessageID(msg) != msg.ID {
		return fmt.Errorf("gossip: message id does not match canonical hash")
	}
	return nil
}

// signGossip canonicalizes frame with Signature zeroed, signs with id, and
// stores the resulting signature back into frame.
func signGossip(frame *wire.Gossip, id *keystore.Identity) error {
	signing := *frame
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return err
	}
	frame.Signature = id.Sign(sigInput)
	return nil
}

// verifyGossipSig verifies a Gossip frame against pubKey using the canonical
// encoding of the signing form (Signature zeroed).
func verifyGossipSig(frame *wire.Gossip, pubKey []byte) bool {
	signing := *frame
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return false
	}
	return keystore.Verify(pubKey, sigInput, frame.Signature)
}

// enqueueRetry inserts (or refreshes) a pending retry slot for a failed
// push or fetch. First-attempt failures start at attempts=1 so the next
// wait uses retryBackoff[1] (2s). frame must be non-nil for opPush and is
// ignored for opFetch.
func (g *Gossiper) enqueueRetry(key retryKey, frame *wire.Gossip) {
	g.pendMu.Lock()
	defer g.pendMu.Unlock()
	now := g.clk.Now()
	state, ok := g.pending[key]
	if !ok {
		state = &retryState{attempts: 1, frame: frame}
	} else {
		state.attempts++
		if frame != nil {
			state.frame = frame
		}
	}
	if state.attempts >= len(retryBackoff) {
		// Budget exhausted. We already warn once when dropping; patch 7
		// reconcile-on-reconnect is the final recovery path.
		g.logger.Warn("gossip: retry budget exhausted",
			slog.Uint64("peer", uint64(key.peer)),
			slog.String("id", key.id.String()),
			slog.String("op", key.op.String()))
		delete(g.pending, key)
		return
	}
	state.nextAt = now.Add(retryBackoff[state.attempts])
	g.pending[key] = state
}

// retryLoop walks g.pending at retryTickInterval and fires any entry whose
// nextAt has passed. Successful attempts remove the entry; failures
// re-enqueue (which advances the backoff schedule).
func (g *Gossiper) retryLoop(ctx context.Context) {
	t := time.NewTicker(retryTickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.drainDueRetries(ctx)
		}
	}
}

// drainDueRetries snapshots the currently-due pending entries under the
// mutex, then fires each outside the lock so a slow dial never blocks new
// inserts. Success removes the entry; failure calls enqueueRetry which
// bumps the attempt count and schedules the next wait.
//
// v1.0.6: the previously-sequential loop over `ready` is now an errgroup
// bounded by fanoutMaxConcurrency, with each attempt wrapped in a
// fanoutPerPeerTimeout context so a single dead peer's retry slot cannot
// stall the rest of the due set. The "only clear if the state we just
// executed is still the one on record" concurrent-safety pattern moves
// inside the goroutine — success still clears under pendMu, failure
// still re-enqueues.
func (g *Gossiper) drainDueRetries(ctx context.Context) {
	now := g.clk.Now()
	type due struct {
		key   retryKey
		state *retryState
	}
	var ready []due
	g.pendMu.Lock()
	for k, s := range g.pending {
		if !now.Before(s.nextAt) {
			ready = append(ready, due{key: k, state: s})
		}
	}
	g.pendMu.Unlock()

	if len(ready) == 0 {
		return
	}

	var eg errgroup.Group
	eg.SetLimit(fanoutMaxConcurrency)
	for _, d := range ready {
		d := d // capture
		eg.Go(func() error {
			dctx, cancel := context.WithTimeout(ctx, fanoutPerPeerTimeout)
			defer cancel()
			// Execute without holding the mutex. Re-enqueue on failure advances
			// the schedule; success clears the slot.
			err := g.executeRetry(dctx, d.key, d.state)
			if err == nil {
				g.pendMu.Lock()
				// Only clear if the state we just executed is still the one on
				// record — a concurrent success elsewhere could have already
				// replaced or cleared it.
				if cur, ok := g.pending[d.key]; ok && cur == d.state {
					delete(g.pending, d.key)
				}
				g.pendMu.Unlock()
				return nil
			}
			g.logger.Warn("gossip: retry",
				slog.Uint64("peer", uint64(d.key.peer)),
				slog.String("id", d.key.id.String()),
				slog.String("op", d.key.op.String()),
				slog.Int("attempt", d.state.attempts+1),
				slog.String("err", err.Error()))
			g.enqueueRetry(d.key, d.state.frame)
			return nil // retry failures never fail the group
		})
	}
	_ = eg.Wait()
}

// executeRetry runs a single pending attempt. Returns nil on success so the
// scheduler can clear the slot; returns the inner error on transient
// failure so the scheduler can re-enqueue.
func (g *Gossiper) executeRetry(ctx context.Context, key retryKey, state *retryState) error {
	switch key.op {
	case opPush:
		if state.frame == nil {
			return fmt.Errorf("retry: push missing frame")
		}
		return g.pushGossip(ctx, key.peer, state.frame)
	case opFetch:
		return g.fetchFrom(ctx, key.peer, key.id)
	case opIHave:
		// Plumtree IHave is a single-id advertisement; retry re-dials
		// and re-sends. We don't stash the frame on the retry state
		// because IHave carries no signature and reconstructing the
		// single-id frame from the key is trivial.
		frame := &wire.IHave{
			GroupID: g.cfg.GroupID,
			IDs:     []entmoot.MessageID{key.id},
		}
		return g.sendIHave(ctx, key.peer, frame)
	case opGraft:
		return g.sendGraft(ctx, key.peer, []entmoot.MessageID{key.id})
	case opPrune:
		return g.sendPrune(ctx, key.peer)
	default:
		return fmt.Errorf("retry: unknown op %d", key.op)
	}
}

// maybeReconcile fires reconcileWith in a goroutine iff the per-peer
// cooldown has elapsed. Trigger sites call this on new inbound connections
// and successful outbound dials so anti-entropy happens whenever tunnel
// state looks fresh, without turning into a reconciliation storm during
// chatty patches.
func (g *Gossiper) maybeReconcile(ctx context.Context, peer entmoot.NodeID) {
	g.pendMu.Lock()
	last, ok := g.lastReconciled[peer]
	now := g.clk.Now()
	if ok && now.Sub(last) < reconcileCooldown {
		g.pendMu.Unlock()
		return
	}
	g.lastReconciled[peer] = now
	g.pendMu.Unlock()

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.reconcileWith(ctx, peer)
	}()
}

// reconcileWith runs a round of anti-entropy against peer: compare merkle
// roots; if they differ, query the peer for message ids it has that we
// don't, and fetch each. Silently no-ops against older peers that don't
// understand the Range message.
func (g *Gossiper) reconcileWith(ctx context.Context, peer entmoot.NodeID) {
	// Step 1: exchange merkle roots so we can short-circuit the common case
	// where the peer has nothing new.
	theirRoot, ok := g.fetchPeerRoot(ctx, peer)
	if !ok {
		return
	}
	ourRoot, err := g.cfg.Store.MerkleRoot(ctx, g.cfg.GroupID)
	if err != nil {
		g.logger.Warn("gossip: reconcile: local merkle root",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return
	}
	if ourRoot == [32]byte(theirRoot) {
		return
	}

	// Step 2: ask for the peer's message ids since our latest-known
	// timestamp. A fresh node starts at 0 and pulls the full history.
	since, err := g.latestLocalTimestamp(ctx)
	if err != nil {
		g.logger.Warn("gossip: reconcile: latest timestamp",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return
	}
	ids, err := g.fetchPeerRange(ctx, peer, since)
	if err != nil {
		// Most likely failure mode: the peer doesn't support Range
		// (pre-patch-7 build). Log at debug and stop — we can only do
		// as well as the peer's capabilities allow. The existing push
		// path will still deliver anything they subsequently publish.
		g.logger.Debug("gossip: reconcile: range query failed",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return
	}

	// Step 3: FetchReq each id we don't have. Missing bodies are a normal
	// intermediate state while retries run, so individual failures are
	// logged at debug; the retry loop handles them separately.
	for _, id := range ids {
		has, hasErr := g.cfg.Store.Has(ctx, g.cfg.GroupID, id)
		if hasErr != nil {
			continue
		}
		if has {
			continue
		}
		if ferr := g.fetchFrom(ctx, peer, id); ferr != nil {
			g.enqueueRetry(retryKey{peer: peer, id: id, op: opFetch}, nil)
		}
	}
}

// fetchPeerRoot opens a connection to peer and sends MerkleReq. Returns the
// peer's root + ok on success, zero + !ok on any error so the caller can
// abort cleanly.
func (g *Gossiper) fetchPeerRoot(ctx context.Context, peer entmoot.NodeID) (wire.MerkleRoot, bool) {
	// v1.0.6 Fix C: short-circuit if peer is in its dial-backoff window.
	// The debug-log site below swallows the error shape, so emitting the
	// same "dial for root" line with the synthetic short-circuit error
	// keeps reconciliation logs consistent with the peer being unreachable.
	if !g.canDial(peer) {
		g.logger.Debug("gossip: reconcile: dial for root",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", fmt.Sprintf("peer %d in dial-backoff", peer)))
		return wire.MerkleRoot{}, false
	}
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		g.recordDialFailure(peer)
		g.logger.Debug("gossip: reconcile: dial for root",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return wire.MerkleRoot{}, false
	}
	g.recordDialSuccess(peer)
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, &wire.MerkleReq{GroupID: g.cfg.GroupID}); err != nil {
		g.logger.Debug("gossip: reconcile: write merkle_req",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return wire.MerkleRoot{}, false
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		g.logger.Debug("gossip: reconcile: read merkle_resp",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return wire.MerkleRoot{}, false
	}
	resp, ok := payload.(*wire.MerkleResp)
	if !ok {
		return wire.MerkleRoot{}, false
	}
	return resp.Root, true
}

// fetchPeerRange asks peer for every message id whose Timestamp >=
// sinceMillis. Returns the slice of ids or an error (including
// ErrUnknownMessage when the peer doesn't understand RangeReq).
func (g *Gossiper) fetchPeerRange(ctx context.Context, peer entmoot.NodeID, sinceMillis int64) ([]entmoot.MessageID, error) {
	// v1.0.6 Fix C: short-circuit if peer is in its dial-backoff window.
	if !g.canDial(peer) {
		return nil, fmt.Errorf("peer %d in dial-backoff", peer)
	}
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		g.recordDialFailure(peer)
		return nil, fmt.Errorf("dial: %w", err)
	}
	g.recordDialSuccess(peer)
	defer conn.Close()
	req := &wire.RangeReq{GroupID: g.cfg.GroupID, SinceMillis: sinceMillis}
	if err := wire.EncodeAndWrite(conn, req); err != nil {
		return nil, fmt.Errorf("write range_req: %w", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		return nil, fmt.Errorf("read range_resp: %w", err)
	}
	resp, ok := payload.(*wire.RangeResp)
	if !ok {
		return nil, fmt.Errorf("range: unexpected response type")
	}
	return resp.IDs, nil
}

// latestLocalTimestamp returns the maximum Timestamp across all messages we
// already hold for the group, or 0 if empty. Used as the `since` parameter
// in reconciliation range queries so we only pull the tail we're missing.
func (g *Gossiper) latestLocalTimestamp(ctx context.Context) (int64, error) {
	msgs, err := g.cfg.Store.Range(ctx, g.cfg.GroupID, 0, 0)
	if err != nil {
		return 0, err
	}
	var max int64
	for _, m := range msgs {
		if m.Timestamp > max {
			max = m.Timestamp
		}
	}
	return max, nil
}
