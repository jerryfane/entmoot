package gossip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/ratelimit"
	"entmoot/pkg/entmoot/reconcile"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"
)

// Retry backoff parameters (v1.0.7). Previously a deterministic
// array [1s, 2s, 4s, 8s, 16s, 30s, 60s×4]; now a decorrelated-
// jitter schedule matching Marc Brooker's AWS pattern (2015).
// Keeps the same envelope (base 1 s, cap 60 s) while randomising
// each interval to prevent correlated retry storms — when all
// nodes share a flakiness window (NAT flap, registry blip) the
// deterministic schedule synchronised every retry to the same
// wall-clock moment, so every retry hit the same bad window.
// Decorrelated jitter is the AWS-blessed formula (Full Jitter is
// equivalent but needs an attempt counter):
//
//	next = min(cap, random(base, prev * 3))
//
// Retry budget remains 10 attempts via retryMaxAttempts — a peer
// that fails 10 straight retries still gets dropped per v1.0.4
// semantics.
const (
	retryBackoffBase = 1 * time.Second
	retryBackoffCap  = 60 * time.Second
	retryMaxAttempts = 10
)

// Transport-ad retry caps (v1.4.1). A separate budget from
// retryMaxAttempts because ads are comparatively scarce, authoritative
// records — six attempts across the decorrelated-jitter schedule is
// plenty for a transient NAT flap, but retrying 10× an already-
// superseded ad would just waste dials. The wall-clock ceiling is the
// independent termination condition: no matter how many attempts have
// run, stop re-dialling five minutes after the original fanout
// failure. Matches the envelope of dialBackoffCap so a once-dead peer
// can't keep an ad retry ping-ponging forever. (v1.4.1)
const (
	adRetryMaxAttempts    = 6
	adRetryWallClockLimit = 5 * time.Minute
)

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

// inlineBodyThreshold bounds when Publish / refanout inline the
// full Message body in a Gossip frame instead of relying on the
// receiver to fetch it back. 4 KiB covers typical chat and small
// documents while stopping short of large payloads that would
// balloon fanout bandwidth — matches 4× the IPFS Bitswap
// WantHaveReplaceSize default (1 KiB) and aligns with GossipSub /
// Scuttlebutt / Matrix "inline small, pull large" convention.
// (v1.0.7)
const inlineBodyThreshold = 4 * 1024

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

// Reconcile cooldown parameters (v1.0.7). Previously a hard-coded
// 60 s constant; now a 30 s base with ±20 % multiplicative jitter.
// Reduced because research (libp2p GossipSub's 1 s heartbeat,
// Plumtree §3.4 continuous lazy-push) shows 60 s was 1–2 orders
// of magnitude too slow for failed-push recovery — only bulk
// Merkle-tree systems (Riak AAE, Cassandra repair) run at the
// minute scale. The 20 % multiplicative jitter prevents fleet-wide
// collisions on the cooldown boundary after correlated events
// (memberlist-style randomStagger).
const (
	reconcileCooldownBase    = 30 * time.Second
	reconcileCooldownJitter  = 6 * time.Second // ±20 %
	reconcileFailureCooldown = 5 * time.Second
)

// Background AE ticker (v1.2.1). Serf-style push-pull: 30 s base with
// jitter, one least-recently-reconciled peer per tick. The cooldown
// gate in maybeReconcile dedupes against reactive triggers (push,
// accept, OnTunnelUp). Skip-when-roots-equal keeps idle meshes quiet.
const (
	reconcilerTickBase   = 30 * time.Second
	reconcilerTickJitter = 6 * time.Second // ±20 %
)

// reconcileSessionTimeout bounds the wallclock budget of a single RBSR
// reconciliation session — the full multi-frame exchange between initiator
// and responder, not per-frame. Anti-entropy against a 10^4-message group
// converges in O(log diff) rounds; the 15-second envelope covers network
// round-trip variance plus body-fetch latency for the ids produced by the
// session. If the session exceeds this budget the initiator tears the
// connection down and the next reconcileCooldown expiry re-tries. (v1.2.1)
const reconcileSessionTimeout = 15 * time.Second

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
	// Transport-ad fanout retry op (v1.4.1). Mirrors opPush semantics —
	// same scheduler goroutine, same decorrelated-jitter backoff — but
	// carries a signed *wire.TransportAd in retryState.ad instead of a
	// Gossip frame. The author / seq fields on retryKey make the slot
	// key unique per (peer, author) and let executeRetry short-circuit
	// superseded ads via TransportAdStore.GetTransportAd before dialling.
	// See enqueueAdRetry.
	opTransportAd retryOp = 6
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
	case opTransportAd:
		return "transport_ad"
	default:
		return "unknown"
	}
}

// retryKey identifies a single pending retry slot. One slot per
// (peer, message, direction) for message-typed retries; for
// opTransportAd the slot is keyed on (peer, author) and `id` is
// zero. At most one transport-ad retry per (peer, author) is in
// flight at a time — a newer ad for the same author replaces the
// previous entry in enqueueAdRetry. (v1.4.1 extends this struct
// with `author` for opTransportAd; all other ops leave it zero.)
type retryKey struct {
	peer   entmoot.NodeID
	id     entmoot.MessageID
	author entmoot.NodeID
	op     retryOp
}

// retryState tracks how many attempts have run and when the next is due.
// frame is populated only for pushes — fetches re-issue FetchReq from id.
// For opTransportAd: ad holds the signed TransportAd to retransmit, seq
// is the ad's sequence number (checked against the store's current seq
// before each retry to drop superseded entries), and firstTry is when
// the very first fanout attempt failed so drainDueRetries can enforce
// adRetryWallClockLimit regardless of the jitter envelope. (v1.4.1)
type retryState struct {
	attempts    int
	nextAt      time.Time
	lastBackoff time.Duration // v1.0.7: feeds nextBackoff(prev); 0 on first failure
	frame       *wire.Gossip
	ad          *wire.TransportAd
	seq         uint64
	firstTry    time.Time
}

// reconcileState caches per-peer anti-entropy cooldown bookkeeping.
// at is when we last fired reconcileWith; cooldown is the jittered
// window chosen at that firing so a "just under the cooldown" peer
// doesn't re-probe the boundary every tick (v1.0.7).
type reconcileState struct {
	at       time.Time
	cooldown time.Duration
}

// defaultFanout is the number of random peers a Publish pushes a new message
// id to when Config.Fanout is zero. Chosen for the canary (3-node group); the
// real gossip math is O(log N + k) and we'll revisit for v1.
const defaultFanout = 3

// transportAdTopic is the topic key passed to the rate limiter for inbound
// TransportAd frames. Namespaced under "_pilot/*" so regular content topics
// never collide. Kept in gossiper.go because receivers consume it; the
// advertiser on the publish side uses the same string by contract. (v1.2.0)
const transportAdTopic = "_pilot/transport/v1"

// transportAdMaxEndpoints bounds the number of (network, addr) pairs one
// TransportAd can advertise. Matches the plan's 4-entry cap — enough for
// tcp+udp over IPv4+IPv6 or a couple of relay hints — and keeps the
// canonical-encoded size inside transportAdMaxBytes with slack. (v1.2.0)
const transportAdMaxEndpoints = 4

// transportAdMaxBytes caps the canonical-encoded size of a single
// TransportAd. 1 KiB with 4 endpoints + Ed25519 signature leaves generous
// headroom; anything larger indicates a malformed or hostile ad. (v1.2.0)
const transportAdMaxBytes = 1024

// transportAdRefreshInterval is the weekly safety-net tick at which the
// advertiser re-publishes this node's TransportAd even when no endpoint
// change was signalled. Chosen shorter than transportAdTTL so an
// unchanged ad stays retrievable at every receiver across a refresh
// cycle. (v1.2.0)
const transportAdRefreshInterval = 6 * 24 * time.Hour

// transportAdTTL is how long an emitted TransportAd stays valid after
// IssuedAt. Receivers filter expired ads on query; the advertiser
// refreshes inside the window so an unchanged endpoint set stays
// continuously advertised. (v1.2.0)
const transportAdTTL = 7 * 24 * time.Hour

// transportAdGCInterval is how often the advertiser opportunistically
// triggers store.GCExpiredTransportAds. Piggybacks on the refresh tick
// so no separate ticker is required; 6 days is dominant. (v1.2.0)

// TransportAdStore is the subset of the message-store surface the
// gossiper needs to process TransportAd gossip. Implemented by
// *store.SQLite. Kept as its own interface so the in-memory MessageStore
// used by unit tests can omit it entirely — the gossiper no-ops every
// transport-ad code path when TransportAdStore is nil. (v1.2.0)
type TransportAdStore interface {
	// PutTransportAd stores ad, applying LWW semantics (seq first, then
	// lexicographic signature as tiebreak). Returns replaced=true iff the
	// incoming ad superseded stored state.
	PutTransportAd(ctx context.Context, ad wire.TransportAd) (bool, error)
	// GetTransportAd returns the current ad for (groupID, authorNodeID)
	// or ok=false if none is stored.
	GetTransportAd(ctx context.Context, groupID entmoot.GroupID, authorNodeID entmoot.NodeID) (wire.TransportAd, bool, error)
	// GetAllTransportAds returns every stored ad for the group, filtered
	// by expiry when includeExpired=false. Used to answer
	// TransportSnapshotReq frames.
	GetAllTransportAds(ctx context.Context, groupID entmoot.GroupID, now time.Time, includeExpired bool) ([]wire.TransportAd, error)
	// BumpTransportAdSeq atomically increments and returns this node's
	// own ad sequence counter for (groupID, authorNodeID).
	BumpTransportAdSeq(ctx context.Context, groupID entmoot.GroupID, authorNodeID entmoot.NodeID) (uint64, error)
	// GCExpiredTransportAds deletes every ad whose NotAfter is strictly
	// before now. Returns the number of rows deleted.
	GCExpiredTransportAds(ctx context.Context, now time.Time) (int64, error)
}

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

	// TransportAdStore is where signed TransportAd records land after
	// validation and where the advertiser reads back the current seq at
	// publish time. nil disables the entire v1.2.0 transport-ad code
	// path — inbound frames are dropped with a debug log, the advertiser
	// loop never runs, and TransportSnapshotReq is answered "empty".
	// Production wires this to the same *store.SQLite the message-store
	// code uses (both sets of tables share one database per group).
	// (v1.2.0)
	TransportAdStore TransportAdStore

	// RateLimiter applies the per-(peer, topic) quota on the inbound
	// TransportAd receive path via AllowTopic. nil disables topic-level
	// rate limiting (global per-peer limits, if any, still apply via the
	// wire layer). The v1.2.0 defaults ship tighter than needed for the
	// transport-ad topic; see ratelimit.DefaultTopicLimits. (v1.2.0)
	RateLimiter *ratelimit.Limiter

	// LocalEndpoints returns the current transport endpoints of this node.
	// Called by the advertiser loop at startup, on EndpointsChanged
	// signal, and on the weekly refresh ticker. Nil (or a func returning
	// an empty slice) means "don't advertise" — useful for tests or for
	// nodes that can't reach pilot. (v1.2.0)
	LocalEndpoints func() []entmoot.NodeEndpoint

	// EndpointsChanged receives a tick whenever LocalEndpoints is known
	// to have changed (e.g. pilot reports a new TCP listen port).
	// Non-blocking send recommended; the advertiser loop collapses
	// bursts. Nil means "no change signal, rely on the weekly refresh
	// alone". (v1.2.0)
	EndpointsChanged <-chan struct{}

	// HideIP, when true, instructs the advertiser loop to suppress
	// every UDP / TCP endpoint from the published TransportAd and
	// publish only a TURN relay entry (if one is available via
	// LocalEndpoints). If HideIP is set AND no TURN endpoint is
	// present, the advertiser emits NOTHING and logs a warning —
	// the node is unreachable until a relay is configured. This
	// opt-in mode trades availability for IP-address privacy and
	// requires pilot-daemon v1.9.0-jf.8+ with a TURN provider for
	// any useful reachability. (v1.4.0)
	HideIP bool
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
	pendMu            sync.Mutex
	pending           map[retryKey]*retryState
	lastReconciled    map[entmoot.NodeID]reconcileState
	reconcileInFlight map[entmoot.NodeID]struct{}
	reconcileFailures map[entmoot.NodeID]reconcileState
	// lastKnownPeerRoot caches the Merkle root each peer reported on the
	// most recent successful reconcile handshake. Phase 7 / Part E
	// consults it before firing a reconcile to tick-skip peers we already
	// know we're aligned with. Guarded by pendMu so it shares the same
	// lock domain as lastReconciled (both are updated at the end of a
	// reconcile round). (v1.2.1)
	lastKnownPeerRoot map[entmoot.NodeID]wire.MerkleRoot

	// rng is a dedicated *rand.Rand for retry-backoff and cooldown
	// jitter (v1.0.7 Fix B). Seeded in New from the clock the same
	// way getPicker seeds its rand source, so Fake-clock tests remain
	// deterministic. math/rand/v2.Rand is NOT goroutine-safe; access
	// is serialised under rngMu. Kept separate from getPicker's rand
	// so peer-sampling determinism isn't entangled with jitter.
	rngMu sync.Mutex
	rng   *rand.Rand

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
	plumMu         sync.Mutex
	eagerPushPeers map[entmoot.NodeID]struct{}
	lazyPushPeers  map[entmoot.NodeID]struct{}
	pendingGraft   map[pendingGraftKey]*time.Timer

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
	// Seed off the clock so Fake-clock tests stay deterministic.
	// Mirrors the getPicker seeding pattern (same file, below) so
	// both rand sources advance predictably in tests.
	seed := clk.Now().UnixNano()
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed>>1)^0x9E3779B97F4A7C15))
	return &Gossiper{
		cfg:               cfg,
		logger:            logger,
		clk:               clk,
		fanout:            fanout,
		pending:           make(map[retryKey]*retryState),
		lastReconciled:    make(map[entmoot.NodeID]reconcileState),
		reconcileInFlight: make(map[entmoot.NodeID]struct{}),
		reconcileFailures: make(map[entmoot.NodeID]reconcileState),
		lastKnownPeerRoot: make(map[entmoot.NodeID]wire.MerkleRoot),
		rng:               rng,
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

// isTrustedSender reports whether remote is in Pilot's current
// trust-peer set. Reuses trustedSet's cached snapshot so a
// receive-path call doesn't trigger extra IPC RTTs.
//
// v1.4.2: returns true on cold-start (nil snapshot) to match
// plumEagerExcept's fail-open precedent. Downstream signature +
// roster-membership checks remain the hard integrity floor, so the
// cold-start window is harmless — at worst, an unknown sender's
// properly-signed ad from a valid roster author is accepted, same
// as if the author had sent it directly.
func (g *Gossiper) isTrustedSender(ctx context.Context, remote entmoot.NodeID) bool {
	trusted := g.trustedSet(ctx)
	if trusted == nil {
		return true // cold-start: fail-open, see doc comment.
	}
	_, ok := trusted[remote]
	return ok
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

func isRetryableStreamError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func setConnDeadlineFromContext(ctx context.Context, conn net.Conn) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
}

func (g *Gossiper) dropPeerSession(peer entmoot.NodeID, op string, cause error) bool {
	dropper, ok := g.cfg.Transport.(PeerSessionDropper)
	if !ok {
		return false
	}
	dropped := dropper.DropPeerSession(peer)
	if dropped {
		g.logger.Debug("gossip: dropped cached peer session",
			slog.Uint64("peer", uint64(peer)),
			slog.String("op", op),
			slog.String("err", cause.Error()))
	}
	return dropped
}

func (g *Gossiper) requestResponse(ctx context.Context, peer entmoot.NodeID, req any, expected wire.MsgType, op string) (any, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if !g.canDial(peer) {
			return nil, fmt.Errorf("peer %d in dial-backoff", peer)
		}
		conn, err := g.cfg.Transport.Dial(ctx, peer)
		if err != nil {
			g.recordDialFailure(peer)
			return nil, fmt.Errorf("%s: dial %d: %w", op, peer, err)
		}
		setConnDeadlineFromContext(ctx, conn)

		err = wire.EncodeAndWrite(conn, req)
		if err != nil {
			_ = conn.Close()
			lastErr = fmt.Errorf("%s: write: %w", op, err)
			if attempt == 0 && isRetryableStreamError(err) {
				g.dropPeerSession(peer, op, err)
				continue
			}
			return nil, lastErr
		}

		msgType, payload, err := wire.ReadAndDecode(conn)
		_ = conn.Close()
		if err != nil {
			lastErr = fmt.Errorf("%s: read: %w", op, err)
			if attempt == 0 && isRetryableStreamError(err) {
				g.dropPeerSession(peer, op, err)
				continue
			}
			if isRetryableStreamError(err) {
				g.recordDialFailure(peer)
			}
			return nil, lastErr
		}
		if msgType != expected {
			return nil, fmt.Errorf("%s: unexpected response type %s", op, msgType.String())
		}
		g.recordDialSuccess(peer)
		return payload, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%s: exhausted retry budget", op)
	}
	return nil, lastErr
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

	// Install the tunnel-up callback (v1.2.1, Part D): every freshly-
	// established Pilot session fires this hook, which triggers a
	// cooldown-gated reconcile against the peer whose tunnel just came
	// up. The hook is de-duplicated against the reactive triggers
	// already wired into the accept loop and fetchFrom — maybeReconcile
	// is async and consults the per-peer cooldown window before firing
	// reconcileWith — so worst-case cost is one cooldown-check.
	//
	// The callback must be cleared BEFORE g.wg.Wait() runs so a late
	// tunnel-up cannot race its g.wg.Add(1) against the in-flight
	// Wait — sync.WaitGroup panics on Add-after-done. The clear
	// happens on every exit path of the accept loop below, not via
	// defer (defers run AFTER wg.Wait).
	g.cfg.Transport.SetOnTunnelUp(func(peer entmoot.NodeID) {
		// Re-check ctx so a stale callback fired during shutdown
		// after the clear-and-wait window doesn't re-enter
		// maybeReconcile and spawn a goroutine that outlives Start.
		if err := ctx.Err(); err != nil {
			return
		}
		g.maybeReconcile(ctx, peer)
	})

	// Background workers (patch 6, patch 7). retryLoop drains the
	// exponential-backoff queue for push/fetch attempts that failed their
	// initial try. Both live under g.wg so Start's drain on shutdown also
	// covers them.
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.retryLoop(ctx)
	}()

	// Background anti-entropy ticker (v1.2.1, Part E). Picks the
	// least-recently-reconciled peer each tick and fires a reconcile
	// against them — unless their cached root matches ours, in which
	// case the tick is a silent no-op. Paired with the reactive
	// maybeReconcile triggers so a partitioned peer with no chatty
	// messages still converges on the next tick after partition heals.
	g.wg.Add(1)
	go g.reconcilerLoop(ctx)

	// Transport-ad advertiser loop (v1.2.0). Publishes this node's own
	// TransportAd on startup, on EndpointsChanged, and on a weekly
	// safety-net ticker. Only runs when TransportAdStore and
	// LocalEndpoints are both configured — otherwise the receive path
	// still works (for peers that run it) but we emit nothing.
	if g.cfg.TransportAdStore != nil && g.cfg.LocalEndpoints != nil {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			g.advertiserLoop(ctx)
		}()
	}

	for {
		conn, remote, err := g.cfg.Transport.Accept(ctx)
		if err != nil {
			// Accept returning is the signal to shut down. Clear the
			// OnTunnelUp callback BEFORE wg.Wait so a late tunnel-up
			// cannot race a fresh g.wg.Add(1) against the in-flight
			// Wait (sync.WaitGroup panics on that). A callback
			// fired-but-not-yet-returned is still fine — the
			// ctx-err re-check inside the closure keeps it from
			// entering maybeReconcile after shutdown begins. (v1.2.1)
			g.cfg.Transport.SetOnTunnelUp(nil)
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
		//
		// v1.2.1 (Part F): also clear any outbound dial-backoff for this peer.
		// Inbound reachability is strong bidirectional-reachability evidence,
		// so a cached backoff window from an earlier outbound failure is
		// almost certainly stale. If our next outbound dial happens to still
		// fail, recordDialFailure will re-arm the backoff from base.
		g.recordDialSuccess(remote)
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
	case *wire.TransportAd:
		g.onTransportAd(ctx, remote, v)
	case *wire.TransportSnapshotReq:
		g.onTransportSnapshotReq(ctx, c, remote, v)
	case *wire.Reconcile:
		// Convention-break (v1.2.1): onReconcileReq holds c open for the
		// full multi-frame RBSR session, unlike every other handler that
		// does "read one frame, respond, return". The caller's defer
		// c.Close() still fires when onReconcileReq eventually returns.
		g.onReconcileReq(ctx, c, remote, v)
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

// onReconcileReq drives the responder side of an RBSR session over a
// single connection. Unlike every other handler, this one holds the conn
// open across multiple request/response frames — the initiator and
// responder alternate reconcile.Range frames until both sides observe
// convergence. The caller (handleConn) must not close c until
// onReconcileReq returns (either via clean done-both or an error path).
//
// This is the only v1.2.1 handler that breaks the "one frame, then
// close" convention — it's explicit here and in the handleConn dispatch
// comment. The conn is still owned by handleConn's defer c.Close(),
// which now fires only after onReconcileReq unwinds.
func (g *Gossiper) onReconcileReq(ctx context.Context, c net.Conn, remote entmoot.NodeID, req *wire.Reconcile) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: reconcile: group mismatch",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}

	adapter := &storeAdapter{store: g.cfg.Store, groupID: g.cfg.GroupID}
	sess := reconcile.NewResponder(reconcile.DefaultConfig(), adapter)

	// Process the frame already received, then loop until both sides
	// signal Done. The responder trusts its own Session.Done() (not the
	// peer's Done flag alone) for its termination half.
	incoming := req.Ranges
	round := req.Round
	peerDone := req.Done
	var missingIDs []entmoot.MessageID
	for {
		out, newlyMissing, sessDone, err := sess.Next(ctx, incoming)
		if err != nil {
			if errors.Is(err, reconcile.ErrMaxRounds) {
				g.logger.Warn("gossip: reconcile: max rounds (responder)",
					slog.Uint64("remote", uint64(remote)),
					slog.Int("round", int(round)))
			} else {
				g.logger.Warn("gossip: reconcile: responder step",
					slog.Uint64("remote", uint64(remote)),
					slog.Int("round", int(round)),
					slog.String("err", err.Error()))
			}
			return
		}
		missingIDs = append(missingIDs, newlyMissing...)
		myDone := sessDone && len(out) == 0
		g.logger.Debug("gossip: reconcile: responder round",
			slog.Uint64("remote", uint64(remote)),
			slog.Int("round", int(round)),
			slog.Int("outgoing_ranges", len(out)),
			slog.Int("newly_missing", len(newlyMissing)),
			slog.Int("total_missing", len(missingIDs)),
			slog.Bool("sess_done", sessDone),
			slog.Bool("peer_done", peerDone))
		resp := &wire.Reconcile{
			GroupID: g.cfg.GroupID,
			Round:   round,
			Ranges:  out,
			Done:    myDone,
		}
		if err := wire.EncodeAndWrite(c, resp); err != nil {
			g.logger.Warn("gossip: reconcile: responder write",
				slog.Uint64("remote", uint64(remote)),
				slog.Int("round", int(round)),
				slog.String("err", err.Error()))
			return
		}
		if peerDone && myDone {
			g.fetchMissingFrom(ctx, remote, missingIDs)
			g.logger.Debug("gossip: reconcile: responder complete",
				slog.Uint64("remote", uint64(remote)),
				slog.Int("round", int(round)),
				slog.Int("missing", len(missingIDs)))
			return
		}

		// Read the next frame from the initiator.
		msgType, payload, err := wire.ReadAndDecode(c)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Clean close from the initiator — either they're done
				// or they observed a fatal error on their side.
				if len(missingIDs) > 0 {
					g.fetchMissingFrom(ctx, remote, missingIDs)
					g.logger.Debug("gossip: reconcile: responder fetched after eof",
						slog.Uint64("remote", uint64(remote)),
						slog.Int("round", int(round)),
						slog.Int("missing", len(missingIDs)))
				}
				return
			}
			g.logger.Warn("gossip: reconcile: responder read",
				slog.Uint64("remote", uint64(remote)),
				slog.Int("round", int(round)),
				slog.String("err", err.Error()))
			return
		}
		if msgType != wire.MsgReconcile {
			g.logger.Warn("gossip: reconcile: responder unexpected type",
				slog.Uint64("remote", uint64(remote)),
				slog.String("got", msgType.String()))
			return
		}
		next, ok := payload.(*wire.Reconcile)
		if !ok {
			g.logger.Warn("gossip: reconcile: responder decode mismatch",
				slog.Uint64("remote", uint64(remote)))
			return
		}
		incoming = next.Ranges
		round = next.Round
		peerDone = next.Done
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

		// Inline-body fast path (v1.0.7). Sender may have attached
		// the full Message. If so, hash-verify against id and store
		// directly — eliminates the fetchFrom round-trip. Integrity
		// checks mirror fetchFrom's trio: id==canonical hash, the
		// Message's own Ed25519 signature verifies against the
		// author's roster pubkey, and the Store.Put succeeds. Falls
		// through to the v1.0.6 fetch path on any mismatch/failure.
		if gos.Body != nil && len(gos.IDs) == 1 && gos.Body.ID == id {
			msg := *gos.Body
			if canonical.MessageID(msg) != id {
				g.logger.Warn("gossip: inline body hash mismatch",
					slog.Uint64("remote", uint64(remote)),
					slog.String("id", id.String()))
				continue
			}
			if err := g.verifyMessage(msg); err != nil {
				g.logger.Warn("gossip: inline body verify",
					slog.Uint64("remote", uint64(remote)),
					slog.String("id", id.String()),
					slog.String("err", err.Error()))
				continue
			}
			if err := g.cfg.Store.Put(ctx, msg); err != nil {
				g.logger.Warn("gossip: inline body store put",
					slog.Uint64("remote", uint64(remote)),
					slog.String("id", id.String()),
					slog.String("err", err.Error()))
				continue
			}
			// Same post-Put trio as fetchFrom (the v1.0.5 refanout hook).
			g.plumCancelGraftsFor(id)
			g.refanout(ctx, remote, id)
			g.maybeReconcile(ctx, remote)
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
	// v1.0.7: inline the body on refanout so downstream peers skip
	// the fetchFrom round-trip. Store.Get should succeed because
	// refanout is only called after Store.Put completes (either
	// from fetchFrom's success path or from the inline-body accept
	// path in onGossip). If it fails (message evicted?), drop to
	// ID-only semantics; downstream will fetch.
	if msg, err := g.cfg.Store.Get(ctx, g.cfg.GroupID, id); err == nil {
		maybeInlineBody(frame, msg)
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
	req := &wire.FetchReq{GroupID: g.cfg.GroupID, ID: id}
	payload, err := g.requestResponse(ctx, peer, req, wire.MsgFetchResp, "fetch_req")
	if err != nil {
		return err
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

// fetchMissingFrom fulfils ids learned during anti-entropy with the same
// body-fetch path used by gossip push and retry. Reconcile sessions exchange
// only IDs; both initiator and responder must pull bodies after discovering
// the peer has messages local storage lacks.
func (g *Gossiper) fetchMissingFrom(ctx context.Context, peer entmoot.NodeID, ids []entmoot.MessageID) {
	seen := make(map[entmoot.MessageID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		has, _ := g.cfg.Store.Has(ctx, g.cfg.GroupID, id)
		if has {
			continue
		}
		if ferr := g.fetchFrom(ctx, peer, id); ferr != nil {
			g.enqueueRetry(retryKey{peer: peer, id: id, op: opFetch}, nil)
		}
	}
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
	// v1.0.7: inline the body so eager-push peers can Store.Put
	// directly and skip the fetchFrom round-trip. No-op for messages
	// over inlineBodyThreshold — those still propagate via the
	// ID-only + fetch path.
	maybeInlineBody(frame, msg)
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

// maybeInlineBody attaches msg to a single-id Gossip frame when the
// canonical encoding of msg fits under inlineBodyThreshold. Sets
// frame.Body on success; no-op if the frame already has multiple
// ids or the body is too large. The caller must signGossip AFTER
// this (the Gossiper sig ignores Body, so ordering doesn't matter
// for correctness — but it's cleaner). (v1.0.7)
func maybeInlineBody(frame *wire.Gossip, msg entmoot.Message) {
	if len(frame.IDs) != 1 {
		return
	}
	enc, err := canonical.Encode(msg)
	if err != nil {
		return
	}
	if len(enc) > inlineBodyThreshold {
		return
	}
	m := msg
	frame.Body = &m
}

// signGossip canonicalizes frame with Signature (and v1.0.7 Body)
// zeroed, signs with id, and stores the resulting signature back into
// frame. Body is excluded from the signing scope so mixed v1.0.6 /
// v1.0.7 peers verify each other's signatures unchanged: a v1.0.6
// sender (no Body) signs as today, a v1.0.7 sender zeroes Body before
// signing, and both produce the same {GroupID, IDs, Timestamp}
// envelope. Body integrity is provided independently by (a) the
// canonical hash of Body matching IDs[0] and (b) the Message's own
// Ed25519 signature.
func signGossip(frame *wire.Gossip, id *keystore.Identity) error {
	signing := *frame
	signing.Signature = nil
	signing.Body = nil // v1.0.7: Body is not covered by the Gossiper sig
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return err
	}
	frame.Signature = id.Sign(sigInput)
	return nil
}

// verifyGossipSig verifies a Gossip frame against pubKey using the canonical
// encoding of the signing form (Signature and Body zeroed — see
// signGossip for why Body is excluded).
func verifyGossipSig(frame *wire.Gossip, pubKey []byte) bool {
	signing := *frame
	signing.Signature = nil
	signing.Body = nil // v1.0.7: match signGossip scope
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return false
	}
	return keystore.Verify(pubKey, sigInput, frame.Signature)
}

// nextBackoff returns the next retry delay given the previous
// delay (or 0 if this is the first failure). Decorrelated jitter:
//
//	next = min(cap, random_between(base, prev * 3))
//
// prev==0 produces a delay in [base, base+1ns) which rounds to
// exactly `base` — same behaviour as the old retryBackoff[0] = 1s.
func (g *Gossiper) nextBackoff(prev time.Duration) time.Duration {
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	lo := int64(retryBackoffBase)
	hi := int64(prev) * 3
	if hi <= lo {
		hi = lo + 1
	}
	span := hi - lo
	next := time.Duration(lo + g.rng.Int64N(span))
	if next > retryBackoffCap {
		next = retryBackoffCap
	}
	return next
}

// jitteredReconcileCooldown returns a cooldown duration drawn
// uniformly from [Base-Jitter, Base+Jitter). Callers cache the
// drawn value alongside lastReconciled so a peer that's been
// "just under" the cooldown doesn't probe the boundary every
// tick (v1.0.7).
func (g *Gossiper) jitteredReconcileCooldown() time.Duration {
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	span := int64(2 * reconcileCooldownJitter)
	return reconcileCooldownBase - reconcileCooldownJitter +
		time.Duration(g.rng.Int64N(span))
}

// jitteredReconcilerTick returns a tick interval drawn uniformly from
// [reconcilerTickBase - reconcilerTickJitter, reconcilerTickBase + reconcilerTickJitter).
// Uses the same rng+mutex pattern as jitteredReconcileCooldown. (v1.2.1)
func (g *Gossiper) jitteredReconcilerTick() time.Duration {
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	span := int64(2 * reconcilerTickJitter)
	delta := time.Duration(g.rng.Int64N(span))
	return reconcilerTickBase - reconcilerTickJitter + delta
}

// reconcilerLoop runs background anti-entropy (v1.2.1). Each tick picks
// the least-recently-reconciled roster member (excluding self) and
// fires maybeReconcile — unless the peer's cached Merkle root already
// matches ours, in which case the tick is a silent no-op.
//
// Interaction with reactive triggers:
//   - The cooldown gate in maybeReconcile dedupes any overlapping
//     reactive trigger (e.g., OnTunnelUp firing moments before the tick).
//   - The skip-when-roots-equal optimization keeps quiescent meshes
//     silent (no reconcile dial per peer per tick).
//
// Uses a plain time.Timer reset each iteration (not time.Ticker) so
// each tick draws fresh jitter.
func (g *Gossiper) reconcilerLoop(ctx context.Context) {
	defer g.wg.Done()

	// Initial delay — one tick before first fire, so startup bootstrap
	// isn't immediately followed by a background reconcile.
	t := time.NewTimer(g.jitteredReconcilerTick())
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.maybeReconcileTick(ctx)
			t.Reset(g.jitteredReconcilerTick())
		}
	}
}

// maybeReconcileTick picks one peer and fires (or skips) a reconcile.
// Factored out for unit-testability; reconcilerLoop is a thin wrapper.
// (v1.2.1)
func (g *Gossiper) maybeReconcileTick(ctx context.Context) {
	peer, ok := g.pickReconcileTickPeer()
	if !ok {
		return // no eligible peer
	}

	// Skip optimization: if peer's cached Merkle root matches ours,
	// this tick is a no-op.
	if cached, cachedOK := g.readLastKnownPeerRoot(peer); cachedOK {
		localRoot, err := g.cfg.Store.MerkleRoot(ctx, g.cfg.GroupID)
		if err == nil && wire.MerkleRoot(localRoot) == cached {
			g.logger.Debug("reconcile: tick skip (roots match cache)",
				slog.Uint64("peer", uint64(peer)))
			// Still update lastReconciled so this peer moves down in
			// the least-recently-reconciled ordering — otherwise we'd
			// keep picking the same peer forever with no work to do.
			g.markReconcileTickTouched(peer)
			return
		}
	}

	g.maybeReconcile(ctx, peer)
}

// pickReconcileTickPeer returns the least-recently-reconciled roster
// member excluding self, or (_, false) if no peer is eligible. Ties
// are broken deterministically via g.rng under rngMu. (v1.2.1)
func (g *Gossiper) pickReconcileTickPeer() (entmoot.NodeID, bool) {
	members := g.cfg.Roster.Members()
	if len(members) == 0 {
		return 0, false
	}

	g.pendMu.Lock()
	defer g.pendMu.Unlock()

	// Collect candidates with their last-reconciled time (zero = never).
	type candidate struct {
		peer entmoot.NodeID
		when time.Time
	}
	cands := make([]candidate, 0, len(members))
	for _, m := range members {
		if m == g.cfg.LocalNode {
			continue
		}
		when := time.Time{} // never reconciled
		if state, ok := g.lastReconciled[m]; ok {
			when = state.at
		}
		cands = append(cands, candidate{peer: m, when: when})
	}
	if len(cands) == 0 {
		return 0, false
	}

	// Find the minimum "when".
	var best entmoot.NodeID
	var bestTime time.Time
	var bestSet bool
	for _, c := range cands {
		if !bestSet || c.when.Before(bestTime) {
			best = c.peer
			bestTime = c.when
			bestSet = true
		}
	}

	// Break ties (peers with identical "when", especially multiple
	// never-reconciled) by picking uniformly at random among the tied
	// set.
	var tied []entmoot.NodeID
	for _, c := range cands {
		if c.when.Equal(bestTime) {
			tied = append(tied, c.peer)
		}
	}
	if len(tied) > 1 {
		g.rngMu.Lock()
		idx := g.rng.IntN(len(tied))
		g.rngMu.Unlock()
		best = tied[idx]
	}
	return best, true
}

// markReconcileTickTouched updates lastReconciled[peer].at to now
// without changing the cooldown. Used when the tick skips because
// roots match: we want this peer to move down the
// least-recently-reconciled queue so the next tick picks a different
// peer. (v1.2.1)
func (g *Gossiper) markReconcileTickTouched(peer entmoot.NodeID) {
	g.pendMu.Lock()
	defer g.pendMu.Unlock()
	s, ok := g.lastReconciled[peer]
	if !ok {
		// No prior reconcile for this peer. We intentionally leave
		// cooldown at 0 — we're not calling maybeReconcile after this
		// marker, so the cooldown value doesn't gate anything. The
		// next reactive or tick trigger will draw a fresh cooldown
		// when it fires maybeReconcile.
		s = reconcileState{}
	}
	s.at = g.clk.Now()
	g.lastReconciled[peer] = s
}

// enqueueAdRetry inserts (or refreshes) a pending retry slot for a
// failed transport-ad fanout. Approach 1 (the discriminator approach
// from the v1.4.1 plan): shares the existing retry scheduler with
// opPush / opIHave / opGraft / opPrune, keyed on (peer, author) with
// op=opTransportAd. The same drainDueRetries goroutine handles both
// schedules; executeRetry dispatches on key.op.
//
// Semantics that differ from enqueueRetry:
//
//   - Retry cap is adRetryMaxAttempts (6), not retryMaxAttempts (10).
//     Six attempts across the decorrelated-jitter envelope (~2 s →
//     ~50 s) is ample for a transient NAT flap while bounding the
//     cost of a persistently-unreachable peer.
//   - A wall-clock ceiling (adRetryWallClockLimit, 5 min) terminates
//     retries regardless of attempt count so a clock-skewed backoff
//     can't hold an entry past the point of usefulness.
//   - A newer ad from the same author supersedes the pending slot
//     (we overwrite ad + seq + firstTry). Stale ad retries are also
//     short-circuited at drain time via TransportAdStore.GetTransportAd
//     before redialling — see drainDueRetries.
//   - Terminal exhaustion is logged at Debug, not Warn, because the
//     ad's own weekly refresh + the receiver's anti-entropy paths
//     re-seed convergence; this failure is typically non-fatal.
//
// ad must be non-nil. Callers (fanoutTransportAd, refanoutTransportAd)
// invoke enqueueAdRetry once on each dial/send failure, after the
// existing Debug log that records the failure. (v1.4.1)
func (g *Gossiper) enqueueAdRetry(peerID entmoot.NodeID, ad *wire.TransportAd) {
	if ad == nil {
		return
	}
	g.pendMu.Lock()
	defer g.pendMu.Unlock()
	now := g.clk.Now()
	key := retryKey{peer: peerID, author: ad.Author.PilotNodeID, op: opTransportAd}
	state, ok := g.pending[key]
	if !ok {
		state = &retryState{attempts: 1, ad: ad, seq: ad.Seq, firstTry: now}
	} else {
		// Refresh the stored ad on every enqueue so a newer seq from
		// the same author displaces the stale retry. A lower seq
		// never wins here: the receiver-side LWW guard drops the
		// retry at drain time even if we kept it.
		if ad.Seq >= state.seq {
			state.ad = ad
			state.seq = ad.Seq
		}
		state.attempts++
	}
	if state.attempts > adRetryMaxAttempts {
		g.logger.Debug("gossip: transport_ad retry budget exhausted",
			slog.Uint64("peer", uint64(key.peer)),
			slog.Uint64("author", uint64(key.author)),
			slog.Uint64("seq", state.seq),
			slog.Int("attempts", state.attempts))
		delete(g.pending, key)
		return
	}
	// Wall-clock ceiling: if we've been retrying for more than
	// adRetryWallClockLimit since the first failure, give up. Belt-
	// and-braces termination in case a skewed backoff envelope keeps
	// us in flight past the point of usefulness.
	if !state.firstTry.IsZero() && now.Sub(state.firstTry) > adRetryWallClockLimit {
		g.logger.Debug("gossip: transport_ad retry wall-clock ceiling hit",
			slog.Uint64("peer", uint64(key.peer)),
			slog.Uint64("author", uint64(key.author)),
			slog.Uint64("seq", state.seq),
			slog.Duration("elapsed", now.Sub(state.firstTry)))
		delete(g.pending, key)
		return
	}
	state.lastBackoff = g.nextBackoff(state.lastBackoff)
	state.nextAt = now.Add(state.lastBackoff)
	g.pending[key] = state
}

// enqueueRetry inserts (or refreshes) a pending retry slot for a failed
// push or fetch. First-attempt failures start at attempts=1 with
// lastBackoff=0, so nextBackoff produces a delay near retryBackoffBase
// on the first wait (matching v1.0.6's retryBackoff[1] ≈ 2s envelope,
// though individual samples vary under decorrelated jitter). frame
// must be non-nil for opPush and is ignored for opFetch.
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
	if state.attempts >= retryMaxAttempts {
		// Budget exhausted. We already warn once when dropping; patch 7
		// reconcile-on-reconnect is the final recovery path.
		g.logger.Warn("gossip: retry budget exhausted",
			slog.Uint64("peer", uint64(key.peer)),
			slog.String("id", key.id.String()),
			slog.String("op", key.op.String()))
		delete(g.pending, key)
		return
	}
	state.lastBackoff = g.nextBackoff(state.lastBackoff)
	state.nextAt = now.Add(state.lastBackoff)
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
			// v1.4.1: transport-ad retries short-circuit if the local
			// store has seen a newer seq for the same author since we
			// queued. Superseded ads never need to be re-sent — the
			// newer ad's own fanout (or its retry) covers the peer.
			if d.key.op == opTransportAd {
				if g.adSuperseded(dctx, d.key, d.state) {
					g.pendMu.Lock()
					if cur, ok := g.pending[d.key]; ok && cur == d.state {
						delete(g.pending, d.key)
					}
					g.pendMu.Unlock()
					g.logger.Debug("gossip: transport_ad retry superseded",
						slog.Uint64("peer", uint64(d.key.peer)),
						slog.Uint64("author", uint64(d.key.author)),
						slog.Uint64("seq", d.state.seq))
					return nil
				}
				// Wall-clock termination: enforce adRetryWallClockLimit
				// at drain time too so a slow backoff envelope can't
				// leave us re-dialling after the ceiling.
				if !d.state.firstTry.IsZero() && g.clk.Now().Sub(d.state.firstTry) > adRetryWallClockLimit {
					g.pendMu.Lock()
					if cur, ok := g.pending[d.key]; ok && cur == d.state {
						delete(g.pending, d.key)
					}
					g.pendMu.Unlock()
					g.logger.Debug("gossip: transport_ad retry wall-clock ceiling hit",
						slog.Uint64("peer", uint64(d.key.peer)),
						slog.Uint64("author", uint64(d.key.author)),
						slog.Uint64("seq", d.state.seq))
					return nil
				}
			}
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
			if d.key.op == opTransportAd {
				// Debug rather than Warn: ad fanout failures are
				// non-fatal — the receiver's own weekly refresh + the
				// anti-entropy paths re-seed convergence. Noisy Warn
				// logs on every transient NAT flap would drown
				// operators.
				g.logger.Debug("gossip: transport_ad retry",
					slog.Uint64("peer", uint64(d.key.peer)),
					slog.Uint64("author", uint64(d.key.author)),
					slog.Uint64("seq", d.state.seq),
					slog.Int("attempt", d.state.attempts+1),
					slog.String("err", err.Error()))
				g.enqueueAdRetry(d.key.peer, d.state.ad)
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

// adSuperseded reports whether the local TransportAdStore holds an ad
// for (ad author) whose Seq is strictly greater than the retry's seq.
// Used by drainDueRetries to drop retries whose ad has already been
// replaced by a NEWER fanout from the same author. Equal-seq is NOT
// superseded — the retry's very own ad was written to the store at
// publish time, so `cur.Seq == state.seq` is the expected self-match
// and must not cancel the retry. TransportAdStore == nil means "no
// state to consult, carry on". (v1.4.1)
func (g *Gossiper) adSuperseded(ctx context.Context, key retryKey, state *retryState) bool {
	if g.cfg.TransportAdStore == nil {
		return false
	}
	cur, ok, err := g.cfg.TransportAdStore.GetTransportAd(ctx, g.cfg.GroupID, key.author)
	if err != nil || !ok {
		return false
	}
	return cur.Seq > state.seq
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
	case opTransportAd:
		// v1.4.1: re-dial the peer and send the cached signed ad.
		// The ad pointer lives on retryState so we skip re-signing;
		// supersession was already checked in drainDueRetries before
		// dispatching here.
		if state.ad == nil {
			return fmt.Errorf("retry: transport_ad missing ad")
		}
		return g.sendTransportAd(ctx, key.peer, state.ad)
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
	now := g.clk.Now()
	if _, ok := g.reconcileInFlight[peer]; ok {
		g.pendMu.Unlock()
		g.logger.Debug("gossip: reconcile: in-flight skip",
			slog.Uint64("peer", uint64(peer)))
		return
	}
	if failed, ok := g.reconcileFailures[peer]; ok && now.Sub(failed.at) < failed.cooldown {
		remaining := failed.cooldown - now.Sub(failed.at)
		g.pendMu.Unlock()
		g.logger.Debug("gossip: reconcile: failure backoff skip",
			slog.Uint64("peer", uint64(peer)),
			slog.Duration("remaining", remaining))
		return
	}
	last, ok := g.lastReconciled[peer]
	if ok && now.Sub(last.at) < last.cooldown {
		remaining := last.cooldown - now.Sub(last.at)
		g.pendMu.Unlock()
		// v1.2.1 (Part F): log cooldown-gated skips at Debug so
		// operators can distinguish "nothing happened" from "deduped"
		// when tracing reconcile behaviour against a chatty peer.
		g.logger.Debug("gossip: reconcile: cooldown skip",
			slog.Uint64("peer", uint64(peer)),
			slog.Duration("remaining", remaining))
		return
	}
	g.reconcileInFlight[peer] = struct{}{}
	g.pendMu.Unlock()

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.finishReconcile(peer, g.reconcileWith(ctx, peer))
	}()
}

func (g *Gossiper) finishReconcile(peer entmoot.NodeID, success bool) {
	g.pendMu.Lock()
	defer g.pendMu.Unlock()
	delete(g.reconcileInFlight, peer)
	now := g.clk.Now()
	if success {
		g.lastReconciled[peer] = reconcileState{
			at:       now,
			cooldown: g.jitteredReconcileCooldown(),
		}
		delete(g.reconcileFailures, peer)
		return
	}
	g.reconcileFailures[peer] = reconcileState{
		at:       now,
		cooldown: reconcileFailureCooldown,
	}
}

// reconcileWith runs a round of RBSR anti-entropy against peer (v1.2.1).
// It returns true only after the peer root check or RBSR/fallback exchange
// completed, so failed attempts get a short retry backoff instead of the
// normal success cooldown.
func (g *Gossiper) reconcileWith(ctx context.Context, peer entmoot.NodeID) bool {
	// Step 1: Merkle root short-circuit. Uses its own conn; returns
	// early with cached root on match so steady-state anti-entropy
	// costs a single RTT.
	rootCtx, rootCancel := context.WithTimeout(ctx, reconcileSessionTimeout)
	defer rootCancel()
	peerRoot, ok := g.fetchPeerRoot(rootCtx, peer)
	if !ok {
		return false
	}
	localRoot, err := g.cfg.Store.MerkleRoot(ctx, g.cfg.GroupID)
	if err != nil {
		g.logger.Warn("gossip: reconcile: local merkle root",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return false
	}
	if [32]byte(peerRoot) == localRoot {
		g.rememberPeerRoot(peer, peerRoot)
		return true
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		missingIDs, rounds, err := g.runReconcileSession(ctx, peer)
		if err == nil {
			// Fulfil newly-discovered missing ids via the existing fetch path
			// so signature verification, store.Put, Plumtree refanout, and
			// retry queuing all compose the same way they do for gossip-push
			// and graft-response acquisitions.
			fetchCtx, fetchCancel := context.WithTimeout(ctx, reconcileSessionTimeout)
			g.fetchMissingFrom(fetchCtx, peer, missingIDs)
			fetchCancel()
			g.rememberPeerRoot(peer, peerRoot)
			g.logger.Debug("gossip: reconcile: session complete",
				slog.Uint64("peer", uint64(peer)),
				slog.Int("rounds", int(rounds)),
				slog.Int("missing", len(missingIDs)))
			return true
		}
		lastErr = err
		if attempt == 0 && isRetryableStreamError(err) {
			g.dropPeerSession(peer, "reconcile", err)
			continue
		}
		break
	}
	if lastErr != nil {
		g.logger.Warn("gossip: reconcile: session failed",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", lastErr.Error()))
	}
	return g.fetchFullRangeFallback(ctx, peer, peerRoot)
}

func (g *Gossiper) runReconcileSession(ctx context.Context, peer entmoot.NodeID) ([]entmoot.MessageID, uint32, error) {
	if !g.canDial(peer) {
		g.logger.Debug("gossip: reconcile: peer in dial-backoff",
			slog.Uint64("peer", uint64(peer)))
		return nil, 0, fmt.Errorf("peer %d in dial-backoff", peer)
	}
	dctx, cancel := context.WithTimeout(ctx, reconcileSessionTimeout)
	defer cancel()
	conn, err := g.cfg.Transport.Dial(dctx, peer)
	if err != nil {
		g.recordDialFailure(peer)
		g.logger.Warn("gossip: reconcile: dial",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return nil, 0, err
	}
	defer conn.Close()
	setConnDeadlineFromContext(dctx, conn)

	adapter := &storeAdapter{store: g.cfg.Store, groupID: g.cfg.GroupID}
	sess, initialRanges, err := reconcile.NewInitiator(reconcile.DefaultConfig(), adapter)
	if err != nil {
		g.logger.Warn("gossip: reconcile: new initiator",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return nil, 0, err
	}

	// Step 3: alternate Reconcile frames until both sides signal Done.
	// The initiator's convergence is tracked by sess.Done() AND the
	// peer's most-recent Done flag — the responder's Session watches
	// the same state on its side.
	var missingIDs []entmoot.MessageID
	round := uint32(0)
	out := initialRanges
	// myDone tracks whether our local session has converged. Set on
	// the outgoing frame so the responder's peerDone gate fires
	// on the very next round instead of waiting for a separate ack.
	myDone := false
	done := false
	sawResponse := false
	for !done {
		req := &wire.Reconcile{
			GroupID: g.cfg.GroupID,
			Round:   round,
			Ranges:  out,
			Done:    myDone,
		}
		if err := wire.EncodeAndWrite(conn, req); err != nil {
			g.logger.Warn("gossip: reconcile: write",
				slog.Uint64("peer", uint64(peer)),
				slog.Int("round", int(round)),
				slog.String("err", err.Error()))
			return nil, round, err
		}
		msgType, payload, err := wire.ReadAndDecode(conn)
		if err != nil {
			g.logger.Warn("gossip: reconcile: read",
				slog.Uint64("peer", uint64(peer)),
				slog.Int("round", int(round)),
				slog.String("err", err.Error()))
			return nil, round, err
		}
		if !sawResponse {
			g.recordDialSuccess(peer)
			sawResponse = true
		}
		if msgType != wire.MsgReconcile {
			g.logger.Warn("gossip: reconcile: unexpected type",
				slog.Uint64("peer", uint64(peer)),
				slog.String("got", msgType.String()))
			return nil, round, fmt.Errorf("unexpected response type %s", msgType.String())
		}
		resp, ok := payload.(*wire.Reconcile)
		if !ok {
			g.logger.Warn("gossip: reconcile: decode mismatch",
				slog.Uint64("peer", uint64(peer)))
			return nil, round, fmt.Errorf("decode mismatch")
		}
		outgoing, newlyMissing, sessDone, err := sess.Next(dctx, resp.Ranges)
		if err != nil {
			if errors.Is(err, reconcile.ErrMaxRounds) {
				g.logger.Warn("gossip: reconcile: max rounds exceeded",
					slog.Uint64("peer", uint64(peer)),
					slog.Int("round", int(round)))
			} else {
				g.logger.Warn("gossip: reconcile: session step",
					slog.Uint64("peer", uint64(peer)),
					slog.Int("round", int(round)),
					slog.String("err", err.Error()))
			}
			return nil, round, err
		}
		missingIDs = append(missingIDs, newlyMissing...)
		round++
		out = outgoing
		myDone = sessDone && len(outgoing) == 0
		// v1.2.1 (Part F): round-progress Debug log. One line per
		// round so operators can see the convergence curve when
		// something looks wrong. Bounded by reconcile.DefaultConfig()
		// .MaxRounds, so at most ~10 lines per reconcile session.
		g.logger.Debug("gossip: reconcile: round",
			slog.Uint64("peer", uint64(peer)),
			slog.Int("round", int(round)),
			slog.Int("outgoing_ranges", len(outgoing)),
			slog.Int("newly_missing", len(newlyMissing)),
			slog.Int("total_missing", len(missingIDs)),
			slog.Bool("sess_done", sessDone),
			slog.Bool("peer_done", resp.Done))
		// Session terminates when our side has converged AND the peer
		// just told us theirs has too. Both flags live on separate
		// sides of the exchange; the peer's Done signal flows in via
		// resp.Done, ours flows out on the next Write via myDone.
		done = myDone && resp.Done
	}

	// Send a final ack so the responder sees our Done=true. The
	// responder's loop exits only when both sides' Done flags line up,
	// so without this ack a responder that only matched our
	// convergence on the same round would sit in Read waiting forever.
	finalFrame := &wire.Reconcile{
		GroupID: g.cfg.GroupID,
		Round:   round,
		Ranges:  nil,
		Done:    true,
	}
	_ = wire.EncodeAndWrite(conn, finalFrame)
	return missingIDs, round, nil
}

func (g *Gossiper) fetchFullRangeFallback(ctx context.Context, peer entmoot.NodeID, peerRoot wire.MerkleRoot) bool {
	fctx, cancel := context.WithTimeout(ctx, reconcileSessionTimeout)
	defer cancel()
	payload, err := g.requestResponse(fctx, peer, &wire.RangeReq{GroupID: g.cfg.GroupID, SinceMillis: 0}, wire.MsgRangeResp, "range_req")
	if err != nil {
		g.logger.Warn("gossip: reconcile: full-range fallback failed",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
		return false
	}
	resp, ok := payload.(*wire.RangeResp)
	if !ok {
		g.logger.Warn("gossip: reconcile: full-range fallback decode mismatch",
			slog.Uint64("peer", uint64(peer)))
		return false
	}
	g.fetchMissingFrom(fctx, peer, resp.IDs)
	localRoot, err := g.cfg.Store.MerkleRoot(fctx, g.cfg.GroupID)
	if err == nil && [32]byte(peerRoot) == localRoot {
		g.rememberPeerRoot(peer, peerRoot)
		g.logger.Debug("gossip: reconcile: full-range fallback complete",
			slog.Uint64("peer", uint64(peer)),
			slog.Int("ids", len(resp.IDs)))
		return true
	}
	if err != nil {
		g.logger.Warn("gossip: reconcile: full-range fallback local merkle root",
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
	} else {
		g.logger.Debug("gossip: reconcile: full-range fallback root still differs",
			slog.Uint64("peer", uint64(peer)),
			slog.Int("ids", len(resp.IDs)))
	}
	return false
}

// rememberPeerRoot stashes the peer's most recently observed Merkle root
// under pendMu. Phase 7 / Part E uses readLastKnownPeerRoot to tick-skip
// peers whose root equals our current root. (v1.2.1)
func (g *Gossiper) rememberPeerRoot(peer entmoot.NodeID, root wire.MerkleRoot) {
	g.pendMu.Lock()
	g.lastKnownPeerRoot[peer] = root
	g.pendMu.Unlock()
}

// readLastKnownPeerRoot returns the most recently observed Merkle root
// for peer along with ok=true if a root is cached, zero + ok=false
// otherwise. Exposed for Phase 7 / Part E's tick-skip path. (v1.2.1)
func (g *Gossiper) readLastKnownPeerRoot(peer entmoot.NodeID) (wire.MerkleRoot, bool) {
	g.pendMu.Lock()
	defer g.pendMu.Unlock()
	r, ok := g.lastKnownPeerRoot[peer]
	return r, ok
}

// fetchPeerRoot opens a connection to peer and sends MerkleReq. Returns the
// peer's root + ok on success, zero + !ok on any error so the caller can
// abort cleanly.
func (g *Gossiper) fetchPeerRoot(ctx context.Context, peer entmoot.NodeID) (wire.MerkleRoot, bool) {
	payload, err := g.requestResponse(ctx, peer, &wire.MerkleReq{GroupID: g.cfg.GroupID}, wire.MsgMerkleResp, "merkle_req")
	if err != nil {
		g.logger.Debug("gossip: reconcile: dial for root",
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

// onTransportAd validates and installs an inbound TransportAd (v1.2.0).
// Validation is ordered cheapest-first to drop hostile traffic before
// doing any cryptographic work: schema + size -> publisher allowlist ->
// roster membership -> per-(peer, topic) rate limit -> signature ->
// store LWW check -> install + refanout.
//
// Failures at each stage are logged at Warn (structural / spam) or
// Debug (rate-limit / no-op replace) so operators can distinguish an
// adversary from a legitimate stale repeat. No stage ever drops the
// connection — a single bad frame is just a single drop.
func (g *Gossiper) onTransportAd(ctx context.Context, remote entmoot.NodeID, ad *wire.TransportAd) {
	if g.cfg.TransportAdStore == nil {
		g.logger.Debug("gossip: transport_ad received but TransportAdStore not configured",
			slog.Uint64("remote", uint64(remote)))
		return
	}

	// 1. Schema + size. Cheapest checks first so a bad frame is
	// dropped before we touch crypto. GroupID check also guards
	// against cross-group replay when one peer is in multiple groups.
	if ad.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: transport_ad for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", ad.GroupID.String()))
		return
	}
	nowMs := g.clk.Now().UnixMilli()
	if ad.IssuedAt <= 0 || ad.NotAfter <= 0 || ad.NotAfter <= ad.IssuedAt {
		g.logger.Warn("gossip: transport_ad bad timestamps",
			slog.Uint64("remote", uint64(remote)),
			slog.Int64("issued_at", ad.IssuedAt),
			slog.Int64("not_after", ad.NotAfter))
		return
	}
	if ad.NotAfter <= nowMs {
		g.logger.Warn("gossip: transport_ad already expired",
			slog.Uint64("remote", uint64(remote)),
			slog.Int64("not_after", ad.NotAfter),
			slog.Int64("now", nowMs))
		return
	}
	if len(ad.Endpoints) == 0 || len(ad.Endpoints) > transportAdMaxEndpoints {
		g.logger.Warn("gossip: transport_ad endpoint count out of range",
			slog.Uint64("remote", uint64(remote)),
			slog.Int("count", len(ad.Endpoints)))
		return
	}
	for i, ep := range ad.Endpoints {
		if err := validateEndpoint(ep); err != nil {
			g.logger.Warn("gossip: transport_ad endpoint invalid",
				slog.Uint64("remote", uint64(remote)),
				slog.Int("idx", i),
				slog.String("err", err.Error()))
			return
		}
	}
	signing := *ad
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		g.logger.Warn("gossip: transport_ad canonical encode",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	if len(sigInput) > transportAdMaxBytes {
		g.logger.Warn("gossip: transport_ad exceeds size cap",
			slog.Uint64("remote", uint64(remote)),
			slog.Int("bytes", len(sigInput)),
			slog.Int("cap", transportAdMaxBytes))
		return
	}

	// 2. Sender gate (v1.4.2). The forwarder must be a Pilot-trusted
	// peer. Authenticity of the ad itself comes from the author's
	// signature (step 5) and the roster-membership check (step 3);
	// any trusted mesh member may legitimately forward a signed ad.
	// Standard gossip-network design — signatures provide integrity,
	// trust handshakes provide sender authentication, membership +
	// LWW + rate-limit defeat spam and replay.
	//
	// v1.4.0-v1.4.1 required ad.Author.PilotNodeID == remote here
	// ("single-hop"). That turned refanoutTransportAd into a
	// structural no-op: any relayer's forwarded frame was rejected
	// by the receiver, so an ad could only reach peers that could
	// direct-dial the author. Multi-hop is needed for NATted peer
	// pairs that can't direct-dial each other (CGNAT + same-LAN
	// false positives) but share a reachable mesh hub.
	//
	// Fail-open on cold-start IPC snapshot matches plumEagerExcept's
	// precedent. Downstream signature + membership remain the hard
	// integrity floor.
	if !g.isTrustedSender(ctx, remote) {
		g.logger.Debug("gossip: transport_ad from untrusted sender",
			slog.Uint64("remote", uint64(remote)),
			slog.Uint64("author", uint64(ad.Author.PilotNodeID)))
		return
	}

	// 3. Membership.
	authorInfo, ok := g.cfg.Roster.MemberInfo(ad.Author.PilotNodeID)
	if !ok {
		g.logger.Warn("gossip: transport_ad from non-member author",
			slog.Uint64("remote", uint64(remote)),
			slog.Uint64("author", uint64(ad.Author.PilotNodeID)))
		return
	}

	// 4. Per-(peer, topic) rate limit. Not a disconnect — just a drop.
	if g.cfg.RateLimiter != nil {
		if err := g.cfg.RateLimiter.AllowTopic(remote, transportAdTopic, len(sigInput)); err != nil {
			g.logger.Warn("gossip: transport_ad rate-limited",
				slog.Uint64("remote", uint64(remote)),
				slog.String("err", err.Error()))
			return
		}
	}

	// 5. Signature. Reuses the same pattern as verifyMessage: the
	// roster-stored pubkey wins over whatever the envelope claims.
	if !keystore.Verify(authorInfo.EntmootPubKey, sigInput, ad.Signature) {
		g.logger.Warn("gossip: transport_ad signature invalid",
			slog.Uint64("remote", uint64(remote)),
			slog.Uint64("author", uint64(ad.Author.PilotNodeID)))
		return
	}

	// 6. Seq check + store. PutTransportAd is atomic and handles the
	// LWW replace-vs-drop decision itself. replaced=false means the
	// stored ad was equal or newer; nothing to do.
	replaced, err := g.cfg.TransportAdStore.PutTransportAd(ctx, *ad)
	if err != nil {
		g.logger.Warn("gossip: transport_ad store put",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	if !replaced {
		g.logger.Debug("gossip: transport_ad not newer than stored",
			slog.Uint64("remote", uint64(remote)),
			slog.Uint64("author", uint64(ad.Author.PilotNodeID)),
			slog.Uint64("seq", ad.Seq))
		return
	}

	// 7. Install into transport + refanout. SetPeerEndpoints is
	// best-effort (pilot may be restarting); log-and-continue so a
	// transient IPC hiccup doesn't block the refanout path that lets
	// the rest of the mesh converge.
	if err := g.cfg.Transport.SetPeerEndpoints(ctx, ad.Author.PilotNodeID, ad.Endpoints); err != nil {
		g.logger.Warn("gossip: transport_ad install peer endpoints",
			slog.Uint64("author", uint64(ad.Author.PilotNodeID)),
			slog.String("err", err.Error()))
	} else {
		g.logger.Debug("gossip: transport_ad installed",
			slog.Uint64("author", uint64(ad.Author.PilotNodeID)),
			slog.Uint64("seq", ad.Seq),
			slog.Int("endpoints", len(ad.Endpoints)))
	}

	// Refanout to eager peers (Plumtree-style) minus the sender.
	// Mirrors the v1.0.5 content-message refanout pattern but carries
	// the MsgTransportAd envelope directly — no IHave/Graft indirection
	// is warranted for a small replace-on-seq record.
	g.refanoutTransportAd(ctx, remote, ad)
}

// refanoutTransportAd pushes ad to every eager peer except `except`.
// Same best-effort parallelism as fanoutPush — a stalled peer can't
// head-of-line-block healthy peers, and dial-backoff is respected.
// Failures feed enqueueAdRetry so transient direct-dial errors
// (e.g. a peer that's mid-switch to relay, a colliding RFC1918 dial)
// no longer lose the ad silently. (v1.4.1: live evidence showed the
// previous log-and-drop policy dropped ads on first-attempt failure
// and relied solely on the weekly refresh to heal; that made peers
// effectively unreachable for the week-long window.)
func (g *Gossiper) refanoutTransportAd(ctx context.Context, except entmoot.NodeID, ad *wire.TransportAd) {
	peers := g.plumEagerExcept(ctx, except)
	if len(peers) == 0 {
		return
	}
	var eg errgroup.Group
	eg.SetLimit(fanoutMaxConcurrency)
	for _, p := range peers {
		p := p
		eg.Go(func() error {
			dctx, cancel := context.WithTimeout(ctx, fanoutPerPeerTimeout)
			defer cancel()
			if err := g.sendTransportAd(dctx, p, ad); err != nil {
				g.logger.Debug("gossip: transport_ad refanout",
					slog.Uint64("peer", uint64(p)),
					slog.Uint64("author", uint64(ad.Author.PilotNodeID)),
					slog.String("err", err.Error()))
				g.enqueueAdRetry(p, ad)
			}
			return nil
		})
	}
	_ = eg.Wait()
}

// sendTransportAd dials peer and writes one TransportAd frame. Used
// both by refanoutTransportAd and the advertiser loop.
func (g *Gossiper) sendTransportAd(ctx context.Context, peer entmoot.NodeID, ad *wire.TransportAd) error {
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
	if err := wire.EncodeAndWrite(conn, ad); err != nil {
		return fmt.Errorf("write transport_ad: %w", err)
	}
	return nil
}

// publishTransportAd builds, signs, stores, and fanouts a fresh
// TransportAd carrying `endpoints` as this node's current network
// presence. Called by advertiserLoop on startup, on
// EndpointsChanged, and on the weekly safety-net tick. Errors at the
// sign / store / install stages are logged and returned so the caller
// can decide whether to retry on a shorter cycle. (v1.2.0)
func (g *Gossiper) publishTransportAd(ctx context.Context, endpoints []entmoot.NodeEndpoint) error {
	if g.cfg.TransportAdStore == nil {
		return errors.New("publishTransportAd: no TransportAdStore")
	}
	if len(endpoints) == 0 {
		return errors.New("publishTransportAd: empty endpoints")
	}
	if len(endpoints) > transportAdMaxEndpoints {
		return fmt.Errorf("publishTransportAd: too many endpoints (%d > %d)",
			len(endpoints), transportAdMaxEndpoints)
	}
	for i, ep := range endpoints {
		if err := validateEndpoint(ep); err != nil {
			return fmt.Errorf("publishTransportAd: endpoint %d: %w", i, err)
		}
	}

	seq, err := g.cfg.TransportAdStore.BumpTransportAdSeq(ctx, g.cfg.GroupID, g.cfg.LocalNode)
	if err != nil {
		return fmt.Errorf("publishTransportAd: bump seq: %w", err)
	}

	issuedAt := g.clk.Now().UnixMilli()
	ad := wire.TransportAd{
		GroupID: g.cfg.GroupID,
		Author: entmoot.NodeInfo{
			PilotNodeID:   g.cfg.LocalNode,
			EntmootPubKey: g.cfg.Identity.PublicKey,
		},
		Seq:       seq,
		Endpoints: append([]entmoot.NodeEndpoint(nil), endpoints...),
		IssuedAt:  issuedAt,
		NotAfter:  g.clk.Now().Add(transportAdTTL).UnixMilli(),
	}
	signing := ad
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return fmt.Errorf("publishTransportAd: canonical encode: %w", err)
	}
	if len(sigInput) > transportAdMaxBytes {
		return fmt.Errorf("publishTransportAd: encoded ad exceeds cap (%d > %d)",
			len(sigInput), transportAdMaxBytes)
	}
	ad.Signature = g.cfg.Identity.Sign(sigInput)

	if _, err := g.cfg.TransportAdStore.PutTransportAd(ctx, ad); err != nil {
		// Not fatal — a store write miss still leaves us able to
		// refanout the in-memory ad. Log and continue.
		g.logger.Warn("gossip: transport_ad publish store put",
			slog.String("err", err.Error()))
	}

	// Install locally so the transport's own view is consistent with
	// what we're telling the rest of the group. For the pilot adapter
	// this is a no-op for self; for the mem transport it records the
	// call for test introspection.
	if err := g.cfg.Transport.SetPeerEndpoints(ctx, g.cfg.LocalNode, ad.Endpoints); err != nil {
		g.logger.Debug("gossip: transport_ad self install",
			slog.String("err", err.Error()))
	}

	// Fanout to every eager peer — except == 0 asks for "all" since 0
	// is never a valid NodeID in production.
	g.fanoutTransportAd(ctx, &ad)
	return nil
}

// fanoutTransportAd sends ad to every eager peer. Used by the initial
// advertiser publish (no sender to exclude). Factored out of
// refanoutTransportAd because the exclusion set differs.
//
// v1.4.1: failures feed enqueueAdRetry so transport-ads survive
// transient direct-dial failure the same way message fanouts have
// since v1.0.4. Previously this path log-and-dropped, which produced
// the 2026-04-24 live outage where laptop's -hide-ip ad never reached
// phobos after the first fanout attempt hit "no route to host".
func (g *Gossiper) fanoutTransportAd(ctx context.Context, ad *wire.TransportAd) {
	peers := g.plumEagerExcept(ctx, 0)
	if len(peers) == 0 {
		return
	}
	var eg errgroup.Group
	eg.SetLimit(fanoutMaxConcurrency)
	for _, p := range peers {
		p := p
		eg.Go(func() error {
			dctx, cancel := context.WithTimeout(ctx, fanoutPerPeerTimeout)
			defer cancel()
			if err := g.sendTransportAd(dctx, p, ad); err != nil {
				g.logger.Debug("gossip: transport_ad fanout",
					slog.Uint64("peer", uint64(p)),
					slog.String("err", err.Error()))
				g.enqueueAdRetry(p, ad)
			}
			return nil
		})
	}
	_ = eg.Wait()
}

// advertiserLoop runs for the lifetime of the gossiper. Publishes a
// TransportAd on startup, on EndpointsChanged signal, and on a weekly
// refresh ticker. Weekly refresh is a safety-net so receivers can GC
// expired entries; normal operation publishes only on change. Also
// opportunistically GCs this node's view of the transport-ad table on
// each tick so disk doesn't grow unbounded. (v1.2.0)
func (g *Gossiper) advertiserLoop(ctx context.Context) {
	// Kick off the initial publish. Don't block the loop if the first
	// endpoint query returns empty — LocalEndpoints may legitimately
	// depend on async pilot state.
	g.tryPublishTransportAd(ctx, "startup")

	var changed <-chan struct{}
	if g.cfg.EndpointsChanged != nil {
		changed = g.cfg.EndpointsChanged
	}

	ticker := time.NewTicker(transportAdRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-changed:
			if !ok {
				// Config closed the channel; disable the change path
				// but keep ticking.
				changed = nil
				continue
			}
			g.tryPublishTransportAd(ctx, "endpoints_changed")
		case <-ticker.C:
			g.tryPublishTransportAd(ctx, "refresh")
			// GC opportunistically on the same cadence to keep the
			// table bounded at one live row per group member.
			if g.cfg.TransportAdStore != nil {
				if n, err := g.cfg.TransportAdStore.GCExpiredTransportAds(ctx, g.clk.Now()); err != nil {
					g.logger.Debug("gossip: transport_ad gc",
						slog.String("err", err.Error()))
				} else if n > 0 {
					g.logger.Debug("gossip: transport_ad gc removed rows",
						slog.Int64("rows", n))
				}
			}
		}
	}
}

// tryPublishTransportAd is a thin wrapper that queries LocalEndpoints,
// applies the HideIP filter, short-circuits on an empty resulting
// slice, and logs publish errors at Warn. reason goes into the log so
// operators can tell a startup publish from a weekly refresh without
// turning on verbose tracing.
//
// HideIP path (v1.4.0): when cfg.HideIP is true, only entries whose
// Network is "turn" are published — UDP/TCP entries are stripped.
// If no TURN entry is present after the filter, the ad is NOT
// published (empty publish would fail validateEndpoint upstream) and
// a Warn is emitted so the operator sees the misconfiguration —
// hide-IP with no relay leaves this peer unreachable by design
// rather than silently falling back to IP advertisement.
func (g *Gossiper) tryPublishTransportAd(ctx context.Context, reason string) {
	if g.cfg.LocalEndpoints == nil {
		return
	}
	eps := g.cfg.LocalEndpoints()
	if g.cfg.HideIP {
		eps = filterTurnOnly(eps)
		if len(eps) == 0 {
			g.logger.Warn("gossip: hide-ip set but no TURN relay available; peer will be unreachable",
				slog.String("reason", reason))
			return
		}
	}
	if len(eps) == 0 {
		g.logger.Debug("gossip: transport_ad skip publish (no local endpoints)",
			slog.String("reason", reason))
		return
	}
	if err := g.publishTransportAd(ctx, eps); err != nil {
		g.logger.Warn("gossip: transport_ad publish",
			slog.String("reason", reason),
			slog.String("err", err.Error()))
	}
}

// filterTurnOnly returns the subset of eps whose Network is "turn".
// Used by the HideIP advertiser path to strip UDP/TCP entries before
// publishing. Preserves input order. Never returns a nil slice
// allocated from the caller's slice — always a fresh allocation so
// the caller's slice is untouched. (v1.4.0)
func filterTurnOnly(eps []entmoot.NodeEndpoint) []entmoot.NodeEndpoint {
	out := make([]entmoot.NodeEndpoint, 0, len(eps))
	for _, ep := range eps {
		if ep.Network == "turn" {
			out = append(out, ep)
		}
	}
	return out
}

// onTransportSnapshotReq answers a Join-time or catch-up request for
// the current TransportAd table. Response is a single frame carrying
// every unexpired ad this peer holds for the group, ordered by author
// for determinism. (v1.2.0)
func (g *Gossiper) onTransportSnapshotReq(ctx context.Context, conn net.Conn, remote entmoot.NodeID, req *wire.TransportSnapshotReq) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: transport_snapshot_req for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}
	if !g.cfg.Roster.IsMember(remote) {
		g.logger.Warn("gossip: transport_snapshot_req from non-member",
			slog.Uint64("remote", uint64(remote)))
		return
	}
	resp := &wire.TransportSnapshotResp{GroupID: g.cfg.GroupID}
	if g.cfg.TransportAdStore != nil {
		ads, err := g.cfg.TransportAdStore.GetAllTransportAds(ctx, g.cfg.GroupID, g.clk.Now(), false)
		if err != nil {
			g.logger.Warn("gossip: transport_snapshot_req get all",
				slog.Uint64("remote", uint64(remote)),
				slog.String("err", err.Error()))
			// Fall through with empty response rather than dropping —
			// an empty snapshot is a valid answer and keeps the join
			// path unblocked.
		} else {
			resp.Ads = ads
		}
	}
	if err := wire.EncodeAndWrite(conn, resp); err != nil {
		g.logger.Warn("gossip: write transport_snapshot_resp",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
	}
}

// validateEndpoint rejects malformed (network, addr) pairs. Networks
// are restricted to the three Pilot knows how to act on ("tcp",
// "udp", "turn"); addr must be parseable by net.SplitHostPort.
// IP-literal hosts are further sanity-checked so a dangling "example"
// can't masquerade as a valid endpoint. "turn" was added in Entmoot
// v1.4.0 / pilot-daemon v1.9.0-jf.8 as the relay-advertising
// network string. (v1.2.0, v1.4.0)
func validateEndpoint(ep entmoot.NodeEndpoint) error {
	switch ep.Network {
	case "tcp", "udp", "turn":
	default:
		return fmt.Errorf("unsupported network %q", ep.Network)
	}
	host, port, err := net.SplitHostPort(ep.Addr)
	if err != nil {
		return fmt.Errorf("parse addr %q: %w", ep.Addr, err)
	}
	if host == "" {
		return fmt.Errorf("empty host in %q", ep.Addr)
	}
	if p, err := strconv.Atoi(port); err != nil || p <= 0 || p > 65535 {
		return fmt.Errorf("invalid port in %q", ep.Addr)
	}
	return nil
}
