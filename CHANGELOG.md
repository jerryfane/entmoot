# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] - 2026-04-21

### Added

- **Transport-endpoint advertisements** (`_pilot/transport/v1`
  topic). Each group member publishes a signed `TransportAd`
  naming their current TCP/UDP endpoints; receivers verify it
  and install the endpoints into Pilot's `peerTCP` map via the
  new jf.7 `SetPeerEndpoints` IPC. Gives Entmoot groups
  peer-to-peer endpoint discovery that works without any
  central-registry cooperation — critical on our deployment
  where the upstream registry silently drops the `endpoints`
  field that Pilot's own multi-transport protocol expects.

  Wire semantics: IPNS-style signed record with a per-author
  monotonic `Seq` counter. Storage: LWW-Register — at most one
  row per (`group_id`, `author`) in the new `transport_ads`
  SQLite table, with lexicographic signature tiebreak on `Seq`
  ties. A publish of 10,000 ads retains exactly one row;
  retained state is structurally O(N_members × 1 ad),
  independent of publish rate.

  Three-layer spam defence in receive order (cheapest-first):
  schema + 1 KiB size cap, publisher allowlist (a peer may only
  advertise its own endpoints), per-(topic, author) token
  bucket via the new `ratelimit.AllowTopic` (10 tokens, refill
  1/hour for `_pilot/transport/v1` — legitimate publisher needs
  ~1/week, 1680× headroom).

  Join-time support: invites now carry per-bootstrap-peer
  endpoint hints (signed by the founder, so authenticated), and
  the Join flow pulls a `TransportSnapshotReq` from the
  bootstrap peer immediately after roster sync. The newcomer's
  `peerTCP` is fully populated before the first post-Join
  dial — TCP fallback is available on the very first dial of
  every group member, not only after the next advertiser
  refresh.

  On entmootd startup, the persistent `transport_ads` table is
  replayed into Pilot so the `peerTCP` map is warm even if
  Pilot was restarted while entmootd was up (and vice versa:
  entmootd restart with Pilot still running converges the map
  from disk). `entmootd join` gains a repeatable
  `-advertise-endpoint network=host:port` flag to feed
  `LocalEndpoints`; auto-discovery from the Pilot daemon's own
  configured listen endpoints is deferred to v1.3, contingent
  on a new `driver.Info` field.

  Pairs with Pilot v1.9.0-jf.7.

## [1.1.0] - 2026-04-21

### Changed

- **Persistent multiplexed sessions over Pilot tunnel.** The Pilot
  transport adapter now maintains one long-lived
  `hashicorp/yamux` session per peer and multiplexes all gossip
  streams over it. Prior to v1.1.0 every gossip frame opened a
  fresh Pilot stream with a full SYN handshake — fast on a healthy
  tunnel but vulnerable to Pilot's ~32 s dial budget when the
  stream SYN/ACK silently wedged (observed live: both peers on a
  node failing simultaneously at the stream-dial layer while the
  underlying tunnel reported encrypted+authenticated).

  Changes:
  - `Dial()` now opens a yamux stream on the cached session
    (0-RTT new-stream after first contact) instead of dialing a
    fresh Pilot stream.
  - `Accept()` reads from an internal channel fed by a background
    loop that accepts inbound Pilot connections, wraps each in a
    yamux server session, and pumps accepted streams into the
    channel.
  - yamux's built-in keepalive (30 s interval, 10 s timeout)
    detects silently-wedged sessions and closes them; the next
    `Dial()` opens a fresh session automatically.

  Wire compatibility: Entmoot's frame format is unchanged; yamux
  multiplexes the same JSON frames over a session. Mixed
  v1.0.x / v1.1.0 peers do NOT interoperate — v1.0.x expects a
  raw stream per frame, v1.1.0 expects a yamux session. Upgrade
  all nodes together.

### Added

- New dep: `github.com/hashicorp/yamux` (BSD-3, used by Consul,
  Nomad, Vault, libp2p's yamux transport).

## [1.0.8] - 2026-04-21

### Fixed

- **Bootstrap self-dial amplification.** An invite's
  `bootstrap_peers` list included the issuer's own NodeID
  (every invite minted until today did — the issuer-is-a-peer
  convention inherited from the v0 roster design). The Gossiper's
  `Join` path already filtered `LocalNode` before dialing via
  Pilot, BUT `entmootd invite create` kept emitting the issuer as
  a bootstrap peer, and Pilot's `ensureTunnel` — reachable
  through non-Join code paths (ambient registry-driven membership
  view maintenance, Pilot's own peer-discovery) — would then
  establish tunnels to the local NodeID. On multi-homed hosts
  Pilot's "same-LAN peer detected" branch fires both for the
  docker-bridge LAN entry and the public-IP entry of the local
  node, establishing multiple duplicate self-tunnels that
  retransmit into each other — observed live on VPS as a
  ~5,900 pps self-amplified loop, 210 % CPU on pilot-daemon,
  saturating its packet buffers and reinflating gossip
  propagation to minute-scale latencies (masquerading as a
  v1.0.7 regression when the real fault was at the transport
  layer).

  Two defense-in-depth fixes:
  - `entmootd invite create` now excludes the issuer's NodeID
    from `bootstrap_peers` at mint time. Makes new invites
    self-documenting. Matches Cassandra's "gossiper
    `live_endpoints` excludes self by construction" invariant.
  - Pilot v1.9.0-jf.6 (shipped simultaneously) adds a
    `protocol.ErrDialToSelf` sentinel at the top of
    `DialConnection` and `ensureTunnel`. Mirrors
    go-libp2p-swarm's canonical `ErrDialToSelf` guard:
    fast-fail with a typed sentinel so caller-side invariant
    violations are visible rather than masked.

  Legacy invites (like the one live in the current canary group)
  still parse correctly — the Gossiper's `Join` path already has
  a self-skip filter at every bootstrap strategy. The new
  defenses close the remaining paths (non-Join callers of
  `ensureTunnel`, and future invite mints from any node).

## [1.0.7] - 2026-04-21

Two-fix bundle attacking the residual ~1.5-minute propagation tail
observed in live v1.0.6 mesh. Together the fixes bring steady-state
hop latency from 60–90 s (p99) to ~RTT (happy path) or ~RTT +
`graftTimeout` (3 s) on flaky peers. Wire-compatible with every
v1.0.x release — additive `Body` field, additive retry-state field,
mixed-version peers interoperate unchanged.

### Added

- **Inline message body on eager-push.** `wire.Gossip` gains an
  optional `Body *entmoot.Message` field. Senders inline the full
  body when the canonical encoding is ≤ 4 KiB (`inlineBodyThreshold`,
  matching 4× IPFS Bitswap's `WantHaveReplaceSize` default);
  receivers hash-verify `Body` against `IDs[0]` and run Ed25519
  signature verification before `Store.Put`, skipping the
  `fetchFrom` round-trip entirely. Cuts every gossip hop for small
  messages from 2 Pilot stream dials to 1.

  Aligns Entmoot with the canonical Plumtree paper (Leitão et al.
  SRDS 2007, §2.2 — `EagerPush(m, mID, ...)` carries the full
  body; ID-only is reserved for lazy-push `IHAVE`). Every surveyed
  production gossip — libp2p GossipSub, Scuttlebutt EBT, Matrix
  federation, Cassandra digest-phase, IPFS Bitswap — does this.

  Wire-compatibility: the Gossiper signature explicitly excludes
  `Body` (zeroed before canonical encoding in both `signGossip`
  and `verifyGossipSig`), so v1.0.6 peers ignoring the new field
  still verify v1.0.7 frames unchanged. `Body` integrity is
  provided independently by (a) `canonical.MessageID(Body) == ID`
  and (b) the Message's own Ed25519 signature — same two checks
  the v1.0.6 `fetchFrom` path already performs.

- **Decorrelated-jitter retry backoff** (Marc Brooker / AWS 2015).
  Replaces the deterministic `[1s, 2s, 4s, 8s, 16s, 30s, 60s×4]`
  schedule with `next = min(cap, random(base, prev*3))`. Retry
  budget unchanged (10 attempts, same `"gossip: retry budget
  exhausted"` log). Prevents fleet-wide retry storms when a
  correlated flakiness event (NAT flap, registry blip) would
  otherwise synchronise every node's retry to the same wall-clock
  moments.

- **Jittered reconcile cooldown.** Fixed 60 s → 30 s ±20 %
  multiplicative jitter (`[24 s, 36 s]`). Cooldown reduced
  because research consensus (libp2p GossipSub 1 s heartbeat,
  Plumtree §3.4 continuous lazy-push, Cassandra gossip 1 s) shows
  60 s was an order of magnitude too slow for failed-push
  recovery — that number belongs to bulk Merkle-tree anti-entropy
  (Riak AAE, Cassandra repair), not per-peer reconcile. Jitter
  prevents fleet-wide collisions on the cooldown boundary
  (memberlist-style `randomStagger`). Each peer caches its
  per-invocation jittered value so a peer "just under" the gate
  doesn't probe the boundary every tick.

### Changed

- `Gossiper` gains a dedicated `*rand.Rand` (seeded from the
  clock, Fake-clock-friendly for tests) for jitter generation;
  separated from `getPicker`'s rand to keep peer-sampling
  determinism independent of jitter.
- `retryState` gains `lastBackoff time.Duration` feeding
  `nextBackoff(prev)`.
- `lastReconciled` map value type becomes `reconcileState{at,
  cooldown}` so the jittered cooldown picked at reconcile time is
  cached alongside the timestamp.
- `retryTickInterval` (500 ms) stays deterministic — the
  scheduler wheel, not a retry deadline.

### Regression tests

- `TestGossipInlineBodySkipsFetch` — B stores the inlined body
  without any outbound dial to A.
- `TestGossipInlineBodyHashMismatchRejected` — forged Body
  (mismatched hash) is dropped, no silent fetch fallback.
- `TestGossipInlineBodyBackwardCompat` — v1.0.6-shape frame
  (`Body == nil`) still propagates via `fetchFrom`.
- `TestRoundTripGossipWithBody` — wire round-trip with Body
  populated preserves every field byte-for-byte.
- `TestRetryBackoffDecorrelatedJitter` — 1000 samples stay within
  `[base, cap]` with non-trivial spread.
- `TestReconcileCooldownJittered` — samples stay within ±20 %
  envelope.

## [1.0.6] - 2026-04-20

Three-fix bundle targeting the "publish takes ~4 minutes to propagate"
regression observed on the live 3-node mesh after v1.0.5 landed. Each
fix mirrors a production pattern from libp2p / Matrix / Cassandra
gossip; together they collapse cascade latency from ~4 min to
sub-10-second steady state. Wire-compatible with v1.0.4 and v1.0.5.

### Added

- **Trust-aware reachability oracle.** `plumEagerExcept` and
  `plumLazyExcept` now consult `Transport.TrustedPeers(ctx)` and
  filter the returned peer list by the trusted set before returning.
  Peers with no Pilot trust pair (structurally unreachable for the
  lifetime of that config) are skipped entirely. Cached on a 1-second
  TTL so bursty fanouts don't hit Pilot IPC per peer per message;
  fails open (degrades to v1.0.5 behaviour) if the IPC query errors.
  Mirrors libp2p's `Network().Connectedness()` check in
  `go-libp2p-pubsub` and Matrix Synapse's `destination_retry_timings`
  — treating reachability as a first-class gossip-layer concern
  instead of relying on transport timeouts.

- **Per-peer exponential dial-backoff cache.** Every
  `Transport.Dial` in the gossiper (7 sites: `pushGossip`,
  `sendIHave`, `sendGraft`, `sendPrune`, `fetchFrom`,
  `fetchPeerRoot`, `fetchPeerRange`) now consults
  `canDial(peer)` before attempting; failures extend an
  exponential window (1 s, 2 s, 4 s, …, capped at 5 min) via
  `recordDialFailure`; successful dial clears the window via
  `recordDialSuccess`. Prevents the retry scheduler from
  independently re-dialing a dead peer on every pending message,
  which previously cost Pilot's full ~32-second SYN retry cycle per
  attempt. Matches libp2p `swarm.DialBackoff` semantics. Reachability
  is recorded on dial completion (not on full-RPC completion) so a
  healthy connection followed by a stream-layer error does not
  trigger spurious backoff.

### Changed

- **Parallel fanout with bounded per-peer dial timeout.**
  `fanoutPush`, `fanoutIHave`, and `drainDueRetries` were
  sequential loops — one stalled peer head-of-line-blocked every
  other peer on the same fanout / retry tick. Replaced with
  `golang.org/x/sync/errgroup.Group.SetLimit(32)`, one goroutine per
  peer, each wrapped in `context.WithTimeout(ctx, 5 * time.Second)`.
  5 s covers relay-over-beacon p99 (~600 ms) with generous slack and
  is far below Pilot's internal 32 s dial ceiling — so a dead peer
  fails fast into the new dial-backoff cache instead of blocking
  the rest of the fanout. Canonical Go pattern; zero behaviour
  change for healthy peers. (New dependency:
  `golang.org/x/sync/errgroup`, promoted from indirect.)

### Fixed

- **4-minute publish-to-propagation latency on partial-trust meshes.**
  Previously, every publish from node A fanned out to the full eager
  set, including peers with no trust pair — each of those sends paid
  Pilot's ~32 s dial budget, and the retry scheduler re-dialed the
  same dead peer for every pending message on every 500 ms tick,
  compounding through the 10-step retry backoff to ~4 min cumulative
  blocking before the first healthy retry slot fired. The combined
  effect of the three fixes above: (A) structurally-unreachable peers
  are never in the fanout set, (B) one slow peer never blocks
  healthy peers, (C) one failed dial suppresses further dials to the
  same peer for an exponential window. Observed live: cross-node
  publish latency on the VPS↔Phobos↔laptop mesh drops from ~4 min
  back to sub-second in steady state.

### Regression tests

- `TestPlumtreeSkipsUntrusted` — fanout never dials an untrusted
  peer; the trust oracle filters the eager/lazy lists before the
  push loop.
- `TestPlumtreeParallelFanoutWithSlowPeer` — one peer's `Dial`
  blocks for 30 s; healthy peers still receive the message within
  1 s; the slow peer's `retryKey` is enqueued within the 5 s
  per-peer timeout.
- `TestPlumtreePerPeerDialBackoff` — after a failed dial, the
  next 10 `pushGossip(peer)` calls short-circuit without invoking
  `Transport.Dial`; window reopens only after
  `dialBackoffCap` elapses (or a successful dial clears the
  state).

## [1.0.5] - 2026-04-20

### Fixed
- Plumtree re-fanout fired only when a message was acquired via the
  live gossip-push path (`onGossip` → `fetchFrom`). Messages acquired
  through either of the other two `fetchFrom` call sites —
  `executeRetry(opFetch)` (retry of an initially-failed fetch) and
  `reconcileWith` (anti-entropy pull driven by `maybeReconcile`) —
  were stored locally but never forwarded through the spanning tree.
  In partial-connectivity topologies this silently broke
  end-to-end delivery: e.g. a hub that could not receive an edge
  node's direct push (because the edge's outbound was NATed away)
  but later pulled the message via reconciliation would never
  propagate to the hub's other edges.

  Fix: centralise the `plumCancelGraftsFor(id) + refanout(peer, id)`
  hook inside `fetchFrom` after a successful `Store.Put`. Every
  acquisition path now triggers the first-seen forwarding step,
  matching the rule libp2p GossipSub applies ("not seen before →
  forward to mesh, irrespective of arrival method"), rather than the
  handler-specific duplication used by the Helium reference Erlang
  Plumtree. The redundant call in `onGossip` is removed.

  Regression test: `TestPlumtreeRefanoutOnFetchFrom` seeds peer B's
  store directly (no Gossip push), drives `A.fetchFrom(B, id)`, and
  asserts C receives the message via A's post-fetch refanout.

## [1.0.4] - 2026-04-20

### Added
- Plumtree-based dissemination (Leitão/Pereira/Rodrigues, "Epidemic
  Broadcast Trees", SRDS 2007). Entmoot's gossip is no longer
  push-only-on-originate: receivers now re-fanout on first-sight,
  with a self-healing spanning tree maintained via three new
  control-frame types on ids that never carry a body.
  - `MsgIHave` (0x0B): lazy advertisement. A publisher eagerly pushes
    full `Gossip` frames to `eagerPushPeers` and sends `IHave`-only
    to `lazyPushPeers`.
  - `MsgGraft` (0x0C): a peer that observed `IHave` for a missing id
    and did not receive the body via eager push within 3 s (matching
    libp2p GossipSub's `IWantFollowupTime`) sends `Graft` to pull the
    body AND be promoted back into the sender's `eagerPushPeers`.
  - `MsgPrune` (0x0D): a peer that receives a duplicate `Gossip`
    `Prunes` the sender — demoting it to `lazyPushPeers` so
    subsequent messages arrive as `IHave`, pruning the redundant
    full-body edge.
  - Initial tree shape: all roster members start in
    `eagerPushPeers`; duplicates self-prune to a spanning subset
    per-message. No explicit tree construction ceremony required.
  - All three new types are unsigned — precedent set by `FetchReq`,
    `RangeReq`, `MerkleReq`. Identity is established at the Pilot
    tunnel + on-Accept roster check, which is already sufficient.

  **Impact**: in a partial-connectivity topology (e.g. the
  three-node canary: VPS public, Phobos private, laptop private, no
  Phobos↔laptop direct edge), a Phobos publish now reaches the
  laptop with VPS as the re-fanout hop. Before v1.0.4 that message
  stopped at VPS. At steady state, message-per-broadcast overhead
  converges from `O(N × fanout)` (naive gossip) toward `O(N)` (one
  eager push per edge of the spanning tree) — the same efficiency
  profile as Riak Core, Helium, Solana's gossip, libp2p
  GossipSub/Episub, and Partisan.

- `eagerPushPeers` / `lazyPushPeers` peer-set maps on `Gossiper`,
  guarded by a new `plumMu`. Each roster member sits in exactly one
  set at any time. Seeded lazily on first Plumtree code path so the
  Gossiper can be constructed before the roster is fully populated
  (matches existing test + production ordering). Lookup helpers
  (`plumEagerExcept`, `plumLazyExcept`, `plumPromoteToEager`,
  `plumDemoteToLazy`, `plumCancelGraftsFor`) keep the mutex
  discipline contained to a small surface.

- `pendingGraft` map keyed on `(id, sender)` storing the outstanding
  `time.AfterFunc` timer per `IHave` advertisement. Any arrival of
  the body via the eager path cancels all pending timers for that
  id so we never emit a `Graft` for something we already have.

- Retry scheduler extended with three new ops (`opIHave`, `opGraft`,
  `opPrune`). `executeRetry` rebuilds the outbound frame from the
  `retryKey` since none of the three control frames carry signed
  content worth caching. Same exponential-backoff budget as
  `opPush`; exhausted slots fall through to anti-entropy
  reconciliation as today.

- `plumtree_test.go`: five new integration tests covering re-fanout
  in a line graph (A ↔ B ↔ C, A ↛ C), PRUNE on duplicate in a
  fully-connected mesh, GRAFT on a forced-lazy edge, initial-state
  eager population, and the basic PRUNE handler unit test. A new
  `filteringTransport` wrapper lets tests build arbitrary directed
  dial graphs over the existing `memTransport` hub.

### Changed
- `skills/entmoot/SKILL.md` metadata bumped to 1.0.4; adds a short
  "Dissemination: Plumtree (Leitão 2007)" note so agents reading
  the skill don't misunderstand the group protocol as push-only.

## [1.0.3] - 2026-04-19

### Fixed
- `entmootd publish` CLI no longer hits the 30 s IPC read-response
  deadline when a gossip fanout target is slow to dial. The
  publish handler now returns as soon as the message is durably
  inserted into the local store; peer delivery runs asynchronously
  via the existing exponential-backoff retry scheduler (patch 6
  in v1.0.2). Response latency is now single-digit milliseconds
  regardless of peer reachability, matching the industry-standard
  pub/sub convention (Kafka durable-log commit, NATS fire-and-
  forget, RabbitMQ publisher confirms): publish means "accepted
  for delivery", not "delivered to subscribers". Retries and
  anti-entropy reconciliation continue to cover any peers that
  were unreachable during the initial fanout.
- `gossip.memTransport.Close` no longer closes the buffered
  `acceptCh` alongside the `closed` signal channel — the redundant
  close raced with concurrent `Dial` goroutines that had already
  entered their select. The `<-t.closed` branch is sufficient to
  unblock both Accept and Dial; the accept channel is garbage-
  collected with the transport. Fixes a race detector failure
  surfaced by the new async-publish path in multi-node tests.

### Changed
- `skills/entmoot/SKILL.md` (version bumped to 1.0.3): clearer
  bootstrap and re-entry guidance for ephemeral agent
  environments. Adds a "Fast path" short-circuit for the
  already-joined case (95% of re-invocations on a long-lived
  host); an explicit warning against deleting
  `~/.pilot/identity.json` (deletion = new `node_id` = silently
  orphaned from every existing group roster); a
  `nohup setsid ... & disown` recipe for `entmootd join` that
  actually survives a pm2 / Telegram-bot parent exit; an
  "invite acquisition" subsection covering inline-JSON invites
  from chat messages (write to a tempfile before passing to
  `entmootd join`); and an explicit
  `export PATH="$HOME/.pilot/bin:$HOME/.entmoot/bin:$PATH"` at
  the top of the Routine-operation section for non-login shell
  contexts.

### Added
- `TestPublishReturnsBeforeFanout` in
  `pkg/entmoot/gossip/gossiper_test.go`: verifies the fast-return
  contract by publishing on node A while node B's accept loop is
  deliberately not running. Asserts Publish completes in <200 ms
  and the message is immediately present in A's local store even
  though the fanout to B is hung on a blocking `net.Pipe` write.

## [1.0.2] - 2026-04-19

### Changed
- `install.sh` source-build fallback now clones the patched Pilot fork
  (`jerryfane/pilotprotocol` main) instead of `TeoSlayer/pilotprotocol`.
  The "install Pilot separately" helper text and `SKILL.md` onboarding
  snippet also point at the fork's installer so agents following the
  documented flow pick up the reliability patches without manual steps.

### Added
- Exponential-backoff retry for gossip push and fetch. Transient `Transport.Dial`
  failures are requeued instead of dropped; a new `retryLoop` goroutine drains
  the `pending` slot map at 500 ms cadence with backoffs `1s, 2s, 4s, 8s, 16s,
  30s, 60s × 4` (10 attempts, ~5 min total). Exhausted slots log
  `gossip: retry budget exhausted` and rely on anti-entropy reconciliation
  for recovery.
- Anti-entropy reconciliation on peer reconnect. Both inbound `Accept` and
  successful outbound `pushGossip` / `fetchFrom` dials invoke `maybeReconcile`,
  rate-limited at one round per peer per 60 s. A round runs `MerkleReq` →
  compare roots → `RangeReq` since local latest timestamp → `FetchReq` for
  each missing id, all via the retry scheduler.
- Wire message types `MsgRangeReq` (`0x09`) and `MsgRangeResp` (`0x0A`) for
  timestamp-bounded id enumeration. Backward-compatible: older peers reject
  them as unknown types and callers silently fall back to push-only gossip.
- `--timeout` flag on `entmootd publish` (default 30 s) for tuning the
  control-socket response deadline on slow networks.

### Changed
- Default `entmootd publish` control-socket deadline raised from 10 s to 30 s
  so real fan-out publishes no longer surface spurious
  `publish: read response err="ipc: ... i/o timeout"` errors on healthy runs.

## [1.0.0] - 2026-04-17

### Added
- SQLite backend for the message store (pure-Go via `modernc.org/sqlite`,
  WAL mode, one database per group).
- Control-socket IPC at `~/.entmoot/control.sock`. Short-lived CLI
  invocations (`publish`, `tail`, `info`) route through a single long-running
  `join` process per host.
- `entmootd query` for historical indexed access to group messages.
- `entmootd tail` for live message streams (SQLite backfill + IPC
  subscription).
- `entmootd info` emits JSON by default and reports a `running` field.
- `Invite.ValidUntil` field with a 24 h default TTL. Expired invites are
  rejected on join.
- `install.sh` one-command installer with a prebuilt-binary fast path and
  source-build fallback.
- `skills/entmoot/SKILL.md` OpenClaw / Agent-Skills skill document.
- Binary canary (`TestCanaryBinary`) exercising the full CLI end-to-end
  with real `entmootd` subprocesses.

### Changed
- CLI restructured to the v1 five-command agent surface. `join` now blocks
  and owns the control socket. `publish` / `info` / `tail` / `query` have
  new semantics and JSON-only output.
- Go code moved under `src/`; `go.mod` at `src/go.mod` with a `replace`
  directive pointing at `../repos/pilotprotocol`.

### Removed
- `entmootd run` (subsumed by `entmootd join`).
- JSONL as the production backend (kept as a dev/debug backend under
  `src/pkg/entmoot/store/jsonl.go`).
