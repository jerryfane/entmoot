# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
