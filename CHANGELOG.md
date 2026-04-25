# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.4.6] - 2026-04-25

Fix a latent IPC error-routing bug surfaced by v1.4.4's
TURN-endpoint poller. Pre-fix, when multiple opcodes had
concurrent in-flight commands, the daemon's untagged Error
frames could be delivered to the wrong waiter — Pilot's
`dial timeout` errors were landing on Info callers, making
TURN-rotation polling appear permanently broken even when
the underlying IPC was healthy.

### Fixed

- **`Driver.deliverError` no longer mis-routes errors across
  concurrent opcodes.** Live evidence 2026-04-25 from VPS:
  every TURN-endpoint poll on phobos AND VPS failed with
  errors like `ipcclient: info: ipcclient: daemon: dial
  timeout` — the InfoStruct call is logically incapable of
  producing a `dial timeout` error, so the error was
  obviously routed from a concurrent gossip Dial call.

  Root cause: pre-fix the pending-reply book was a per-opcode
  map; `deliverError` walked the map (Go map iteration is
  randomised) and picked the head of a non-empty queue. Pilot's
  Error frame (`0x0A`) carries no correlation back to the
  command that caused it, so the routing decision was
  effectively a coin flip whenever two opcodes had waiters.

  Fix: replaced the per-opcode `map[Opcode][]chan` with a
  single global FIFO `[]pendingEntry`. Successful replies
  still match by opcode (linear scan, queue is shallow);
  Error frames pop the head, which is FIFO-correct because
  Pilot replies in command-issue order on a single socket.

- **Behaviour pre-v1.4.4 was OK by accident.** Entmoot used to
  issue IPC commands serially, so at most one opcode had a
  waiter at a time and the broken `deliverError` happened to
  pick the right one. v1.4.4's poller broke that invariant by
  running InfoStruct concurrently with the gossip layer's
  Dial/Send/Listen. v1.4.6 corrects the routing for any future
  concurrent IPC use too.

### Tests

- `pkg/entmoot/transport/pilot/ipcclient/error_routing_test.go`:
  - `TestErrorRouting_FIFOAcrossOpcodes` — issues Bind then
    Info concurrently; mock daemon replies with Error+InfoOK;
    asserts Bind (the FIRST issuer) gets the Error and Info
    succeeds. Pre-fix this would fail intermittently; post-fix
    it's deterministic.
  - `TestErrorRouting_OnlyOnePending` — single-pending case
    still works, guarding against a regression where the FIFO
    refactor might break the simpler path.

### Compat

No wire changes. Drop-in patch — both v1.4.4 and v1.4.5 had the
same routing bug; anyone on v1.4.4+ should upgrade. Pilot daemon
is unchanged; the daemon's untagged Error frame remains a known
protocol limitation that we work around client-side. A proper
correlation-tag fix would require a Pilot wire change (track
upstream).

## [1.4.5] - 2026-04-25

Hotfix on top of v1.4.4: the TURN-endpoint polling loop's 3 s
timeout was too tight on low-power ARM hardware, causing 100 %
poll failures and effectively disabling rotation detection.

### Fixed

- **Poll timeout bumped from 3 s to 15 s.** Live evidence
  2026-04-25 from phobos (Raspberry-Pi-class ARM box): three
  successive polls timed out at exactly 3 s, while a manual
  `pilotctl info` over a separate IPC connection returned in
  under 1 s. Pilot's Info handler aggregates uptime / peers /
  connections / ports into a single JSON marshal; on
  low-power ARM the serialization cost spikes well above 3 s
  under concurrent IPC load (gossip fanout retries,
  transport_ad publishes). v1.4.4's tuning was x86-only.
  15 s = half the 30 s poll interval, so a stuck IPC can't
  cause overlapping polls.

- **Poll failures log at WARN, not Debug.** Failed polls mean
  TURN rotation detection is offline for that cycle; without a
  WARN-level log the operator has no visibility unless already
  running at `-log-level=debug` (which phobos happened to be).
  One log per poll interval is bounded — not noisy. Manual
  pilotctl Info still works in parallel; only the polling
  goroutine's bound is too short.

### Compat

No wire changes. No new fields. Pure constant + log-level tweak.
Drop-in replacement for v1.4.4 — anyone already on v1.4.4 should
upgrade; the polling feature was effectively dead-on-arrival on
ARM-class hardware.

## [1.4.4] - 2026-04-25

Detect Cloudflare TURN allocation rotation and re-publish the
transport-ad immediately, instead of letting peers run on the
6-day safety-net refresh interval.

### Fixed

- **TURN-rotation propagation gap.** `entmootd`'s `LocalEndpoints`
  callback was a one-time snapshot of the `-advertise-endpoint` CLI
  flags, never refreshed. When pilot-daemon's TURN allocation
  rotated (port changes on restart or credential refresh — typical
  laptop or churned-credential scenarios with `-turn-provider=
  cloudflare`), the gossip advertiser kept publishing the stale
  CLI snapshot. Remote peers retained the stale TURN relay address
  in their cached transport_ad and their outbound frames silently
  dropped at Cloudflare's anycast edge.

  Live evidence 2026-04-25 from phobos<->laptop: phobos cached
  `104.30.149.4:20414` (laptop's previous allocation); laptop's
  current allocation was `:9587`. Phobos's `peer switched to
  turn-relay remote=:20414` log fired at every dial and traffic
  silently went into a dead TURN allocation. New transport_ad was
  never emitted because Entmoot had no way to detect the
  rotation.

  Fix: `entmootd` now polls `pilot-daemon.Info().TURNEndpoint`
  every 30 s, compares to the last-published value, and signals
  the gossip advertiser via `gossip.Config.EndpointsChanged` on
  any change. The advertiser's `LocalEndpoints` callback merges
  the live-polled TURN address on top of the static CLI snapshot
  (CLI `turn=` entries become a fallback for the cold start
  before the first poll completes). Rotation is observed and
  re-advertised within 30 s + transport_ad fanout latency
  (typically <1 s); peers receive the fresh TURN relay addr via
  the standard last-writer-wins on transport_ad sequence.

  Privacy-preserving by construction: the poller only reads from
  the local pilot-daemon over its existing IPC socket; no
  additional network footprint, no leak surface added.

### Tests

- `cmd/entmootd/turn_endpoint_poller_test.go`:
  - `TestTURNEndpointPoller_DetectsRotationAndSignals` — same
    value → no signal; rotated value → CurrentTURN updates and
    Changed receives a tick.
  - `TestTURNEndpointPoller_ErrorPreservesPreviousValue` —
    transient pilot IPC errors do NOT clobber the cached TURN
    addr (otherwise a single hiccup would withdraw the
    advertisement).
  - `TestTURNEndpointPoller_SignalCoalesces` — buffered-1 channel
    collapses bursts; the advertiser re-reads LocalEndpoints
    freshly each tick anyway, so the coalescing is correct
    behavior, not a lost signal.
  - `TestTURNEndpointPoller_RunStopsOnContextCancel` — clean
    shutdown on root-context cancel; no goroutine leak.

### Compat

No wire changes. No new flags. The CLI snapshot of
`-advertise-endpoint` is still consulted — non-TURN entries
(`tcp=`, `udp=`) pass through unchanged, and `turn=` entries are
preserved as a cold-start fallback if the first IPC poll fails.
Daemons running pilot-daemon < v1.9.0-jf.8 (no TURN field in
Info) decode `TURNEndpoint=""` and the poller treats them
exactly like a non-TURN configuration: empty live value, fall
back to CLI flags.

## [1.4.3] - 2026-04-24

Companion release to pilot-daemon v1.9.0-jf.11a. Surfaces
Entmoot's app-layer `-hide-ip` flag's relationship with the
newly-composable Pilot-layer privacy flags (`-turn-provider`,
`-no-registry-endpoint`, `-outbound-turn-only`) so operators can
see at startup whether their local pilot-daemon actually delivers
the privacy posture the app-layer flag asks for.

### Added

- **`pkg/entmoot/transport/pilot/ipcclient.Info`** grows
  `OutboundTURNOnly bool` and `NoRegistryEndpoint bool` fields
  (both `omitempty`). Mirrors pilot-daemon v1.9.0-jf.11a's
  `DaemonInfo` extension. Pre-jf.11a daemons decode the fields as
  `false`; no wire change.

- **`entmootd -hide-ip` startup check.** When `-hide-ip` is set,
  the daemon's join path now queries the local pilot-daemon's
  `Info()` at startup and compares:
  - Pilot has `TURNEndpoint != ""` (i.e. `-turn-provider` is set
    and the initial TURN allocation succeeded)?
  - Pilot has `OutboundTURNOnly = true` (i.e. outbound traffic
    routes through TURN; RFC 8828 Mode 3)?
  - Pilot has `NoRegistryEndpoint = true` (i.e. registry.Lookup
    returns "endpoint unknown")?

  Any missing piece produces a `slog.Warn` naming the specific
  leak channel that remains open and pointing at pilot-daemon's
  `-hide-ip` preset as the one-flag remedy. All three present
  produces a single `slog.Info` confirming full hide-ip posture.

  This closes a class of silent mis-configuration where an
  operator sets `entmootd -hide-ip` alone, sees no errors, and
  believes they're private — while pilot-daemon is still
  publishing their IP to the registry or routing traffic
  direct-outbound. The check is best-effort: if the Info IPC
  call fails (slow daemon, older daemon without the new fields,
  etc.), we log at Debug and proceed normally — never block
  startup.

### Wire compatibility

- **No wire changes at the Entmoot layer.** Transport-ad format
  is unchanged; group/roster/message wire formats are unchanged;
  IPC framing is unchanged. The only new thing on the IPC wire
  is two `omitempty` JSON fields in `InfoOK` responses, which
  pre-v1.4.3 Entmoots decode-and-ignore anyway because the
  untyped `Driver.Info()` path doesn't project them into any
  typed Go struct.
- **Pre-jf.11a pilot-daemons**: the new typed fields decode as
  `false`, which causes the startup check to warn about missing
  `OutboundTURNOnly` and `NoRegistryEndpoint`. That's the
  correct behaviour — pre-jf.11a daemons really don't have those
  flags, so the warning accurately describes the privacy gap.
- **v1.4.3 ↔ v1.4.2 / v1.4.1 / v1.4.0** all interop cleanly at
  the gossip layer; this release is additive and behavioral-
  change-free except for the new startup log lines under
  `-hide-ip`.

### Dependencies

- **No new dependencies.** Go standard library only.

## [1.4.2] - 2026-04-24

### Changed

- **Relaxed `onTransportAd` sender gate so trusted mesh peers can
  forward signed ads.** Before v1.4.2, the receive path at
  `gossiper.go:2711` rejected any frame whose `remote` (IPC-level
  sender) wasn't the ad's `Author.PilotNodeID`. The explicit intent
  was "single-hop" delivery, which turned `refanoutTransportAd`
  into a structural no-op: any intermediate relayer's forwarded
  frame was dropped downstream. Live evidence on 2026-04-24
  (asymmetric-TURN test via Pilot v1.9.0-jf.9): laptop couldn't
  direct-dial phobos (same-LAN false-positive #85 + CGNAT),
  the VPS received laptop's ad and refanouted to phobos, and
  phobos dropped the frame at this gate. End-to-end propagation
  was blocked despite both peers being reachable via the VPS.

  v1.4.2 replaces the author=sender gate with a trusted-sender
  gate via a new `isTrustedSender(ctx, remote)` helper backed by
  the existing `trustedSet(ctx)` cache. Log level drops from
  `Warn` to `Debug`; a non-trusted sender is routine (trust churn,
  trust-auto-approve race) and doesn't warrant operator attention.

  The downstream defenses are unchanged — **signature verification
  against the author's roster-recorded pubkey, roster-membership
  check on the author, per-(peer, topic) rate limit, and LWW on
  seq via `PutTransportAd`**. The signature remains the integrity
  root: any tampering with the ad body invalidates the signature
  and the receiver still drops. This is the standard gossip-
  network design (Bitcoin, IPFS, Gnutella): forwarders are
  trusted to relay but not to fabricate.

  Fail-open on cold-start (`trustedSet` returns nil before the
  first IPC snapshot lands) matches `plumEagerExcept`'s existing
  precedent; during the ~seconds-long warmup the downstream
  signature + membership checks still gate inbound ads.

### Operational impact

- **Hide-ip peers become reachable via any shared mesh hub.** A
  hide-ip peer whose direct-dial path to another peer is blocked
  (NAT, CGNAT, same-LAN false-positive, transient tunnel state)
  can now still have its transport-ad propagate through any third
  peer both sides can reach. For the current deployment, this
  enables phobos to install laptop's TURN endpoint via the VPS
  as a relay, which in turn lets phobos engage Pilot jf.9's
  `turn-relay` transport.

### Wire compatibility

- **No wire-format changes.** A v1.4.2 forwarder refanouting to a
  v1.4.1 receiver still gets dropped by the v1.4.1 receive-side
  author=sender gate; only upgrading both endpoints unlocks full
  multi-hop. A v1.4.1 publisher fanning out to a v1.4.2 receiver
  is accepted (the receiver's relaxed gate doesn't care which
  version the sender runs).

### Dependencies

- **No new dependencies.** Go standard library only.

## [1.4.1] - 2026-04-24

### Fixed

- **Transport-ad fanout no longer silently drops the ad on first-attempt
  direct-dial failure.** Before v1.4.1, both the publisher-side
  `fanoutTransportAd` and the receiver-side `refanoutTransportAd` called
  `Transport.Dial` exactly once per peer per burst and log-and-dropped
  on any error — so a NAT flap, a same-LAN colliding-subnet
  false-positive, a stale tunnel, or any other transient dial failure
  lost the ad until the author's next weekly refresh. Live evidence on
  2026-04-24: laptop's `-hide-ip` ad (Pilot v1.9.0-jf.9 asymmetric TURN
  test) reached the VPS but never reached phobos because the VPS's
  refanout dial to phobos timed out while phobos's tunnel was mid-
  switch to relay, and the VPS then dropped the ad. End-to-end jf.9
  couldn't be exercised until the ad propagated, so the live test was
  blocked.

  Fix: fanout / refanout failures now feed the existing message-retry
  scheduler. Dial failures enqueue a retry entry keyed on (peer,
  author); the same scheduler goroutine and decorrelated-jitter backoff
  (~200 ms → ~50 s, inherited from v1.0.7) that already covered message
  pushes now also re-attempt transport-ad sends. Retries cap at **6
  attempts** (independent of the 10-attempt message cap) with a
  **5-minute wall-clock ceiling**, and short-circuit via
  `TransportAdStore.GetTransportAd` before re-dialling if a newer seq
  from the same author is already stored locally — so a superseded ad
  never re-wastes a dial. Terminal failures log at `Debug` (weekly
  refresh re-seeds convergence; no operator action needed). `gossiper.go`
  scheduler is extended in place via a new `opTransportAd` retry op;
  the old log-and-drop comment about "weekly refresh makes retry state
  redundant" is removed — live evidence disproves it.

### Limitations

- **Retry state is in-memory only.** If `entmootd` restarts before a
  queued ad retry has fired, the retry is lost. Same behaviour as the
  pre-existing message retry scheduler; durable retry is out of scope
  for v1.4.1 and scheduled for v1.4.2 alongside transport-ad RBSR
  anti-entropy.

### Wire compatibility

- **No wire-format changes.** `TransportAd`, `Gossip`, and every other
  frame is byte-for-byte identical to v1.4.0. A v1.4.1 publisher
  retrying a fanout to a v1.4.0 (or earlier) receiver sees the same
  receive path light up as today — the retry is invisible to the
  remote. A v1.4.0 publisher hitting a transient failure continues to
  drop-on-first-try; only the upgraded side benefits.

### Dependencies

- **No new dependencies.** Go standard library only; no module graph
  changes.

## [1.4.0] - 2026-04-24

### Added

- **`-hide-ip` global flag on `entmootd`.** Opt-in, default `false`.
  When set on a node that invokes `entmootd join`, the gossiper's
  transport-ad advertiser suppresses every UDP/TCP endpoint coming
  out of Pilot and publishes only the TURN relay entry from
  `Info.TURNEndpoint`. With no TURN relay available, the advertiser
  emits **no ad at all** and logs a `Warn` — the node becomes
  unreachable until a relay is configured. This is deliberate:
  silently falling back to IP advertisement would defeat the
  privacy goal the flag exists for. Requires `pilot-daemon
  v1.9.0-jf.8+` for any useful reachability; earlier daemons do
  not expose `turn_endpoint` in Info.

- **`pkg/entmoot/transport/pilot/ipcclient.Info`** typed struct.
  Replaces the previous map-keyed access pattern for callers that
  want compile-time field access. The existing `Driver.Info`
  method (returning `map[string]interface{}`) is unchanged for
  backwards-compat; a new `Driver.InfoStruct` returns the typed
  `Info`. The struct's `TURNEndpoint string \`json:"turn_endpoint,omitempty"\``
  field decodes to `""` when talking to jf.7 daemons (which omit
  the key) or to jf.8 daemons without a TURN provider configured.

- **`turn` accepted as a transport-ad network string** on both the
  advertiser path (`-advertise-endpoint turn=relay:3478` at
  `entmootd join`) and the validator (`validateEndpoint` in the
  gossiper). The wire format itself is unchanged — `NodeEndpoint.Network`
  has always been a free-form short string; v1.4.0 just widens the
  set of strings Entmoot produces.

### Wire compatibility

- **Transport-ad frame format is unchanged.** A v1.0+ receiver
  decoding a `TransportAd{Endpoints: [{Network:"turn", Addr:...}]}`
  sees a well-formed payload and hands it through
  `Transport.SetPeerEndpoints` unmodified. Old daemons will reject
  the `"turn"` network string at the Pilot IPC layer; new daemons
  (`pilot-daemon v1.9.0-jf.8+`) route it to `AddPeerTURNEndpoint`.
  No coordinated upgrade is required — a v1.4.0 peer broadcasting
  a TURN ad to a v1.3.0 peer just sees its new-daemon peer install
  the endpoint on the remote side.

- **`Info.TURNEndpoint` is `omitempty` on both ends.** jf.7 daemons
  never set it; v1.4.0 decodes the absence as `""` and behaves
  exactly as it did in v1.3.0.

- **A v1.4.0 gossiper with `-hide-ip` becomes unreachable until the
  local pilot-daemon is upgraded to jf.8+** (the daemon must be
  able to both advertise a TURN endpoint and route inbound TURN
  traffic). This is the expected upgrade path, not a regression:
  the flag is opt-in.

### Behaviour

- When `-hide-ip` is set AND no TURN endpoint is available, the
  advertiser emits a `slog.Warn` of the form `"hide-ip set but no
  TURN relay available; peer will be unreachable"` and skips the
  publish. Operators must see this in log tailing to understand
  why their peer is offline.

- When `-hide-ip` is set AND a TURN endpoint is available, the
  advertiser publishes a single-entry ad with `Network="turn"`
  and drops UDP/TCP entries in the same callback-returned slice.

- When `-hide-ip` is unset (the default), behaviour is identical
  to v1.3.0: every endpoint the `-advertise-endpoint` flag was
  fed is published as-is. The TURN network string is supported on
  this path too, so an operator who wants UDP+TCP+TURN all
  advertised can pass all three.

## [1.3.0] - 2026-04-23

### Changed

- **License: Apache License 2.0.** Entmoot previously had no declared
  license. Establishing one required decoupling the binary from its
  only copyleft dependency; see below.

- **IPC to `pilot-daemon` is now an in-tree client.** Entmoot no
  longer imports `github.com/TeoSlayer/pilotprotocol/pkg/driver`.
  In its place, a new package
  `pkg/entmoot/transport/pilot/ipcclient` provides an independent
  Go implementation of the Pilot IPC wire protocol, written from
  Pilot's public specification
  (`pilotprotocol/docs/SPEC.md`). Runtime behavior is unchanged —
  `entmootd` still speaks the same Unix-socket wire protocol to
  the same `pilot-daemon` process. The rewrite removes Pilot's
  AGPL-3.0 source from Entmoot's compiled binary, freeing Entmoot
  to adopt its own license.

### Added

- **`pkg/entmoot/transport/pilot/ipcclient`** — 9 files (~1500 LOC)
  implementing the framing, opcodes, demuxer, connection,
  listener, driver, and type surface needed by Entmoot's Pilot
  transport adapter. Covered by unit tests, a demuxer-correctness
  suite, and an in-process fake-daemon integration test. The
  package documents every clarification that went beyond SPEC.md
  (AcceptedConn port prefix; TrustedPeers as a Handshake
  sub-command; SetPeerEndpoints TLV layout and daemon limits;
  Error frame encoding; Recv-before-register race handling).

- **`pkg/entmoot/transport/pilot/addr.go`** — local `Addr` and
  `ParseSocketAddr` types replacing the former
  `pilotprotocol/pkg/protocol` import. Matches the Pilot
  socket-address string format for wire compatibility.

- **`LICENSE`, `NOTICE`, `CONTRIBUTING.md`** at the repository
  root. `CONTRIBUTING.md` establishes a Developer Certificate of
  Origin requirement for future contributions.

### Removed

- **`github.com/TeoSlayer/pilotprotocol` go-module dependency.**
  Both the `require` entry and the `replace` directive are gone;
  `go mod graph | grep pilotprotocol` is empty.

### Wire compatibility

No changes to any wire-format emitted on the network. Entmoot
v1.3.0 is drop-in compatible with all earlier v1.x peers and with
`pilot-daemon` v1.9.0-jf.7. Upgrading a single node in a mesh does
not require coordinated upgrades.

## [1.2.1] - 2026-04-22

### Fixed

- **Anti-entropy catches gaps anywhere in the log.** The v1.2.0
  reconcile path used `fetchPeerRange(since = latestLocalTimestamp)`
  as its gap-recovery query, which could only heal peers that were
  strictly *ahead* of us — if we held a newer message than the gap
  we were missing, the range query excluded the gap entirely. Live
  testing on 2026-04-22 surfaced this when two messages from a peer
  who had been briefly unreachable never propagated even though
  Merkle roots clearly differed. v1.2.1 replaces the cursor-based
  path with Range-Based Set Reconciliation (Meyer, SRDS 2023;
  arXiv:2212.13567), the same algorithm used by Willow / Earthstar
  / iroh and by Negentropy (Nostr NIP-77). Bandwidth is proportional
  to the symmetric difference (not the set size), convergence is
  O(log N) rounds, and the protocol catches gaps anywhere in the
  timeline by construction.

### Added

- **Range-Based Set Reconciliation** package
  (`pkg/entmoot/reconcile`). Greenfield Go implementation of the
  RBSR state machine with a Negentropy-style 16-byte fingerprint —
  `SHA-256(count_u64_le || sum_mod_2^256(SHA-256(id)))[:16]` —
  which is commutative, incrementally computable, and resistant
  to duplicate-cancellation / Gaussian-elimination attacks that
  plain XOR fingerprints suffer from. Defaults: leaf threshold
  16, fanout per round 16, max rounds 10 (safety-net — `WARN` on
  overflow).

- **`MsgReconcile = 0x11`** wire opcode carrying
  `{group_id, round, ranges, done}`. The RBSR session holds a
  yamux stream open across multiple alternating frames until both
  sides flag `done=true`. This is the first handler in the
  codebase that keeps a conn alive across frames; the convention
  is flagged explicitly in `onReconcileReq`.

- **`Transport.SetOnTunnelUp(cb)`** callback surface. Pilot
  adapter fires the callback after every freshly-established
  yamux session (outbound and inbound), with panic-recover and
  goroutine isolation so a slow reconcile dial can't block the
  transport. In-memory transport fires symmetrically on both
  sides of each `Dial` / `Accept`. `gossiper.Start` installs
  `SetOnTunnelUp(func(p){ g.maybeReconcile(ctx, p) })` so every
  reconnect triggers an immediate cooldown-gated reconcile —
  directly addressing the Pilot-tunnel-flap pattern observed on
  2026-04-22 where yamux orphaning across rekeys left stale
  peers invisible until an organic publish arrived.

- **Background anti-entropy ticker** (`reconcilerLoop`). Picks
  the least-recently-reconciled roster member each 30 s ±20 %
  jittered tick, one peer per tick. Skip optimization:
  `lastKnownPeerRoot[peer]` caches each peer's Merkle root after
  a successful reconcile; if the local root still matches, the
  tick is a silent no-op. An idle three-peer mesh costs zero
  dials per tick at steady state. Bounds worst-case
  post-partition catch-up latency to one tick interval regardless
  of whether anyone publishes — the problem that kept two
  specific messages stuck on VPS for minutes after recovery,
  because reconcile was reactive-only on inbound gossip.

- **`MessageStore.IterMessageIDsInIDRange`** — new interface
  method returning message IDs sorted by byte-order in a
  half-open `[lo, hi)` range. Implemented on Memory, JSONL, and
  SQLite backends. SQLite uses the auto-generated primary-key
  index on `message_id` (confirmed via `EXPLAIN QUERY PLAN`).
  The byte ordering is distinct from — and does not replace —
  the existing topological ordering used by `MerkleRoot()`; RBSR
  uses byte order because it aligns with the SQLite PK and makes
  range splitting arithmetic trivial.

- **Dial-backoff hygiene**: successful inbound Accept from a peer
  now clears that peer's outbound dial-backoff window
  (`recordDialSuccess(remote)` at the Accept-success site).
  Inbound reachability is strong bidirectional-reachability
  evidence, so a cached outbound-backoff from an earlier failure
  is almost certainly stale. If the next outbound dial does fail,
  the backoff re-arms from base.

- **Debug logs at reconcile decision points**: cooldown-gated
  skips, tick-skips when roots already match, per-round progress
  (peer, round, outgoing ranges, newly-missing count, done
  flags). Makes it straightforward to distinguish "no reconcile
  happened" from "deduplicated" from "converged in N rounds"
  when tracing operational issues.

### Wire compatibility

- v1.2.1 ↔ v1.2.1: `MsgReconcile` path.
- v1.2.0 → v1.2.1: legacy peer issues `MsgRangeReq`; v1.2.1
  retains `onRangeReq` + `onMerkleReq` as compat responders.
  Pair converges via the v1.2.0 buggy-but-functional path.
- v1.2.1 → v1.2.0: v1.2.1 issues `MsgReconcile`; v1.2.0 responder
  sees unknown opcode and drops the stream; v1.2.1 reads EOF,
  emits `Debug "session ended early (peer may be v1.2.0)"`, and
  returns without state corruption. Historical gaps between
  mismatched-version peers do not heal until the whole mesh is
  upgraded — acceptable for our three-peer deployment; staged
  upgrades should cycle through in order.

### Internals

- New field `Gossiper.lastKnownPeerRoot map[NodeID]wire.MerkleRoot`
  guarded by `pendMu`, updated at the end of each successful
  reconcile.
- Dead code removed: `fetchPeerRange` and `latestLocalTimestamp`
  (both superseded by RBSR).
- `reconcileSessionTimeout = 15 s`, `reconcilerTickBase = 30 s`,
  `reconcilerTickJitter = 6 s` new constants.
- `jitteredReconcilerTick()` helper alongside the existing
  `jitteredReconcileCooldown()`.

### Known

- `test/canary/TestCanaryPilot` intermittently hangs (pre-existing
  since v1.2.0) on `driver.sendAndWaitTimeout` in the Pilot IPC
  adapter — the call is not `context.Context`-bounded. Stack
  trace is entirely in `transport/pilot/*` (untouched by v1.2.1).
  Fix is a Pilot-fork concern (prospective jf.8): bound the IPC
  wait with the caller's context. v1.2.1's added OnTunnelUp
  callback creates more tunnel-establish events in the canary,
  which slightly increases the flake surface but does not
  introduce the bug.

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
