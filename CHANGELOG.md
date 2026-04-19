# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
