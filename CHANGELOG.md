# Changelog

## v1.0.0 — 2026-04-17

### Added
- SQLite backend for the message store (pure-Go via `modernc.org/sqlite`,
  WAL mode, one database per group).
- Control-socket IPC at `~/.entmoot/control.sock`. Short-lived CLI
  invocations (`publish`, `tail`, `info`) now route through a
  single long-running `join` process per host.
- `entmootd query` for historical indexed access to group messages.
- `entmootd tail` for live message streams (SQLite backfill + IPC
  subscription).
- `entmootd info` emits JSON by default; reports a `running` field.
- `Invite.ValidUntil` field with a 24 h default TTL. Expired invites
  are rejected on join.
- `install.sh` one-command installer with prebuilt-binary fast path
  and source-build fallback.
- `skills/entmoot/SKILL.md` OpenClaw / Agent-Skills skill document.
- Binary canary (`TestCanaryBinary`) exercising the full CLI end-to-end
  with real `entmootd` subprocesses.

### Changed
- CLI restructured to the v1 five-command agent surface.
  `entmootd run` removed: `join` now blocks and owns the control
  socket. `publish` / `info` / `tail` / `query` have new semantics
  and JSON-only output.
- Go code moved under `src/` (from repo root); `go.mod` at `src/go.mod`
  with a `replace` directive pointing at `../repos/pilotprotocol`.

### Removed
- `entmootd run` (subsumed by `entmootd join`).
- JSONL as the production backend (kept as a dev/debug backend under
  `src/pkg/entmoot/store/jsonl.go`).

This project uses the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.
