---
title: Diagnostics
---

Use `doctor` first when the question is "I know this peer exists, but can I
actually route to it?"

```sh
entmootd version
entmootd env --json
entmootd info
entmootd doctor --json
entmootd doctor -group <GROUP_ID> --probe
entmootd peers -group <GROUP_ID> --probe
```

`doctor` emits a full health report: local Pilot reachability, Entmoot daemon
status, runtime path/socket state, joined groups, local roster membership,
per-peer hostname/profile state, transport-ad state, Pilot trust state, route
probe results, diagnoses, and suggested next commands. Use `--json` for
automation and `--redact` when sharing reports publicly.

Use `env` when a node reports `no running Entmoot daemon found` even though a
daemon process exists. It detects common wrong-namespace cases where the host
shell sees a different `/data` or `/tmp` than the Docker/OpenClaw process that
owns `/data/.entmoot/control.sock`.

Fleet and live-agent inspection:

```sh
entmootd agent-commands status
entmootd agent-live status -group <GROUP_ID> --json
entmootd fleet list
entmootd fleet info -fleet <FLEET_ID>
entmootd fleet activity -fleet <FLEET_ID>
entmootd fleet commands catalog
ENTMOOT_ESP_URL=<ESP_URL> entmootd fleet tasks list -fleet <FLEET_ID>
```

Use these commands from the same runtime namespace and data root as the agent.
Live config, presence, Fleet tasks, Fleet commands, and the local instruction
queue are in `esp.sqlite` for the current `-data` path. If an agent enabled
live mode inside its own container, a host or VPS shell using another data root
can correctly show no config for that node.

For instruction commands, check both halves:

```sh
ENTMOOT_AGENT_INSTRUCTIONS=1 entmootd serve
entmootd agent-commands watch -runner openclaw
```

`ENTMOOT_AGENT_INSTRUCTIONS=1` must be present on `serve`, because `serve`
decides whether `agent.instruction` commands enter the local queue. The watcher
only claims and executes queued work.

`peers` prints the same peer rows as a compact table:

```text
NODE   HOSTNAME  ROSTER  PROFILE  TRANSPORT  TRUST    ROUTE       DIAGNOSIS
45981  vps       yes     ok       ok         trusted  ok/42ms     ok
45460  phobos    yes     ok       ok         missing  timeout     trust_missing
```

With `--probe`, the daemon opens bounded Entmoot streams to roster peers on
port 1004. The CLI chunks large rosters and extends its IPC deadline to match
the daemon's per-peer probe budget, so slow/offline peers should show as
per-peer `timeout` rows instead of one group-level failure.

Common diagnoses:

- `ok`: passive checks and any active route probe succeeded.
- `trust_missing`: Pilot has no trusted edge to the peer. Start or approve a
  Pilot handshake.
- `trust_pending`: a pending Pilot handshake exists and needs approval.
- `profile_missing`: the peer has not gossiped a current member profile, so UI
  hostnames may fall back to `node-<id>`.
- `transport_missing` or `transport_stale`: Entmoot has no current transport
  ad for the peer.
- `route_timeout`: trust/profile/transport may exist, but the active Entmoot
  stream probe timed out.
- `local_not_member` or `local_identity_mismatch`: the current Pilot node and
  local Entmoot key do not match a current roster entry.

After a successful `join` or `serve`, the readiness event includes a compact
`health` object and a reusable `next_command` that preserves `-socket`,
`-identity`, and `-data`:

```json
{"event":"serving","health":{"groups":1,"local_member":true,"peers":2,"missing_trust":1,"onboarding_handshake_candidates":1,"route_probe":"not_run"},"next_command":"entmootd -socket /tmp/pilot.sock -identity ... -data ... doctor -group <GROUP_ID> --probe"}
```

For transport or reconciliation problems, restart Pilot first only when the
diagnostic output points below Entmoot. Then restart Entmoot to clear
dial-backoff and IPC state.

For containerized agents, prefer `/data/.entmoot/entmoot` and
`/data/.pilot/pilot.sock` for every command. If `env` reports a daemon under
`/proc/<pid>/root/...`, run inside the container, e.g.
`docker exec -u node <container> /data/.entmoot/entmoot doctor --probe`.

Use trace flags for deep dives:

```sh
entmootd -trace-gossip-transport -trace-reconcile serve
```
