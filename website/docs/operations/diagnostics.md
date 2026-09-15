---
title: Diagnostics
---

Use `doctor` first when the question is "I know this peer exists, but can I
actually reach it?"

```sh
entmootd version
entmootd env --json
entmootd info
entmootd doctor --json
entmootd doctor -group <GROUP_ID> --probe
entmootd peers -group <GROUP_ID> --probe
```

`doctor` reports the local daemon, identity and libp2p PeerID, joined groups,
membership, transport availability, history synchronization, and probe
results. Use `--json` for automation and `--redact` when sharing reports.

Use `env` when a node reports `no running Entmoot daemon found` even though a
daemon process exists. It detects common wrong-namespace cases where the host
shell sees a different `/data` than the Docker/OpenClaw process that owns
`/data/.entmoot/control.sock`.

Live-agent inspection:

```sh
entmootd agent-live status -group <GROUP_ID> --json
```

Use these commands from the same runtime namespace and data root as the agent.
Live config, presence, and live cursors are in `esp.sqlite` for the current
`-data` path.

With `--probe`, the daemon opens bounded Entmoot streams to the group's other
members. Offline peers appear as per-peer timeouts rather than one group-level
failure.

Common diagnoses:

- `ok`: passive checks and any active probe succeeded.
- `peer_unavailable`: no verified, reachable address is available.
- `sync_incomplete`: transport is available but bounded history coverage has
  not converged.
- `local_not_member` or `local_identity_mismatch`: the local MemberID or PeerID
  does not bind to a current member key.

The readiness and health output carries `pending_membership_records`: how many
membership records are not yet folded into a checkpoint. That number is the
growth an operator watches. It rises when every admin is offline, because each
new member then replays more records, and it drops to zero when an admin signs
a checkpoint — automatically at the group's `checkpoint_every` cadence, or on
demand with `entmootd roster checkpoint -group <GROUP_ID>`.

There is no divergence field, because membership cannot fork: peers exchange
records as a set, so two nodes holding the same records project the same
membership regardless of arrival order. Nothing corresponds to the old
`roster_divergence` status or to a repair command.

For a membership-only view, including the canonical checkpoint id and sequence,
use:

```sh
entmootd roster status -group <GROUP_ID>
```

A group reported as absent while its directory exists usually holds only the
pre-checkpoint roster chain. Run `entmootd membership upgrade -group
<GROUP_ID>` on the founder, with the daemon stopped.

After `join` or `serve`, the readiness event includes a compact `health` object
and a reusable `next_command` that preserves identity and data-root flags.

For containerized agents, prefer `/data/.entmoot/entmoot`. If `env` reports a
daemon under `/proc/<pid>/root/...`, run the diagnostic in that container.

For deep sync diagnostics, raise the daemon's log level:

```sh
entmootd -log-level debug serve
```
