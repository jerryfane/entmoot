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
their membership and message counts, each group's Merkle root, and one row per
member with the peer id derived from its key. Without `--probe` those rows come
from local state and say nothing about whether a peer answers.

`--probe` asks the running daemon to dial every other member and perform one
membership read for the group, which is the question behind "why will this
group not converge": it proves the peer is up, speaks the protocol, and serves
this group to this node. A connection alone would not. Each peer row then
carries `reachable`, `answered`, `latency_ms`, `relayed`, how many addresses
were tried, and a one-line reason when it failed.

Three outcomes, not two. `reachable` is a peer that answered and served us.
`answered` without `reachable` is a peer that replied and refused, with its own
code in `refusal` - the membership it serves does not include this node, which
is what a removed member sees from every peer and is not a network fault.
Neither means nothing answered, and `error` says what failed.

The probe runs in the daemon, because the daemon owns the libp2p host, the
peerstore and the relay configuration; a second process dialling with its own
identity would answer a different question. Without a daemon the group reports
`probe_status: runtime_unavailable` and no peer row claims anything.

`--timeout` is the budget for the whole probe, not per peer, so a thirty-member
group does not cost thirty timeouts; `probe_status` reads `incomplete` when it
ran out. Use `--json` for automation, and `--redact` when sharing a report.

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

`peers` prints the same member rows `doctor` builds and takes the same
`--probe` and `--timeout`. Without `--probe` a listed peer means a member whose
key is in the group, not a peer proved reachable.

Common diagnoses:

Each group carries `local_member_status`. `doctor` assigns exactly two values:

- `ok`: this node's key is in the group's current membership.
- `not_in_roster`: the group has no member with this node's key - it was
  removed, or never joined.

A join's health summary uses the same field with its own vocabulary: `ok`,
`missing` when the daemon is up but this node is not in some joined group's
member set, and `runtime_unavailable` when no daemon was reachable to ask.
That summary's `route_probe` is always `not_requested`; no route probe is
wired into the readiness event.

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
