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
this group to this node. A connection alone would not. Each dialled peer's row
then carries a `probe` object holding `reachable`, `answered`, `latency_ms`,
`relayed`, how many addresses were tried, and a one-line reason when it
failed. This node's own row has no `probe` object: it is not dialled.

Three outcomes, not two, all inside `probe`. `reachable` is a peer that
answered and served us. `answered` without `reachable` is a peer that replied
and refused, with its own code in `refusal` - the membership it serves does
not include this node, which is what a removed member sees from every peer
and is not a network fault. Neither means nothing answered, and `error` says
what failed - including the one error that is not a network fault at all,
`not attempted: probe budget spent`, explained with the budget below.

The probe runs in the daemon, because the daemon owns the libp2p host, the
peerstore and the relay configuration; a second process dialling with its own
identity would answer a different question. Without a daemon the group reports
`probe_status` starting with `runtime_unavailable` and no peer row claims
anything.

`--timeout` is the budget for the whole probe, not a per-peer timeout, so a
group of any size costs one timeout, not one per member. It is divided,
floored and clamped. In this paragraph "peers" means the members this node
dials - the group minus itself:

- **Each attempt gets `max(timeout / peers, 500ms)`.** The division is why a
  large group costs one timeout rather than one per member; the 500ms floor is
  why a small group is not given a sub-millisecond deadline that reports a
  healthy peer unreachable for arithmetic reasons. So with two peers,
  `--timeout 5s` is 2.5s per attempt; with thirty peers it is 500ms each. A
  timeout below 500ms is raised to it.
- **Peers are dialled eight at a time.** Slow waves consume the budget, and a
  peer whose turn comes after it is gone is reported
  `not attempted: probe budget spent`.
- **The probe may run up to 500ms past the timeout.** A wave that starts with
  any time left gets a full 500ms rather than a doomed fraction of it. After
  that overshoot, remaining peers go unattempted.
- **A refusal is fast; a stalled handshake is not.** A peer that refuses
  answers in milliseconds, so most probes return well inside the budget. A
  peer that accepts the connection and then stalls - a NAT black hole, a
  dropped SYN-ACK - consumes its whole attempt, which is the case `--probe`
  exists to find.
- **`--timeout` above 60s is clamped to 60s.**

When a probe ran and the budget ran out first, `probe_status` starts with
`incomplete`. Every value but `ok` appends its reason after the kind -
`incomplete: the probe budget was spent before every member was tried` - so
match on the prefix, not the whole string. A probe that never ran reports its
own kind instead, `runtime_unavailable` or `failed`, and leaves every peer
unattempted. With peers that stall, `--timeout` of `0.5s x ceil(peers / 8)`
always dials every peer; because each wave only needs a sliver of budget left
to earn its full 500ms, a shorter timeout often suffices, so treat that figure
as the safe number rather than the minimum. Past roughly 960 stalling peers
the 60s ceiling makes `incomplete` unavoidable in one run. Use `--json` for
automation, and `--redact` when sharing a report.

Use `env` when a node reports `no running Entmoot daemon found` even though a
daemon process exists. It detects common wrong-namespace cases where the host
shell sees a different `/data` than the Docker/OpenClaw process that owns
`/data/.entmoot/control.sock`.

`peers` prints the same member rows `doctor` builds and takes the same
`--probe` and `--timeout`. Its rows are the group's membership, this node
included - not the narrower "peers" of the timeout arithmetic above. Without
`--probe` a listed row means a member whose key is in the group, not a member
proved reachable.

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
