---
title: serve
---

```sh
entmootd serve [-group GID...]
```

`serve` is the normal long-running Entmoot daemon command after a node has
joined at least one group. It starts the integrated libp2p host, opens the local
control socket, and starts GossipSub plus bounded membership/history sync for
persisted groups under `~/.entmoot/groups/`.

Use `join` once with a signed invite; use `serve` for service managers and
restarts. Expired or missing invite files do not affect `serve`.

For `/data`-backed agents, run commands through `/data/.entmoot/entmoot` so the
identity, data root, and connectivity profile match the supervised daemon.

Useful global flags:

```sh
-group <GROUP_ID>
-connectivity direct|relay-only
-controlled-relay <CIRCUIT_RELAY_MULTIADDR>
-listen-port 1004
```

Without `-group`, every locally joined group with a membership store is
served. A group that still holds only the pre-checkpoint roster chain is NOT
served: on start the daemon tries to adopt a founder-signed checkpoint 0 for
it. When the attempt errors — a recorded peer that cannot be dialled, or a
chain that cannot be read — it warns `membership adopt: no checkpoint yet`
with the reason in `err`. When it completes without one, which includes having
no peer recorded to ask, it warns `membership adopt: group awaits checkpoint 0
from its founder` and names the `membership upgrade` command in `hint`. If no other group qualifies, `serve` then exits 3 with
`serve: no joined groups found; run entmootd join <invite> once`. Adoption is
retried at start and then by a dedicated one-minute ticker, not by the
group maintenance loop, so the group begins being served as soon as a peer
supplies checkpoint 0. A directory with neither store is skipped with the
warning `serve: skipping group with no membership state`, and naming it with
`-group` is an error instead. Run
`entmootd membership upgrade -group <GROUP_ID>` on the founder to mint
checkpoint 0.

While serving, each session pulls membership from up to eight reachable members
every 15 seconds and pushes records it signs locally straight away. If an
answer did not fit in one response, the session immediately asks the same peer
again rather than waiting for the next round. After a sync that applied
anything, a session belonging to the founder or a delegated admin signs a
checkpoint if the group's `checkpoint_every` threshold of pending records has
been reached, which retires those records.

With `-group`, missing or invalid group state is an error. Relay-only mode
requires at least one `-controlled-relay` flag. Direct mode accepts the same
flag as a DCUtR hole-punch rendezvous, which is what makes a peer behind NAT
reachable before the connection is upgraded to a direct one.
