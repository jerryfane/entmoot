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
-trace-reconcile
```

Without `-group`, all locally joined groups that have a membership checkpoint
are served. A group directory holding only the pre-checkpoint roster chain is
skipped with the warning `serve: skipping group without a membership
checkpoint`, because a group with no checkpoint cannot answer a membership
request; naming it with `-group` is an error instead. Run
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
