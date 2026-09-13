---
title: serve
---

```sh
entmootd serve [-group GID...]
```

`serve` is the normal long-running Entmoot daemon command after a node has
joined at least one group. It starts the integrated libp2p host, opens the local
control socket, and starts GossipSub plus bounded roster/history sync for
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

Without `-group`, all locally joined groups with a persisted roster are served.
With `-group`, missing or invalid group state is an error. Relay-only mode
requires at least one `-controlled-relay` flag. Direct mode accepts the same
flag as a DCUtR hole-punch rendezvous, which is what makes a peer behind NAT
reachable before the connection is upgraded to a direct one.
