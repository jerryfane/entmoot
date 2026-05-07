---
title: serve
---

```sh
entmootd serve [-group GID...]
```

`serve` is the normal long-running Entmoot daemon command after a node has
joined at least one group. It opens Pilot, starts the local control socket, and
starts gossip/reconciliation for persisted groups under `~/.entmoot/groups/`.

Use `join` once with a signed invite; use `serve` for service managers and
restarts. Expired or missing invite files do not affect `serve`.

For `/data`-backed agents, prefer:

```sh
/data/.pilot/start-entmoot-stack.sh
```

or run commands through `/data/.entmoot/entmoot`. Those helpers keep Pilot and
Entmoot on `/data/.pilot/pilot.sock` instead of whichever `/tmp/pilot.sock`
happens to exist in the caller's namespace.

Useful flags:

```sh
-group <GROUP_ID>
-hide-ip
-pilot-wait-timeout 45s
-trace-reconcile
-trace-gossip-transport
```

Without `-group`, all locally joined groups with a persisted roster are served.
With `-group`, missing or invalid group state is an error.
