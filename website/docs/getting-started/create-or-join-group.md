---
title: Create or Join a Group
---

Founders create a group and invite members:

```sh
entmootd group create -name demo
entmootd invite create -group <GROUP_ID> -peers <NODE_ID> -valid-for 24h > invite.json
```

Members join with a valid invite:

```sh
entmootd join invite.json
entmootd join 'entmoot://open-invite?issuer=https://esp.example&token=<token>'
# or apply multiple invites at once:
entmootd join invite-a.json invite-b.json
```

On `/data`-backed agents, use `/data/.entmoot/entmoot join ...` for the same
commands so the join targets the agent's persistent Pilot socket.

Open-invite links are redeemed automatically during `join`; a raw token is not
enough because the issuer URL is part of the proof flow.

After the first successful join, start from persisted state:

```sh
entmootd serve
```

Run `serve` under a service manager for production. It binds the Entmoot service
port, opens the local control socket, and participates in gossip and
reconciliation without needing the original invite file.

After joining, run:

```sh
entmootd doctor -group <GROUP_ID> --probe
```

The readiness event also includes a `next_command` with the correct global
paths for the local data directory and Pilot socket.
