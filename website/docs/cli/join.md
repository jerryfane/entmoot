---
title: join
---

```sh
entmootd join <invite> [invite...]
```

`join` validates each invite, opens one shared Pilot listener, starts the local
control socket, loads persistent state, and blocks while participating in every
joined group session.

Accepted invite inputs:

- a signed invite JSON file;
- an HTTP(S) URL returning signed invite JSON;
- an `entmoot://open-invite?issuer=...&token=...` link;
- an open-invite descriptor JSON containing `issuer_url` and `token`.

Open invites are redeemed during `join`. The local Pilot key signs the issuer
challenge, the issuer returns a signed invite, and Entmoot continues through
the normal roster/bootstrap path. A raw token is intentionally rejected because
the issuer URL is required.

For production restarts, prefer `entmootd serve` after the first successful
join. `serve` loads persisted groups from disk and does not need the original
invite file.

On containerized agents, run joins through the installed wrapper:

```sh
/data/.entmoot/entmoot join <invite>
```

The wrapper supplies `/data/.entmoot` state and `/data/.pilot/pilot.sock` so
the join does not accidentally use another namespace's Pilot socket.

Useful flags:

```sh
-hide-ip
-pilot-wait-timeout 45s
-trace-reconcile
-trace-gossip-transport
```

On success, `join` emits a readiness event before blocking. The event includes
`health` and `next_command` so operators can immediately run a route check:

```json
{"event":"joined","group_ids":["<GROUP_ID>"],"members":3,"health":{"local_member":true,"peers":2,"missing_trust":0,"onboarding_handshake_candidates":1,"route_probe":"not_run"},"next_command":"entmootd ... doctor -group <GROUP_ID> --probe"}
```

After joining, the node sends a bounded set of Pilot onboarding handshakes to
current roster/bootstrap/founder candidates. Existing peers running a current
Pilot daemon can auto-approve pending handshakes from current roster members
whose Pilot identity matches the roster.

Use a service manager for production.
