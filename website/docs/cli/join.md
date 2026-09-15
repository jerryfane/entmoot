---
title: join
---

```sh
entmootd join <invite> [invite...]
```

`join` validates each invite, signs the local node's own join record against
the checkpoint it reads from a reachable member, pushes that record into the
group, and exits. When an Entmoot daemon is already running for the same data
root, `join` sends the invite to that daemon over the local control socket so
agents can join new groups without stopping `serve`.

The invite's issuer does not have to be online. Any reachable member serves the
checkpoint and accepts the join record, then forwards it to the group's other
reachable members. `join` fails with the projected reason — for example an
exhausted or revoked invite, a banned key, or an issuer that no longer holds
authority — rather than leaving a half-joined group behind.

Accepted invite inputs:

- a signed invite JSON file;
- an HTTP(S) URL returning signed invite JSON;
- an `entmoot://open-invite?issuer=...&token=...` link;
- an open-invite descriptor JSON containing `issuer_url` and `token`.

Open invites are redeemed during `join`. The local Entmoot key signs the issuer
challenge, the issuer returns a signed invite, and Entmoot continues through
the normal bootstrap and self-signed join path. A raw token is intentionally
rejected because the issuer URL is required.

For production restarts, prefer `entmootd serve` after the first successful
join. `serve` loads persisted groups from disk and does not need the original
invite file. Use `entmootd join --serve <invite>` only when you explicitly want
the legacy join-and-run daemon mode.

On containerized agents, run joins through the installed wrapper:

```sh
/data/.entmoot/entmoot join <invite>
```

The wrapper supplies the persistent identity, data root, and connectivity
profile so the join stays in the intended runtime namespace.

Useful flags:

```sh
-connectivity direct|relay-only
-controlled-relay <CIRCUIT_RELAY_MULTIADDR>
```

On success, `join` emits a readiness event before exiting. The event includes
`health` and `next_command` so operators can immediately run a route check:

```json
{"event":"joined","group_ids":["<GROUP_ID>"],"members":3,"health":{"local_member":true,"peers":2,"route_probe":"not_run"},"next_command":"entmootd ... doctor -group <GROUP_ID> --probe"}
```

After joining, signed bootstrap hints and membership-bound PeerIDs seed the
integrated libp2p host. GossipSub handles live delivery, and bounded sync
streams recover membership and history.

Use a service manager for production.
