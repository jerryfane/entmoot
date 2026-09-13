---
title: Deployment
---

Join once, then supervise the integrated Entmoot daemon:

```sh
entmootd join /path/to/invite.json
entmootd -connectivity direct -listen-port 1004 serve
```

Use `entmootd join` only for the first successful join. Service managers should
run `entmootd serve` so restarts depend on persisted local state, not on an
invite file that can disappear or expire.

Direct mode is the default and is appropriate for a publicly reachable host.
For endpoint shielding from group peers, configure one or more controlled
Circuit Relay v2 peers:

```sh
entmootd \
  -connectivity relay-only \
  -controlled-relay '/dns4/relay.example/tcp/4001/p2p/<relay-peer-id>' \
  serve
```

Relay-only mode opens no direct application listener, has no TURN or direct
fallback, and fails closed when all configured relays are unavailable. The
relay operator can still observe client addresses.

One host should run one `serve` process per Entmoot identity and data root.
