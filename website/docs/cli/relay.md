---
title: relay serve
---

`relay serve` runs a dedicated, bounded Circuit Relay v2 host. Its identity must
be separate from every Entmoot application identity.

```sh
entmootd relay serve \
  -identity ~/.entmoot/relay-identity.json \
  -allow-new-identity \
  -listen /ip4/0.0.0.0/tcp/4001 \
  -announce /ip4/203.0.113.10/tcp/4001 \
  -allow-peer <APPLICATION_PEER_ID> \
  -allow-peer <SECOND_APPLICATION_PEER_ID>
```

`-allow-new-identity` is needed only for the first launch. Later starts fail if
the configured identity is missing rather than silently replacing the relay
PeerID.

At least one `-allow-peer` is required. Only allowlisted peers may reserve a
slot or use a circuit, and both circuit endpoints must be allowlisted.

Default resource policy:

| Flag | Default |
|---|---:|
| `-reservation-ttl` | `1h` |
| `-circuit-duration` | `15m` |
| `-circuit-bytes` | `67108864` per direction |
| `-max-reservations` | `128` |
| `-max-circuits-per-peer` | `16` |
| `-max-reservations-per-ip` | `8` |
| `-max-reservations-per-asn` | `32` |

On readiness, the process emits one JSON object containing `event=relay_ready`,
the relay PeerID, full advertised multiaddrs, allowlist size, and active limits.
Use one of those addresses as each client's `-controlled-relay` value.

## Relaying from the daemon instead

`serve` can run the same service on its own host, so one process both talks to
a group and relays for its members:

```sh
entmootd -identity ~/.entmoot/identity.json -data ~/.entmoot \
  -relay-service -relay-allow-peer <PEER_ID> serve
```

`-relay-service` requires at least one `-relay-allow-peer` and refuses to start
without one: the point is to relay for your own peers, not for strangers. The
resource limits above are the same, and are not separately configurable here.

Choose the dedicated process when the relay and the application identity should
not be linked. A relay must publish an address, so a daemon that relays
publishes one too, under its **member** identity: everyone who reserves a slot
learns that this member lives at this address. That is already true of any
publicly reachable daemon, because the addresses it announces go into the
invites it mints - so on a public host the flag costs nothing, while on a peer
whose address you want kept private it defeats the purpose. `-relay-service` is
refused outright in the `relay-only` profile for that reason.

The other difference is failure isolation: a relay on the daemon's host shares
its process, so exhausting one takes the other down with it, and the two cannot
be restarted independently.
