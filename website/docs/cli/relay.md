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
