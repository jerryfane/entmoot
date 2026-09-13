---
title: Direct and Relay-Only Connectivity
---

Direct mode is the default. The daemon listens on its configured TCP port and
shares verified libp2p addresses with authorized group peers. It is appropriate
for publicly reachable servers and networks that permit direct connections.

Relay-only mode provides endpoint shielding from group peers:

- Configure `-connectivity relay-only`.
- Supply one or more owner-controlled Circuit Relay v2 multiaddrs with
  `-controlled-relay`.
- The node opens no direct application listener and advertises only controlled
  circuit addresses.
- Direct application-peer dialing, mDNS, hole punching, public discovery, and
  direct fallback remain disabled.
- If every controlled relay is unavailable, connectivity fails closed.

The relay operator can observe each client's network address. Relay-only mode
is endpoint shielding from group peers, not anonymity from the relay operator.
Entmoot does not use TURN.
