---
title: Direct and Relay-Only Connectivity
---

Direct mode is the default. The daemon listens on its configured TCP port and
shares verified libp2p addresses with authorized group peers. It is appropriate
for publicly reachable servers and networks that permit direct connections.

Two peers that both sit behind NAT cannot dial each other: neither accepts
unsolicited inbound connections. Direct mode therefore speaks DCUtR, the libp2p
hole-punching protocol, on every peer. A peer that is not publicly reachable
also needs a rendezvous point, so direct mode accepts `-controlled-relay`:

- The peer reserves a slot on each configured relay and publishes the resulting
  circuit address, which libp2p advertises once AutoNAT reports private
  reachability.
- A remote member reaches it through that circuit, and DCUtR then upgrades the
  relayed connection to a direct one. Relayed bandwidth is used only for the
  upgrade, not for the whole session.
- Hole punching fails against symmetric NAT and carrier-grade NAT. Those peers
  keep working over the relay, so relay budgets still matter.
- DCUtR registers once the host observes a public address, so a connection
  opened in the first moments after start may stay relayed until it is
  re-established.
- Without `-controlled-relay`, direct mode still answers hole punches for other
  peers; it just has no rendezvous point of its own.

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
