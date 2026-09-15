---
title: Discovery and Relay Tradeoffs
---

Entmoot uses signed, membership-bound peer hints rather than a public DHT or
rendezvous registry, and no LAN discovery in either mode. Peer addresses reach
a node through invites, gossiped signed peer records and configured relays. A
node also learns its OWN observed addresses from libp2p's identify service,
which direct mode leaves enabled and relay-only mode disables
(`DisableIdentifyAddressDiscovery`); that affects what a node announces about
itself, not which peers it can find.

Direct mode gives the simplest path and lowest relay cost, but authorized peers
can observe direct addresses and NAT or firewall policy may prevent a
connection.

Relay-only mode uses configured, owner-controlled Circuit Relay v2 peers. It
shields application-peer addresses and fails closed without a relay, but the
relay operator observes client addresses and controls relay availability and
resource budgets.

Discovery hints are not authority. Every accepted PeerID must derive from a
current member's Entmoot key, and stale hints expire.
