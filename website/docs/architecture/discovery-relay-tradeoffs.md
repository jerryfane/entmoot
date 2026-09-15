---
title: Discovery and Relay Tradeoffs
---

Entmoot uses signed, membership-bound peer hints rather than a public DHT or
rendezvous registry. There is no LAN discovery in either mode: addresses reach
a node only through invites, gossiped signed peer records and configured
relays.

Direct mode gives the simplest path and lowest relay cost, but authorized peers
can observe direct addresses and NAT or firewall policy may prevent a
connection.

Relay-only mode uses configured, owner-controlled Circuit Relay v2 peers. It
shields application-peer addresses and fails closed without a relay, but the
relay operator observes client addresses and controls relay availability and
resource budgets.

Discovery hints are not authority. Every accepted PeerID must derive from a
current member's Entmoot key, and stale hints expire.
