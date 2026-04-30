---
title: Pilot Integration
---

Entmoot uses Pilot as a lower-layer transport.

The `join` process:

- Waits for Pilot IPC readiness.
- Binds the Entmoot service port, normally `1004`.
- Dials peers through Pilot virtual addresses.
- Accepts inbound Pilot streams for gossip and reconciliation.
- Installs peer endpoints learned from signed transport ads.
- Subscribes to TURN endpoint changes when Pilot supports push events.
- Reads the local Pilot hostname and republishes it as signed Entmoot member
  profile metadata for group/member display.

Entmoot does not choose low-level routes. Pilot decides whether a frame uses
direct UDP, TCP fallback, beacon relay, cached authenticated connections, peer
TURN, or the local node's own TURN allocation under strict privacy mode.

Pilot hostnames are display metadata, not routing metadata. Operators set them
with `pilotctl set-hostname <name>`. Entmoot reads the local value from Pilot,
signs it as a group-scoped member profile, and gossips it to peers. ESP/member
APIs can then show readable node names without adding new registry behavior.
