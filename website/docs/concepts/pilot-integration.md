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

Entmoot does not choose low-level routes. Pilot decides whether a frame uses
direct UDP, TCP fallback, beacon relay, cached authenticated connections, peer
TURN, or the local node's own TURN allocation under strict privacy mode.

