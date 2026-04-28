---
title: Transport Ads, Hide-IP, TURN, and Rendezvous
---

Transport ads are signed Entmoot records that publish how a group member can be
reached. They are verified against the roster and installed into Pilot through
IPC.

`-hide-ip` suppresses direct UDP/TCP endpoint advertisement from Entmoot
transport ads. It should be paired with Pilot's strict privacy posture when the
operator wants no source-IP leaks.

Pilot's rendezvous service is discovery only:

- It publishes and fetches signed TURN endpoint records.
- It does not relay Entmoot messages.
- It does not choose routes.
- It cannot forge endpoint records without the node's signing key.

If rendezvous is down, established tunnels and healthy non-rendezvous routes
continue. Cold-start recovery between strict hide-IP peers may stall until a
fresh endpoint reaches peers through another channel.

