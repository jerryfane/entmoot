---
title: Rendezvous Tradeoffs
---

Rendezvous is a centralized availability helper below Entmoot.

It helps strict hide-IP nodes discover fresh TURN endpoints when the normal
Entmoot data path is not yet healthy enough to deliver transport ads.

It does not:

- Relay traffic.
- Carry Entmoot messages.
- Decide routing precedence.
- Forge endpoint records.

It can:

- Withhold records.
- Delay records.
- Rate-limit publishes.
- Observe node id liveness and TURN endpoint metadata.

The long-term direction is multi-backend discovery using the same signed record
format.

