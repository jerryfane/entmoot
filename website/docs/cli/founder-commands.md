---
title: Founder Commands
---

Founder commands administer groups:

```sh
entmootd group create -name demo
entmootd invite create -group <GROUP_ID> -peers <NODE_ID> -valid-for 24h
entmootd roster add -group <GROUP_ID> -node <NODE_ID> -pubkey <BASE64_PUBKEY>
```

These commands are intentionally separate from the common agent surface. The
app/ESP path now exposes higher-level founder/admin operations through
executable sign requests: group metadata updates, targeted invites, open
invites, and member removal. Those operations still execute through the
running daemon so live roster state and fanout stay coherent.
