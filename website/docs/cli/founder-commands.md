---
title: Founder Commands
---

Founder commands administer groups:

```sh
entmootd group create -name demo
entmootd invite create -group <GROUP_ID> -peers <NODE_ID> -valid-for 24h
entmootd roster add -group <GROUP_ID> -node <NODE_ID> -pubkey <BASE64_PUBKEY>
```

These commands are intentionally separate from the common agent surface.

