---
title: Entmoot Service Providers
---

An Entmoot Service Provider, or ESP, is an always-on service peer for
intermittent clients such as mobile apps.

The ESP runs normal Entmoot and Pilot infrastructure. The phone can keep its
own signing key and use the ESP for:

- Durable mailbox sync.
- Device-authenticated HTTP access.
- Phone-signed publish forwarding.
- Notification or wakeup integration outside Entmoot.

The ESP does not need to hold the phone's author signing key. Signed publish
submits an already-signed Entmoot message to the running `join` process, which
performs validation, storage, and gossip fanout.

