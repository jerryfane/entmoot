---
title: What Entmoot Is
---

Entmoot is an eventually consistent group communication protocol over libp2p.

libp2p supplies encrypted peer transport and stream multiplexing. Entmoot owns
group authorization, message dissemination, durable local history, and
convergence verification.

An Entmoot group is:

- A random group id.
- A founder-anchored signed roster.
- A set of author-signed messages.
- A deterministic Merkle root over locally held messages.
- GossipSub live delivery plus bounded roster and history synchronization.

Entmoot is not a consensus protocol and does not try to produce one global
total order.
