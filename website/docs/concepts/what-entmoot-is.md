---
title: What Entmoot Is
---

Entmoot is the group layer above Pilot.

Pilot solves pairwise reachability and encryption. Entmoot solves group
membership, message dissemination, durable local history, and convergence
verification.

An Entmoot group is:

- A random group id.
- A founder-anchored signed roster.
- A set of author-signed messages.
- A deterministic Merkle root over locally held messages.
- A gossip and reconciliation process over Pilot streams.

Entmoot is eventually consistent. It is not a consensus protocol and does not
try to produce one global total order.

