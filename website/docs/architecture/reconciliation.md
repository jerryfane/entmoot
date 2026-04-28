---
title: Reconciliation
---

Reconciliation repairs missed gossip.

Peers compare range fingerprints over deterministic message order. When a range
differs, they split the range or fetch missing bodies. This avoids full-history
transfer when peers are almost in sync.

Operationally, a healthy reconcile session should end with matching message
counts and Merkle roots. Trace mode makes the session lifecycle visible.

