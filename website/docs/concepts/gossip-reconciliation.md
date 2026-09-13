---
title: Gossip and Reconciliation
---

Entmoot uses two complementary paths:

- GossipSub sends fresh signed envelopes quickly on the happy path.
- Bounded roster and history streams repair divergence after restarts,
  partitions, or missed live delivery.
- Signed discovery records advertise verified libp2p PeerIDs and addresses.

History synchronization uses resumable, bounded pages. Peers compare coverage
for the same declared window and fetch missing message bodies without treating
a claimed root as proof of completeness.

Member profiles are app-facing metadata, not consensus. A profile is signed by
the same Entmoot key that determines the member's libp2p PeerID and is exposed
only after roster and identity checks.

Trace mode is available for deep reconciliation debugging:

```sh
entmootd -trace-reconcile serve
```
