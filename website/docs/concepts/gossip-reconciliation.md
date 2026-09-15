---
title: Gossip and Reconciliation
---

Entmoot uses two complementary paths:

- GossipSub sends fresh signed envelopes quickly on the happy path.
- Bounded membership and history streams recover state after restarts,
  partitions, or missed live delivery.
- Signed discovery records advertise verified libp2p PeerIDs and addresses.

History synchronization uses resumable, bounded pages. Peers compare coverage
for the same declared window and fetch missing message bodies without treating
a claimed root as proof of completeness.

Membership synchronization is a set difference, not a chain walk and not a
paged snapshot. Each session pulls membership every 15 seconds from up to
eight reachable members, and a locally signed record is pushed immediately
rather than waiting for the next round. There is no fork to detect, no
per-peer backoff table, and no repair step: nodes holding the same records
project the same membership. An unreachable peer is skipped and retried on the
next tick.

A message names the checkpoint its author held. A message that names a
checkpoint this node has not seen yet is held briefly in quarantine and
ingested after the next membership sync, which is the common case for "the
author's join record has not arrived yet".

Member profiles are app-facing metadata, not consensus. A profile is signed by
the same Entmoot key that determines the member's libp2p PeerID and is exposed
only after membership and identity checks.

Trace mode is available for deep reconciliation debugging:

```sh
entmootd -trace-reconcile serve
```
