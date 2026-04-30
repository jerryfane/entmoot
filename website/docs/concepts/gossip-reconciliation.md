---
title: Gossip and Reconciliation
---

Entmoot uses two complementary paths:

- Gossip sends fresh message ids and bodies quickly on the happy path.
- Reconciliation repairs divergence after restarts, partitions, stale streams,
  or missed fanout.
- System gossip also carries signed transport ads and member-profile ads.

Reconciliation uses range-based set reconciliation. Peers compare compact
range fingerprints and fetch missing bodies only when a range differs.

Member-profile gossip is app-facing metadata, not consensus. It carries a
member's current Pilot hostname so peers and ESP clients can display readable
member names. New joiners request a member-profile snapshot during bootstrap so
existing hostnames appear promptly instead of waiting for the normal refresh
cycle.

Trace mode is available for deep debugging:

```sh
entmootd -trace-reconcile join invite.json
entmootd -trace-gossip-transport join invite.json
```
