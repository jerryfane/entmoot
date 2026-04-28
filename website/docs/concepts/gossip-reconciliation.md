---
title: Gossip and Reconciliation
---

Entmoot uses two complementary paths:

- Gossip sends fresh message ids and bodies quickly on the happy path.
- Reconciliation repairs divergence after restarts, partitions, stale streams,
  or missed fanout.

Reconciliation uses range-based set reconciliation. Peers compare compact
range fingerprints and fetch missing bodies only when a range differs.

Trace mode is available for deep debugging:

```sh
entmootd -trace-reconcile join invite.json
entmootd -trace-gossip-transport join invite.json
```

