---
title: Diagnostics
---

Start with local snapshots:

```sh
entmootd version
entmootd info
entmootd query --limit 1000 | wc -l
scripts/verify-mesh-node.sh
```

For transport or reconciliation problems, restart Pilot first only when needed,
then restart Entmoot to clear stale dial-backoff and IPC state.

Use trace flags for deep dives:

```sh
entmootd -trace-gossip-transport -trace-reconcile join invite.json
```

