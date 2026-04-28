---
title: Peer Upgrades
---

Upgrade order:

1. Update Pilot only when the Entmoot release depends on a newer Pilot.
2. Restart Pilot and wait for IPC readiness.
3. Restart Entmoot.
4. Verify local message count and Merkle root.

```sh
scripts/wait-pilot-ready.sh --timeout 45
scripts/verify-mesh-node.sh
```

Peer updates are operational state changes. Do them separately from docs-only
releases.

