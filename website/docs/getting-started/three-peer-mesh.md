---
title: Three-Peer Mesh
---

A healthy mesh has:

- One running `entmootd serve` process per host and data root, hosting one or
  more group sessions.
- The same group roster on every peer.
- Verified PeerID-to-roster-key bindings.
- Matching message counts and coverage roots after convergence.

After restarting peers, verify locally:

```sh
scripts/verify-mesh-node.sh
```

For manual checks:

```sh
entmootd version
entmootd info
entmootd doctor -group <GROUP_ID> --probe
entmootd query --limit 1000 | wc -l
```

Compare those outputs across peers. `doctor --probe` should show current roster
membership, transport availability, synchronization health, and probe results
for each non-local peer.
