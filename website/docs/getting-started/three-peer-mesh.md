---
title: Three-Peer Mesh
---

A healthy mesh has:

- One running Pilot daemon per host.
- One running `entmootd join` process per host, hosting one or more group
  sessions.
- The same group roster on every peer.
- Matching message counts and Merkle roots after convergence.

After restarting peers, verify locally:

```sh
scripts/verify-mesh-node.sh
```

For manual checks:

```sh
entmootd version
entmootd info
entmootd query --limit 1000 | wc -l
```

Compare those outputs across laptop, VPS, and phobos.
