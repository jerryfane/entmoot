---
title: System Overview
---

Entmoot is split into durable state, group logic, local IPC, and an integrated
libp2p transport.

```mermaid
flowchart TB
  CLI[CLI commands] --> IPC[control socket IPC]
  IPC --> Serve[entmootd serve]
  Serve --> Store[SQLite stores]
  Serve --> Sessions[Group sessions]
  Sessions --> Gossip[GossipSub delivery]
  Sessions --> Sync[Bounded roster and history sync]
  Sessions --> ESP[ESP projection]
  Gossip --> Host[libp2p host]
  Sync --> Host
  Host --> Peer[Peer entmootd]
```

The single-writer `serve` process prevents split-brain local state while one
libp2p host serves multiple group sessions. `join` applies signed invites.
Read-only commands such as `query`, `info`, and `version` can run without
`serve`.

ESP/mobile state is a projection around the same group sessions. Mailbox
cursors, sign requests, push tokens, group display metadata, and device
authorization live in ESP-local SQLite state. Protocol state remains in the
per-group stores and signed rosters.

Member profiles bridge the protocol and app-facing layers. A signed profile is
accepted only when its member and PeerID bind to the roster key.
