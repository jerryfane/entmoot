---
title: System Overview
---

Entmoot is split into durable state, group logic, local IPC, and Pilot
transport.

```mermaid
flowchart TB
  CLI[CLI commands] --> IPC[control socket IPC]
  IPC --> Join[entmootd join]
  Join --> Store[SQLite stores]
  Join --> Gossip[Gossip and reconcile]
  Gossip --> Pilot[Pilot IPC client]
  Pilot --> Daemon[pilot-daemon]
```

The single-writer `join` process prevents split-brain local state. Read-only
commands such as `query`, `info`, and `version` can run without `join`.

