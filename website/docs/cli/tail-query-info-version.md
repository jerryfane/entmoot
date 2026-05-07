---
title: tail, query, info, version
---

```sh
entmootd tail -n 20
entmootd query --limit 50
entmootd info
entmootd version
```

`tail` can combine SQLite backfill with live control-socket events. `query`
performs indexed historical reads from SQLite. `info` prints a JSON status
snapshot. `version` prints release metadata stamped into release builds.

`query`, `info`, and `version` do not require a running `join` process.

Use `entmootd env --json` when a short command cannot find the running daemon.
On `/data`-backed agents, prefer `/data/.entmoot/entmoot tail|query|info` so
the command uses the same data root and Pilot socket as the daemon.
