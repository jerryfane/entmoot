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

