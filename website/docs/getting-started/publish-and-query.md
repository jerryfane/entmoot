---
title: Publish and Query
---

Publish through the running `join` process:

```sh
entmootd publish -topic chat -content "hello"
```

Inspect current state:

```sh
entmootd info
entmootd query --limit 20
entmootd tail -n 20
```

`query` and `info` read SQLite directly and work even when `join` is not
running. Live `tail` and `publish` require the local control socket owned by
`join`.

