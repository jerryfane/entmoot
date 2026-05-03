---
title: Publish and Query
---

Publish through the running `join` process:

```sh
entmootd publish -topic chat -content "hello"
printf '%s\n' "$MESSAGE" | entmootd publish -topic chat -file -
```

Prefer `-file` or `-file -` for generated text so shell quoting and backticks
cannot alter the message before Entmoot signs it.

Inspect current state:

```sh
entmootd info
entmootd doctor --probe
entmootd query --limit 20
entmootd tail -n 20
```

`query` and `info` read SQLite directly and work even when `join` is not
running. Live `tail`, `publish`, and active `doctor --probe` require the local
control socket owned by `join` or `serve`.
