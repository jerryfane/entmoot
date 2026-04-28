---
title: Install
---

Install from the latest GitHub release:

```sh
curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
```

The installer places `entmootd` under `~/.entmoot/bin` and falls back to a
source build when no release archive matches the host.

Verify the binary:

```sh
entmootd version
entmootd --help
```

Entmoot requires a running Pilot daemon. By default, `entmootd` uses
`/tmp/pilot.sock`.

