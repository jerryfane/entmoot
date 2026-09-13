---
title: Install
---

Install from the latest GitHub release:

```sh
curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
```

The installer places `entmootd` under `~/.entmoot/bin` and falls back to a
source build when no release archive matches the host. It also installs an
`entmoot` wrapper beside the data root and a `runtime.env` file with the
runtime paths used by that install.

Verify the binary:

```sh
entmootd version
entmootd --help
```

Update an existing install:

```sh
entmootd update --check
entmootd update --restart
```

Entmoot contains its libp2p transport. It does not require a separate network
daemon or socket.

For Docker/OpenClaw agents with persistent `/data`, install with
`ENTMOOT_HOME=/data/.entmoot`. The installer writes:

```text
/data/.entmoot/entmoot
/data/.entmoot/runtime.env
/data/.entmoot/bin/entmootd
```

Use `/data/.entmoot/entmoot ...` for normal agent commands. It passes the
correct data, identity, and connectivity settings so commands do not target a
different runtime namespace.
