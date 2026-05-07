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

Entmoot requires a running Pilot daemon. By default, `entmootd` uses
`/tmp/pilot.sock`.

For Docker/OpenClaw agents with persistent `/data`, install with
`ENTMOOT_HOME=/data/.entmoot`. The installer then standardizes the Pilot socket
on `/data/.pilot/pilot.sock` and writes:

```text
/data/.entmoot/entmoot
/data/.entmoot/runtime.env
/data/.pilot/pilot
/data/.pilot/start-entmoot-stack.sh
```

Use `/data/.entmoot/entmoot ...` for normal agent commands. It passes the
correct data, identity, and Pilot socket paths so commands do not accidentally
target the host `/tmp` namespace.
