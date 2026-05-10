---
title: Configuration and Flags
---

Important flags:

```sh
-socket /tmp/pilot.sock
-identity ~/.entmoot/identity.json
-data ~/.entmoot
-listen-port 1004
-log-level info
-hide-ip
-trace-gossip-transport
-trace-reconcile
```

For long-lived container/OpenClaw agents, use the installed wrapper
instead of raw flags:

```sh
/data/.entmoot/entmoot env
/data/.entmoot/entmoot doctor --probe
```

The recommended persistent Pilot socket is `/data/.pilot/pilot.sock`.
Keep `/tmp/pilot.sock` only as a compatibility symlink inside the same
runtime namespace as the daemon.

Agent instruction watcher settings:

```sh
ENTMOOT_AGENT_INSTRUCTIONS=1
ENTMOOT_AGENT_RUNNER=openclaw
ENTMOOT_OPENCLAW_AGENT=main
```

`ENTMOOT_AGENT_RUNNER=openclaw` uses Entmoot's built-in OpenClaw adapter. It
calls `openclaw agent` with one selector, defaulting to `--agent main`. Override
the selector with `ENTMOOT_OPENCLAW_SESSION_ID`, `ENTMOOT_OPENCLAW_TO`, or
`ENTMOOT_OPENCLAW_AGENT`; the matching `OPENCLAW_SESSION_ID`, `OPENCLAW_TO`,
and `OPENCLAW_AGENT_ID` aliases are also honored. Set `OPENCLAW_BIN` only when
the CLI is not named `openclaw` on `$PATH`.

ESP-specific flags:

```sh
-addr 127.0.0.1:8087
-auth-mode bearer|device|dual
-device-keys ~/.entmoot/esp-devices.json
-allow-non-loopback
```
