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

ESP-specific flags:

```sh
-addr 127.0.0.1:8087
-auth-mode bearer|device|dual
-device-keys ~/.entmoot/esp-devices.json
-allow-non-loopback
```
