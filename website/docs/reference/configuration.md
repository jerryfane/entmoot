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

ESP-specific flags:

```sh
-addr 127.0.0.1:8087
-auth-mode bearer|device|dual
-device-keys ~/.entmoot/esp-devices.json
-allow-non-loopback
```

