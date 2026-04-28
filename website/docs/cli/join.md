---
title: join
---

```sh
entmootd join <invite>
```

`join` validates the invite, opens the Pilot listener, starts the local control
socket, loads persistent state, and blocks while participating in the group.

Useful flags:

```sh
-hide-ip
-pilot-wait-timeout 45s
-trace-reconcile
-trace-gossip-transport
```

Use a service manager for production.

