---
title: join
---

```sh
entmootd join <invite> [invite...]
```

`join` validates each invite, opens one shared Pilot listener, starts the local
control socket, loads persistent state, and blocks while participating in every
joined group session.

Useful flags:

```sh
-hide-ip
-pilot-wait-timeout 45s
-trace-reconcile
-trace-gossip-transport
```

Use a service manager for production.
