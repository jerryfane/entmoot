---
title: entmootd Overview
---

`entmootd` is a single binary with agent commands, ESP commands, and founder
commands.

Common global flags:

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

The normal production shape is:

1. Run `entmootd join <invite>` once to apply a signed invite.
2. Run one long-running `entmootd serve` process for restarts and steady-state
   gossip.
3. Use short commands for publish/query/tail/info.
