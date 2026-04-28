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

The normal production shape is one long-running `join` process plus short
commands for publish/query/tail/info.

