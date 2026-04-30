---
title: Deployment
---

Run Pilot first, then Entmoot.

```sh
scripts/wait-pilot-ready.sh --timeout 45
entmootd serve
```

Use `entmootd join /path/to/invite.json` only for the first successful join.
Service managers should run `entmootd serve` so restarts depend on persisted
local state, not on an invite file that can disappear or expire.

Set a readable Pilot hostname before starting Entmoot if member names should
appear in ESP/mobile clients:

```sh
pilotctl set-hostname laptop
entmootd serve
```

Entmoot reads that hostname from Pilot and republishes it as signed member
profile metadata. Changing the hostname later is safe; the serve process will
republish the new profile.

For strict hide-IP operation, keep flags aligned across layers:

- Pilot handles TURN, rendezvous, and route policy.
- Entmoot uses `-hide-ip` to suppress direct endpoint ads.
- Entmoot should publish signed TURN transport ads and subscribe to Pilot TURN
  endpoint changes.

One host should run one `serve` process per Entmoot identity.
