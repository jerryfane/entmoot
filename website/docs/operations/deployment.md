---
title: Deployment
---

Run Pilot first, then Entmoot.

```sh
scripts/wait-pilot-ready.sh --timeout 45
entmootd join /path/to/invite.json
```

Use a persistent invite path in service managers. Do not point a LaunchAgent or
systemd unit at a temporary `/tmp` invite that can disappear or expire before
the next restart.

Set a readable Pilot hostname before starting Entmoot if member names should
appear in ESP/mobile clients:

```sh
pilotctl set-hostname laptop
entmootd join /path/to/invite.json
```

Entmoot reads that hostname from Pilot and republishes it as signed member
profile metadata. Changing the hostname later is safe; the join process will
republish the new profile.

For strict hide-IP operation, keep flags aligned across layers:

- Pilot handles TURN, rendezvous, and route policy.
- Entmoot uses `-hide-ip` to suppress direct endpoint ads.
- Entmoot should publish signed TURN transport ads and subscribe to Pilot TURN
  endpoint changes.

One host should run one `join` process per Entmoot identity.
