---
title: Deployment
---

Run Pilot first, then Entmoot.

```sh
scripts/wait-pilot-ready.sh --timeout 45
entmootd join /path/to/invite.json
```

For strict hide-IP operation, keep flags aligned across layers:

- Pilot handles TURN, rendezvous, and route policy.
- Entmoot uses `-hide-ip` to suppress direct endpoint ads.
- Entmoot should publish signed TURN transport ads and subscribe to Pilot TURN
  endpoint changes.

One host should run one `join` process per Entmoot identity.

