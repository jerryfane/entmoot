---
title: Peer Upgrades
---

Current release pairing: Entmoot `v1.5.35` with Pilot `v1.9.0-jf.15.24`.

Upgrade order:

1. Update Pilot only when the Entmoot release depends on a newer Pilot.
2. Restart Pilot and wait for IPC readiness.
3. Restart Entmoot.
4. Verify local message count and Merkle root.
5. Verify the Pilot hostname if the release affects ESP/member display data.
6. Run `entmootd doctor -group <GROUP_ID> --probe` before declaring the peer
   healthy.

```sh
scripts/wait-pilot-ready.sh --timeout 45
pilotctl info
scripts/verify-mesh-node.sh
entmootd doctor -group <GROUP_ID> --probe
```

Peer updates are operational state changes. Do them separately from docs-only
releases.

For hostname-aware members, set names on every peer before the final Entmoot
restart:

```sh
pilotctl set-hostname vps
pilotctl set-hostname phobos
pilotctl set-hostname laptop
```

After restart, `GET /v1/groups/{group_id}/members` should eventually expose
the signed hostnames for roster members that are online and reachable.

Current Entmoot releases expect the matching Pilot fork to advertise tracked
stream send acknowledgements, node lookup/challenge signing, and pending
handshake notifications. If open invites or auto-approval fail after an
Entmoot-only upgrade, upgrade Pilot and restart in the order above.
