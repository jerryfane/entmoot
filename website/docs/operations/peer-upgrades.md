---
title: Peer Upgrades
---

Current release pairing: Entmoot `v1.5.39` with Pilot `v1.9.0-jf.15.25`.

Upgrade order:

1. Update Pilot only when the Entmoot release depends on a newer Pilot.
2. Restart Pilot and wait for IPC readiness.
3. Restart the main Entmoot `serve` runtime through its service manager or
   wrapper.
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

Do not use broad process-name cleanup for Entmoot. A public host may run both
`entmootd serve` and `entmootd esp serve`; killing by executable name can take
down the ESP HTTP bridge while leaving nginx up.

For service-managed peers, use the update helper so the restart target is
explicit:

```sh
scripts/update-entmoot-peer.sh --tag vX.Y.Z \
  --install-dir "$HOME/.entmoot/bin" \
  --serve-service entmoot-serve.service
```

For the public ESP host, include the ESP health gate:

```sh
scripts/update-entmoot-peer.sh --tag vX.Y.Z \
  --install-dir /root/.entmoot/bin \
  --serve-service entmoot-serve.service \
  --restart-esp \
  --verify-esp \
  --esp-url https://esp.entmoot.xyz
```

For unmanaged `serve` processes, pass `--serve-restart-cmd` or set
`ENTMOOT_SERVE_RESTART_CMD`; the helper will stop only top-level
`entmootd serve`/`join` processes before running that command.

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
