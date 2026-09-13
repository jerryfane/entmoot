---
title: Peer Upgrades
---

Entmoot releases contain the complete application and libp2p transport. There
is no separately versioned transport daemon.

Upgrade order:

1. Install the intended Entmoot release.
2. Restart the main `entmootd serve` runtime through its service manager or
   wrapper.
3. Restart a separately supervised ESP only when the release affects ESP.
4. Verify local identity, message count, and history coverage.
5. Run `entmootd doctor -group <GROUP_ID> --probe` before declaring the peer
   healthy.

```sh
entmootd version
scripts/verify-mesh-node.sh
entmootd doctor -group <GROUP_ID> --probe
```

Do not use broad process-name cleanup. A public host may run both `entmootd
serve` and `entmootd esp serve`; killing by executable name can take down the
ESP HTTP bridge while leaving nginx up.

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
`ENTMOOT_SERVE_RESTART_CMD`; the helper stops only the intended top-level
runtime before running that command.

Peer updates are operational state changes. Do them separately from docs-only
releases.
