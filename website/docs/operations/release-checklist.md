---
title: Release Checklist
---

For every Entmoot tag:

1. Move completed changelog entries out of `[Unreleased]`.
2. Run tests from `src/`.
3. Commit implementation and release bookkeeping.
4. Tag and push.
5. Verify the GitHub release archives.
6. Update Pilot too when Entmoot depends on new Pilot capabilities.
7. Update peers when the release is meant to deploy. Use service-scoped
   restarts only; do not broad-kill `entmootd`, because ESP runs as
   `entmootd esp serve`.
8. For the public ESP host, run `scripts/verify-esp-service.sh` and require
   public `/healthz` to return `200`.
9. Compare `entmootd version`, `entmootd doctor --probe`, message counts, and
   Merkle roots across peers.

```sh
cd src
go test ./... -count=1
cd ..
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
scripts/verify-esp-service.sh --public-url https://esp.entmoot.xyz
```
