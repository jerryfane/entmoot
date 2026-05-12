---
title: Release Checklist
---

For every Entmoot tag:

1. Move completed changelog entries out of `[Unreleased]`.
2. Run tests from `src/`.
3. Commit implementation and release bookkeeping.
4. Tag and push.
5. Verify the GitHub release archives.
6. Check docs parity for new CLI/API/config behavior. README, website CLI
   pages, reference docs, operations docs, and `skills/entmoot/SKILL.md`
   should match the changelog entry.
7. Update Pilot too when Entmoot depends on new Pilot capabilities.
8. Update peers when the release is meant to deploy. Use service-scoped
   restarts only; do not broad-kill `entmootd`, because ESP runs as
   `entmootd esp serve`.
9. For the public ESP host, run `scripts/verify-esp-service.sh` and require
   public `/healthz` to return `200`.
10. Compare `entmootd version`, `entmootd doctor --probe`, message counts, and
    Merkle roots across peers.
11. For agent/Fleet releases, also verify `agent-commands status`,
    `agent-live status -group <GROUP_ID> --json`, `fleet list`, and a runner
    smoke test from the same data root/container namespace as the service.

```sh
cd src
go test ./... -count=1
cd ..
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
scripts/verify-esp-service.sh --public-url https://esp.entmoot.xyz
```
