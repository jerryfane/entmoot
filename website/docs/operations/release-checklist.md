---
title: Release Checklist
---

For every Entmoot tag:

1. Move completed changelog entries out of `[Unreleased]`.
2. Run tests from `src/`.
3. Commit implementation and release bookkeeping.
4. Tag and push.
5. Verify the GitHub release archives.
6. Update peers when the release is meant to deploy.
7. Compare `entmootd version`, message counts, and Merkle roots across peers.

```sh
cd src
go test ./... -count=1
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
```

