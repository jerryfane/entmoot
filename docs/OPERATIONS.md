# Operations

## Pilot and Entmoot Restart Order

Restart Pilot first, wait until its local IPC is usable, then restart
Entmoot. Entmoot `join` also waits for Pilot readiness by default, but the
explicit wait helper keeps manual upgrades easier to inspect.

```sh
pkill -f pilot-daemon
# restart pilot-daemon with the existing service manager and flags

scripts/wait-pilot-ready.sh --timeout 45

pkill entmootd
# restart entmootd with the existing join command and invite
```

On macOS LaunchAgents, use the same ordering:

```sh
launchctl kickstart -k gui/$(id -u)/org.pilot.daemon
scripts/wait-pilot-ready.sh --timeout 45
launchctl kickstart -k gui/$(id -u)/org.entmoot.entmootd
```

## Entmoot Pilot Wait Flags

`entmootd join` waits for the configured Pilot socket, `Info`, and `Listen`
to succeed before continuing startup.

- `-pilot-wait-timeout=45s`: maximum readiness wait. Set `0s` to restore the
  old single-attempt behavior.
- `-pilot-wait-base-delay=250ms`: initial retry delay.
- `-pilot-wait-max-delay=3s`: maximum retry delay.

Retry delays use bounded jitter so several peers restarting together do not
hammer the local Pilot IPC socket in lockstep.

## Local Verification

After both daemons are up, run:

```sh
scripts/verify-mesh-node.sh
```

The helper prints a local Pilot/Entmoot snapshot: peer auth, Pilot info,
Entmoot message count, and recent transport/reconcile log lines. It does not
perform SSH or modify state.

## Release Checklist

Use this checklist for every Entmoot tag so the GitHub release, deployed
peers, and changelog stay aligned.

1. Move completed `CHANGELOG.md` entries out of `[Unreleased]` into a dated
   release section.
2. Run the local test suite:

   ```sh
   cd src
   go test ./... -count=1
   ```

3. Commit the implementation and release-bookkeeping changes.
4. Tag and push:

   ```sh
   git tag vX.Y.Z
   git push origin main
   git push origin vX.Y.Z
   ```

5. Verify the tag-triggered GitHub release succeeds and the expected
   darwin/linux amd64/arm64 archives are uploaded.
6. Update each peer from the released tag or source checkout, then restart in
   the standard order: Pilot first only when needed, then Entmoot.
7. Verify every peer reports:

   ```sh
   entmootd info
   entmootd query --limit 1000 | wc -l
   ```

   Message counts and Merkle roots should match across laptop, VPS, and
   phobos before considering the release complete.
