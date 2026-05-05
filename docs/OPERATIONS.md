# Operations

## Pilot and Entmoot Restart Order

Restart Pilot first, wait until its local IPC is usable, then restart
Entmoot. Entmoot `serve` also waits for Pilot readiness by default, but the
explicit wait helper keeps manual upgrades easier to inspect.

```sh
pkill -f pilot-daemon
# restart pilot-daemon with the existing service manager and flags

scripts/wait-pilot-ready.sh --timeout 45

pkill entmootd
# restart entmootd with the existing service manager; it should run entmootd serve
```

On macOS LaunchAgents, use the same ordering:

```sh
launchctl kickstart -k gui/$(id -u)/org.pilot.daemon
scripts/wait-pilot-ready.sh --timeout 45
launchctl kickstart -k gui/$(id -u)/org.entmoot.entmootd
```

## Entmoot Pilot Wait Flags

`entmootd join` and `entmootd serve` wait for the configured Pilot socket,
`Info`, and `Listen` to succeed before continuing startup.

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
entmootd doctor --json
entmootd doctor -group <GROUP_ID> --probe
entmootd peers -group <GROUP_ID> --probe
```

The helper prints a local Pilot/Entmoot snapshot: peer auth, Pilot info,
Entmoot message count, and recent transport/reconcile log lines. It does not
perform SSH or modify state.

`doctor` is the preferred first-line check for live routing. It reports local
membership, Pilot trust/pending state, member-profile hostname visibility,
transport-ad freshness, active Entmoot stream probes, diagnoses, and suggested
next commands. Use it before restarting services unless the failure is clearly
below Entmoot.

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
6. Update each peer from the released tag with `entmootd update --restart`
   where possible; use the installer or source checkout only as fallback.
   Restart in the standard order: Pilot first only when needed, then Entmoot. Current
   Entmoot invite/open-invite/onboarding flows require the matching Pilot fork
   capabilities for tracked send acknowledgements, node lookup/challenge
   signing, and pending-handshake notifications.
7. Verify every peer reports:

   ```sh
   entmootd version
   entmootd info
   entmootd doctor -group <GROUP_ID> --probe
   entmootd query --limit 1000 | wc -l
   ```

   Versions, message counts, and Merkle roots should match across laptop,
   VPS, and phobos before considering the release complete.
