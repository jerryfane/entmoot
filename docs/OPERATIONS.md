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
