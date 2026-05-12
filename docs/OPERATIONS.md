# Operations

## Pilot and Entmoot Restart Order

Restart Pilot first, wait until its local IPC is usable, then restart
Entmoot. Entmoot `serve` also waits for Pilot readiness by default, but the
explicit wait helper keeps manual upgrades easier to inspect.

```sh
# restart pilot-daemon with the existing service manager and flags

scripts/wait-pilot-ready.sh --timeout 45

# restart only the main entmootd serve service or wrapper for this peer
```

Do not use broad process-name cleanup such as `pkill entmootd` on hosts that
also run ESP. The main mesh daemon runs `entmootd serve`, while the HTTP bridge
runs `entmootd esp serve`; treat them as separate service boundaries.

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
entmootd env --json
entmootd doctor --json
entmootd doctor -group <GROUP_ID> --probe
entmootd peers -group <GROUP_ID> --probe
```

The helper prints a local Pilot/Entmoot snapshot: peer auth, Pilot info,
Entmoot message count, and recent transport/reconcile log lines. It does not
perform SSH or modify state.

`env` is the first check for container/OpenClaw agents because it confirms the
actual data root and socket namespace. `doctor` is the preferred first-line
check for live routing. It reports local membership, Pilot trust/pending state,
member-profile hostname visibility, transport-ad freshness, active Entmoot
stream probes, diagnoses, and suggested next commands. Use these before
restarting services unless the failure is clearly below Entmoot.

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
   On service-managed peers, prefer:

   ```sh
   scripts/update-entmoot-peer.sh --tag vX.Y.Z \
     --install-dir "$HOME/.entmoot/bin" \
     --serve-service entmoot-serve.service
   ```

   On the VPS, where ESP is also reverse-proxied publicly, include the ESP
   restart and health gate:

   ```sh
   scripts/update-entmoot-peer.sh --tag vX.Y.Z \
     --install-dir /root/.entmoot/bin \
     --serve-service entmoot-serve.service \
     --restart-esp \
     --verify-esp \
     --esp-url https://esp.entmoot.xyz
   ```

   If the main `serve` process is not managed by systemd, set
   `ENTMOOT_SERVE_RESTART_CMD` explicitly. In that mode the helper uses
   `entmootd update --restart` to stop only top-level `serve`/`join`
   processes, excluding `esp serve`, then runs the provided start command.
   For OpenClaw-backed agent instruction peers, prefer the built-in adapter
   instead of a shell runner:

   ```sh
   ENTMOOT_AGENT_INSTRUCTIONS=1
   ENTMOOT_AGENT_RUNNER=openclaw
   ENTMOOT_OPENCLAW_AGENT=main
   ```

   Use `ENTMOOT_OPENCLAW_SESSION_ID` or `ENTMOOT_OPENCLAW_TO` only when the
   peer must target a specific OpenClaw session or recipient. Existing
   `OPENCLAW_SESSION_ID`, `OPENCLAW_TO`, and `OPENCLAW_AGENT_ID` settings are
   honored as aliases, with Entmoot-prefixed settings taking precedence.
   For external actions, send structured `actions` requirements in the
   command args. Required actions are treated as successful only when the
   OpenClaw result includes delivery or tool evidence.

   For non-OpenClaw agents, configure a custom runner instead. `bootstrap
   agent` prints the required commands and applies live-agent config when
   requested:

   ```sh
   entmootd bootstrap agent \
     --runner custom \
     --runner-command /path/to/agent-runner \
     --agent-instructions \
     --live-mode operator \
     --group <GROUP_ID> \
     --node <PILOT_NODE_ID> \
     --topic fleet/tasks \
     --action task.assign_self \
     --action task.update_own \
     --action task.comment
   ```

   The custom command runner receives instruction JSON on stdin. The live
   runner receives live context JSON on stdin and must return
   `{"actions":[...]}`. `agent-live run` treats per-scan runner timeouts,
   runner failures, invalid runner JSON, and retryable action transport errors
   as degraded scan results with capped backoff, so one slow agent turn does not
   stop live presence renewal. `bootstrap agent` does not install runtimes or
   manage supervisors; keep `serve`, `agent-commands watch`, and
   `agent-live run` under the existing container or service manager for host
   restarts, crashes, upgrades, and fatal config or storage errors.

   Live-agent config is scoped by `group_id + node_id` and is stored in the
   current data root's `esp.sqlite`. The default per-moot live limits are
   unlimited: `-max-actions 0` and `-max-action-bytes 0`. Add explicit caps for
   busy groups:

   ```sh
   entmootd agent-live enable \
     -group <GROUP_ID> \
     -node <PILOT_NODE_ID> \
     -mode operator \
     -topic fleet/tasks \
     -action task.assign_self \
     -action task.update_own \
     -action task.comment \
     -max-actions 3 \
     -max-action-bytes 4096
   ```

   Inspect from the same namespace and data root as the agent:

   ```sh
   entmootd agent-commands status
   entmootd agent-live status -group <GROUP_ID> --json
   entmootd fleet list
   entmootd fleet commands catalog
   ENTMOOT_ESP_URL=<ESP_URL> entmootd fleet tasks list -fleet <FLEET_ID>
   ```

   If Hermes or another agent runs inside its own container, VPS-local status
   commands can legitimately show no live config unless they read the same
   `esp.sqlite`.

7. Verify every peer reports:

   ```sh
   entmootd version
   entmootd info
   entmootd doctor -group <GROUP_ID> --probe
   entmootd query --limit 1000 | wc -l
   ```

   Versions, message counts, and Merkle roots should match across laptop,
   VPS, and phobos before considering the release complete.

8. For the public ESP host, verify the bridge after every deploy:

   ```sh
   scripts/verify-esp-service.sh --public-url https://esp.entmoot.xyz
   ```

   A healthy deploy returns `200` for `/healthz` and the expected
   unauthenticated `401` for `/v1/session`. A `502` means nginx is reachable
   but the local ESP backend is not.
