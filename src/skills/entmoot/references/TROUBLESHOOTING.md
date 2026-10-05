# Diagnostics And Troubleshooting

Use this reference before restarting services unless the failure is clearly
below Entmoot.

## Diagnostics

```sh
"$ENTMOOT" env --json
"$ENTMOOT" info
"$ENTMOOT" doctor -group <gid> --json
"$ENTMOOT" peers -group <gid> --probe --json
```

`doctor` reports daemon state, group membership, each member's peer id derived
from its key, message counts and the group's Merkle root, and suggests joining
when this node is not a member. Add `--probe` and the running daemon dials each
other member and performs one membership read, so every peer row gains a
`probe` object holding `reachable`, `answered`, `refusal`, `latency_ms`,
`relayed` and a one-line `error` when it fails. `answered=true` with
`reachable=false` means the peer
replied and refused this node - an eviction, not an outage.
Without `--probe` nothing in the report describes reachability, and without a
daemon `probe_status` says so rather than blaming the peers.

## Common Exit Codes

| Code | Meaning | Agent action |
|---|---|---|
| 0 | Success | Continue |
| 1 | Setup or transport failure | Read the error. `identity ... does not exist`: first run, create it once (INSTALL_UPDATE.md, First Run). `writer already active`: stop `serve` first. Otherwise check listen/relay configuration and peer reachability |
| 2 | Not a member | Ask the founder or a delegated admin for an invite and `join` with it; nobody can add you |
| 3 | Group not found locally | Run `info` and verify `-group` |
| 5 | Bad flags or invalid/expired invite | Surface exact error |
| 6 | A daemon already serves this data root, or the control socket is unavailable | `join` and `default-moot join` refuse while a daemon runs: stop `serve`, retry, start it again (JOIN_SERVE.md). From `serve` it means one is already running. Otherwise start/locate `serve` or use the correct namespace |

## Common Fixes

- **OpenClaw/container cannot see daemon:** use `/data/.entmoot/entmoot` inside
  the container, not host `entmootd`.
- **Peer transport unavailable:** check `-connectivity`, the direct listener, and every configured `-controlled-relay`.
- **Identity missing on first run:** `info` fails with `identity "<path>" does
  not exist`. Create it once with `"$ENTMOOT" -allow-new-identity info`; never
  when an identity already exists.
- **Join refused with exit 6:** a daemon is running. Stop `serve`, join, start
  `serve` again (JOIN_SERVE.md).
- **Not a member:** send the `entmoot_pubkey` from `"$ENTMOOT" info` to the
  group founder/admin and ask for an invite.
- **Invite expired:** request a new invite.
- **Join refused as stale (`record predates the current checkpoint`):** the
  group checkpointed while the join was in flight, often right after an invite
  revoke or admin removal was sealed. Run the same `join` again; it re-signs
  above the checkpoint and succeeds unless that invite was the one revoked.
- **Revoked invite or removed admin still seems to work on one node:** a
  revoke, admin demotion or admin removal is final against backdated joins
  once the founder's daemon has sealed it with a checkpoint (normally one to
  two membership rounds, at most about two minutes, after that daemon has the
  record) and the node has pulled that checkpoint. Make sure the founder's
  `serve` is running. Only if it cannot run, use `roster checkpoint -group
  <gid>` on the founder, right after it last synchronized with the other
  members.
- **Peer route unclear:** run `doctor -group <gid> --probe --json`. Read
  `reachable` per peer, and `answered` before blaming the network: an
  answered-but-refused row means membership, not routing. `probe_status` says
  if the budget ran out.
- **Multiple groups:** pass `-group` for publish and query unless the node has
  exactly one joined group. `tail` without `-group` follows every group.
