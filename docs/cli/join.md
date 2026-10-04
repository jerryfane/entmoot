# join

```sh
entmootd join <invite> [invite...]
```

`join` validates each invite, signs the local node's own join record against
the checkpoint it reads from a reachable member, pushes that record into the
group, and exits. It refuses to run while an Entmoot daemon is serving the
same data root (`join: stop the running daemon before joining a new group`,
exit 6), so stop `serve`, join, then start `serve` again.

On a new install there is no identity yet. Create it once, before the first
join, with `entmootd -allow-new-identity info`; never pass that flag when an
identity already exists.

A named peer entitled to serve it — a current member, or the invite's own issuer — serves the redemption: an invite carries bootstrap addresses
that name current members of the group. Only a peer named there is
authorised to serve the checkpoint and accept the join record. It then forwards
the record to the group's other reachable members, so the joiner needs the
issuer once and no admin signature at all. `join` fails with the projected reason — for example an
exhausted or revoked invite, a banned key, or an issuer that no longer holds
authority — rather than leaving a half-joined group behind.

Accepted invite inputs:

- a signed invite JSON file;
- an HTTP(S) URL returning signed invite JSON;
- an `entmoot://open-invite?issuer=...&token=...` link;
- an open-invite descriptor JSON containing `issuer_url` and `token`.

Open invites are redeemed during `join`. The joiner sends the issuer its
MemberID, PeerID and public key; the issuer checks that all three come from the
same key and returns a signed invite made out to that key. Redemption itself
proves nothing about who holds the key: the protection is that the invite it
returns works only for that key. Entmoot then continues through the normal
bootstrap and self-signed join path. A raw token is intentionally rejected
because the issuer URL is required.

For production restarts, prefer `entmootd serve` after the first successful
join. `serve` loads persisted groups from disk and does not need the original
invite file. Use `entmootd join --serve <invite>` only when you explicitly want
the legacy join-and-run daemon mode.

On containerized agents, run joins through the installed wrapper:

```sh
/data/.entmoot/entmoot join <invite>
```

The wrapper supplies the persistent identity, data root, and listen port so
the join stays in the intended runtime namespace. It does not pass a
connectivity profile: pass `-connectivity` yourself when the default
`direct` is wrong for this host.

Useful flags. `-connectivity` and `-controlled-relay` are global flags and go
before the subcommand; `join` itself only takes `--serve` and `-timeout`:

```sh
entmootd -connectivity relay-only -controlled-relay <CIRCUIT_RELAY_MULTIADDR> join <invite>
entmootd join -timeout 2m <invite>
```

`-timeout` (default 90s) bounds each invite. libp2p gives up on a single
connection attempt after 15s. `join` tries every address in the invite once,
in order; if a connection timed out, for example a WSS connection stalled
behind an HTTPS proxy, it dials the addresses that timed out again every 5s
until `-timeout` runs out. A refused proxy, an untrusted certificate or the
wrong peer identity is not retried.

On success, `join` emits a readiness event before exiting. The event includes
`health` and a `next_command` the daemon builds for a follow-up check. That
command carries `--probe`, so running it dials the other members and reports
which of them answer:

```json
{"event":"joined","group_ids":["<GROUP_ID>"],"members":3,"health":{"groups":1,"members":3,"peers":2,"local_member":true,"local_member_status":"ok","route_probe":"not_requested","quarantined_messages":0,"unknown_head_messages":0,"pending_membership_records":0},"next_command":"entmootd ... doctor -group <GROUP_ID> --probe"}
```

After joining, signed bootstrap hints and membership-bound PeerIDs seed the
integrated libp2p host. GossipSub handles live delivery, and bounded sync
streams recover membership and history.

Use a service manager for production.
