---
title: Three-Peer Mesh
---

A healthy mesh has:

- One running `entmootd serve` process per host and data root, hosting one or
  more group sessions.
- The same membership on every peer: the same canonical checkpoint id and the
  same member set.
- Verified PeerID-to-member-key bindings.
- Matching message counts and coverage roots after convergence.

## Bringing one up

Peer A is the founder; B and C join. Nobody is written into the group by an
admin: each joiner signs its own join record and redeems an invite.

On A:

```sh
entmootd group create -name demo --json
entmootd serve
```

Still on A, mint one invite per joiner, naming A's own libp2p multiaddr as the
bootstrap peer:

```sh
entmootd invite create -group <GROUP_ID> -target-pubkey <B_PUBKEY_B64> \
  -bootstrap /ip4/<A_IP>/tcp/1004/p2p/<A_PEER_ID> -valid-for 24h > invite-b.json
entmootd invite create -group <GROUP_ID> -target-pubkey <C_PUBKEY_B64> \
  -bootstrap /ip4/<A_IP>/tcp/1004/p2p/<A_PEER_ID> -valid-for 24h > invite-c.json
```

On B and then C:

```sh
entmootd join invite-b.json
entmootd serve
```

`join` reads a checkpoint from the bootstrap peer, signs its own join record,
and pushes it. The peer that accepts the record forwards it to the group's
other reachable members, so C learns about B without either of them being
named in the other's invite.

The issuer does not have to be online when an invite is used. If A is stopped
after B has joined, an invite A already minted still works as long as the
invite names a reachable member as a bootstrap peer and A still holds
authority in the group. Point the third invite at B:

```sh
entmootd invite create -group <GROUP_ID> -target-pubkey <C_PUBKEY_B64> \
  -bootstrap /ip4/<B_IP>/tcp/1004/p2p/<B_PEER_ID> -valid-for 24h > invite-c.json
```

## Verifying

After restarting peers, verify locally:

```sh
scripts/verify-mesh-node.sh
```

For manual checks:

```sh
entmootd version
entmootd info
entmootd doctor -group <GROUP_ID> --probe
entmootd roster status -group <GROUP_ID>
entmootd query --limit 1000 | wc -l
```

Compare those outputs across peers. `doctor --probe` should show current
membership, transport availability, synchronization health, and probe results
for each non-local peer. `roster status` should show the same `checkpoint` and
`sequence` on every peer once a checkpoint has been signed, and the same member
list.

`pending` in `roster status` (also `pending_membership_records` in health
output) is the number of records not yet folded into a checkpoint. Peers may
briefly differ there while a record is still propagating; they must not differ
in the projected member list once they hold the same records. A checkpoint is
signed automatically at the group's `checkpoint_every` cadence, or on demand:

```sh
entmootd roster checkpoint -group <GROUP_ID>
```

That command takes the group's writer lease, so stop the local daemon first,
then restart it. The other peers adopt the checkpoint on their next membership
sync.
