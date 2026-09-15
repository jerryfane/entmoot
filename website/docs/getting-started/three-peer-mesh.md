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

No admin signs a join, but the issuing node must be reachable when its invite
is used: `invite create` refuses any `-bootstrap` that does not end in the
issuing node's own peer id, so an invite cannot point a newcomer at a different
member. To admit C while A is stopped, B has to issue the invite itself, which
means A must delegate admin authority to B first — `invite create` exits 2 for
a member that is neither founder nor delegated admin. Both steps take the
group's writer lease, so each runs with that node's daemon stopped:

```sh
# on A, daemon stopped
entmootd roster admin grant -group <GROUP_ID> -member <B_MEMBER_ID>
```

Restart A long enough for B to sync the policy record (`entmootd roster status
-group <GROUP_ID>` on B lists B under `admins`), then stop A and issue from B:

```sh
# on B, daemon stopped
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
