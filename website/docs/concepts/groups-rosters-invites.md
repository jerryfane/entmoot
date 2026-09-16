---
title: Groups, Membership, and Invites
---

Group membership is a signed checkpoint plus a set of signed membership
records. Projecting the records onto the checkpoint yields the membership: who
can author messages, and which libp2p PeerIDs may participate in group
protocols.

Membership is not a chain. There is no head to append to, no fork to detect,
and no repair command.

## Joining is self-signed

A joiner signs its own admission record (`kind: join`) and attaches the invite
that authorises it. The invite is a founder- or admin-signed
`BootstrapCapability`; the joiner's own signature is the act of joining.

Nobody signs a joiner in: the joiner signs its own join record. Authority is
still checked at redemption, against group state rather than a fresh signature:
the founder may always issue, and a delegated admin may issue only while it is
still an unbanned member — which is why an invite from a demoted, removed or
banned admin stops working everywhere at once, while a founder's invite keeps
working regardless of the founder's own membership. An invite names bootstrap addresses, and
`invite create` accepts any current member's address — the issuing node's own,
any you name, and up to four more it already knows, bounded in bytes as well
as count — and only a peer the invite
names may serve the checkpoint and accept the join record — and only while it
is still a member itself, or is the invite's own issuer, whose authority is
checked on every read. So a newcomer can
join while the issuer is offline, as long as one named member is reachable.
Naming other members is safe because the newcomer pins the founder's key from
the invite and verifies the membership it is served against that key: a named
peer can serve or fail, not forge. Once the record is accepted, the serving
peer forwards it to the group's other reachable members.

## Membership is a set

Record kinds:

| Kind | Signed by | Effect |
|---|---|---|
| `join` | the joining member itself | adds the subject, redeeming an invite unless the join rule is `open` |
| `leave` | the leaving member itself | removes the subject |
| `rekey` | the old key of the member | moves membership from the old identity to a new one |
| `remove` | founder or delegated admin | removes the subject; also bans it when the record says so |
| `unban` | founder only | lifts a ban |
| `policy` | founder only | replaces the whole group policy, including the admin set |
| `revoke_invite` | founder or delegated admin | invalidates one invite by nonce |

Records are merged by a deterministic total order derived from their contents:
timestamp first, then kind rank, then the founder's record before a delegated
admin's, then record id. Kind rank is a decision, not
an accident:

1. `join` — somebody admitted in the same instant is a member when the records
   that follow are judged.
2. `rekey` — a leave and a join of one person.
3. `remove`, `unban`, `policy`, `revoke_invite` — a removal beats a
   simultaneous join, because admitting is recoverable and failing to remove is
   not.
4. `leave` — a leave at the same instant as anything else still sticks.

Two nodes that hold the same records project the same membership regardless of
the order the records arrived in. A record that cannot take effect —
unauthorised, superseded, refused by the join rule, already true — is ignored
rather than rejected, because a peer is entitled to send records this node
cannot use.

A member may leave and may rotate its own key without an admin. Both are
statements a member makes about itself.

## Checkpoints retire history

Any admin — not only the founder — periodically signs a checkpoint. A
checkpoint carries the complete member set, the policy, the bans, the
invite-use counts, a `previous` link, and the count and timestamp of the
records it folds in.

A checkpoint replaces the records it covers. Records older than the canonical
checkpoint's timestamp are refused as stale, so a change that was discarded
cannot come back later on a slow link.

Signing one is the daemon's job, not an operator's. Every maintenance round,
each node that may sign checks whether the group has reached its cadence
(`checkpoint_every`, default 64 effective records) and signs if it has —
including when it has heard from nobody, because the records it signed itself
count too. `entmootd roster checkpoint` exists for the case where you want one
now rather than at the cadence.

Several admins reaching the cadence at once each sign one; they all describe
the same membership, and every node picks the same winner by the rule below, so
a duplicate costs one signature and nothing else.

### Who may sign one

The checkpoint **before** it decides. A checkpoint's own admin set is written
by its signer, so it is never consulted when judging that signature: a peer
could otherwise name itself an admin and be believed by any node that holds no
record from the covered window — which is every joiner, and every node that
was quiet through that window.

Two consequences follow:

- An admin granted authority inside the window a checkpoint covers signs the
  checkpoint **after next**, not the one carrying its own grant. A checkpoint
  only that admin's grantor could verify would leave every later checkpoint
  unreachable as well, for naming an unknown predecessor.
- A node starting from nothing adopts a **founder-signed** checkpoint. An
  invite pins the founder's key and nothing else, so that is the only
  signature such a node can check. Admins still sign checkpoints — that is
  what retires history while the founder is away — and where two exist at one
  sequence, the founder-signed one wins, so the chain a joiner walks stays
  anchored.

Each node keeps the chain from its newest founder-signed checkpoint forward. A
founder that never checkpoints therefore leaves a longer chain behind;
`roster status` shows it as the gap between the anchor and the canonical
sequence, and the remedy is one `roster checkpoint` on the founder.

### Being told you were removed

A removed node can read nothing: every door is shut to it. So a peer that
refuses it answers with the record its own projection acted on when it dropped
that member, plus the `policy` records that came before that removal and are
still held — and nothing else. The subject needs those: a member removed by a
delegated admin may never have seen the grant that made that admin able to act.

Policy records say who may act, not who is in the group, so the disclosure is
bounded to how the admin set moved in that window. The node learns why it was
refused without being handed the membership it no longer belongs to.

The node acts only on that signed record, never on the refusal itself: a peer's
word is not evidence, and a node that evicted itself on an unproven claim could
be talked out of a group by anybody. Once the record applies, the node drops
itself, says so in the log, and stops publishing.

A removal already folded into a checkpoint cannot be proven this way: showing
that one identity is absent from a checkpoint means showing the whole member
set. Such a node is told only that it is unauthorised. Closing that needs a
commitment over the member set, and is not implemented.

### What a checkpoint may not claim

- A RECORD more than five minutes ahead of the reading node's clock is refused
  too. Records merge in timestamp order, so an unbounded timestamp is
  authority: a member could date one years ahead and win every contest about
  itself until that date — re-admitting itself over a removal, or keeping a
  membership it had left — while holding no authority at all.
- A checkpoint timestamp more than five minutes ahead of the reading node's
  clock is refused. The timestamp decides which records the checkpoint covers, so one
  dated next year would make every legitimate record stale and freeze the node.
- A checkpoint that says it replaces a linear roster chain must name the head
  of the chain that node holds. One claiming an upgrade where there is no chain
  is refused rather than installed.

Two consequences matter operationally:

- A new member downloads one checkpoint instead of replaying a group's whole
  history. Storage follows group size, not group age.
- A membership lookup against a cited checkpoint is constant time. Measured on
  the implementation: 330ns at 1k members and 172ns at 100k members, against
  3.5ms and 316ms for walking the equivalent pre-checkpoint chain.

## Invites

Invites are out-of-band bootstrap bundles. They include:

- Group id.
- Founder MemberID, libp2p PeerID, and Entmoot public key.
- The checkpoint the issuer minted the invite against (`roster_head`).
- Bootstrap peers.
- Expiration time.
- Issuer signature.

Targeted invites name the joining Entmoot public key. Entmoot derives and
verifies the full-width MemberID and libp2p PeerID from that key, so one
identity binds across application and transport layers, and the join record
that redeems the invite must carry that same key.

### A delegated admin's invite is worth its issuer's current standing

A delegated admin's invite is worth exactly that admin's current authority:
remove or demote the issuer and its outstanding invites stop working everywhere
at once, with no revocation step and no per-node bookkeeping. The founder is
exempt - its invites keep admitting joiners even after it removes itself,
because the anchor a joiner pins is the founder key, not the founder's
membership. Revoking a founder's invite takes `invite revoke`.

Use limits (`max_uses`) and revocations (`revoke_invite` records) are projected
from the group's own signed state, so every node reaches the same answer
offline. Joins citing the same invite nonce are ordered by timestamp then
record id, and only the first `max_uses` distinct identities are admitted.

There is no admission reservation ledger and no reserve/commit race. The
local `bootstrap-admission.db` file is only a record of the invites this node
issued, used by `entmootd invite list`.

### Open invites

Open invites are ESP-issued tokens with an issuer URL, expiry, max-use count,
and optional bootstrap peers. They are not themselves joinable bundles. A
joiner redeems one by proving possession of its Entmoot key:

1. The joiner asks the issuer for a bounded, domain-separated challenge.
2. The local Entmoot identity signs that challenge.
3. The issuer verifies the MemberID, PeerID, public-key binding, and signature;
   consumes a use; mints a normal signed invite; and stores the result for safe
   retries.
4. The joiner applies the signed invite through the normal bootstrap path, and
   signs its own join record against the checkpoint it reads.

`entmootd join` understands `entmoot://open-invite?issuer=...&token=...` links
and open-invite descriptor JSON, so agents do not need to manually redeem open
invites. A raw token is rejected because it does not identify the issuer.

Open invites are not public directory listing. A group can be public and still
invite-only, or open-invite and unlisted. Public listing is driven by a
founder-signed `entmoot.public_moot.v1` descriptor and is described in
[Public Moot Directory](./public-moot-directory).

### Bearer invites are bearer credentials

`entmootd invite create -open` mints an invite with no target identity.
Whoever holds it can join until it expires, is revoked, or runs out of uses.
That exposure is the point of a bearer invite; prefer target-bound invites when
the joining key is known.

## Join rule and policy

The founder-signed group policy holds:

- `join_rule`: `invite` (default) or `open`. Under `open`, a join needs no
  invite; the group's own signed policy is the authority.
- `checkpoint_every`: effective records between checkpoints, default 64.
- `admins`: the delegated-admin set, at most 16, never containing the founder.

Only the founder signs a `policy` record.

## Removal, bans, and moderation

Removal is an authority record. A delegated admin may remove an ordinary
member; only the founder may remove an admin; any authority may remove itself.
A `remove` that bans additionally bars the subject from rejoining, and only the
founder may `unban`.

A plain removal is recoverable: a join with a later timestamp re-admits the
member. A ban is not, until it is lifted.

Removed members are excluded from future membership projection, diagnostics
onboarding, and auto-approval. Live messages are authorised against current
membership; historical messages are authorised against membership at the
checkpoint the message cites.

## Limits

- One membership response is capped at 4 MiB. A checkpoint carries the whole
  member set, so this bounds group size over the wire at roughly 20k members. A
  group larger than that cannot sync its membership in one answer, and says so
  rather than syncing half a group.
- One answer carries at most 512 records. Records are a set, so a truncated
  answer is still progress: the caller applies what it got and asks again.
- A group that has no checkpoint cannot be served. Pre-checkpoint groups need
  `entmootd membership upgrade` first; see
  [Founder Commands](../cli/founder-commands).
