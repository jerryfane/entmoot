# Security Model

Security layers:

- libp2p authenticates and encrypts peer transport.
- Entmoot verifies that each PeerID derives from the member's Ed25519 key.
- Entmoot membership authorizes group participation: a founder-signed
  checkpoint plus signed membership records projected onto it.
- Membership records are self-signed where they are statements about the
  signer (join, leave, rekey) and founder/admin-signed where they are
  statements about somebody else (remove, ban, unban, policy, invite
  revocation).
- A delegated admin's invite is worth that admin's current standing: removing
  or demoting it invalidates its outstanding invites everywhere at once. A
  founder's invites survive its own removal and need `invite revoke`.
- Entmoot messages are author-signed and name the checkpoint the author held.
- ESP device auth signs HTTP requests from registered devices.
- Mailbox cursors are local service state, not consensus state.

Membership cannot fork, so there is nothing to repair. Records merge by a
total order derived from their contents, so two nodes holding the same records
reach the same membership. A checkpoint refuses records older than itself,
which is what stops a discarded change from being replayed back in.

Records are ordered by their signer's timestamp, which nothing else bounds
from below, so a revocation or a demotion is final only against joins dated
after it until a checkpoint covers it. Every change that takes authority away
(invite revocation, admin demotion, removal or departure, closing an open
group) is therefore sealed by the founder's daemon with a checkpoint dated at
the change, once a membership round has pulled what the reachable members
hold or, failing that, after a bounded wait counted only in rounds that got
through to a member and only once every addressable member has been asked,
so that no member can hold the seal off, members that hang cannot crowd an
honest one out of it, and a founder cut off from every member never seals
its own view. A node refuses a join dated before the change once that
checkpoint has reached it. Until then - normally one to two membership rounds
on the founder and at most about four minutes while it reaches any member
(longer only when more than about 70 members hang), one more round for the
other nodes, longer while the founder's daemon is down or reaches nobody - a
node can still admit such a join. Invite expiry has no such
checkpoint: a join dated inside an expired invite's validity window is
admitted until a later checkpoint covers the window.

A fresh joiner trusts the founder key pinned by its verified invite and checks
the served checkpoint's founder signature. A migrated group's legacy anchor
describes history on the founder's node; the joiner need not possess that old
roster and gains no authority to verify legacy messages from the anchor alone.
This remote-admission path refuses to replace local legacy artifacts. Local
conversion remains separate and requires the checkpoint to name the roster
chain actually held on disk.

Bearer (`-open`) invites are bearer credentials: whoever holds the link can
join until it expires, is revoked, or runs out of uses.

Entmoot currently does not encrypt group content at rest or end-to-end across
the whole group. libp2p encrypts each network connection, while message bodies
remain plaintext in each authorized member's local store.

