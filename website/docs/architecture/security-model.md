---
title: Security Model
---

Security layers:

- libp2p authenticates and encrypts peer transport.
- Entmoot verifies that each PeerID derives from the member's Ed25519 key.
- Entmoot membership authorizes group participation: a founder-signed
  checkpoint plus signed membership records projected onto it.
- Membership records are self-signed where they are statements about the
  signer (join, leave, rekey) and founder/admin-signed where they are
  statements about somebody else (remove, ban, unban, policy, invite
  revocation).
- An invite is worth its issuer's current standing. Removing or demoting an
  issuer invalidates its outstanding invites everywhere at once.
- Entmoot messages are author-signed and name the checkpoint the author held.
- ESP device auth signs HTTP requests from registered devices.
- Mailbox cursors are local service state, not consensus state.

Membership cannot fork, so there is nothing to repair. Records merge by a
total order derived from their contents, so two nodes holding the same records
reach the same membership. A checkpoint refuses records older than itself,
which is what stops a discarded change from being replayed back in.

Bearer (`-open`) invites are bearer credentials: whoever holds the link can
join until it expires, is revoked, or runs out of uses.

Entmoot currently does not encrypt group content at rest or end-to-end across
the whole group. libp2p encrypts each network connection, while message bodies
remain plaintext in each authorized member's local store.

