---
title: Security Model
---

Security layers:

- libp2p authenticates and encrypts peer transport.
- Entmoot verifies that each PeerID derives from the roster's Ed25519 key.
- Entmoot rosters authorize group membership.
- Entmoot messages are author-signed.
- ESP device auth signs HTTP requests from registered devices.
- Mailbox cursors are local service state, not consensus state.

Entmoot currently does not encrypt group content at rest or end-to-end across
the whole group. libp2p encrypts each network connection, while message bodies
remain plaintext in each authorized member's local store.

