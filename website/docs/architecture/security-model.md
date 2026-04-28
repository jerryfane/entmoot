---
title: Security Model
---

Security layers:

- Pilot encrypts pairwise transport.
- Entmoot rosters authorize group membership.
- Entmoot messages are author-signed.
- ESP device auth signs HTTP requests from registered devices.
- Mailbox cursors are local service state, not consensus state.

Entmoot currently does not encrypt group content at rest or end-to-end across
the group. Pilot tunnels protect transport, but message bodies are plaintext in
the local store.

