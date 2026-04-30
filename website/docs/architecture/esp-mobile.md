---
title: ESP / Mobile Architecture
---

The mobile architecture follows an ESP model:

- The service peer stays online.
- The mobile client is intermittent.
- The phone may retain its signing key.
- The ESP stores mailbox cursors and forwards already-signed messages.
- The ESP exposes app-facing group/member projections for mobile UI.

This avoids requiring iOS to run a full always-on `pilot-daemon` and
`entmootd join` process. Push notifications or app backends can wake the phone,
but Entmoot itself remains the group protocol and store.

The ESP projection is deliberately non-authoritative. Group display fields
(`name`, `description`, `tags`, and `metadata`) live in ESP-local state.
Member hostnames come from signed member-profile gossip and are checked against
the current roster key before exposure. Neither mechanism changes message
authorship, roster membership, or the group id.

The mobile bootstrap read path is split from durable sync. `history` gives an
initial latest-message page without moving mailbox cursors; mailbox pull/ack is
the durable per-client cursor path after the app is connected.
