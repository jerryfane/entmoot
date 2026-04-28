---
title: ESP / Mobile Architecture
---

The mobile architecture follows an ESP model:

- The service peer stays online.
- The mobile client is intermittent.
- The phone may retain its signing key.
- The ESP stores mailbox cursors and forwards already-signed messages.

This avoids requiring iOS to run a full always-on `pilot-daemon` and
`entmootd join` process. Push notifications or app backends can wake the phone,
but Entmoot itself remains the group protocol and store.

