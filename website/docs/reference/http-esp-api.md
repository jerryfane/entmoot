---
title: HTTP ESP API
---

ESP HTTP routes:

```text
GET  /healthz
GET  /v1/mailbox/pull
POST /v1/mailbox/ack
GET  /v1/mailbox/cursor
POST /v1/messages
```

Authentication modes:

- `bearer`: shared token.
- `device`: Ed25519 device signatures.
- `dual`: either mode during rollout.

Device-authenticated requests sign method, path with query, timestamp, nonce,
and body hash.

