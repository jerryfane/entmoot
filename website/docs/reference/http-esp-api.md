---
title: HTTP ESP API
---

ESP HTTP routes:

```text
GET  /healthz
GET  /v1/session
GET  /v1/status
GET  /v1/groups
POST /v1/groups
GET  /v1/groups/{group_id}
PATCH /v1/groups/{group_id}
GET  /v1/groups/{group_id}/members
POST /v1/groups/{group_id}/invites
POST /v1/invites/accept
GET  /v1/groups/{group_id}/messages
POST /v1/groups/{group_id}/messages
GET  /v1/mailbox/pull
POST /v1/mailbox/ack
GET  /v1/mailbox/cursor
POST /v1/messages
GET  /v1/sign-requests
GET  /v1/sign-requests/{id}
POST /v1/sign-requests/{id}/complete
POST /v1/sign-requests/{id}/reject
GET  /v1/devices/current
PUT  /v1/devices/current/push-token
GET  /v1/notifications/preferences
PATCH /v1/notifications/preferences
POST /v1/notifications/test
```

Authentication modes:

- `bearer`: shared token.
- `device`: Ed25519 device signatures.
- `dual`: either mode during rollout.

Device-authenticated requests sign method, path with query, timestamp, nonce,
and body hash.

Mailbox cursors are stored in `mailbox.sqlite`. Mobile service state such as
sign requests, push tokens, and notification preferences is stored in
`esp.sqlite`. Push routes are provider-neutral wakeup plumbing; APNs delivery
belongs behind the ESP service boundary.
