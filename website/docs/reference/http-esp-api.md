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

Sign requests expose canonical signing metadata when the ESP can execute the
result. For `message_publish`, `payload` is only the draft/debug request body;
the phone must base64-decode `signing_payload` and sign those canonical
Entmoot message signing bytes. Complete the request with the returned
`signing_payload_sha256` plus the author `signature`; the ESP verifies both and
forwards the resulting message through signed publish.

Create a message draft sign request:

```http
POST /v1/groups/<group_id>/messages
Content-Type: application/json

{"author":{"pilot_node_id":45491,"entmoot_pubkey":"<base64-ed25519-pubkey>"},"topics":["chat"],"content":"aGVsbG8="}
```

Response:

```json
{"sign_request":{"id":"<id>","kind":"message_publish","group_id":"<base64>","payload":{"message":{"group_id":"<base64>","author":{"pilot_node_id":45491,"entmoot_pubkey":"<base64-ed25519-pubkey>"},"timestamp":1777392000000,"topics":["chat"],"content":"aGVsbG8="}},"signing_payload":"<base64 canonical message signing bytes>","signing_payload_sha256":"<sha256>","status":"pending"}}
```

Complete it:

```json
{"signature":"<base64 ed25519 signature>","signing_payload_sha256":"<sha256>"}
```

Mailbox cursors are stored in `mailbox.sqlite`. Mobile service state such as
sign requests, push tokens, and notification preferences is stored in
`esp.sqlite`. Push routes are provider-neutral wakeup plumbing; APNs delivery
belongs behind the ESP service boundary.
