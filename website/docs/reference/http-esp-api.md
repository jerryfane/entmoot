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
DELETE /v1/groups/{group_id}/members/{node_id}
POST /v1/groups/{group_id}/invites
POST /v1/groups/{group_id}/open-invites
POST /v1/invites/accept
POST /v1/open-invites/accept
POST /v1/open-invites/{token}/challenge
POST /v1/open-invites/{token}/redeem
GET  /v1/groups/{group_id}/history
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

Mutating ESP routes that create or complete sign requests, or update the
current device push token, accept `Idempotency-Key`. The ESP stores the request
body hash and original JSON response in `esp.sqlite`: repeat with the same key
and body replays the first response; same key with a different body returns
`idempotency_conflict`.

Sign requests expose canonical signing metadata when the ESP can execute the
result. For `message_publish`, `payload` is only the draft/debug request body;
the phone must base64-decode `signing_payload` and sign those canonical
Entmoot message signing bytes. Complete the request with the returned
`signing_payload_sha256` plus the author `signature`; the ESP verifies both and
forwards the resulting message through signed publish.

`group_create`, `group_update`, `invite_create`, `open_invite_create`,
`invite_accept`, `open_invite_accept`, and `member_remove` are executable when
`esp serve` is connected to a running `join` daemon. Device sign requests
verify the completion signature with the registered device key. Completion
stores the operation response in `result`; `message_publish` also keeps
`publish_result` for compatibility. If the ESP has no operation executor
configured, executable operation completion fails with `operation_unavailable`.

Group updates are ESP-local display metadata. They do not mutate Entmoot's
roster protocol. Device-auth callers for admin-scoped operations must have the
group in both `groups` and `admin_groups`; membership and admin rights are
checked again when the sign request is completed.

Group list/get responses may include `name`, `description`, `tags`, and an
opaque JSON `metadata` object. `name`, `description`, and `tags` are projected
from metadata for app convenience; clients should treat the raw metadata object
as forward-compatible app data.

Member list responses may include `hostname`. Hostnames are learned from signed
member-profile gossip scoped to the group and are display hints only. The ESP
only exposes a profile when the profile author's Entmoot key still matches the
current roster entry for that Pilot node id.

Admin invite and member-management routes:

- `DELETE /v1/groups/{group_id}/members/{node_id}` creates a
  `member_remove` sign request. Completion appends the signed roster removal
  through the running daemon and fans out the new roster head.
- `POST /v1/groups/{group_id}/invites` creates an `invite_create` sign request
  and returns a targeted signed invite after completion. Entmoot verifies the
  target Pilot node id/public key binding before adding the roster entry.
- `POST /v1/groups/{group_id}/open-invites` creates an
  `open_invite_create` sign request. Completion stores an issuer-scoped token
  with expiry, max-use count, and optional bootstrap peers, and returns
  `issuer_url`, `token`, `link`, `expires_at_ms`, `max_uses`, and `use_count`.
  The daemon must be available at creation time so unusable tokens are not
  issued.
- `POST /v1/invites/accept` creates an `invite_accept` sign request for a full
  signed invite bundle.
- `POST /v1/open-invites/accept` creates an `open_invite_accept` sign request.
  Completion validates the issuer challenge for the requested token/local
  identity, obtains a local Pilot signature, redeems a normal signed invite,
  persists the redeemed invite for retry safety, and joins the group. Issuer
  redirects are disabled so URL validation cannot be bypassed.

Public open-invite issuer endpoints:

- `POST /v1/open-invites/{token}/challenge` accepts the redeemer Pilot node id,
  Pilot public key, and Entmoot public key. It returns a bounded,
  domain-separated challenge and caps active unused challenges.
- `POST /v1/open-invites/{token}/redeem` verifies the Pilot signature and
  returns a signed invite. Replays for the same redeemer return the stored
  result after re-validating proof.

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

Read the latest group history without advancing a mailbox cursor:

```http
GET /v1/groups/<group_id>/history?client_id=ios-1&limit=50
```

`limit` must be between 1 and 200. Device-auth clients may omit `client_id`;
the device id is used. The response shape matches mailbox pull, but the read is
stateless and cursor-neutral.

Mailbox cursors are stored in `mailbox.sqlite`. Mobile service state such as
sign requests, push tokens, and notification preferences is stored in
`esp.sqlite`. Push routes are provider-neutral wakeup plumbing; APNs delivery
belongs behind the ESP service boundary. APNs is configured on `esp serve`
with Team ID, Key ID, bundle topic, `.p8` key path, and optional sandbox mode.
Push payloads are background wakeups only; message content stays in mailbox
sync.
