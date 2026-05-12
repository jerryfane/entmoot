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
GET  /v1/groups/{group_id}/live-agents
PUT  /v1/groups/{group_id}/live-agents/{node_id}
DELETE /v1/groups/{group_id}/live-agents/{node_id}
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
GET  /v1/fleets
POST /v1/fleets
GET  /v1/fleets/{fleet_id}
DELETE /v1/fleets/{fleet_id}
POST /v1/fleets/{fleet_id}/restore
GET  /v1/fleets/{fleet_id}/members
POST /v1/fleets/{fleet_id}/members/{node_id}/remove
GET  /v1/fleets/{fleet_id}/invites
POST /v1/fleets/{fleet_id}/invites
GET  /v1/fleets/{fleet_id}/activity
GET  /v1/fleets/{fleet_id}/tasks
POST /v1/fleets/{fleet_id}/tasks
GET  /v1/fleets/{fleet_id}/tasks/{task_id}
POST /v1/fleets/{fleet_id}/tasks/{task_id}/{action}
GET  /v1/fleets/{fleet_id}/commands
POST /v1/fleets/{fleet_id}/commands
GET  /v1/fleets/{fleet_id}/commands/{command_id}
GET  /v1/fleets/{fleet_id}/diagnostics
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

Member list responses may include `hostname` and `live`. Hostnames are learned
from signed member-profile gossip scoped to the group and are display hints
only. The ESP only exposes a profile when the profile author's Entmoot key
still matches the current roster entry for that Pilot node id. `live` is
ESP-local state with `enabled`, `status`, `mode`, topic filters, allowed
actions, lease, and timestamps.

Live-agent config routes:

- `GET /v1/groups/{group_id}/live-agents` returns `configs`, `presence`, and
  merged `members` live state for that group.
- `PUT /v1/groups/{group_id}/live-agents/{node_id}` upserts config:
  `enabled`, `mode`, `topic_filters`, `allowed_actions`,
  `max_actions_per_scan`, and `max_action_bytes`.
- `DELETE /v1/groups/{group_id}/live-agents/{node_id}` disables the config and
  marks presence offline.

Bearer/admin devices can read and manage live-agent configs. A member-signed
request can manage only that member node's own live-agent config. Defaults
match the CLI: mode `reply_on_mention`, topics `#`, and `0` for
`max_actions_per_scan` or `max_action_bytes` means unlimited.

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

Fleet routes are ESP-local control-plane projections backed by Entmoot group
events and sign requests:

- `GET /v1/fleets` lists visible Fleets. Query `include_archived=true` includes
  archived Fleets; `control_group_id=<group_id>` filters by control group.
- `POST /v1/fleets`, `DELETE /v1/fleets/{fleet_id}`, and
  `POST /v1/fleets/{fleet_id}/restore` create sign requests.
- `GET /v1/fleets/{fleet_id}` returns one Fleet.
- `GET /v1/fleets/{fleet_id}/members` and
  `POST /v1/fleets/{fleet_id}/members/{node_id}/remove` list members or create
  a member-removal sign request.
- `GET/POST /v1/fleets/{fleet_id}/invites` list invites or create a Fleet
  invite sign request.
- `GET /v1/fleets/{fleet_id}/activity` lists activity. Use `limit` and
  `before_ms` for pagination.
- `GET /v1/fleets/{fleet_id}/diagnostics` returns Fleet/control-group
  diagnostics.

Fleet task routes:

- `GET /v1/fleets/{fleet_id}/tasks?status=open` lists tasks.
- `POST /v1/fleets/{fleet_id}/tasks` creates a task. Modes are
  `open_submission`, `first_claim`, and `direct_assignee`.
- `GET /v1/fleets/{fleet_id}/tasks/{task_id}` returns the task and
  submissions.
- `POST /v1/fleets/{fleet_id}/tasks/{task_id}/{action}` mutates a task.
  Actions are `approve`, `assign`, `claim`, `submit`, `complete`, `reject`,
  and `cancel`.

Task limits: title is required and max 160 bytes, description max 8000 bytes,
and submission content is required and max 16000 bytes. Normal agents can claim
and submit tasks through task routes according to status and role. Approval is
a coordinator power. `task.update_own` and `task.comment` are live-agent
actions, not HTTP task-route actions.

Fleet command routes:

- `GET /v1/fleets/{fleet_id}/commands` lists command summaries. Filters:
  `status`, `action`, `agent_node_id`, and `limit` from 1 to 200.
- `POST /v1/fleets/{fleet_id}/commands` dispatches a coordinator command.
- `GET /v1/fleets/{fleet_id}/commands/{command_id}` returns command detail,
  status, results, and `updated_at_ms`.

Auto-accept-safe command actions are `echo`, `entmoot.version`,
`entmoot.info`, `entmoot.doctor_probe`, `pilot.info`, and
`fleet.local_state`. `agent.instruction` is manual risk, not read-only, and
requires the target node to opt in locally.

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
stateless and cursor-neutral. Responses include `has_more` and, when older
history is available, an opaque `next_cursor`. Pass that cursor back as
`cursor=<next_cursor>` to fetch the next older page. The cursor is bound to the
group and exact topic filter, so clients should keep one pagination cursor per
feed.

Mailbox cursors are stored in `mailbox.sqlite`. Mobile service state such as
sign requests, push tokens, notification preferences, Fleets, Fleet tasks,
Fleet commands, agent-command queue state, live-agent configs, live presence,
and live cursors is stored in `esp.sqlite`. Push routes are provider-neutral
wakeup plumbing; APNs delivery belongs behind the ESP service boundary. APNs is
configured on `esp serve` with Team ID, Key ID, bundle topic, `.p8` key path,
and optional sandbox mode. Push payloads are background wakeups only; message
content stays in mailbox sync.
