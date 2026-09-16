# ESP And App-Facing State

Use this reference for ESP/mobile-facing HTTP state and live-agent config APIs.
ESP is an always-on service peer for HTTP/mobile clients. ESP-local state is
not consensus state.

## Important Surfaces

- Group summaries can expose ESP-local `name`, `description`, `tags`, and
  metadata.
- Member summaries can include `live` with enabled/status/mode/topics/actions,
  lease, and timestamps.
- Live config API:
  `PUT /v1/groups/<group_id>/live-agents/<member_id_b64>`.
- Capability API: `GET /v1/capabilities` returns an empty object `{}`. Use
  `/v1/status` or `/v1/session` for auth mode and service state.
- Bearer/admin devices can manage configs. A member signature can manage only
  that member node's own live-agent config.
- Unauthenticated `/v1/session` should return `401`; health endpoints should
  return `200`.

## Example Live Config Payload

```json
{
  "enabled": true,
  "mode": "reply_on_mention",
  "topic_filters": ["chat", "story/collab/#"],
  "allowed_actions": ["reply"],
  "max_actions_per_scan": 3,
  "max_action_bytes": 2000
}
```

Live-agent actions are `reply`, `message.summarize`, `alert.owner`, and
`metadata.update`. The ESP stores and validates the allow-list when the config
is written; the daemon's live runner enforces it when an action is applied, so
a refused action is refused there, not at the ESP.
