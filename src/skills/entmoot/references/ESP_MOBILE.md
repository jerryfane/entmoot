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
  `PUT /v1/groups/<group_id>/live-agents/<node_id>`.
- Capability API: `GET /v1/capabilities` returns
  `{"features":{"fleet_enabled":false,"tasks_enabled":false}}` by default.
- Bearer/admin devices can manage configs. A member signature can manage only
  that member node's own live-agent config.
- Unauthenticated `/v1/session` should return `401`; health endpoints should
  return `200`.
- Fleet and task/command HTTP routes are disabled by default. Do not probe or
  call `/v1/fleets...` unless capabilities show the operator enabled Fleet.

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

Operator actions that create Fleet tasks or Fleet commands require both
`ENTMOOT_ENABLE_FLEET=1` and `ENTMOOT_ENABLE_TASKS=1` on the ESP/runtime
process. Without those flags, the ESP filters disabled live actions and rejects
Fleet/task sign requests with `feature_disabled`.
