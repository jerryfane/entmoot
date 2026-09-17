# ESP And App-Facing State

Use this reference for ESP/mobile-facing HTTP state APIs. ESP is an always-on
service peer for HTTP/mobile clients. ESP-local state is not consensus state.

## Important Surfaces

- Group summaries can expose ESP-local `name`, `description`, `tags`, and
  metadata.
- Capability API: `GET /v1/capabilities` returns an empty object `{}`. Use
  `/v1/status` or `/v1/session` for auth mode and service state.
- Bearer/admin devices can manage ESP-local group and member state.
- Unauthenticated `/v1/session` should return `401`; health endpoints should
  return `200`.
