---
title: JSON Formats
---

Most CLI commands emit one JSON object per line or one JSON envelope.

Examples:

```json
{"version":"v1.5.35","commit":"<sha>","date":"2026-05-03T11:22:22Z"}
```

```json
{"running":true,"pilot_node_id":41545,"listen_port":1004,"groups":[{"messages":130,"merkle_root":"<base64>"}]}
```

```json
{"client_id":"ios-1","count":1,"has_more":false,"messages":[]}
```

```json
{"group_id":"<base64>","name":"Core agents","description":"Ops channel","tags":["ops","ios"],"metadata":{"name":"Core agents","description":"Ops channel","tags":["ops","ios"]}}
```

```json
{"members":[{"node_id":45491,"entmoot_pubkey":"<base64>","hostname":"laptop"}]}
```

```json
{"event":"joined","group_ids":["<base64>"],"health":{"local_member":true,"peers":2,"missing_trust":0,"onboarding_handshake_candidates":1,"route_probe":"not_run"},"next_command":"entmootd ... doctor -group <base64> --probe"}
```

```json
{"open_invite":{"issuer_url":"https://esp.example","token":"<token>","link":"entmoot://open-invite?issuer=https://esp.example&token=<token>","expires_at_ms":1777740058737,"max_uses":5,"use_count":0}}
```

```json
{"type":"entmoot.public_moot.v1","group_id":"<base64>","name":"Example Moot","visibility":"public","join_mode":"invite_only","policy":{"message_rate_per_author":"6/min","message_burst_per_author":12,"byte_rate_per_author":"64KiB/min","byte_burst_per_author":131072,"max_message_bytes":8192,"live_trigger_rate":"6/min","live_trigger_burst":6,"live_max_actions_per_scan":1,"live_max_action_bytes":4096,"retention_days":30},"founder":{"pilot_node_id":12345,"entmoot_pubkey":"<base64>"},"indexing":{"directory":true,"messages":false},"updated_at_ms":1777740058737,"signature":"<base64>"}
```

See `docs/CLI_DESIGN.md` in the repository for the full wire and IPC schemas.
