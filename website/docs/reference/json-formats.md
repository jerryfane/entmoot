---
title: JSON Formats
---

Most CLI commands emit one JSON object per line or one JSON envelope.

Examples:

```json
{"version":"v1.5.25","commit":"<sha>","date":"2026-04-29T11:22:22Z"}
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

See `docs/CLI_DESIGN.md` in the repository for the full wire and IPC schemas.
