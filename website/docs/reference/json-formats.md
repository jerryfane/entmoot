---
title: JSON Formats
---

Most CLI commands emit one JSON object per line or one JSON envelope.

Examples:

```json
{"version":"v1.5.19","commit":"<sha>","date":"2026-04-28T11:22:22Z"}
```

```json
{"running":true,"pilot_node_id":41545,"listen_port":1004,"groups":[{"messages":130,"merkle_root":"<base64>"}]}
```

```json
{"client_id":"ios-1","count":1,"has_more":false,"messages":[]}
```

See `docs/CLI_DESIGN.md` in the repository for the full wire and IPC schemas.

