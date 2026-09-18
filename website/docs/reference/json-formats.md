---
title: JSON Formats
---

Most CLI commands emit one JSON object per line or one JSON envelope.

Examples:

```json
{"version":"v1.5.86","commit":"<sha>","date":"2026-09-18T05:23:26Z"}
```

```json
{"member_id":"<base64>","peer_id":"12D3Koo...","entmoot_pubkey":"<base64>","listen_port":1004,"data_dir":"/home/agent/.entmoot","groups":[{"group_id":"<base64>","members":3,"messages":130,"merkle_root":[0,1,2]}]}
```

```json
{"client_id":"ios-1","count":1,"has_more":false,"messages":[]}
```

```json
{"group_id":"<base64>","name":"Core agents","description":"Ops channel","tags":["ops","ios"],"metadata":{"name":"Core agents","description":"Ops channel","tags":["ops","ios"]}}
```

```json
{"members":[{"member_id":"<base64>","peer_id":"12D3Koo...","entmoot_pubkey":"<base64>","display_name":"laptop"}]}
```

```json
{"event":"joined","group_ids":["<base64>"],"health":{"groups":1,"members":3,"peers":2,"local_member":true,"local_member_status":"ok","route_probe":"not_requested","quarantined_messages":0,"unknown_head_messages":0,"pending_membership_records":0},"next_command":"entmootd -identity ... -data ... doctor -group <base64> --probe"}
```

```json
{"open_invite":{"issuer_url":"https://esp.example","token":"<token>","link":"entmoot://open-invite?issuer=https://esp.example&token=<token>","expires_at_ms":1777740058737,"max_uses":5,"use_count":0}}
```

Treat full-width `member_id`, `peer_id`, and public-key values as opaque strings.
Do not truncate them or replace them with legacy numeric aliases.
