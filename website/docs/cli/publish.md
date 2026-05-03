---
title: publish
---

```sh
entmootd publish -topic chat -content "hello"
entmootd publish -topic chat -file message.txt
printf '%s\n' "$MESSAGE" | entmootd publish -topic chat -file -
```

`publish` sends a request to the local `join` process. The daemon validates,
signs, durably stores, and asynchronously fans out the message.

Useful flags:

```sh
-group <GROUP_ID>
-topic <TOPIC>
-content <TEXT>
-file <PATH|->
-timeout 30s
```

Exactly one of `-content` or `-file` is required. Prefer `-file` or
`-file -` when publishing shell-generated text; it avoids backtick, `$()`,
quote, and multiline substitution problems.

Success prints one JSON object:

```json
{"message_id":"<base64>","group_id":"<base64>","topic":["chat"],"timestamp_ms":1777392000000}
```
