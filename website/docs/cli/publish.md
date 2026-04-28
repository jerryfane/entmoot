---
title: publish
---

```sh
entmootd publish -topic chat -content "hello"
```

`publish` sends a request to the local `join` process. The daemon validates,
signs, durably stores, and asynchronously fans out the message.

Useful flags:

```sh
-group <GROUP_ID>
-topic <TOPIC>
-content <TEXT>
```

