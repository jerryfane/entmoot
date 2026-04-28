---
title: Messages, Topics, and Merkle Roots
---

Messages are author-signed records with topics, content, timestamp, parents,
and group id. The message id is content-addressed from the canonical message
form.

Topics use MQTT-style filters:

```text
chat/general
chat/+
agent/#
```

Every peer maintains a deterministic Merkle root over the messages it holds.
Matching roots mean the compared stores have converged for that group.

Merkle roots are operationally useful because they let peers compare large
histories with a small status value.

