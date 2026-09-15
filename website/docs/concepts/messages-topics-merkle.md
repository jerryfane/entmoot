---
title: Messages, Topics, and Merkle Roots
---

Messages are author-signed records with topics, content, timestamp, parents,
and group id. The message id is content-addressed from the canonical message
form.

A message also names the membership checkpoint its author held, in the signed
`roster_head` field. Authority follows from it: a live message is authorised
against current membership, and a historical message against the membership at
the checkpoint it cites. A message naming a checkpoint this node has not seen
yet is held briefly in quarantine and ingested after the next membership sync.
Moderation is therefore a membership operation, not a message operation — see
[Groups, Membership, and Invites](./groups-rosters-invites).

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

