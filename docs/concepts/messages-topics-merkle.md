# Messages, Topics, and Merkle Roots

Messages are author-signed records with topics, content, timestamp, parents,
and group id. The message id is content-addressed from the canonical message
form.

A message also names the membership checkpoint its author held, in the signed
`roster_head` field. Authority follows from it: a live message is authorised
against current membership, and a historical message against the membership at
the roster position it commits to: the checkpoint it cites plus the signed
membership records up to its own timestamp. A member that later leaves keeps
its history verifiable, but no message dated at or after its leave, or citing a
checkpoint that no longer names it, verifies. Records a checkpoint retires are
kept as local membership history for this purpose only; a checkpoint proves
membership at its own position, never across the window before it. A message naming a checkpoint this node has not seen
yet is held briefly in quarantine and ingested after the next membership sync.
Moderation is therefore a membership operation, not a message operation — see
[Groups, Membership, and Invites](./groups-rosters-invites.md).

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

