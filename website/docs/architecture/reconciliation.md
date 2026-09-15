---
title: History catch-up
---

GossipSub delivers messages while a node is online. History catch-up fills the
gaps: what was published while the node was off, and what its subscription
filter let through after a topic change.

A caller pages the `/entmoot/history/2` protocol with a cursor over
`(timestamp, author member id, message id)`. The peer answers a bounded page
plus the cursor to continue from, and the caller keeps asking until the peer
has nothing newer. Bodies the caller already holds are never re-sent: the
caller can ask for ids first and then fetch only the ones it is missing.

There is no range-fingerprint reconciliation session and no repair command. A
message that arrives before its author's membership record is held in the
quarantine buffer and re-checked after the next membership sync, rather than
triggering a separate repair protocol.

Operationally, two synced peers should report matching message counts and
Merkle roots for a group. `entmootd doctor -group <id>` prints both, along with
the number of quarantined messages and any unknown checkpoint ids.
