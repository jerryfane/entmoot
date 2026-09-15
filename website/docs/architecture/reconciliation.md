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

Operationally, two synced peers should report the same message count for a
group: `entmootd doctor -group <id>` prints `members=` and `messages=` per
group, and `--json` adds each group's Merkle root, which is the value to
compare across peers.

Messages waiting on a membership record are not a doctor field. The counts
`quarantined_messages` and `unknown_head_messages` appear in the `health`
block of the readiness event that `join` prints and that a joined daemon
emits on its control socket.
