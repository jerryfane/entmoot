---
title: libp2p Integration
---

Entmoot owns one integrated libp2p host per running data root.

The `serve` process:

- Derives the libp2p PeerID from the persisted Entmoot Ed25519 identity.
- Listens directly or reserves addresses through configured Circuit Relay v2
  peers, according to the selected connectivity profile.
- Uses signed, membership-bound discovery hints instead of a public peer
  registry.
- Delivers live messages through authorized GossipSub envelopes.
- Synchronizes membership and history through bounded libp2p streams.
- Exposes local control operations over an Entmoot Unix socket.

Entmoot stream protocols:

| Protocol | Purpose |
|---|---|
| `/entmoot/membership/1` | Serves checkpoints, plus the membership records a caller does not already hold. A member reads it under its membership; a joiner reads it with an invite, because it must see a checkpoint before it can sign itself in. |
| `/entmoot/membership-push/1` | Accepts one signed membership record. A joiner delivers its own join this way, and a member propagates a change immediately instead of waiting for the next pull. |
| `/entmoot/history/2` | Bounded, resumable message history synchronization. |
| `/entmoot/peer-records/1` | Serves the signed peer records this node holds for other members. Members-only, never a bootstrap target. |

A membership caller sends the checkpoint id and sequence it holds plus the
record ids it holds; the server answers with what is missing. The answer is
computed from live state, so there is no paging snapshot to hold open: partial
progress is always safe, and a truncated answer just means "ask again". A node
that accepts a pushed record forwards it once to the group's other reachable
members, which is enough for a join to reach members the joiner never
contacted.

Application authorization remains separate from secure transport. A valid
libp2p connection is not group membership; the remote PeerID must bind to a
current member key before it can participate.

No Pilot daemon, Pilot socket, TURN allocation, or external Pilot registry is
part of the current runtime.
