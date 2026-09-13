---
title: libp2p Integration
---

Entmoot owns one integrated libp2p host per running data root.

The `serve` process:

- Derives the libp2p PeerID from the persisted Entmoot Ed25519 identity.
- Listens directly or reserves addresses through configured Circuit Relay v2
  peers, according to the selected connectivity profile.
- Uses signed, roster-bound discovery hints instead of a public peer registry.
- Delivers live messages through authorized GossipSub envelopes.
- Synchronizes rosters and history through bounded libp2p streams.
- Exposes local control operations over an Entmoot Unix socket.

Application authorization remains separate from secure transport. A valid
libp2p connection is not group membership; the remote PeerID must bind to a
current roster key before it can participate.

No Pilot daemon, Pilot socket, TURN allocation, or external Pilot registry is
part of the current runtime.
