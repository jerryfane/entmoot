// Package ipcclient is an independent Go implementation of the Pilot IPC
// wire protocol as described in /root/pilotprotocol/docs/SPEC.md
// (Pilot Protocol Wire Specification v0.5, §6 "IPC Protocol
// (Daemon <-> Driver)"). It is NOT derived from the pilotprotocol source
// code.
//
// The package provides the minimal surface Entmoot's gossip transport
// adapter requires: connecting to a Pilot daemon's Unix IPC socket,
// discovering local node identity, binding a listen port, opening and
// accepting virtual stream connections, sharing externally-sourced
// transport endpoints, and enumerating trusted peers.
//
// # Architecture
//
// The IPC socket is a single Unix stream carrying length-prefixed frames
// (4-byte big-endian length, maximum payload 1 MiB). Each frame's first
// byte is an opcode identifying the command or response; the remaining
// bytes are the opcode-specific payload.
//
// Multiple application connections are multiplexed on the single IPC
// socket by 32-bit conn_id values assigned by the daemon on Dial/Accept.
// A single read goroutine ("demuxer") reads frames off the socket and
// dispatches them by opcode:
//
//   - Recv frames are routed to the matching conn_id's receive channel
//     (Conn.Read consumes from there).
//   - AcceptedConn frames are routed to the Listener bound on the
//     relevant port.
//   - Command-response frames (BindOK, DialOK, InfoOK, HandshakeOK,
//     CloseOK, SetPeerEndpointsOK) are delivered to the goroutine that
//     issued the matching command via a per-opcode FIFO of pending-reply
//     channels.
//   - Error frames are delivered to the most recently issued in-flight
//     command (there is no opcode for "which command this error applies
//     to" in the wire format, so we pair by FIFO order).
//
// A single writer mutex serializes outbound frames so Send/Close/Dial/etc.
// cannot interleave bytes on the socket.
//
// # Spec clarifications
//
// SPEC.md leaves a few behaviors implicit; each decision below was
// confirmed against observable daemon behavior — we describe what we
// concluded, not how the daemon code is structured.
//
//  1. AcceptedConn payload layout. SPEC §6.1 lists opcode 0x05 as
//     "Accept" with payload "[4B conn_id][6B remote addr][2B port]". In
//     practice the daemon prefixes this with the 2-byte bound local
//     port so a client can route the notification to the correct
//     listener when it holds several on one socket:
//     [2B local_port][4B conn_id][6B remote_addr][2B remote_port].
//     This package parses that full layout and routes by local_port.
//
//  2. TrustedPeers does not have its own top-level opcode. It is
//     dispatched through the Handshake opcode (0x0F) with a 1-byte
//     sub-command byte (0x05 = "trusted"), and the daemon responds with
//     HandshakeOK (0x10) carrying a JSON document of shape
//     {"trusted": [{"node_id": <uint32>, ...}, ...]}. This follows the
//     "sub-command byte as first byte of payload" pattern SPEC §6.2 and
//     §6.3 describe for Network and Managed; Handshake uses the same
//     pattern although SPEC does not tabulate its sub-commands.
//
//  3. SetPeerEndpoints (0x25) payload is a compact TLV, not JSON:
//     [4B nodeID][1B count] followed by `count` entries of
//     [1B network_len][network bytes][1B addr_len][addr bytes].
//     The daemon responds with a SetPeerEndpointsOK (0x26) carrying a
//     JSON status document. Maximum 8 endpoints per call; network names
//     up to 16 bytes; addrs up to 255 bytes — matching the daemon's
//     validation envelope.
//
//  4. Error frame (0x0A) payload is [2B error_code][NB UTF-8 message].
//     The code is currently unused by callers; the message string is
//     surfaced as the Go error text. Error frames are paired with
//     in-flight commands on a first-in-first-out basis per the socket's
//     causal order — the daemon responds to each command serially so
//     no response can overtake another's error.
//
//  5. Recv data before Read. Recv (0x07) frames for a conn_id may
//     arrive on the socket between the daemon's DialOK/AcceptedConn and
//     the client's first Read on the resulting Conn. The demuxer
//     buffers such early data in the Conn's receive channel (which is
//     created eagerly when Dial/Accept returns) so no bytes are
//     dropped.
package ipcclient
