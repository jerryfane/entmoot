package pilot

// This file mirrors the small Addr/ParseSocketAddr surface that used
// to come from github.com/TeoSlayer/pilotprotocol/pkg/protocol. Both
// the wire/text format (N:NNNN.HHHH.LLLL[:PORT]) and the Go shape
// (two-field struct {Network uint16; Node uint32}) are fixed by the
// Pilot Protocol wire specification (SPEC.md §1) — they are facts
// about the protocol, not expressions of any one implementation.

import (
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

// Addr is the Pilot virtual address type used by the pilot Transport
// when talking to the ipcclient package. Defined here as a thin alias
// so call sites in pilot.go keep reading as
// "protocol.Addr{Network: ..., Node: ...}"-shaped literals.
type Addr = ipcclient.Addr

// SocketAddr is Addr paired with a 16-bit port.
type SocketAddr = ipcclient.SocketAddr

// ParseSocketAddr parses the canonical text form
// "N:NNNN.HHHH.LLLL:PORT" as emitted by a Pilot daemon's RemoteAddr
// surface. Delegates to the in-tree ipcclient parser.
func ParseSocketAddr(s string) (SocketAddr, error) {
	return ipcclient.ParseSocketAddr(s)
}
