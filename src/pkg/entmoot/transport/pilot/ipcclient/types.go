package ipcclient

import (
	"fmt"
	"strconv"
	"strings"
)

// Endpoint describes one transport endpoint for a peer. Network is a
// short identifier like "tcp", "udp", or "turn"; Addr is a "host:port"
// string the daemon will resolve on its own side. The daemon imposes
// limits of 16 bytes on Network and 255 bytes on Addr. "turn" was
// added in Entmoot v1.4.0 / pilot-daemon v1.9.0-jf.8 — earlier
// daemons reject the string at SetPeerEndpoints time. (v1.4.0)
type Endpoint struct {
	Network string
	Addr    string
}

// Info is the typed shape of the daemon's InfoOK JSON body.
// Driver.Info continues to return the untyped map for callers that
// need flexibility (e.g. Pilot adds fields faster than we can roll
// Entmoot); Driver.InfoStruct returns this typed form for callers
// that want compile-time field access.
//
// Backwards-compat: every field is annotated `omitempty`, so a jf.7
// daemon that omits `turn_endpoint` decodes cleanly into an Info
// with TURNEndpoint="". Downstream callers treat the empty string
// as "no TURN relay available". (v1.4.0)
type Info struct {
	// NodeID is the daemon's Pilot node id. Present on every
	// version of the daemon's InfoOK reply.
	NodeID uint32 `json:"node_id,omitempty"`
	// Hostname is the daemon's reported hostname. Best-effort;
	// unused by Entmoot but surfaced for debugging.
	Hostname string `json:"hostname,omitempty"`
	// TURNEndpoint is the daemon's advertised TURN relay
	// "host:port" for this node, if one was configured at daemon
	// start. Added in pilot-daemon v1.9.0-jf.8. Older daemons
	// omit the field; new daemons without a TURN provider also
	// omit it. In both cases the decoded value is "". (v1.4.0)
	TURNEndpoint string `json:"turn_endpoint,omitempty"`
}

// Addr is a Pilot virtual address: a 16-bit network ID paired with a
// 32-bit node ID. SPEC.md §1.1. The text representation
// (SPEC.md §1.2) is "N:NNNN.HHHH.LLLL" where N is the network ID in
// decimal, NNNN the same in hex, and HHHH.LLLL the node ID as two
// 4-hex-digit groups.
type Addr struct {
	Network uint16
	Node    uint32
}

// String renders the address in Pilot's canonical text format.
func (a Addr) String() string {
	return fmt.Sprintf("%d:%04X.%04X.%04X",
		a.Network,
		a.Network,
		(a.Node>>16)&0xFFFF,
		a.Node&0xFFFF,
	)
}

// parseAddr parses the text form N:NNNN.HHHH.LLLL. Returns an error on
// any deviation from the documented shape, including a mismatch
// between the decimal and hex network IDs.
func parseAddr(s string) (Addr, error) {
	colon := strings.IndexByte(s, ':')
	if colon < 0 {
		return Addr{}, fmt.Errorf("ipcclient: addr %q missing ':'", s)
	}
	netDec, err := strconv.ParseUint(s[:colon], 10, 16)
	if err != nil {
		return Addr{}, fmt.Errorf("ipcclient: addr %q: decimal network: %w", s, err)
	}
	groups := strings.Split(s[colon+1:], ".")
	if len(groups) != 3 {
		return Addr{}, fmt.Errorf("ipcclient: addr %q: want 3 hex groups, got %d", s, len(groups))
	}
	for _, g := range groups {
		if len(g) != 4 {
			return Addr{}, fmt.Errorf("ipcclient: addr %q: hex group %q not 4 digits", s, g)
		}
	}
	netHex, err := strconv.ParseUint(groups[0], 16, 16)
	if err != nil {
		return Addr{}, fmt.Errorf("ipcclient: addr %q: hex network: %w", s, err)
	}
	if netHex != netDec {
		return Addr{}, fmt.Errorf("ipcclient: addr %q: decimal %d != hex %04X", s, netDec, netHex)
	}
	hi, err := strconv.ParseUint(groups[1], 16, 16)
	if err != nil {
		return Addr{}, fmt.Errorf("ipcclient: addr %q: high node: %w", s, err)
	}
	lo, err := strconv.ParseUint(groups[2], 16, 16)
	if err != nil {
		return Addr{}, fmt.Errorf("ipcclient: addr %q: low node: %w", s, err)
	}
	return Addr{
		Network: uint16(netDec),
		Node:    uint32(hi)<<16 | uint32(lo),
	}, nil
}

// SocketAddr is a virtual address plus a 16-bit port. Text format
// "N:NNNN.HHHH.LLLL:PORT".
type SocketAddr struct {
	Addr Addr
	Port uint16
}

func (sa SocketAddr) String() string {
	return fmt.Sprintf("%s:%d", sa.Addr.String(), sa.Port)
}

// ParseSocketAddr parses the text form of a Pilot socket address.
func ParseSocketAddr(s string) (SocketAddr, error) {
	last := strings.LastIndexByte(s, ':')
	if last < 0 {
		return SocketAddr{}, fmt.Errorf("ipcclient: socket addr %q missing port", s)
	}
	addr, err := parseAddr(s[:last])
	if err != nil {
		return SocketAddr{}, err
	}
	port, err := strconv.ParseUint(s[last+1:], 10, 16)
	if err != nil {
		return SocketAddr{}, fmt.Errorf("ipcclient: socket addr %q: port: %w", s, err)
	}
	return SocketAddr{Addr: addr, Port: uint16(port)}, nil
}
