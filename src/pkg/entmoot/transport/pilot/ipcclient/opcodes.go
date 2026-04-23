package ipcclient

// Opcode is a one-byte Pilot IPC command or response identifier. Values
// are fixed by the wire protocol (SPEC.md §6.1). Only the subset this
// package actively emits or receives is enumerated; unused opcodes from
// the spec (SendTo/RecvFrom datagrams, hostname management, tags,
// webhooks, task-exec, network/managed admin, health) are intentionally
// omitted — Entmoot does not use them, and keeping the set tight
// reduces accidental surface.
type Opcode byte

const (
	opBind              Opcode = 0x01
	opBindOK            Opcode = 0x02
	opDial              Opcode = 0x03
	opDialOK            Opcode = 0x04
	opAcceptedConn      Opcode = 0x05
	opSend              Opcode = 0x06
	opRecv              Opcode = 0x07
	opClose             Opcode = 0x08
	opCloseOK           Opcode = 0x09
	opError             Opcode = 0x0A
	opInfo              Opcode = 0x0D
	opInfoOK            Opcode = 0x0E
	opHandshake         Opcode = 0x0F
	opHandshakeOK       Opcode = 0x10
	opSetPeerEndpoints  Opcode = 0x25
	opSetPeerEndpointsOK Opcode = 0x26
)

// Handshake sub-command byte values. The Handshake opcode (0x0F) takes
// a 1-byte sub-command as the first byte of its payload. Only the
// "trusted" sub-command (enumerate trusted peers) is used by this
// package; the others are listed for reference.
const (
	subHandshakeSend    byte = 0x01
	subHandshakeApprove byte = 0x02
	subHandshakeReject  byte = 0x03
	subHandshakePending byte = 0x04
	subHandshakeTrusted byte = 0x05
	subHandshakeRevoke  byte = 0x06
)

// addrSize is the on-wire size of a Pilot virtual address:
// 2 bytes network ID + 4 bytes node ID. SPEC.md §1.1.
const addrSize = 6

// maxFrameSize is the largest IPC payload (bytes after the length
// prefix) the daemon will accept and the largest this package emits.
// SPEC.md §6.
const maxFrameSize = 1 << 20 // 1 MiB
