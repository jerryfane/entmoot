// Package ipc implements the Entmoot local control-socket protocol used by
// cmd/entmootd's join process to serve client commands (publish, tail, info)
// over a Unix-domain socket.
//
// Framing (CLI_DESIGN §5.2):
//
//	[4-byte big-endian length][1-byte msg_type][JSON body]
//
// length = size of (msg_type + body). The framing shape matches the peer
// wire protocol for developer familiarity, but the type-number namespace
// (0x10..0x1F), the payload set, and the error taxonomy are all local to
// this package. Sharing a framing library with pkg/entmoot/wire was
// rejected in CLI_DESIGN §5.2: peer wire crosses encrypted Pilot tunnels
// to potentially-untrusted peers; ipc frames move between cooperating
// processes on the same host. Conflating the two security models would
// be wrong.
//
// This package deliberately has no replay-protection or rate-limiting
// path: the socket is 0600 and bound to a single local user, so both
// concerns are handled by filesystem permissions rather than the codec.
package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize is the hard cap on a single ipc frame's payload, measured
// as the 4-byte length prefix's value. It covers the 1-byte msg_type plus
// the JSON body. Frames larger than this are rejected on both encode and
// decode with ErrOversized.
const MaxFrameSize = 16 * 1024 * 1024

// lengthPrefixSize is the number of bytes used for the big-endian length
// prefix at the start of every frame.
const lengthPrefixSize = 4

// WriteFrame encodes and writes a single frame to w. body must be the JSON
// body bytes with no framing of its own. The function writes
// [4B len][1B type][body] as a single contiguous buffer.
//
// Returns ErrOversized if 1+len(body) exceeds MaxFrameSize. Any error
// from w.Write is surfaced wrapped with context.
func WriteFrame(w io.Writer, t MsgType, body []byte) error {
	// length field covers (msg_type byte + body bytes).
	payloadLen := 1 + len(body)
	if payloadLen > MaxFrameSize {
		return fmt.Errorf("ipc: frame payload %d bytes: %w", payloadLen, ErrOversized)
	}

	// Assemble a single buffer so the frame hits the socket atomically
	// from the caller's perspective. io.Writer's contract says a
	// successful Write must consume all bytes or return an error; we
	// rely on that.
	buf := make([]byte, lengthPrefixSize+payloadLen)
	binary.BigEndian.PutUint32(buf[0:lengthPrefixSize], uint32(payloadLen))
	buf[lengthPrefixSize] = byte(t)
	copy(buf[lengthPrefixSize+1:], body)

	n, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("ipc: write frame: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("ipc: short write: wrote %d of %d bytes", n, len(buf))
	}
	return nil
}

// ReadFrame reads exactly one frame from r and returns the msg_type byte
// plus the JSON body. Common error conditions:
//
//   - io.EOF when r reports EOF before any of the 4-byte length prefix
//     is read (clean end-of-stream).
//   - io.ErrUnexpectedEOF when r reports EOF partway through the length
//     prefix, the type byte, or the body (truncated frame).
//   - ErrOversized when the decoded length exceeds MaxFrameSize.
//   - ErrMalformedFrame when the declared length is zero (a frame must
//     contain at least the 1-byte msg_type).
//
// Any other error from r.Read is surfaced wrapped with context.
// ReadFrame does not parse the body; it returns the raw bytes so callers
// can dispatch on MsgType before unmarshaling.
func ReadFrame(r io.Reader) (MsgType, []byte, error) {
	var lengthBuf [lengthPrefixSize]byte
	n, err := io.ReadFull(r, lengthBuf[:])
	if err != nil {
		// io.ReadFull returns io.EOF if zero bytes were read and
		// io.ErrUnexpectedEOF if some but not all were read.
		if errors.Is(err, io.EOF) && n == 0 {
			return 0, nil, io.EOF
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, fmt.Errorf("ipc: read length prefix: %w", err)
	}

	length := binary.BigEndian.Uint32(lengthBuf[:])
	if length == 0 {
		return 0, nil, fmt.Errorf("ipc: zero-length frame: %w", ErrMalformedFrame)
	}
	if length > MaxFrameSize {
		return 0, nil, fmt.Errorf("ipc: frame length %d: %w", length, ErrOversized)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, fmt.Errorf("ipc: read frame body: %w", err)
	}

	// payload[0] is msg_type, payload[1:] is body. A valid frame therefore
	// has len(body) == length-1 (possibly zero).
	return MsgType(payload[0]), payload[1:], nil
}
