// Package wire implements the Entmoot :1004 wire protocol: a length-prefixed
// frame carrying a 1-byte message-type tag followed by a JSON body, plus the
// typed payload structs and an encode/decode dispatcher.
//
// Framing (ARCHITECTURE §4):
//
//	[4-byte big-endian length][1-byte msg_type][JSON body]
//
// length = size of (msg_type + body). A frame therefore has a minimum total
// size of 5 bytes (length=1, type alone) and a maximum of 4 + MaxFrameSize
// bytes on the wire. Empty JSON bodies are rejected at the codec layer for
// every v0 message type.
//
// This package does not implement replay protection or rate limiting; those
// live in neighboring packages and wrap ReadAndDecode / the net.Conn.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	entmoot "entmoot/pkg/entmoot"
)

// MaxFrameSize is the hard cap on a single frame's payload, measured as the
// 4-byte length prefix's value. It covers the 1-byte msg_type plus the JSON
// body. Frames larger than this are rejected on both encode and decode with
// entmoot.ErrOversized.
const MaxFrameSize = 16 * 1024 * 1024

// lengthPrefixSize is the number of bytes used for the big-endian length
// prefix at the start of every frame.
const lengthPrefixSize = 4

// WriteFrame encodes and writes a single frame to w. body must be the JSON
// body bytes with no framing of its own. The function writes
// [4B len][1B type][body] in a single Write call when w supports it, falling
// back to the buffer being passed directly to w.Write.
//
// Returns entmoot.ErrOversized if 1+len(body) exceeds MaxFrameSize. Any error
// from w.Write is surfaced as-is. Partial writes are handled by io.Writer
// implementations that do not fully consume the buffer — callers that wrap w
// with their own Writer must honor io.Writer's contract.
func WriteFrame(w io.Writer, t MsgType, body []byte) error {
	// length field covers (msg_type byte + body bytes).
	payloadLen := 1 + len(body)
	if payloadLen > MaxFrameSize {
		return fmt.Errorf("wire: frame payload %d bytes: %w", payloadLen, entmoot.ErrOversized)
	}

	// Assemble a single buffer so the frame hits the wire atomically from
	// the caller's perspective. io.Writer contract says a successful Write
	// must consume all bytes or return an error; we rely on that.
	buf := make([]byte, lengthPrefixSize+payloadLen)
	binary.BigEndian.PutUint32(buf[0:lengthPrefixSize], uint32(payloadLen))
	buf[lengthPrefixSize] = byte(t)
	copy(buf[lengthPrefixSize+1:], body)

	// Use io.Writer directly. Short writes are the writer's problem per the
	// io.Writer contract; we surface whatever error Write returns.
	n, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("wire: write frame: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("wire: short write: wrote %d of %d bytes", n, len(buf))
	}
	return nil
}

// ReadFrame reads exactly one frame from r and returns the msg_type byte plus
// the JSON body. Common error conditions:
//
//   - io.EOF when r reports EOF before any of the 4-byte length prefix is
//     read (clean end-of-stream).
//   - io.ErrUnexpectedEOF when r reports EOF partway through the length
//     prefix, the type byte, or the body (truncated frame).
//   - entmoot.ErrOversized when the decoded length exceeds MaxFrameSize.
//   - entmoot.ErrMalformedFrame when the declared length is zero (a frame
//     must contain at least the 1-byte msg_type).
//
// Any other error from r.Read is surfaced as-is (wrapped with context).
// ReadFrame does not parse the body; it returns the raw bytes so callers can
// choose codec dispatch or replay checks before unmarshaling.
func ReadFrame(r io.Reader) (MsgType, []byte, error) {
	var lengthBuf [lengthPrefixSize]byte
	n, err := io.ReadFull(r, lengthBuf[:])
	if err != nil {
		// io.ReadFull returns io.EOF if zero bytes were read and
		// io.ErrUnexpectedEOF if some but not all bytes were read.
		if errors.Is(err, io.EOF) && n == 0 {
			return 0, nil, io.EOF
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, fmt.Errorf("wire: read length prefix: %w", err)
	}

	length := binary.BigEndian.Uint32(lengthBuf[:])
	if length == 0 {
		return 0, nil, fmt.Errorf("wire: zero-length frame: %w", entmoot.ErrMalformedFrame)
	}
	if length > MaxFrameSize {
		return 0, nil, fmt.Errorf("wire: frame length %d: %w", length, entmoot.ErrOversized)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, fmt.Errorf("wire: read frame body: %w", err)
	}

	// payload[0] is msg_type, payload[1:] is body. A valid frame therefore
	// has len(body) == length-1 (possibly zero).
	return MsgType(payload[0]), payload[1:], nil
}
