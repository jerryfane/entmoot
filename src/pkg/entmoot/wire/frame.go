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
// body. Per-type caps below are normally tighter. Readers and writers enforce
// both limits before allocating or emitting a body.
const MaxFrameSize = 512 * 1024

// lengthPrefixSize is the number of bytes used for the big-endian length
// prefix at the start of every frame.
const lengthPrefixSize = 4

// FrameReadChunkSize bounds the first allocation made after a frame header is
// accepted. Larger allowed bodies grow only as bytes actually arrive.
const FrameReadChunkSize = 32 * 1024

// MaxFrameBodySize returns the maximum JSON body size for t. The table is the
// shared ingress/egress contract, so a writer cannot emit a frame a reader
// would reject. Unknown frame types are rejected before their body is read.
func MaxFrameBodySize(t MsgType) (int, bool) {
	switch t {
	case MsgHello:
		return 16 * 1024, true
	case MsgRosterReq, MsgRangeReq:
		return 8 * 1024, true
	case MsgRosterResp, MsgTransportSnapshotResp, MsgMemberProfileSnapshotResp:
		return MaxFrameSize - 1, true
	case MsgGossip, MsgFetchResp, MsgAcceptanceReq:
		return 384 * 1024, true
	case MsgFetchReq, MsgMerkleReq, MsgPrune, MsgTransportSnapshotReq,
		MsgMemberProfileSnapshotReq, MsgDiagPingReq, MsgDiagPingResp:
		return 4 * 1024, true
	case MsgMerkleResp, MsgTransportAd, MsgMemberProfileAd, MsgAcceptanceResp:
		return 64 * 1024, true
	case MsgRangeResp, MsgReconcile:
		return 128 * 1024, true
	case MsgIHave, MsgGraft:
		return 32 * 1024, true
	default:
		return 0, false
	}
}

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
	maxBody, ok := MaxFrameBodySize(t)
	if !ok {
		return fmt.Errorf("wire: frame type %s: %w", t, entmoot.ErrUnknownMessage)
	}
	if len(body) > maxBody {
		return fmt.Errorf("wire: %s body %d exceeds cap %d: %w", t, len(body), maxBody, entmoot.ErrOversized)
	}
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

// FrameAdmission runs after the length and type have been read and validated,
// but before any body allocation or read. frameSize is the complete wire size:
// length prefix, type byte, and declared body.
type FrameAdmission func(t MsgType, frameSize int) error

// ReadFrame reads one frame without an external admission check.
func ReadFrame(r io.Reader) (MsgType, []byte, error) {
	return ReadFrameWithAdmission(r, nil)
}

// ReadFrameWithAdmission reads exactly one frame. Per-type size validation and
// admission happen before body allocation. Accepted bodies are read in bounded
// chunks so a stalled peer cannot force allocation of its full declaration.
func ReadFrameWithAdmission(r io.Reader, admit FrameAdmission) (MsgType, []byte, error) {
	var lengthBuf [lengthPrefixSize]byte
	n, err := io.ReadFull(r, lengthBuf[:])
	if err != nil {
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

	var typeBuf [1]byte
	if _, err := io.ReadFull(r, typeBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, fmt.Errorf("wire: read frame type: %w", err)
	}
	t := MsgType(typeBuf[0])
	maxBody, ok := MaxFrameBodySize(t)
	if !ok {
		return 0, nil, fmt.Errorf("wire: frame type %s: %w", t, entmoot.ErrUnknownMessage)
	}
	bodyLen := int(length) - 1
	if bodyLen > maxBody {
		return 0, nil, fmt.Errorf("wire: %s body %d exceeds cap %d: %w", t, bodyLen, maxBody, entmoot.ErrOversized)
	}
	if admit != nil {
		if err := admit(t, lengthPrefixSize+int(length)); err != nil {
			return 0, nil, err
		}
	}
	if bodyLen == 0 {
		return t, nil, nil
	}

	initialCapacity := bodyLen
	if initialCapacity > FrameReadChunkSize {
		initialCapacity = FrameReadChunkSize
	}
	body := make([]byte, 0, initialCapacity)
	var chunk [FrameReadChunkSize]byte
	for remaining := bodyLen; remaining > 0; {
		next := remaining
		if next > len(chunk) {
			next = len(chunk)
		}
		readN, err := io.ReadFull(r, chunk[:next])
		if readN > 0 {
			body = append(body, chunk[:readN]...)
			remaining -= readN
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, nil, io.ErrUnexpectedEOF
			}
			return 0, nil, fmt.Errorf("wire: read frame body: %w", err)
		}
	}
	return t, body, nil
}
