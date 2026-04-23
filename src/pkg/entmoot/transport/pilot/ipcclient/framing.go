package ipcclient

import (
	"encoding/binary"
	"errors"
	"io"
)

// writeFrame writes a single length-prefixed frame: a 4-byte big-endian
// payload length followed by the payload bytes. Returns an error if the
// payload exceeds maxFrameSize or if the underlying writer fails
// mid-frame.
//
// The write is issued as two calls to w.Write: first the length header,
// then the payload. Callers that wrap a net.Conn MUST serialize calls
// to writeFrame themselves (e.g. behind a mutex) — this function does
// not guard against interleaving.
func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) > maxFrameSize {
		return ErrFrameTooLarge
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// readFrame reads one length-prefixed frame from r. The returned byte
// slice is a freshly-allocated buffer owned by the caller — the reader
// may be reused immediately.
//
// A zero-length frame (header of 0x00000000) is accepted and returns
// an empty non-nil slice. A frame whose declared length exceeds
// maxFrameSize is rejected with ErrFrameTooLarge before any payload
// bytes are consumed, so the reader is still positioned at the next
// header (callers who want to continue reading after this error may do
// so, but typically an over-sized frame means the stream is desynced
// and should be treated as a fatal protocol error).
//
// If the header or payload is truncated by EOF, returns
// io.ErrUnexpectedEOF. A clean EOF at a frame boundary (no bytes
// consumed) returns io.EOF unchanged so callers can distinguish
// graceful shutdown from protocol errors.
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	n, err := io.ReadFull(r, hdr[:])
	if err != nil {
		// io.ReadFull maps Reader EOF after 0 bytes to io.EOF, and EOF
		// after a partial read to io.ErrUnexpectedEOF. Pass those
		// through so callers can distinguish clean shutdown from a
		// truncated header.
		if errors.Is(err, io.EOF) && n == 0 {
			return nil, io.EOF
		}
		return nil, err
	}
	size := binary.BigEndian.Uint32(hdr[:])
	if size > maxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if size == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		// Payload truncation is always unexpected — we already know a
		// non-zero payload was announced.
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf, nil
}
