package ipcclient

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestWriteFrameHeaderThenPayload verifies the on-wire layout: the
// 4-byte big-endian length header is emitted first, followed by the
// exact payload bytes. Without this invariant any reader
// (including the daemon) desyncs.
func TestWriteFrameHeaderThenPayload(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	payload := []byte{0x01, 0x02, 0x03, 0xFE, 0xFF}
	if err := writeFrame(&buf, payload); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	got := buf.Bytes()
	if len(got) != 4+len(payload) {
		t.Fatalf("wrote %d bytes, want %d", len(got), 4+len(payload))
	}
	if ln := binary.BigEndian.Uint32(got[:4]); ln != uint32(len(payload)) {
		t.Fatalf("length prefix = %d, want %d", ln, len(payload))
	}
	if !bytes.Equal(got[4:], payload) {
		t.Fatalf("payload bytes mismatch: got %x, want %x", got[4:], payload)
	}
}

// TestWriteReadRoundTrip confirms that readFrame inverts writeFrame
// byte-for-byte across a range of payload sizes, including the
// edge cases of 0 bytes and the 1 MiB maximum.
func TestWriteReadRoundTrip(t *testing.T) {
	t.Parallel()
	sizes := []int{0, 1, 34, 1024, 65535, maxFrameSize}
	for _, size := range sizes {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i * 31)
		}
		var buf bytes.Buffer
		if err := writeFrame(&buf, payload); err != nil {
			t.Fatalf("size=%d writeFrame: %v", size, err)
		}
		got, err := readFrame(&buf)
		if err != nil {
			t.Fatalf("size=%d readFrame: %v", size, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("size=%d round-trip mismatch at head=%x", size, got[:min(8, len(got))])
		}
		if buf.Len() != 0 {
			t.Fatalf("size=%d leftover bytes: %d", size, buf.Len())
		}
	}
}

// TestWriteFrameRejectsOversized ensures we never send a frame the
// daemon would reject — both as a liveness check and to keep a bug in
// the caller from wedging the IPC stream.
func TestWriteFrameRejectsOversized(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	payload := make([]byte, maxFrameSize+1)
	err := writeFrame(&buf, payload)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("writeFrame over-sized err = %v, want ErrFrameTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("writeFrame wrote %d bytes despite rejecting", buf.Len())
	}
}

// TestReadFrameRejectsOversizedHeader guards the symmetric case: a
// malicious or corrupted header announcing a frame larger than the
// max must be rejected before any payload bytes are consumed.
func TestReadFrameRejectsOversizedHeader(t *testing.T) {
	t.Parallel()
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(maxFrameSize+1))
	// No payload follows — reader should reject before asking for it.
	r := bytes.NewReader(hdr[:])
	_, err := readFrame(r)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("readFrame oversized header err = %v, want ErrFrameTooLarge", err)
	}
}

// TestReadFrameCleanEOFAtBoundary checks that an EOF at a frame
// boundary (zero bytes consumed into the header) surfaces as io.EOF,
// not io.ErrUnexpectedEOF. The demuxer uses this distinction to
// distinguish graceful socket shutdown from protocol corruption.
func TestReadFrameCleanEOFAtBoundary(t *testing.T) {
	t.Parallel()
	_, err := readFrame(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("readFrame at EOF returned %v, want io.EOF", err)
	}
}

// TestReadFrameTruncatedHeader returns io.ErrUnexpectedEOF when
// fewer than 4 bytes of header arrive.
func TestReadFrameTruncatedHeader(t *testing.T) {
	t.Parallel()
	_, err := readFrame(bytes.NewReader([]byte{0x00, 0x00}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readFrame truncated-header err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// TestReadFrameTruncatedPayload returns io.ErrUnexpectedEOF when the
// header announces N bytes but fewer than N follow.
func TestReadFrameTruncatedPayload(t *testing.T) {
	t.Parallel()
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 16)
	buf := append(hdr[:], []byte("only8byt")...) // 8 payload bytes of 16 announced
	_, err := readFrame(bytes.NewReader(buf))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readFrame truncated-payload err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// TestReadFrameZeroLengthOK exercises the degenerate but legal
// zero-payload frame: the header says 0 and no payload bytes follow;
// we return a non-nil empty slice so callers can distinguish
// "received an empty frame" from "received nothing".
func TestReadFrameZeroLengthOK(t *testing.T) {
	t.Parallel()
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 0)
	got, err := readFrame(bytes.NewReader(hdr[:]))
	if err != nil {
		t.Fatalf("readFrame zero-length: %v", err)
	}
	if got == nil {
		t.Fatal("readFrame zero-length returned nil, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("readFrame zero-length returned %d bytes, want 0", len(got))
	}
}

// TestReadFramePropagatesArbitraryWriterError confirms non-EOF I/O
// errors surface unchanged (not masked as ErrUnexpectedEOF).
func TestReadFramePropagatesArbitraryWriterError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("disk ate the bytes")
	r := &errReader{err: sentinel}
	_, err := readFrame(r)
	if !errors.Is(err, sentinel) {
		t.Fatalf("readFrame err = %v, want sentinel %v", err, sentinel)
	}
}

type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestWriteFramePropagatesShortWrite ensures an underlying writer that
// fails partway through the header surfaces its error rather than
// being swallowed.
func TestWriteFramePropagatesShortWrite(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("pipe broke")
	w := &errWriter{err: sentinel}
	err := writeFrame(w, []byte("hello"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("writeFrame err = %v, want %v", err, sentinel)
	}
}

type errWriter struct{ err error }

func (w *errWriter) Write(p []byte) (int, error) { return 0, w.err }

// TestErrorStringsStable locks in the user-visible prefix for two
// load-bearing errors so consumers can match on them if they need to.
func TestErrorStringsStable(t *testing.T) {
	t.Parallel()
	if !strings.Contains(ErrFrameTooLarge.Error(), "1 MiB") {
		t.Errorf("ErrFrameTooLarge string = %q, missing '1 MiB'", ErrFrameTooLarge.Error())
	}
	if !strings.HasPrefix(ErrClosed.Error(), "ipcclient:") {
		t.Errorf("ErrClosed string = %q, missing ipcclient prefix", ErrClosed.Error())
	}
}
