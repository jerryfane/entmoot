package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
)

// TestWriteReadFrameRoundTrip covers writing then reading frames of
// realistic sizes: a 1-byte body, a 100-byte body, a 1 KiB body, and a
// 1 MiB body. Each must come back with the same msg_type and
// byte-identical body.
func TestWriteReadFrameRoundTrip(t *testing.T) {
	sizes := []int{1, 100, 1024, 1024 * 1024}
	for _, sz := range sizes {
		body := make([]byte, sz)
		for i := range body {
			body[i] = byte(i % 251)
		}

		var buf bytes.Buffer
		if err := WriteFrame(&buf, MsgPublishReq, body); err != nil {
			t.Fatalf("size=%d: WriteFrame: %v", sz, err)
		}

		// Length prefix sanity: should be 1+sz.
		gotLen := binary.BigEndian.Uint32(buf.Bytes()[:4])
		if int(gotLen) != 1+sz {
			t.Fatalf("size=%d: length prefix = %d, want %d", sz, gotLen, 1+sz)
		}

		gotType, gotBody, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("size=%d: ReadFrame: %v", sz, err)
		}
		if gotType != MsgPublishReq {
			t.Fatalf("size=%d: msg_type = %v, want %v", sz, gotType, MsgPublishReq)
		}
		if !bytes.Equal(gotBody, body) {
			t.Fatalf("size=%d: body differs (len=%d vs %d)", sz, len(gotBody), len(body))
		}
	}
}

// TestWriteFrameRejectsOversized verifies that WriteFrame returns
// ErrOversized when the payload would exceed MaxFrameSize and does not
// write any bytes to the underlying writer.
func TestWriteFrameRejectsOversized(t *testing.T) {
	// payload = 1 + len(body). We need 1+len(body) > MaxFrameSize.
	// Simplest: len(body) == MaxFrameSize, so payload == MaxFrameSize+1.
	body := make([]byte, MaxFrameSize)
	var buf bytes.Buffer
	err := WriteFrame(&buf, MsgPublishReq, body)
	if !errors.Is(err, ErrOversized) {
		t.Fatalf("err = %v, want ErrOversized", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("buffer should be untouched, got %d bytes", buf.Len())
	}
}

// TestWriteFrameAtExactMaxSize confirms the boundary — a frame whose
// (1 + body) equals MaxFrameSize is accepted.
func TestWriteFrameAtExactMaxSize(t *testing.T) {
	body := make([]byte, MaxFrameSize-1)
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgPublishReq, body); err != nil {
		t.Fatalf("at-max WriteFrame: %v", err)
	}
	gotType, gotBody, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("at-max ReadFrame: %v", err)
	}
	if gotType != MsgPublishReq || len(gotBody) != len(body) {
		t.Fatalf("at-max round-trip mismatch (type=%v, body=%d)", gotType, len(gotBody))
	}
}

// TestReadFrameRejectsOversizedLengthPrefix verifies that ReadFrame
// returns ErrOversized when the 4-byte length prefix encodes a value
// larger than MaxFrameSize, without reading the purported body.
func TestReadFrameRejectsOversizedLengthPrefix(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(MaxFrameSize+1))
	r := bytes.NewReader(header[:])
	_, _, err := ReadFrame(r)
	if !errors.Is(err, ErrOversized) {
		t.Fatalf("err = %v, want ErrOversized", err)
	}
}

// TestReadFrameRejectsZeroLength verifies that a length prefix of 0 is
// rejected with ErrMalformedFrame — a frame must contain at least the
// 1-byte msg_type.
func TestReadFrameRejectsZeroLength(t *testing.T) {
	var header [4]byte
	r := bytes.NewReader(header[:])
	_, _, err := ReadFrame(r)
	if !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("err = %v, want ErrMalformedFrame", err)
	}
}

// TestReadFrameEOFAtStart: empty reader returns io.EOF (clean close).
func TestReadFrameEOFAtStart(t *testing.T) {
	r := bytes.NewReader(nil)
	_, _, err := ReadFrame(r)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

// TestReadFrameMidFrameEOF covers three truncation points:
//   - 2/4 bytes of the length prefix (mid-prefix)
//   - full prefix but no type byte (body truncated to 0)
//   - full prefix + type byte but body is shorter than advertised
//
// All three should surface io.ErrUnexpectedEOF.
func TestReadFrameMidFrameEOF(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{name: "mid-prefix", data: []byte{0x00, 0x00}},
		{name: "no-type-byte", data: []byte{0x00, 0x00, 0x00, 0x05}},
		{
			name: "short-body",
			// length=5 → 1 type byte + 4-byte body, but only 2 body
			// bytes supplied here.
			data: []byte{0x00, 0x00, 0x00, 0x05, byte(MsgPublishReq), 0x01, 0x02},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.data)
			_, _, err := ReadFrame(r)
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

// TestPartialWritesViaPipe exercises the framing over an io.Pipe, which
// delivers writes to the reader goroutine without buffering. This
// confirms that ReadFrame handles the wire as a stream.
func TestPartialWritesViaPipe(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	body := bytes.Repeat([]byte{0xAB}, 8192)

	var wg sync.WaitGroup
	wg.Add(1)
	var writeErr error
	go func() {
		defer wg.Done()
		writeErr = WriteFrame(pw, MsgInfoResp, body)
		pw.Close()
	}()

	gotType, gotBody, err := ReadFrame(pr)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	wg.Wait()
	if writeErr != nil {
		t.Fatalf("WriteFrame: %v", writeErr)
	}
	if gotType != MsgInfoResp {
		t.Fatalf("msg_type = %v, want %v", gotType, MsgInfoResp)
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("body differs")
	}
}

// TestMsgTypeString covers the String() lookup for every defined
// constant and the unknown-fallback.
func TestMsgTypeString(t *testing.T) {
	cases := map[MsgType]string{
		MsgPublishReq:        "publish_req",
		MsgPublishResp:       "publish_resp",
		MsgSignedPublishReq:  "signed_publish_req",
		MsgSignedPublishResp: "signed_publish_resp",
		MsgTailSubscribe:     "tail_subscribe",
		MsgTailEvent:         "tail_event",
		MsgInfoReq:           "info_req",
		MsgInfoResp:          "info_resp",
		MsgError:             "error",
	}
	for tt, want := range cases {
		if got := tt.String(); got != want {
			t.Errorf("MsgType(0x%02x).String() = %q, want %q", uint8(tt), got, want)
		}
	}
	if got := MsgType(0x00).String(); got != "unknown(0x00)" {
		t.Errorf("zero String() = %q", got)
	}
	if got := MsgType(0xFF).String(); got != "unknown(0xff)" {
		t.Errorf("0xFF String() = %q", got)
	}
	// A byte inside the ipc namespace (0x10..0x1F) that is not
	// registered must also be reported as unknown.
	if got := MsgType(0x18).String(); got != "unknown(0x18)" {
		t.Errorf("0x18 String() = %q", got)
	}
}
