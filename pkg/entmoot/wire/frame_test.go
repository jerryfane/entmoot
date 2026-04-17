package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"

	entmoot "entmoot/pkg/entmoot"
)

// TestWriteReadFrameRoundTrip covers writing then reading frames of
// realistic sizes: a 1-byte body, a 100-byte body, a 1 KiB body, and a
// 1 MiB body. Each must come back with the same msg_type and byte-identical
// body.
func TestWriteReadFrameRoundTrip(t *testing.T) {
	sizes := []int{1, 100, 1024, 1024 * 1024}
	for _, sz := range sizes {
		body := make([]byte, sz)
		for i := range body {
			body[i] = byte(i % 251)
		}

		var buf bytes.Buffer
		if err := WriteFrame(&buf, MsgGossip, body); err != nil {
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
		if gotType != MsgGossip {
			t.Fatalf("size=%d: msg_type = %v, want %v", sz, gotType, MsgGossip)
		}
		if !bytes.Equal(gotBody, body) {
			t.Fatalf("size=%d: body differs (len=%d vs %d)", sz, len(gotBody), len(body))
		}
	}
}

// TestWriteFrameRejectsOversized verifies that WriteFrame returns
// entmoot.ErrOversized when the payload would exceed MaxFrameSize.
func TestWriteFrameRejectsOversized(t *testing.T) {
	// payload = 1 + len(body). We need 1+len(body) > MaxFrameSize.
	// Simplest: len(body) == MaxFrameSize, so payload == MaxFrameSize+1.
	body := make([]byte, MaxFrameSize)
	var buf bytes.Buffer
	err := WriteFrame(&buf, MsgGossip, body)
	if !errors.Is(err, entmoot.ErrOversized) {
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
	if err := WriteFrame(&buf, MsgGossip, body); err != nil {
		t.Fatalf("at-max WriteFrame: %v", err)
	}
	gotType, gotBody, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("at-max ReadFrame: %v", err)
	}
	if gotType != MsgGossip || len(gotBody) != len(body) {
		t.Fatalf("at-max round-trip mismatch (type=%v, body=%d)", gotType, len(gotBody))
	}
}

// TestReadFrameRejectsOversizedLengthPrefix verifies that ReadFrame returns
// entmoot.ErrOversized when the 4-byte length prefix encodes a value larger
// than MaxFrameSize, without reading the purported body.
func TestReadFrameRejectsOversizedLengthPrefix(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(MaxFrameSize+1))
	r := bytes.NewReader(header[:])
	_, _, err := ReadFrame(r)
	if !errors.Is(err, entmoot.ErrOversized) {
		t.Fatalf("err = %v, want ErrOversized", err)
	}
}

// TestReadFrameRejectsZeroLength verifies that a length prefix of 0 is
// rejected with entmoot.ErrMalformedFrame — a frame must contain at least
// the 1-byte msg_type.
func TestReadFrameRejectsZeroLength(t *testing.T) {
	var header [4]byte
	// length == 0 means no type byte either; malformed.
	r := bytes.NewReader(header[:])
	_, _, err := ReadFrame(r)
	if !errors.Is(err, entmoot.ErrMalformedFrame) {
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
//   - 2/4 bytes of the length prefix (mid-prefix).
//   - full prefix but no type byte (mid-frame, body truncated to 0).
//   - full prefix + type byte but body is shorter than advertised.
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
			// length=5 → 1 type byte + 4-byte body, but only 2 body bytes here.
			data: []byte{0x00, 0x00, 0x00, 0x05, byte(MsgHello), 0x01, 0x02},
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
// delivers writes to the reader goroutine without buffering. This confirms
// that ReadFrame handles the wire as a stream (not a single Write) and that
// WriteFrame's single Write on the writer side is consumed correctly.
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
		writeErr = WriteFrame(pw, MsgFetchReq, body)
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
	if gotType != MsgFetchReq {
		t.Fatalf("msg_type = %v, want %v", gotType, MsgFetchReq)
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("body differs")
	}
}

// TestMsgTypeString covers the String() lookup for every defined constant
// and the unknown-fallback.
func TestMsgTypeString(t *testing.T) {
	cases := map[MsgType]string{
		MsgHello:      "hello",
		MsgRosterReq:  "roster_req",
		MsgRosterResp: "roster_resp",
		MsgGossip:     "gossip",
		MsgFetchReq:   "fetch_req",
		MsgFetchResp:  "fetch_resp",
		MsgMerkleReq:  "merkle_req",
		MsgMerkleResp: "merkle_resp",
	}
	for t_, want := range cases {
		if got := t_.String(); got != want {
			t.Errorf("MsgType(0x%02x).String() = %q, want %q", uint8(t_), got, want)
		}
	}
	if got := MsgType(0x00).String(); got != "unknown(0x00)" {
		t.Errorf("zero String() = %q", got)
	}
	if got := MsgType(0xFF).String(); got != "unknown(0xff)" {
		t.Errorf("0xFF String() = %q", got)
	}
}
