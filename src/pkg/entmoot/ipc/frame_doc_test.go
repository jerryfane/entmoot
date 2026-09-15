package ipc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"
)

// TestFrameLayoutMatchesTheDocumentedFormat pins the wire format that
// docs/CLI_DESIGN.md §5.2 publishes to anyone writing a client:
// [4-byte big-endian length][1-byte message type][JSON body], with the length
// counting the type byte. A client built to a wrong description sends a body
// the daemon reads as a type byte, so the doc and this test must agree.
func TestFrameLayoutMatchesTheDocumentedFormat(t *testing.T) {
	body := []byte(`{"type":"info"}`)
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgInfoReq, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	raw := buf.Bytes()
	if len(raw) != 4+1+len(body) {
		t.Fatalf("frame is %d bytes, want 4 prefix + 1 type + %d body", len(raw), len(body))
	}
	if got := binary.BigEndian.Uint32(raw[:4]); got != uint32(1+len(body)) {
		t.Fatalf("length prefix = %d, want %d (type byte plus body)", got, 1+len(body))
	}
	if MsgType(raw[4]) != MsgInfoReq {
		t.Fatalf("byte 5 = %#x, want the message type %#x", raw[4], byte(MsgInfoReq))
	}
	if !json.Valid(raw[5:]) || !bytes.Equal(raw[5:], body) {
		t.Fatalf("bytes after the type byte = %q, want the JSON body", raw[5:])
	}
}
