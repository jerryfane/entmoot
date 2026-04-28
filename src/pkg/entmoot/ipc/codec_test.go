package ipc

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"

	entmoot "entmoot/pkg/entmoot"
)

// mustGroupID returns a GroupID filled with the given byte for test
// brevity.
func mustGroupID(fill byte) entmoot.GroupID {
	var g entmoot.GroupID
	for i := range g {
		g[i] = fill
	}
	return g
}

// mustMessageID returns a MessageID filled with the given byte.
func mustMessageID(fill byte) entmoot.MessageID {
	var m entmoot.MessageID
	for i := range m {
		m[i] = fill
	}
	return m
}

// newKey generates an Ed25519 keypair for realistic signature-shaped
// bytes.
func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

// roundTrip encodes v, writes and reads through a buffer, decodes, and
// asserts equality via reflect.DeepEqual. Returns the decoded value so
// individual tests can make narrower assertions.
func roundTrip(t *testing.T, v any) any {
	t.Helper()
	var buf bytes.Buffer
	if err := EncodeAndWrite(&buf, v); err != nil {
		t.Fatalf("EncodeAndWrite: %v", err)
	}
	_, got, err := ReadAndDecode(&buf)
	if err != nil {
		t.Fatalf("ReadAndDecode: %v", err)
	}
	if !reflect.DeepEqual(got, v) {
		t.Fatalf("round-trip mismatch\n  got : %#v\n  want: %#v", got, v)
	}
	return got
}

func TestRoundTripPublishReqWithGroup(t *testing.T) {
	gid := mustGroupID(0x11)
	roundTrip(t, &PublishReq{
		GroupID: &gid,
		Topics:  []string{"entmoot/test", "entmoot/demo"},
		Content: []byte("hello from ipc"),
	})
}

func TestRoundTripPublishReqAutoGroup(t *testing.T) {
	// Nil GroupID exercises the "auto-pick" path's wire shape.
	roundTrip(t, &PublishReq{
		GroupID: nil,
		Topics:  []string{"entmoot/auto"},
		Content: []byte("auto"),
	})
}

func TestRoundTripPublishResp(t *testing.T) {
	roundTrip(t, &PublishResp{
		MessageID:   mustMessageID(0xAA),
		GroupID:     mustGroupID(0x22),
		TimestampMS: 1_700_000_000_500,
	})
}

func TestRoundTripSignedPublishReqResp(t *testing.T) {
	pub, priv := newKey(t)
	msg := entmoot.Message{
		ID:      mustMessageID(0xAC),
		GroupID: mustGroupID(0x23),
		Author: entmoot.NodeInfo{
			PilotNodeID:   45491,
			EntmootPubKey: pub,
		},
		Timestamp: 1_700_000_000_700,
		Topics:    []string{"mobile/service"},
		Content:   []byte("phone signed"),
	}
	msg.Signature = ed25519.Sign(priv, []byte("signed-publish-test"))
	roundTrip(t, &SignedPublishReq{Message: msg})
	roundTrip(t, &SignedPublishResp{
		Status:      "accepted",
		MessageID:   msg.ID,
		GroupID:     msg.GroupID,
		Author:      msg.Author.PilotNodeID,
		TimestampMS: msg.Timestamp,
	})
}

func TestRoundTripTailSubscribeAll(t *testing.T) {
	roundTrip(t, &TailSubscribe{})
}

func TestRoundTripTailSubscribeScoped(t *testing.T) {
	gid := mustGroupID(0x33)
	roundTrip(t, &TailSubscribe{
		GroupID: &gid,
		Topic:   "entmoot/#",
	})
}

func TestRoundTripTailEvent(t *testing.T) {
	_, priv := newKey(t)
	pub := priv.Public().(ed25519.PublicKey)
	msg := entmoot.Message{
		ID:      mustMessageID(0xEE),
		GroupID: mustGroupID(0x44),
		Author: entmoot.NodeInfo{
			PilotNodeID:   7,
			EntmootPubKey: pub,
		},
		Timestamp: 1_700_000_000_600,
		Topics:    []string{"entmoot/tail"},
		Content:   []byte("tail body"),
	}
	msg.Signature = ed25519.Sign(priv, []byte("tail-sig-input"))
	roundTrip(t, &TailEvent{Message: msg})
}

func TestRoundTripInfoReq(t *testing.T) {
	roundTrip(t, &InfoReq{})
}

func TestRoundTripInfoResp(t *testing.T) {
	pub, _ := newKey(t)
	var root [32]byte
	for i := range root {
		root[i] = byte(i)
	}
	resp := &InfoResp{
		PilotNodeID:   0xDEADBEEF,
		EntmootPubKey: pub,
		ListenPort:    1004,
		DataDir:       "/home/agent/.entmoot",
		Running:       true,
		Groups: []GroupInfo{
			{
				GroupID:    mustGroupID(0x01),
				Members:    3,
				Messages:   42,
				MerkleRoot: &root,
			},
			{
				// Running=false-per-group shape: nil MerkleRoot
				// must round-trip as nil.
				GroupID:  mustGroupID(0x02),
				Members:  1,
				Messages: 0,
			},
		},
	}
	got := roundTrip(t, resp).(*InfoResp)
	if got.Groups[0].MerkleRoot == nil || *got.Groups[0].MerkleRoot != root {
		t.Fatalf("merkle root mismatch after round-trip")
	}
	if got.Groups[1].MerkleRoot != nil {
		t.Fatalf("nil MerkleRoot should round-trip as nil, got %v", got.Groups[1].MerkleRoot)
	}
}

func TestRoundTripErrorFrame(t *testing.T) {
	gid := mustGroupID(0x55)
	ef := &ErrorFrame{
		// Deliberately leave Type empty; Encode must fill it in.
		Code:    CodeGroupNotFound,
		GroupID: &gid,
		Message: "group is not joined",
	}
	// Encode fills Type, so DeepEqual on the post-encode struct is
	// what we compare against.
	var buf bytes.Buffer
	if err := EncodeAndWrite(&buf, ef); err != nil {
		t.Fatalf("EncodeAndWrite: %v", err)
	}
	if ef.Type != "error" {
		t.Fatalf("Encode did not set Type; got %q", ef.Type)
	}
	_, got, err := ReadAndDecode(&buf)
	if err != nil {
		t.Fatalf("ReadAndDecode: %v", err)
	}
	gotEF, ok := got.(*ErrorFrame)
	if !ok {
		t.Fatalf("payload = %T, want *ErrorFrame", got)
	}
	if !reflect.DeepEqual(gotEF, ef) {
		t.Fatalf("round-trip mismatch\n  got : %#v\n  want: %#v", gotEF, ef)
	}
}

// TestEncodeUnknownType ensures Encode with a struct the codec does not
// know about returns ErrUnknownMessage, for both pointer and value forms.
func TestEncodeUnknownType(t *testing.T) {
	type Bogus struct{ X int }
	_, _, err := Encode(&Bogus{X: 1})
	if !errors.Is(err, ErrUnknownMessage) {
		t.Fatalf("err = %v, want ErrUnknownMessage", err)
	}

	// Value (non-pointer) payloads are also rejected: the codec only
	// accepts pointer forms.
	_, _, err = Encode(PublishReq{})
	if !errors.Is(err, ErrUnknownMessage) {
		t.Fatalf("value-form err = %v, want ErrUnknownMessage", err)
	}

	// nil any falls through the default case — also unknown.
	_, _, err = Encode(nil)
	if !errors.Is(err, ErrUnknownMessage) {
		t.Fatalf("nil-form err = %v, want ErrUnknownMessage", err)
	}
}

// TestDecodeUnknownType exercises bytes outside the ipc namespace
// (0x00, 0xFF) and an unused byte inside the namespace (0x18).
func TestDecodeUnknownType(t *testing.T) {
	for _, b := range []MsgType{0x00, 0x09, 0x18, 0x20, 0xFF} {
		_, err := Decode(b, []byte(`{}`))
		if !errors.Is(err, ErrUnknownMessage) {
			t.Errorf("Decode(0x%02x) err = %v, want ErrUnknownMessage", uint8(b), err)
		}
	}
}

// TestDecodeMalformedJSON ensures bad JSON surfaces as ErrMalformedFrame
// for every non-empty-accepting type.
func TestDecodeMalformedJSON(t *testing.T) {
	types := []MsgType{
		MsgPublishReq, MsgPublishResp,
		MsgSignedPublishReq, MsgSignedPublishResp,
		MsgTailSubscribe, MsgTailEvent,
		MsgInfoResp, MsgError,
	}
	for _, tt := range types {
		_, err := Decode(tt, []byte(`not json`))
		if !errors.Is(err, ErrMalformedFrame) {
			t.Errorf("Decode(%s, bad-json) err = %v, want ErrMalformedFrame", tt, err)
		}
	}
}

// TestDecodeInfoReqEmptyBody confirms InfoReq accepts both "{}" and
// an empty body, since it has no fields.
func TestDecodeInfoReqEmptyBody(t *testing.T) {
	for _, body := range [][]byte{nil, []byte(`{}`)} {
		v, err := Decode(MsgInfoReq, body)
		if err != nil {
			t.Fatalf("Decode(info_req, %q): %v", body, err)
		}
		if _, ok := v.(*InfoReq); !ok {
			t.Fatalf("Decode(info_req): %T, want *InfoReq", v)
		}
	}
}

// TestEncodeAndWriteReadAndDecodePipe runs the end-to-end
// Encode+Write → Read+Decode path over an io.Pipe across goroutines,
// as the daemon will see it at runtime over a net.Conn.
func TestEncodeAndWriteReadAndDecodePipe(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	gid := mustGroupID(0x99)
	req := &PublishReq{
		GroupID: &gid,
		Topics:  []string{"entmoot/pipe"},
		Content: []byte("pipe body"),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var writeErr error
	go func() {
		defer wg.Done()
		writeErr = EncodeAndWrite(pw, req)
		pw.Close()
	}()

	gotType, payload, err := ReadAndDecode(pr)
	if err != nil {
		t.Fatalf("ReadAndDecode: %v", err)
	}
	wg.Wait()
	if writeErr != nil {
		t.Fatalf("EncodeAndWrite: %v", writeErr)
	}
	if gotType != MsgPublishReq {
		t.Fatalf("msg_type = %v, want %v", gotType, MsgPublishReq)
	}
	gotReq, ok := payload.(*PublishReq)
	if !ok {
		t.Fatalf("payload = %T, want *PublishReq", payload)
	}
	if !reflect.DeepEqual(gotReq, req) {
		t.Fatalf("round-trip mismatch\n  got : %#v\n  want: %#v", gotReq, req)
	}
}

// TestReadAndDecodeSurfacesDecodeError confirms that when ReadFrame
// succeeds but Decode fails (malformed JSON for a known type), the
// msg_type is still returned so callers can log context.
func TestReadAndDecodeSurfacesDecodeError(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgPublishReq, []byte(`not json`)); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	tt, v, err := ReadAndDecode(&buf)
	if !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("err = %v, want ErrMalformedFrame", err)
	}
	if tt != MsgPublishReq {
		t.Fatalf("msg_type = %v, want %v", tt, MsgPublishReq)
	}
	if v != nil {
		t.Fatalf("payload = %#v, want nil on decode error", v)
	}
}
