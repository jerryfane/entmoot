package wire

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"reflect"
	"testing"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
)

// mustGroupID returns a GroupID filled with the given byte for test brevity.
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

// mustRosterEntryID returns a RosterEntryID filled with the given byte.
func mustRosterEntryID(fill byte) entmoot.RosterEntryID {
	var r entmoot.RosterEntryID
	for i := range r {
		r[i] = fill
	}
	return r
}

// newKey generates an Ed25519 keypair for realistic signature-shaped bytes.
func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

// roundTrip encodes v, writes and reads through a buffer, decodes, and
// asserts equality via reflect.DeepEqual. It also returns the decoded value
// so individual tests can make narrower assertions.
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

func TestRoundTripHello(t *testing.T) {
	pub, priv := newKey(t)
	msg := &Hello{
		NodeID:    0xDEADBEEF,
		PubKey:    pub,
		Groups:    []entmoot.GroupID{mustGroupID(0x01), mustGroupID(0x02)},
		Timestamp: 1_700_000_000_000,
	}
	// Realistic: sign a random blob so Signature is 64 non-zero bytes.
	msg.Signature = ed25519.Sign(priv, []byte("entmoot-test"))
	roundTrip(t, msg)
}

func TestRoundTripRosterReq(t *testing.T) {
	head := mustRosterEntryID(0x77)
	msg := &RosterReq{
		GroupID:   mustGroupID(0x10),
		SinceHead: &head,
	}
	roundTrip(t, msg)

	// Also cover SinceHead=nil (omitempty path).
	roundTrip(t, &RosterReq{GroupID: mustGroupID(0x10)})
}

func TestRoundTripRosterResp(t *testing.T) {
	pub, priv := newKey(t)
	entry := entmoot.RosterEntry{
		ID:        mustRosterEntryID(0xAB),
		Op:        "add",
		Subject:   entmoot.NodeInfo{PilotNodeID: 42, EntmootPubKey: pub},
		Actor:     1,
		Timestamp: 1_700_000_000_100,
	}
	// Sign a stable byte blob so Signature round-trips byte-for-byte.
	entry.Signature = ed25519.Sign(priv, []byte("roster-entry-sig-input"))

	msg := &RosterResp{
		GroupID: mustGroupID(0x22),
		Entries: []entmoot.RosterEntry{entry},
	}

	got := roundTrip(t, msg).(*RosterResp)
	if len(got.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(got.Entries))
	}
	if !bytes.Equal(got.Entries[0].Signature, entry.Signature) {
		t.Fatalf("signature bytes differ after round-trip")
	}
}

func TestRoundTripGossip(t *testing.T) {
	_, priv := newKey(t)
	msg := &Gossip{
		GroupID:   mustGroupID(0x33),
		IDs:       []entmoot.MessageID{mustMessageID(0xA1), mustMessageID(0xA2)},
		Timestamp: 1_700_000_000_200,
	}
	msg.Signature = ed25519.Sign(priv, []byte("gossip-sig-input"))
	roundTrip(t, msg)

	// v1.0.7: Body absent decodes back as a nil Body (the omitempty
	// path). Explicitly cover this even though the default-gossip case
	// above already exercises it, so a future tag change to `json:"body"`
	// (no omitempty) without updating encode/decode is caught loudly.
	got := roundTrip(t, &Gossip{
		GroupID:   mustGroupID(0x34),
		IDs:       []entmoot.MessageID{mustMessageID(0xB1)},
		Timestamp: 1_700_000_000_201,
		Signature: ed25519.Sign(priv, []byte("gossip-no-body")),
	}).(*Gossip)
	if got.Body != nil {
		t.Fatalf("Body should decode back as nil when omitted, got %#v", got.Body)
	}
}

// TestRoundTripGossipWithBody exercises the v1.0.7 inline-body path:
// encoding a Gossip frame with a populated Body field must survive
// EncodeAndWrite + ReadAndDecode round-tripping byte-for-byte, so
// receivers observe the exact Message the sender inlined.
func TestRoundTripGossipWithBody(t *testing.T) {
	_, priv := newKey(t)
	pub := priv.Public().(ed25519.PublicKey)
	body := entmoot.Message{
		GroupID: mustGroupID(0x77),
		Author: entmoot.NodeInfo{
			PilotNodeID:   9,
			EntmootPubKey: pub,
		},
		Timestamp: 1_700_000_000_500,
		Topics:    []string{"entmoot/inline"},
		Content:   []byte("hello inline"),
	}
	body.ID = canonical.MessageID(body)
	body.Signature = ed25519.Sign(priv, []byte("body-sig-input"))

	msg := &Gossip{
		GroupID:   body.GroupID,
		IDs:       []entmoot.MessageID{body.ID},
		Timestamp: 1_700_000_000_501,
		Body:      &body,
	}
	msg.Signature = ed25519.Sign(priv, []byte("gossip-with-body-sig-input"))

	got := roundTrip(t, msg).(*Gossip)
	if got.Body == nil {
		t.Fatalf("Body should not be nil after round-trip")
	}
	if got.Body.ID != body.ID {
		t.Fatalf("Body.ID differs after round-trip: got %s want %s",
			got.Body.ID, body.ID)
	}
	if !bytes.Equal(got.Body.Signature, body.Signature) {
		t.Fatalf("Body.Signature differs after round-trip")
	}
	if !bytes.Equal(got.Body.Content, body.Content) {
		t.Fatalf("Body.Content differs after round-trip")
	}
}

func TestRoundTripFetchReq(t *testing.T) {
	roundTrip(t, &FetchReq{
		GroupID: mustGroupID(0x44),
		ID:      mustMessageID(0xB0),
	})
}

func TestRoundTripFetchRespNotFound(t *testing.T) {
	msg := &FetchResp{
		GroupID:  mustGroupID(0x55),
		ID:       mustMessageID(0xC0),
		NotFound: true,
	}
	got := roundTrip(t, msg).(*FetchResp)
	if got.Message != nil {
		t.Fatalf("Message should be nil, got %#v", got.Message)
	}
	if !got.NotFound {
		t.Fatalf("NotFound should be true")
	}
}

func TestRoundTripFetchRespMessage(t *testing.T) {
	_, priv := newKey(t)
	pub := priv.Public().(ed25519.PublicKey)
	m := entmoot.Message{
		GroupID: mustGroupID(0x66),
		Author: entmoot.NodeInfo{
			PilotNodeID:   7,
			EntmootPubKey: pub,
		},
		Timestamp: 1_700_000_000_300,
		Topics:    []string{"entmoot/test"},
		Parents:   []entmoot.MessageID{mustMessageID(0xD1)},
		Content:   []byte("hello, entmoot"),
	}
	// Compute a realistic id using canonical encoding; this makes the
	// round-trip assertion meaningful.
	m.ID = canonical.MessageID(m)
	m.Signature = ed25519.Sign(priv, []byte("msg-sig-input"))

	msg := &FetchResp{
		GroupID: m.GroupID,
		ID:      m.ID,
		Message: &m,
	}
	got := roundTrip(t, msg).(*FetchResp)
	if got.Message == nil {
		t.Fatalf("Message should not be nil")
	}
	if got.Message.ID != m.ID {
		t.Fatalf("Message.ID differs")
	}
	if !bytes.Equal(got.Message.Signature, m.Signature) {
		t.Fatalf("Message.Signature differs")
	}
}

func TestRoundTripMerkleReq(t *testing.T) {
	roundTrip(t, &MerkleReq{GroupID: mustGroupID(0x77)})
}

func TestRoundTripMerkleResp(t *testing.T) {
	var root MerkleRoot
	for i := range root {
		root[i] = byte(i)
	}
	roundTrip(t, &MerkleResp{
		GroupID:      mustGroupID(0x88),
		Root:         root,
		MessageCount: 42,
	})
}

// TestEncodeUnknownType ensures that an Encode call with a struct the codec
// does not know about returns entmoot.ErrUnknownMessage.
func TestEncodeUnknownType(t *testing.T) {
	type Bogus struct{ X int }
	_, _, err := Encode(&Bogus{X: 1})
	if !errors.Is(err, entmoot.ErrUnknownMessage) {
		t.Fatalf("err = %v, want ErrUnknownMessage", err)
	}

	// Value (non-pointer) payloads are also rejected: the codec only accepts
	// pointer forms.
	_, _, err = Encode(Hello{})
	if !errors.Is(err, entmoot.ErrUnknownMessage) {
		t.Fatalf("value-form err = %v, want ErrUnknownMessage", err)
	}
}

// TestDecodeUnknownType exercises the two sentinel unknown bytes: the
// reserved 0x00 and 0xFF (which will never be assigned a meaning without
// exhausting the entire u8 space).
func TestDecodeUnknownType(t *testing.T) {
	for _, b := range []MsgType{0x00, 0xFF} {
		_, err := Decode(b, []byte(`{}`))
		if !errors.Is(err, entmoot.ErrUnknownMessage) {
			t.Errorf("Decode(0x%02x) err = %v, want ErrUnknownMessage", uint8(b), err)
		}
	}
}

// TestDecodeMalformedJSON ensures bad JSON surfaces as ErrMalformedFrame.
func TestDecodeMalformedJSON(t *testing.T) {
	_, err := Decode(MsgHello, []byte(`not json`))
	if !errors.Is(err, entmoot.ErrMalformedFrame) {
		t.Fatalf("bad-json err = %v, want ErrMalformedFrame", err)
	}
}

// TestDecodeEmptyBody ensures an empty body is rejected. None of the v0
// message types permit it.
func TestDecodeEmptyBody(t *testing.T) {
	_, err := Decode(MsgHello, []byte{})
	if !errors.Is(err, entmoot.ErrMalformedFrame) {
		t.Fatalf("empty-body err = %v, want ErrMalformedFrame", err)
	}
}

// TestEncodeAndWriteReadAndDecodeHello is the scripted end-to-end path the
// parent plan calls out explicitly.
func TestEncodeAndWriteReadAndDecodeHello(t *testing.T) {
	pub, priv := newKey(t)
	h := &Hello{
		NodeID:    0xCAFEBABE,
		PubKey:    pub,
		Groups:    []entmoot.GroupID{mustGroupID(0x01)},
		Timestamp: 1_700_000_001_234,
	}
	h.Signature = ed25519.Sign(priv, []byte("hello-sig-input"))

	var buf bytes.Buffer
	if err := EncodeAndWrite(&buf, h); err != nil {
		t.Fatalf("EncodeAndWrite: %v", err)
	}
	gotType, payload, err := ReadAndDecode(&buf)
	if err != nil {
		t.Fatalf("ReadAndDecode: %v", err)
	}
	if gotType != MsgHello {
		t.Fatalf("msg_type = %v, want %v", gotType, MsgHello)
	}
	gotHello, ok := payload.(*Hello)
	if !ok {
		t.Fatalf("payload = %T, want *Hello", payload)
	}
	if !reflect.DeepEqual(gotHello, h) {
		t.Fatalf("hello differs after round-trip\n  got:  %#v\n  want: %#v", gotHello, h)
	}
}

// TestMerkleRootJSONShape verifies that MerkleRoot marshals as a base64
// string, not Go's default numeric-array form, so the wire shape stays
// debuggable.
func TestMerkleRootJSONShape(t *testing.T) {
	var root MerkleRoot
	for i := range root {
		root[i] = byte(i)
	}
	b, err := root.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	// First char should be a quote (string), not '['.
	if len(b) == 0 || b[0] != '"' {
		t.Fatalf("MerkleRoot JSON = %s, want string form", b)
	}
}
