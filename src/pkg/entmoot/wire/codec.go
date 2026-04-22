package wire

import (
	"encoding/json"
	"fmt"
	"io"

	entmoot "entmoot/pkg/entmoot"
)

// Encode marshals a typed payload to its JSON body bytes and returns the
// matching MsgType. It does not frame the result — callers pass the body to
// WriteFrame (or use EncodeAndWrite).
//
// Supported payloads (pointers only): *Hello, *RosterReq, *RosterResp,
// *Gossip, *FetchReq, *FetchResp, *MerkleReq, *MerkleResp. Any other type
// returns entmoot.ErrUnknownMessage.
//
// The pointer-only restriction keeps the API tight and mirrors how callers
// construct messages: `&wire.Hello{...}`. Value types would round-trip
// identically but doubling the type switch buys nothing.
func Encode(v any) (MsgType, []byte, error) {
	var t MsgType
	switch v.(type) {
	case *Hello:
		t = MsgHello
	case *RosterReq:
		t = MsgRosterReq
	case *RosterResp:
		t = MsgRosterResp
	case *Gossip:
		t = MsgGossip
	case *FetchReq:
		t = MsgFetchReq
	case *FetchResp:
		t = MsgFetchResp
	case *MerkleReq:
		t = MsgMerkleReq
	case *MerkleResp:
		t = MsgMerkleResp
	case *RangeReq:
		t = MsgRangeReq
	case *RangeResp:
		t = MsgRangeResp
	case *IHave:
		t = MsgIHave
	case *Graft:
		t = MsgGraft
	case *Prune:
		t = MsgPrune
	case *TransportAd:
		t = MsgTransportAd
	case *TransportSnapshotReq:
		t = MsgTransportSnapshotReq
	case *TransportSnapshotResp:
		t = MsgTransportSnapshotResp
	case *Reconcile:
		t = MsgReconcile
	default:
		return 0, nil, fmt.Errorf("wire: encode %T: %w", v, entmoot.ErrUnknownMessage)
	}

	body, err := json.Marshal(v)
	if err != nil {
		return 0, nil, fmt.Errorf("wire: marshal %s: %w", t, err)
	}
	return t, body, nil
}

// Decode parses body as the payload for message type t and returns the
// typed value boxed in any. Callers type-switch on the concrete pointer
// type (e.g. *Hello, *RosterResp).
//
// Returns entmoot.ErrUnknownMessage for an unrecognized t (including 0x00
// and any byte not listed in the MsgType constants) and
// entmoot.ErrMalformedFrame when json.Unmarshal fails or when body is empty
// (every v0 message type has required fields, so an empty JSON body is
// always wrong).
func Decode(t MsgType, body []byte) (any, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("wire: empty body for %s: %w", t, entmoot.ErrMalformedFrame)
	}

	switch t {
	case MsgHello:
		return decodeAs[Hello](t, body)
	case MsgRosterReq:
		return decodeAs[RosterReq](t, body)
	case MsgRosterResp:
		return decodeAs[RosterResp](t, body)
	case MsgGossip:
		return decodeAs[Gossip](t, body)
	case MsgFetchReq:
		return decodeAs[FetchReq](t, body)
	case MsgFetchResp:
		return decodeAs[FetchResp](t, body)
	case MsgMerkleReq:
		return decodeAs[MerkleReq](t, body)
	case MsgMerkleResp:
		return decodeAs[MerkleResp](t, body)
	case MsgRangeReq:
		return decodeAs[RangeReq](t, body)
	case MsgRangeResp:
		return decodeAs[RangeResp](t, body)
	case MsgIHave:
		return decodeAs[IHave](t, body)
	case MsgGraft:
		return decodeAs[Graft](t, body)
	case MsgPrune:
		return decodeAs[Prune](t, body)
	case MsgTransportAd:
		return decodeAs[TransportAd](t, body)
	case MsgTransportSnapshotReq:
		return decodeAs[TransportSnapshotReq](t, body)
	case MsgTransportSnapshotResp:
		return decodeAs[TransportSnapshotResp](t, body)
	case MsgReconcile:
		return decodeAs[Reconcile](t, body)
	default:
		return nil, fmt.Errorf("wire: type 0x%02x: %w", uint8(t), entmoot.ErrUnknownMessage)
	}
}

// decodeAs unmarshals body into a fresh *T and wraps parse errors in
// entmoot.ErrMalformedFrame so callers can distinguish "bad JSON" from
// "unknown type".
func decodeAs[T any](t MsgType, body []byte) (any, error) {
	out := new(T)
	if err := json.Unmarshal(body, out); err != nil {
		return nil, fmt.Errorf("wire: unmarshal %s: %w: %v", t, entmoot.ErrMalformedFrame, err)
	}
	return out, nil
}

// EncodeAndWrite is Encode followed by WriteFrame. It is the convenience
// entry point for call sites that have a typed payload and a writer (Pilot
// stream, bytes.Buffer in tests, etc). Returns any error from either step.
func EncodeAndWrite(w io.Writer, v any) error {
	t, body, err := Encode(v)
	if err != nil {
		return err
	}
	return WriteFrame(w, t, body)
}

// ReadAndDecode is ReadFrame followed by Decode. It returns the msg_type,
// the typed payload, and any error. Callers type-switch on the returned any
// to dispatch handlers.
//
// Replay protection (Phase B2) and rate limiting (Phase C) are intentionally
// not in this path; they compose by either wrapping ReadAndDecode in an
// outer function (recommended for B2) or by wrapping the io.Reader itself
// (recommended for C).
func ReadAndDecode(r io.Reader) (MsgType, any, error) {
	t, body, err := ReadFrame(r)
	if err != nil {
		return 0, nil, err
	}
	v, err := Decode(t, body)
	if err != nil {
		return t, nil, err
	}
	return t, v, nil
}
