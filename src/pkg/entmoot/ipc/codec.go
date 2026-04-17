package ipc

import (
	"encoding/json"
	"fmt"
	"io"
)

// Encode marshals a typed payload to its JSON body bytes and returns
// the matching MsgType. It does not frame the result — callers pass the
// body to WriteFrame, or use EncodeAndWrite for the combined path.
//
// Supported payloads (pointers only): *PublishReq, *PublishResp,
// *TailSubscribe, *TailEvent, *InfoReq, *InfoResp, *ErrorFrame. Anything
// else returns ErrUnknownMessage.
//
// As a convenience, Encode fills in ErrorFrame.Type = "error" when the
// caller left it empty. The wire shape is defined to always carry that
// field (CLI_DESIGN §5.4), so we do not require callers to remember it.
func Encode(v any) (MsgType, []byte, error) {
	var t MsgType
	switch payload := v.(type) {
	case *PublishReq:
		t = MsgPublishReq
	case *PublishResp:
		t = MsgPublishResp
	case *TailSubscribe:
		t = MsgTailSubscribe
	case *TailEvent:
		t = MsgTailEvent
	case *InfoReq:
		t = MsgInfoReq
	case *InfoResp:
		t = MsgInfoResp
	case *ErrorFrame:
		t = MsgError
		if payload.Type == "" {
			payload.Type = "error"
		}
	default:
		return 0, nil, fmt.Errorf("ipc: encode %T: %w", v, ErrUnknownMessage)
	}

	body, err := json.Marshal(v)
	if err != nil {
		return 0, nil, fmt.Errorf("ipc: marshal %s: %w", t, err)
	}
	return t, body, nil
}

// Decode parses body as the payload for message type t and returns the
// typed value boxed in any. Callers type-switch on the concrete pointer
// type (e.g. *PublishResp, *ErrorFrame).
//
// Returns ErrUnknownMessage for an unrecognized t (0x00, any byte
// outside the 0x10..0x1F ipc namespace, or an unused byte within it) and
// ErrMalformedFrame when json.Unmarshal fails.
//
// InfoReq's body is "{}"; an empty body is also tolerated for InfoReq so
// a sloppy client does not trip ErrMalformedFrame. All other types
// require a valid JSON body.
func Decode(t MsgType, body []byte) (any, error) {
	switch t {
	case MsgPublishReq:
		return decodeAs[PublishReq](t, body)
	case MsgPublishResp:
		return decodeAs[PublishResp](t, body)
	case MsgTailSubscribe:
		return decodeAs[TailSubscribe](t, body)
	case MsgTailEvent:
		return decodeAs[TailEvent](t, body)
	case MsgInfoReq:
		// InfoReq is an empty struct; accept either "{}" or "" for
		// forward tolerance.
		if len(body) == 0 {
			return &InfoReq{}, nil
		}
		return decodeAs[InfoReq](t, body)
	case MsgInfoResp:
		return decodeAs[InfoResp](t, body)
	case MsgError:
		return decodeAs[ErrorFrame](t, body)
	default:
		return nil, fmt.Errorf("ipc: type 0x%02x: %w", uint8(t), ErrUnknownMessage)
	}
}

// decodeAs unmarshals body into a fresh *T and wraps parse errors in
// ErrMalformedFrame so callers can distinguish "bad JSON" from
// "unknown type".
func decodeAs[T any](t MsgType, body []byte) (any, error) {
	out := new(T)
	if err := json.Unmarshal(body, out); err != nil {
		return nil, fmt.Errorf("ipc: unmarshal %s: %w: %v", t, ErrMalformedFrame, err)
	}
	return out, nil
}

// EncodeAndWrite is Encode followed by WriteFrame. It is the convenience
// entry point for call sites that have a typed payload and a writer
// (a net.Conn, a bytes.Buffer in tests, etc). Returns any error from
// either step, unwrapped.
func EncodeAndWrite(w io.Writer, v any) error {
	t, body, err := Encode(v)
	if err != nil {
		return err
	}
	return WriteFrame(w, t, body)
}

// ReadAndDecode is ReadFrame followed by Decode. It returns the
// msg_type, the typed payload, and any error. Callers type-switch on
// the returned any to dispatch handlers. The msg_type is also returned
// on Decode failure (with a nil payload and a non-nil error) so callers
// can log "malformed publish_req" rather than "malformed frame".
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
