package wire

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	entmoot "entmoot/pkg/entmoot"
)

// MsgType is the 1-byte message type tag in a frame's framing header. Zero
// (0x00) is reserved as "unknown" so that a freshly-allocated MsgType does
// not accidentally alias a valid message and is always rejected by Decode.
type MsgType uint8

// Registered v0 message types. ARCHITECTURE §4 lists these. announce_group
// is deferred from v0 per the plan's "Deferred from v0" section and has no
// type constant here.
const (
	// MsgHello announces the sender's identity and the groups it participates
	// in. Bidirectional; first message on every connection.
	MsgHello MsgType = 0x01
	// MsgRosterReq requests the current roster for a group.
	MsgRosterReq MsgType = 0x02
	// MsgRosterResp carries a list of signed roster entries for a group.
	MsgRosterResp MsgType = 0x03
	// MsgGossip pushes one or more message ids (hashes only) for a group.
	MsgGossip MsgType = 0x04
	// MsgFetchReq requests a single message body by id.
	MsgFetchReq MsgType = 0x05
	// MsgFetchResp carries either the requested message body or a not-found
	// flag.
	MsgFetchResp MsgType = 0x06
	// MsgMerkleReq asks for the current Merkle root over a group's messages.
	MsgMerkleReq MsgType = 0x07
	// MsgMerkleResp returns the current Merkle root and message count.
	MsgMerkleResp MsgType = 0x08
)

// String returns the human-readable wire name for t, suitable for logs. It
// returns "unknown(0xNN)" for any unregistered type byte.
func (t MsgType) String() string {
	switch t {
	case MsgHello:
		return "hello"
	case MsgRosterReq:
		return "roster_req"
	case MsgRosterResp:
		return "roster_resp"
	case MsgGossip:
		return "gossip"
	case MsgFetchReq:
		return "fetch_req"
	case MsgFetchResp:
		return "fetch_resp"
	case MsgMerkleReq:
		return "merkle_req"
	case MsgMerkleResp:
		return "merkle_resp"
	default:
		return fmt.Sprintf("unknown(0x%02x)", uint8(t))
	}
}

// MerkleRoot is a 32-byte hash that marshals as a base64 JSON string rather
// than Go's default array-of-numbers encoding. It mirrors the treatment
// GroupID and MessageID get in pkg/entmoot/types.go. Use this named type in
// wire payloads whenever a raw 32-byte root needs to travel over JSON;
// entmoot.Group.MerkleRoot keeps its [32]byte shape and is re-marshaled by
// Group's own MarshalJSON.
type MerkleRoot [32]byte

// String returns the base64 (standard encoding, with padding) form of the
// root.
func (r MerkleRoot) String() string {
	return base64.StdEncoding.EncodeToString(r[:])
}

// MarshalJSON encodes the root as a base64 JSON string.
func (r MerkleRoot) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// UnmarshalJSON decodes a base64 JSON string into the 32-byte root.
func (r *MerkleRoot) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("MerkleRoot: expected base64 string: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("MerkleRoot: invalid base64: %w", err)
	}
	if len(raw) != len(r) {
		return fmt.Errorf("MerkleRoot: expected %d bytes, got %d", len(r), len(raw))
	}
	copy(r[:], raw)
	return nil
}

// Hello announces the sender's identity and the groups it participates in.
// It is the first message on every connection and is signed by the sender's
// Entmoot Ed25519 pubkey (not Pilot's — see ARCHITECTURE §2.5 / plan
// assumption 2). Receivers verify the signature over the canonical encoding
// of the Hello with Signature zeroed.
type Hello struct {
	// NodeID is the sender's Pilot node id.
	NodeID entmoot.NodeID `json:"node_id"`
	// PubKey is the sender's raw Ed25519 Entmoot public key (32 bytes).
	PubKey []byte `json:"pubkey"`
	// Groups is the list of groups the sender participates in and is willing
	// to exchange roster / gossip / fetch messages for on this connection.
	Groups []entmoot.GroupID `json:"groups"`
	// Timestamp is unix milliseconds at send time. Replay protection in B2
	// uses this against the 5-minute past / 30-second future window.
	Timestamp int64 `json:"timestamp"`
	// Signature is Ed25519 over the canonical encoding of the Hello with
	// Signature zeroed.
	Signature []byte `json:"signature"`
}

// RosterReq requests the current roster for a group. Optional SinceHead
// allows future delta responses; in v0 responders ignore it and return all.
type RosterReq struct {
	// GroupID identifies the target group.
	GroupID entmoot.GroupID `json:"group_id"`
	// SinceHead, when set, hints to the responder that the requester already
	// holds the roster up to and including this entry id. v0 responders
	// ignore this and return the full roster.
	SinceHead *entmoot.RosterEntryID `json:"since_head,omitempty"`
}

// RosterResp carries a list of roster entries in apply order. Each entry is
// already individually signed by the founder (v0 admin model), so RosterResp
// itself is not re-signed — the responder is not a claimant of authorship,
// only a relayer.
type RosterResp struct {
	// GroupID identifies the group the entries belong to.
	GroupID entmoot.GroupID `json:"group_id"`
	// Entries is the ordered slice of signed roster entries.
	Entries []entmoot.RosterEntry `json:"entries"`
}

// Gossip pushes message ids (hashes only) for a group. The receiver pulls
// bodies via FetchReq for ids it has not already seen. Gossip is signed by
// the sender's Entmoot pubkey so replay dedupe can be scoped per-sender.
type Gossip struct {
	// GroupID identifies the owning group.
	GroupID entmoot.GroupID `json:"group_id"`
	// IDs is the list of message ids being advertised.
	IDs []entmoot.MessageID `json:"ids"`
	// Timestamp is unix milliseconds at send time; used by B2's replay
	// window.
	Timestamp int64 `json:"timestamp"`
	// Signature is Ed25519 over the canonical encoding of the Gossip with
	// Signature zeroed.
	Signature []byte `json:"signature"`
}

// FetchReq requests a single message body by id.
type FetchReq struct {
	// GroupID identifies the owning group.
	GroupID entmoot.GroupID `json:"group_id"`
	// ID is the message id being requested.
	ID entmoot.MessageID `json:"id"`
}

// FetchResp carries either the requested message or a NotFound flag. Callers
// set either Message (present) or NotFound=true (absent); both fields may be
// zero on the wire when serializing a "hit" with omitempty-style Message.
type FetchResp struct {
	// GroupID identifies the owning group.
	GroupID entmoot.GroupID `json:"group_id"`
	// ID is the requested message id.
	ID entmoot.MessageID `json:"id"`
	// Message is the populated message body. nil when NotFound is true.
	Message *entmoot.Message `json:"message,omitempty"`
	// NotFound is true when the responder does not have ID for this group.
	NotFound bool `json:"not_found,omitempty"`
}

// MerkleReq asks for the current Merkle root over a group's messages.
type MerkleReq struct {
	// GroupID identifies the owning group.
	GroupID entmoot.GroupID `json:"group_id"`
}

// MerkleResp returns the current Merkle root and the number of messages the
// responder considers part of that tree. v0 does not include proofs; the
// canary compares roots directly. Subsequent fetches resolve any diff.
type MerkleResp struct {
	// GroupID identifies the owning group.
	GroupID entmoot.GroupID `json:"group_id"`
	// Root is the current Merkle root, base64-encoded on the wire via the
	// MerkleRoot named-type JSON methods.
	Root MerkleRoot `json:"root"`
	// MessageCount is the number of message ids that went into Root.
	MessageCount int `json:"message_count"`
}
