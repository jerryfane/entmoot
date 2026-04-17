// Package entmoot defines the core data model shared by every sub-package:
// group identifiers, messages, roster entries, invites, and supporting types.
//
// All fields carry snake_case JSON tags so wire and on-disk representations
// match the spec in ARCHITECTURE.md. 32-byte identifier types marshal as
// base64 strings rather than Go's default array-of-numbers encoding.
package entmoot

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// NodeID is a typed alias for Pilot's 32-bit node identifier.
type NodeID uint32

// GroupID is the 32-byte random identifier of an Entmoot group.
//
// It is content-independent; two groups with the same name have distinct IDs.
type GroupID [32]byte

// String returns the base64 (standard encoding, with padding) representation
// of the group id.
func (g GroupID) String() string {
	return base64.StdEncoding.EncodeToString(g[:])
}

// MarshalJSON encodes the group id as a base64 JSON string.
func (g GroupID) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.String())
}

// UnmarshalJSON decodes a base64 JSON string into the 32-byte group id.
func (g *GroupID) UnmarshalJSON(data []byte) error {
	return decodeBase64Array32("GroupID", data, g[:])
}

// MessageID is the 32-byte sha256-derived identifier of a message.
type MessageID [32]byte

// String returns the base64 representation of the message id.
func (m MessageID) String() string {
	return base64.StdEncoding.EncodeToString(m[:])
}

// MarshalJSON encodes the message id as a base64 JSON string.
func (m MessageID) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

// UnmarshalJSON decodes a base64 JSON string into the 32-byte message id.
func (m *MessageID) UnmarshalJSON(data []byte) error {
	return decodeBase64Array32("MessageID", data, m[:])
}

// RosterEntryID is the 32-byte identifier of a roster log entry.
type RosterEntryID [32]byte

// String returns the base64 representation of the roster entry id.
func (r RosterEntryID) String() string {
	return base64.StdEncoding.EncodeToString(r[:])
}

// MarshalJSON encodes the roster entry id as a base64 JSON string.
func (r RosterEntryID) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// UnmarshalJSON decodes a base64 JSON string into the 32-byte roster entry id.
func (r *RosterEntryID) UnmarshalJSON(data []byte) error {
	return decodeBase64Array32("RosterEntryID", data, r[:])
}

// merkleRoot is a private helper alias used to give Group.MerkleRoot the same
// base64 JSON treatment as the id types without introducing a new exported
// type.
type merkleRoot [32]byte

// NodeInfo binds a Pilot node id to the Ed25519 public key used for Entmoot
// signatures produced by that node.
type NodeInfo struct {
	// PilotNodeID is the 32-bit Pilot node id.
	PilotNodeID NodeID `json:"pilot_node_id"`
	// EntmootPubKey is the raw Ed25519 public key (32 bytes). encoding/json
	// marshals []byte as base64 automatically.
	EntmootPubKey []byte `json:"entmoot_pubkey"`
}

// Group is the top-level record for an Entmoot group: identity, founder,
// membership policy, current roster head, and current Merkle root.
type Group struct {
	// ID is the 32-byte group identifier.
	ID GroupID `json:"id"`
	// Name is an informational UTF-8 display name (not unique).
	Name string `json:"name"`
	// Founder is the node that created the group; anchors roster signature
	// validation in v0.
	Founder NodeInfo `json:"founder"`
	// Policy is an opaque membership-policy blob. v0 is founder-only; this
	// field is reserved for v1+ multi-admin/quorum schemes.
	Policy json.RawMessage `json:"policy,omitempty"`
	// RosterHead is the id of the current head of the roster log.
	RosterHead RosterEntryID `json:"roster_head"`
	// MerkleRoot is the current root of the group's message Merkle tree.
	MerkleRoot [32]byte `json:"merkle_root"`
}

// MarshalJSON encodes Group with MerkleRoot as base64 rather than a numeric
// array.
func (g Group) MarshalJSON() ([]byte, error) {
	type alias Group
	return json.Marshal(&struct {
		MerkleRoot string `json:"merkle_root"`
		*alias
	}{
		MerkleRoot: base64.StdEncoding.EncodeToString(g.MerkleRoot[:]),
		alias:      (*alias)(&g),
	})
}

// UnmarshalJSON decodes Group, accepting MerkleRoot as a base64 string.
func (g *Group) UnmarshalJSON(data []byte) error {
	type alias Group
	aux := &struct {
		MerkleRoot string `json:"merkle_root"`
		*alias
	}{alias: (*alias)(g)}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	return decodeBase64Into("merkle_root", aux.MerkleRoot, g.MerkleRoot[:])
}

// Message is a single group message. Messages form a DAG via Parents.
type Message struct {
	// ID is sha256(canonical(message with ID and Signature zeroed)).
	ID MessageID `json:"id"`
	// GroupID is the owning group.
	GroupID GroupID `json:"group_id"`
	// Author carries the author's Pilot node id and Ed25519 pubkey.
	Author NodeInfo `json:"author"`
	// Timestamp is unix milliseconds at compose time.
	Timestamp int64 `json:"timestamp"`
	// Topics are MQTT-style hierarchical topic strings used by subscribers.
	Topics []string `json:"topics,omitempty"`
	// Parents are the (at most three, per ARCHITECTURE §3.2) highest-timestamp
	// message ids the author had seen when composing. Genesis messages have
	// an empty slice.
	Parents []MessageID `json:"parents,omitempty"`
	// Content is opaque application bytes.
	Content []byte `json:"content,omitempty"`
	// References are soft application-level links (reply, correction, etc).
	References []MessageID `json:"references,omitempty"`
	// Signature is the Ed25519 signature over the canonical encoding of the
	// message with ID and Signature zeroed.
	Signature []byte `json:"signature,omitempty"`
}

// RosterEntry is one signed record in a group's append-only roster log.
type RosterEntry struct {
	// ID is the roster entry identifier.
	ID RosterEntryID `json:"id"`
	// Op is one of "add", "remove", or "policy_change".
	Op string `json:"op"`
	// Subject is the node being added or removed. For "policy_change" the
	// subject's NodeInfo may be zero and the change lives in Policy.
	Subject NodeInfo `json:"subject"`
	// Policy is the new policy blob for "policy_change" entries.
	Policy json.RawMessage `json:"policy,omitempty"`
	// Actor is the Pilot node id of the signer. v0 requires this to be the
	// founder; all other actors are rejected by the roster layer.
	Actor NodeID `json:"actor"`
	// Timestamp is unix milliseconds when the entry was produced.
	Timestamp int64 `json:"timestamp"`
	// Parents are previous heads being superseded.
	Parents []RosterEntryID `json:"parents,omitempty"`
	// Signature is the Ed25519 signature over the canonical encoding of the
	// entry with Signature zeroed.
	Signature []byte `json:"signature,omitempty"`
}

// Filter is a set of MQTT-style topic patterns. A message matches a filter
// if any pattern matches its topics (match semantics live in the topic
// package, added in phase A1).
type Filter []string

// BootstrapPeer names one candidate peer for joining a group. Hostname is
// optional and only useful when the dialer wants a direct address hint.
type BootstrapPeer struct {
	// NodeID is the Pilot node id of the peer.
	NodeID NodeID `json:"node_id"`
	// Hostname is an optional address hint (e.g. IP:port or DNS name).
	Hostname string `json:"hostname,omitempty"`
}

// Invite is an out-of-band bundle produced by an existing group member that
// lets a new peer join: founder anchor, an authenticated roster head, and a
// short list of reachable bootstrap peers. See ARCHITECTURE §5.1.
type Invite struct {
	// GroupID identifies the target group.
	GroupID GroupID `json:"group_id"`
	// Founder is the group's founder, used to anchor roster signature
	// validation.
	Founder NodeInfo `json:"founder"`
	// RosterHead is the id of the roster head the issuer vouches for.
	RosterHead RosterEntryID `json:"roster_head"`
	// MerkleRoot is the current message Merkle root the issuer advertises.
	MerkleRoot [32]byte `json:"merkle_root"`
	// BootstrapPeers is a short list (3-5) of recently-online members the new
	// node should try first.
	BootstrapPeers []BootstrapPeer `json:"bootstrap_peers"`
	// IssuedAt is the unix-milliseconds timestamp when the invite was produced.
	IssuedAt int64 `json:"issued_at"`
	// Issuer is the group member that produced this invite (often but not
	// always the founder).
	Issuer NodeInfo `json:"issuer"`
	// Signature is the Ed25519 signature over the canonical encoding of the
	// invite with Signature zeroed, signed by the issuer.
	Signature []byte `json:"signature,omitempty"`
}

// MarshalJSON encodes Invite with MerkleRoot as base64 rather than a numeric
// array.
func (i Invite) MarshalJSON() ([]byte, error) {
	type alias Invite
	return json.Marshal(&struct {
		MerkleRoot string `json:"merkle_root"`
		*alias
	}{
		MerkleRoot: base64.StdEncoding.EncodeToString(i.MerkleRoot[:]),
		alias:      (*alias)(&i),
	})
}

// UnmarshalJSON decodes Invite, accepting MerkleRoot as a base64 string.
func (i *Invite) UnmarshalJSON(data []byte) error {
	type alias Invite
	aux := &struct {
		MerkleRoot string `json:"merkle_root"`
		*alias
	}{alias: (*alias)(i)}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	return decodeBase64Into("merkle_root", aux.MerkleRoot, i.MerkleRoot[:])
}

// decodeBase64Array32 unmarshals a base64 JSON string into a 32-byte slice.
// The quoted string must decode to exactly 32 bytes.
func decodeBase64Array32(name string, data []byte, dst []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("%s: expected base64 string: %w", name, err)
	}
	return decodeBase64Into(name, s, dst)
}

// decodeBase64Into decodes s into dst, requiring an exact byte-length match.
func decodeBase64Into(name, s string, dst []byte) error {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("%s: invalid base64: %w", name, err)
	}
	if len(raw) != len(dst) {
		return fmt.Errorf("%s: expected %d bytes, got %d", name, len(dst), len(raw))
	}
	copy(dst, raw)
	return nil
}
