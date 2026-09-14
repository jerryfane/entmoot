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

// NodeID is retained only for immutable Pilot-era signed records and upgrade mappings.
type NodeID uint32

// MemberID is the full-width application identity derived from an Entmoot
// Ed25519 public key.
type MemberID [32]byte

func (m MemberID) String() string {
	return base64.StdEncoding.EncodeToString(m[:])
}

func (m MemberID) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

func (m *MemberID) UnmarshalJSON(data []byte) error {
	return decodeBase64Array32("MemberID", data, m[:])
}

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

// NodeInfo binds an Entmoot signing key to its application and transport
// identities. PilotNodeID is serialized only for immutable legacy records.
type NodeInfo struct {
	// PilotNodeID is retained solely for exact legacy signed encodings.
	PilotNodeID NodeID `json:"pilot_node_id,omitempty"`
	// EntmootPubKey is the raw Ed25519 public key (32 bytes). encoding/json
	// marshals []byte as base64 automatically.
	EntmootPubKey []byte `json:"entmoot_pubkey"`
	// MemberID is present on Pilot-independent versioned records.
	MemberID *MemberID `json:"member_id,omitempty"`
	// PeerID is the same-key libp2p identity for operational records.
	PeerID string `json:"peer_id,omitempty"`
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
	// ID is sha256(canonical author-signed form with ID and Signature zeroed).
	ID MessageID `json:"id"`
	// Version is zero for legacy messages and 2 for the group-bound signing
	// form.
	Version uint8 `json:"version,omitempty"`
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
	// RosterHead is the group-bound roster checkpoint under which the author
	// was admitted. It is absent only on legacy v1 messages.
	RosterHead *RosterEntryID `json:"roster_head,omitempty"`
	// Signature authenticates the message signing form, including RosterHead.
	// Membership at RosterHead is the only authority a message needs: a member
	// publishes on its own signature, and moderation is roster removal.
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
	// Actor is retained only for immutable version-0 records.
	Actor NodeID `json:"actor,omitempty"`
	// ActorMemberID identifies the signer of version-2 records.
	ActorMemberID *MemberID `json:"actor_member_id,omitempty"`
	// Timestamp is unix milliseconds when the entry was produced.
	Timestamp int64 `json:"timestamp"`
	// Parents are previous heads being superseded.
	Parents []RosterEntryID `json:"parents,omitempty"`
	// Version selects the signed roster-entry format. Zero is the byte-exact
	// legacy format; new entries use version 2.
	Version uint8 `json:"version,omitempty"`
	// GroupID binds version-2 entries to one group. It is a pointer so the
	// field is absent from legacy JSON and legacy signatures remain stable.
	GroupID *GroupID `json:"group_id,omitempty"`
	// Sequence is the one-based position of a version-2 entry in its linear
	// roster chain.
	Sequence uint64 `json:"sequence,omitempty"`
	// Signature is the Ed25519 signature over canonical.RosterEntrySigningBytes.
	Signature []byte `json:"signature,omitempty"`
}

// Filter is a set of MQTT-style topic patterns. A message matches a filter
// if any pattern matches its topics (match semantics live in the topic
// package, added in phase A1).
type Filter []string

// NodeEndpoint describes one transport endpoint (network+addr). Used in
// BootstrapPeer.Endpoints (invite-embedded hints for newcomers) and
// wire.TransportAd.Endpoints (gossiped advertisements). Shape mirrors
// registry.NodeEndpoint for easy cross-serialisation. (v1.2.0)
type NodeEndpoint struct {
	Network string `json:"network"`
	Addr    string `json:"addr"`
}

// BootstrapPeer names one candidate peer for joining a group. Hostname is
// optional and only useful when the dialer wants a direct address hint.
type BootstrapPeer struct {
	// NodeID is the Pilot node id of the peer.
	NodeID NodeID `json:"node_id"`
	// Hostname is an optional address hint (e.g. IP:port or DNS name).
	Hostname string `json:"hostname,omitempty"`
	// Endpoints is an optional list of transport endpoints the peer is
	// reachable at. Added in v1.2.0 so invites can embed authoritative
	// address hints that don't require the joiner to consult a Pilot
	// registry. omitempty is load-bearing: legacy invites without the
	// field produce the same canonical bytes they always did, so their
	// signatures continue to verify after the upgrade. (v1.2.0)
	Endpoints []NodeEndpoint `json:"endpoints,omitempty"`
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
	// ValidUntil is the unix-milliseconds timestamp after which the invite
	// must be rejected by joiners. Zero means "no expiry asserted" and is
	// only accepted for backwards compatibility with pre-v1 bundles and
	// test fixtures; real v1 issuers always set it (default IssuedAt + 24h
	// per docs/CLI_DESIGN.md §9).
	ValidUntil int64 `json:"valid_until"`
	// Issuer is the group member that produced this invite (often but not
	// always the founder).
	Issuer NodeInfo `json:"issuer"`
	// Signature is the Ed25519 signature over the canonical encoding of the
	// invite with Signature zeroed, signed by the issuer.
	Signature []byte `json:"signature,omitempty"`
}

// BootstrapCapability is an issuer-signed, expiring grant for a bounded set of
// bootstrap endpoints. Target fields bind it to one fresh identity; leaving
// them empty makes it an open invite that any holder may redeem. MaxUses caps
// how many distinct identities may enroll with it (absent or zero means one).
//
// Founder is the group's trust anchor, which the joiner pins. Issuer is the
// member that actually signed the grant: absent when the founder issued it,
// and otherwise a delegated admin, so a group can admit members while the
// founder is away.
type BootstrapCapability struct {
	GroupID           GroupID       `json:"group_id"`
	TargetPublicKey   []byte        `json:"target_public_key,omitempty"`
	TargetMemberID    MemberID      `json:"target_member_id,omitempty"`
	TargetPeerID      string        `json:"target_peer_id,omitempty"`
	Founder           NodeInfo      `json:"founder"`
	Issuer            *NodeInfo     `json:"issuer,omitempty"`
	RosterHead        RosterEntryID `json:"roster_head"`
	AllowedPeerIDs    []string      `json:"allowed_peer_ids,omitempty"`
	AllowedMultiaddrs []string      `json:"allowed_multiaddrs,omitempty"`
	MaxUses           int           `json:"max_uses,omitempty"`
	Nonce             [32]byte      `json:"nonce"`
	IssuedAtMS        int64         `json:"issued_at_ms"`
	ExpiresAtMS       int64         `json:"expires_at_ms"`
	Signature         []byte        `json:"signature,omitempty"`
}

// SigningAuthority returns the identity whose key signs and is answerable for
// this grant: the delegated issuer when present, otherwise the founder.
func (c BootstrapCapability) SigningAuthority() NodeInfo {
	if c.Issuer != nil {
		return *c.Issuer
	}
	return c.Founder
}

// IsOpenInvite reports whether the capability is unbound to a single target
// identity, so any holder may redeem it while uses remain.
func (c BootstrapCapability) IsOpenInvite() bool {
	return len(c.TargetPublicKey) == 0
}

// Uses returns the number of distinct identities permitted to enroll with this
// capability. Zero or negative MaxUses means one.
func (c BootstrapCapability) Uses() int {
	if c.MaxUses <= 0 {
		return 1
	}
	return c.MaxUses
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
