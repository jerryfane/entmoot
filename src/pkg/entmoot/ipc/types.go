package ipc

import (
	"fmt"

	entmoot "entmoot/pkg/entmoot"
)

// MsgType is the 1-byte message type tag in an ipc frame's framing header.
// The ipc namespace uses 0x10..0x1F so frames are visually distinct from
// the peer-wire namespace (0x01..0x08) at the byte level — that aids log
// inspection and hex-dump debugging when both protocols are in play.
// Zero (0x00) is reserved as "unknown" so a freshly-allocated MsgType
// never accidentally aliases a valid message.
type MsgType uint8

// v1 ipc message types. See CLI_DESIGN §5.3. Numbering is stretched to
// leave room for additional types (e.g. a future ping/pong pair) without
// renumbering the existing ones.
const (
	// MsgPublishReq carries an authored message from the client to the
	// daemon for signing, persistence, and gossip.
	MsgPublishReq MsgType = 0x10
	// MsgPublishResp acknowledges a publish with the assigned message id,
	// resolved group id, and server-side timestamp.
	MsgPublishResp MsgType = 0x11
	// MsgTailSubscribe opens a live event stream filtered by group
	// and/or topic.
	MsgTailSubscribe MsgType = 0x12
	// MsgTailEvent delivers one message to a live subscriber.
	MsgTailEvent MsgType = 0x13
	// MsgInfoReq requests a daemon snapshot (identity, listen port,
	// per-group counts).
	MsgInfoReq MsgType = 0x14
	// MsgInfoResp returns the snapshot requested by MsgInfoReq.
	MsgInfoResp MsgType = 0x15
	// MsgSignedPublishReq carries an already-signed message to the daemon for
	// verification, durable persistence, and gossip fanout.
	MsgSignedPublishReq MsgType = 0x16
	// MsgSignedPublishResp acknowledges acceptance of an already-signed
	// message. Fanout remains asynchronous, matching PublishResp semantics.
	MsgSignedPublishResp MsgType = 0x17
	// MsgJoinGroupReq asks the running daemon to add a group session from a
	// signed invite without starting a second entmootd process.
	MsgJoinGroupReq MsgType = 0x18
	// MsgJoinGroupResp acknowledges that a group session exists.
	MsgJoinGroupResp MsgType = 0x19
	// MsgError carries a structured error frame. 0x1F is chosen as the
	// last slot in the 0x10..0x1F namespace so it's easy to spot.
	MsgError MsgType = 0x1F
)

// String returns the human-readable ipc name for t, suitable for logs.
// It returns "unknown(0xNN)" for any unregistered type byte.
func (t MsgType) String() string {
	switch t {
	case MsgPublishReq:
		return "publish_req"
	case MsgPublishResp:
		return "publish_resp"
	case MsgTailSubscribe:
		return "tail_subscribe"
	case MsgTailEvent:
		return "tail_event"
	case MsgInfoReq:
		return "info_req"
	case MsgInfoResp:
		return "info_resp"
	case MsgSignedPublishReq:
		return "signed_publish_req"
	case MsgSignedPublishResp:
		return "signed_publish_resp"
	case MsgJoinGroupReq:
		return "join_group_req"
	case MsgJoinGroupResp:
		return "join_group_resp"
	case MsgError:
		return "error"
	default:
		return fmt.Sprintf("unknown(0x%02x)", uint8(t))
	}
}

// PublishReq is the request body a client sends to author a message in a
// group. GroupID is optional: a nil GroupID means "auto-pick if exactly
// one group is joined" (CLI_DESIGN §3: publish -group is optional when
// there's only one choice). The daemon resolves GroupID before signing.
type PublishReq struct {
	// GroupID names the target group. nil means "auto-pick the single
	// joined group"; an INVALID_ARGUMENT error is returned when nil
	// would be ambiguous.
	GroupID *entmoot.GroupID `json:"group_id,omitempty"`
	// Topics are MQTT-style hierarchical topic strings. A single message
	// can carry multiple topics (CLI_DESIGN §9 decision #4).
	Topics []string `json:"topics"`
	// Content is opaque application bytes. encoding/json base64s it.
	Content []byte `json:"content"`
}

// PublishResp acknowledges a successful publish. The daemon echoes back
// the resolved GroupID so clients that sent nil can learn which group
// received the message, and a server-side TimestampMS so clients do not
// have to second-guess their own clock.
type PublishResp struct {
	// MessageID is the content-addressed id the daemon assigned.
	MessageID entmoot.MessageID `json:"message_id"`
	// GroupID is the resolved target group (useful when PublishReq
	// omitted it).
	GroupID entmoot.GroupID `json:"group_id"`
	// TimestampMS is the daemon's unix-milliseconds timestamp at signing
	// time.
	TimestampMS int64 `json:"timestamp_ms"`
}

// SignedPublishReq carries a fully-authored and signed message from a client
// that holds its own Entmoot signing key. The daemon verifies roster
// membership, signature, and canonical id before accepting it.
type SignedPublishReq struct {
	Message entmoot.Message `json:"message"`
}

// SignedPublishResp acknowledges a successfully accepted signed message.
type SignedPublishResp struct {
	Status      string            `json:"status"`
	MessageID   entmoot.MessageID `json:"message_id"`
	GroupID     entmoot.GroupID   `json:"group_id"`
	Author      entmoot.NodeID    `json:"author"`
	TimestampMS int64             `json:"timestamp_ms"`
}

// JoinGroupReq carries a signed invite to the running daemon. The daemon
// verifies and joins through the same gossip bootstrap path as startup join.
type JoinGroupReq struct {
	Invite entmoot.Invite `json:"invite"`
}

// JoinGroupResp reports the active session created or found for JoinGroupReq.
type JoinGroupResp struct {
	Status  string          `json:"status"`
	GroupID entmoot.GroupID `json:"group_id"`
	Members int             `json:"members"`
}

// TailSubscribe opens a live message stream. GroupID is optional (nil
// means all groups); Topic is an MQTT-style pattern and an empty value is
// interpreted as "#" (match everything).
type TailSubscribe struct {
	// GroupID scopes the subscription. nil means "all joined groups".
	GroupID *entmoot.GroupID `json:"group_id,omitempty"`
	// Topic is an MQTT-style subscription pattern. Empty defaults to "#".
	Topic string `json:"topic,omitempty"`
}

// TailEvent is one live message delivered to a subscriber.
type TailEvent struct {
	// Message is the full authored message (signed, with ID populated).
	Message entmoot.Message `json:"message"`
}

// InfoReq is the empty request body for MsgInfoReq. It exists as a named
// struct so the codec can accept/emit a stable JSON shape ("{}").
type InfoReq struct{}

// InfoResp is the daemon's snapshot response. Groups is a per-group
// summary; MerkleRoot inside each GroupInfo is nil when Running is false
// (daemon not running means no authoritative root was available).
type InfoResp struct {
	// PilotNodeID is the Pilot node id the daemon runs as.
	PilotNodeID entmoot.NodeID `json:"pilot_node_id"`
	// EntmootPubKey is the Ed25519 public key the daemon signs with
	// (raw 32 bytes; encoding/json base64s it).
	EntmootPubKey []byte `json:"entmoot_pubkey"`
	// ListenPort is the TCP port the daemon accepts peer connections on.
	ListenPort uint16 `json:"listen_port"`
	// DataDir is the filesystem root under which the daemon keeps state.
	DataDir string `json:"data_dir"`
	// Groups summarises each joined group.
	Groups []GroupInfo `json:"groups"`
	// Running is true when the response is served by a live daemon,
	// false when a client reads SQLite directly (CLI_DESIGN §9, #6).
	Running bool `json:"running"`
}

// GroupInfo summarises one joined group inside an InfoResp.
type GroupInfo struct {
	// GroupID is the 32-byte group identifier.
	GroupID entmoot.GroupID `json:"group_id"`
	// Members is the current roster size.
	Members int `json:"members"`
	// Messages is the count of stored messages.
	Messages int `json:"messages"`
	// MerkleRoot is the current message Merkle root. nil when
	// InfoResp.Running is false.
	MerkleRoot *[32]byte `json:"merkle_root,omitempty"`
}
