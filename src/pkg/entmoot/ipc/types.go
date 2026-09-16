package ipc

import (
	"encoding/json"
	"fmt"

	entmoot "entmoot/pkg/entmoot"
	entpolicy "entmoot/pkg/entmoot/policy"
)

// MsgType is the 1-byte message type tag in an ipc frame's framing header.
// The ipc namespace starts at 0x10 so frames are visually distinct from
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
	// MsgInviteCreateReq asks the running daemon to create a signed invite
	// for an active group; the invited node signs itself in when it redeems
	// the invite, so creating one does not change membership here.
	MsgInviteCreateReq MsgType = 0x1A
	// MsgInviteCreateResp returns the signed invite created from live daemon
	// state, including the roster head the daemon can serve.
	MsgInviteCreateResp MsgType = 0x1B
	// MsgMemberRemoveReq asks the running daemon to append a signed roster
	// remove entry for an active group.
	MsgMemberRemoveReq MsgType = 0x1C
	// MsgMemberRemoveResp acknowledges the updated live roster head.
	MsgMemberRemoveResp MsgType = 0x1D
	// MsgInviteAuthorityCheckReq asks whether this daemon can mint invites
	// for an active group without mutating roster state.
	MsgInviteAuthorityCheckReq MsgType = 0x1E
	// MsgInviteAuthorityCheckResp acknowledges local invite authority.
	MsgInviteAuthorityCheckResp MsgType = 0x20
	// MsgGroupDeactivateReq asks the daemon to stop a group session without
	// changing roster membership.
	MsgGroupDeactivateReq MsgType = 0x23
	// MsgGroupDeactivateResp acknowledges that the group session was stopped.
	MsgGroupDeactivateResp MsgType = 0x24
	// MsgError carries a structured error frame. 0x1F is kept stable so
	// existing logs and clients can continue spotting error frames.
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
	case MsgInviteCreateReq:
		return "invite_create_req"
	case MsgInviteCreateResp:
		return "invite_create_resp"
	case MsgMemberRemoveReq:
		return "member_remove_req"
	case MsgMemberRemoveResp:
		return "member_remove_resp"
	case MsgInviteAuthorityCheckReq:
		return "invite_authority_check_req"
	case MsgInviteAuthorityCheckResp:
		return "invite_authority_check_resp"
	case MsgGroupDeactivateReq:
		return "group_deactivate_req"
	case MsgGroupDeactivateResp:
		return "group_deactivate_resp"
	case MsgError:
		return "error"
	default:
		return fmt.Sprintf("unknown(0x%02x)", uint8(t))
	}
}

// PublishReq is the request body a client sends to author a message in a
// group. GroupID is optional: a nil GroupID means "auto-pick if exactly
// one group is joined" (publish -group is optional when
// there's only one choice). The daemon resolves GroupID before signing.
type PublishReq struct {
	// GroupID names the target group. nil means "auto-pick the single
	// joined group"; an INVALID_ARGUMENT error is returned when nil
	// would be ambiguous.
	GroupID *entmoot.GroupID `json:"group_id,omitempty"`
	// Topics are MQTT-style hierarchical topic strings. A single message
	// can carry multiple topics.
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
	Status         string            `json:"status"`
	MessageID      entmoot.MessageID `json:"message_id"`
	GroupID        entmoot.GroupID   `json:"group_id"`
	AuthorMemberID entmoot.MemberID  `json:"author_member_id"`
	TimestampMS    int64             `json:"timestamp_ms"`
}

// JoinGroupReq carries a target-bound bootstrap capability or requests
// activation of a group already joined in persistent local state.
type JoinGroupReq struct {
	Capability    *entmoot.BootstrapCapability `json:"capability,omitempty"`
	LocalGroupID  *entmoot.GroupID             `json:"local_group_id,omitempty"`
	GroupMetadata json.RawMessage              `json:"group_metadata,omitempty"`
	GroupPolicy   *entpolicy.Policy            `json:"group_policy,omitempty"`
	TimeoutMS     int64                        `json:"timeout_ms,omitempty"`
}

// JoinGroupResp reports the active session created or found for JoinGroupReq.
type JoinGroupResp struct {
	Status    string            `json:"status"`
	GroupID   entmoot.GroupID   `json:"group_id"`
	Issuer    *entmoot.NodeInfo `json:"issuer,omitempty"`
	Members   int               `json:"members"`
	Readiness json.RawMessage   `json:"readiness,omitempty"`
}

// InviteCreateReq asks the live founder daemon to mint a bootstrap capability
// from its current roster and advertised addresses. TargetPublicKey is
// required unless Open is set, which mints a bearer invite any holder may
// redeem; MaxUses caps how many distinct identities may redeem it (zero
// means one).
type InviteCreateReq struct {
	GroupID             entmoot.GroupID `json:"group_id"`
	TargetPublicKey     []byte          `json:"target_public_key,omitempty"`
	Open                bool            `json:"open,omitempty"`
	BootstrapMultiaddrs []string        `json:"bootstrap_multiaddrs"`
	MaxUses             int             `json:"max_uses,omitempty"`
	ValidForMS          int64           `json:"valid_for_ms,omitempty"`
	ValidUntilMS        int64           `json:"valid_until_ms,omitempty"`
	// NoFallbackPeers suppresses the other-member addresses the daemon would
	// otherwise attach so the invite outlives this node's uptime. Set it when
	// the invite will be shared widely and disclosing members' addresses is
	// not wanted; the CLI exposes the same choice as -no-fallback-peers.
	NoFallbackPeers bool `json:"no_fallback_peers,omitempty"`
}

type InviteCreateResp struct {
	Status     string                      `json:"status"`
	GroupID    entmoot.GroupID             `json:"group_id"`
	Capability entmoot.BootstrapCapability `json:"capability"`
	RosterHead entmoot.RosterEntryID       `json:"roster_head"`
	Members    int                         `json:"members"`
}

type InviteAuthorityCheckReq struct {
	GroupID entmoot.GroupID `json:"group_id"`
}

type InviteAuthorityCheckResp struct {
	Status     string                `json:"status"`
	GroupID    entmoot.GroupID       `json:"group_id"`
	RosterHead entmoot.RosterEntryID `json:"roster_head"`
	Members    int                   `json:"members"`
	// MemberPeerIDs are the transport peer ids of the group's current members,
	// which is the set an invite's bootstrap addresses may name. A caller that
	// stores a bootstrap list before any capability exists — an ESP open
	// invite — needs it to refuse an address naming nobody, rather than
	// handing out a link every redemption will reject.
	MemberPeerIDs []string `json:"member_peer_ids,omitempty"`
	// LocalPeerID is this daemon's own transport peer id. The mint accepts it
	// as a bootstrap peer even when this node is not a member — a founder may
	// issue after standing down — so a caller validating a list early needs it
	// to avoid being stricter than the mint.
	LocalPeerID string `json:"local_peer_id,omitempty"`
}

type MemberRemoveReq struct {
	GroupID entmoot.GroupID  `json:"group_id"`
	Target  entmoot.NodeInfo `json:"target"`
}

type MemberRemoveResp struct {
	Status     string                `json:"status"`
	GroupID    entmoot.GroupID       `json:"group_id"`
	RosterHead entmoot.RosterEntryID `json:"roster_head"`
	Members    int                   `json:"members"`
	// OutstandingOpenInvites lists base64 nonces of bearer invites this node
	// issued that are still live. A removal does not void them: they name no
	// target, and they are worth their issuer's authority, so an invite from a
	// still-serving admin keeps working until revoked or expired.
	OutstandingOpenInvites []string `json:"outstanding_open_invites,omitempty"`
	// OutstandingESPOpenInvites counts ESP-hosted open-invite tokens still
	// redeemable for this group. They are a second bearer path and are revoked
	// through the ESP API, not by a roster change. It is nil when that store
	// could not be read, because reporting zero would understate what is
	// outstanding.
	OutstandingESPOpenInvites *int `json:"outstanding_esp_open_invites"`
	// ESPOpenInvitesError reports why the ESP open-invite store could not be
	// read, when it could not.
	ESPOpenInvitesError string `json:"esp_open_invites_error,omitempty"`
	// InviteLedgerError reports that the removal was applied but the local
	// invite ledger could not be read, so the outstanding list is incomplete.
	// It is named for the ledger, not for a revocation: removal performs none,
	// because an invite carries its issuer's authority and loses it with the
	// removal. The CLI path reports the identical condition under the same
	// name.
	InviteLedgerError string `json:"invite_ledger_error,omitempty"`
}

type GroupDeactivateReq struct {
	GroupID entmoot.GroupID `json:"group_id"`
}

type GroupDeactivateResp struct {
	Status  string          `json:"status"`
	GroupID entmoot.GroupID `json:"group_id"`
}

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
// summary; MerkleRoot inside each GroupInfo is nil when Running is false.
type InfoResp struct {
	MemberID      entmoot.MemberID `json:"member_id"`
	PeerID        string           `json:"peer_id"`
	EntmootPubKey []byte           `json:"entmoot_pubkey"`
	ListenPort    uint16           `json:"listen_port"`
	DataDir       string           `json:"data_dir"`
	Groups        []GroupInfo      `json:"groups"`
	Running       bool             `json:"running"`
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
