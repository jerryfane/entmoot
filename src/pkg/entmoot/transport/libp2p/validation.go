package libp2ptransport

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/topic"
)

const (
	MaxMessageParents        = 3
	MaxMessageTopics         = 16
	MaxMessageTopicBytes     = 256
	MaxMessageReferences     = 64
	MaxCanonicalMessageBytes = 256 * 1024
	MaxMessageFutureSkew     = 2 * time.Minute
)

func ValidateMessageShape(message entmoot.Message, now time.Time) error {
	switch message.Version {
	case 0:
		if message.RosterHead != nil {
			return fmt.Errorf("libp2p: legacy message must not carry roster_head")
		}
	case 2:
		if message.RosterHead == nil {
			return fmt.Errorf("libp2p: version-2 message requires roster_head")
		}
		if err := entmoot.ValidateOperationalMemberInfo(message.Author); err != nil {
			return fmt.Errorf("libp2p: message author identity: %w", err)
		}
	default:
		return fmt.Errorf("libp2p: unsupported message version %d", message.Version)
	}
	if len(message.Parents) > MaxMessageParents {
		return fmt.Errorf("libp2p: message has %d parents, cap is %d", len(message.Parents), MaxMessageParents)
	}
	if len(message.Topics) > MaxMessageTopics {
		return fmt.Errorf("libp2p: message has %d topics, cap is %d", len(message.Topics), MaxMessageTopics)
	}
	for index, name := range message.Topics {
		if len(name) > MaxMessageTopicBytes {
			return fmt.Errorf("libp2p: topic %d is %d bytes, cap is %d", index, len(name), MaxMessageTopicBytes)
		}
		if err := topic.ValidTopic(name); err != nil {
			return fmt.Errorf("libp2p: topic %d: %w", index, err)
		}
	}
	if len(message.References) > MaxMessageReferences {
		return fmt.Errorf("libp2p: message has %d references, cap is %d", len(message.References), MaxMessageReferences)
	}
	if message.Timestamp > now.Add(MaxMessageFutureSkew).UnixMilli() {
		return fmt.Errorf("libp2p: message timestamp exceeds future skew %s", MaxMessageFutureSkew)
	}
	encoded, err := canonical.Encode(message)
	if err != nil {
		return fmt.Errorf("libp2p: canonical message encoding: %w", err)
	}
	if len(encoded) > MaxCanonicalMessageBytes {
		return fmt.Errorf("libp2p: canonical message is %d bytes, cap is %d", len(encoded), MaxCanonicalMessageBytes)
	}
	return nil
}

// VerifyLiveAuthor validates a current member's author-signed message before
// the founder issues the acceptance certificate.
func VerifyLiveAuthor(groupRoster *roster.RosterLog, message entmoot.Message, now time.Time) error {
	if groupRoster == nil {
		return fmt.Errorf("%w: missing roster", entmoot.ErrNotMember)
	}
	if err := ValidateMessageShape(message, now); err != nil {
		return err
	}
	if message.Version != 2 || message.Author.MemberID == nil || message.RosterHead == nil || *message.RosterHead != groupRoster.Head() {
		return fmt.Errorf("%w: live message must name the current full-width member and roster head", entmoot.ErrNotMember)
	}
	author, ok := groupRoster.MemberInfoByID(*message.Author.MemberID)
	if !ok {
		return fmt.Errorf("%w: live author %s", entmoot.ErrNotMember, message.Author.MemberID.String())
	}
	if err := entmoot.ValidateMemberInfo(author); err != nil {
		return err
	}
	if err := verifyOperationalAuthor(message.Author, author); err != nil {
		return err
	}
	return signing.VerifyMessage(message, author)
}

// VerifyLiveMessage authenticates a live message against the roster. Current
// membership at the named checkpoint is the whole authority: there is no
// per-message admission certificate, so a group keeps working when any
// particular member, founder included, is offline.
func VerifyLiveMessage(groupRoster *roster.RosterLog, message entmoot.Message, now time.Time) error {
	return VerifyLiveAuthor(groupRoster, message, now)
}

// VerifyHistoricalMessage accepts version-2 messages at a known roster
// checkpoint. Version-0 messages require VerifyHistoricalMessageWithProof.
func VerifyHistoricalMessage(groupRoster *roster.RosterLog, message entmoot.Message, now time.Time) error {
	return VerifyHistoricalMessageWithProof(groupRoster, message, now, nil)
}

// VerifyHistoricalMessageWithProof accepts immutable legacy messages only when
// their ID is included in the founder-signed conversion commitment.
func VerifyHistoricalMessageWithProof(groupRoster *roster.RosterLog, message entmoot.Message, now time.Time, proof *merkle.Proof) error {
	if groupRoster == nil {
		return fmt.Errorf("%w: missing roster", entmoot.ErrNotMember)
	}
	if err := ValidateMessageShape(message, now); err != nil {
		return err
	}
	switch message.Version {
	case 0:
		var author entmoot.NodeInfo
		found := false
		for _, entry := range groupRoster.Entries() {
			if entry.Op == "add" && entry.Subject.PilotNodeID == message.Author.PilotNodeID &&
				bytes.Equal(entry.Subject.EntmootPubKey, message.Author.EntmootPubKey) {
				author, found = entry.Subject, true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: legacy historical author %d", entmoot.ErrNotMember, message.Author.PilotNodeID)
		}
		if proof == nil {
			return errors.New("libp2p: legacy historical message lacks conversion proof")
		}
		root, count, err := legacyHistoryCommitment(groupRoster)
		if err != nil {
			return err
		}
		if proof.LeafCount != count || !merkle.Verify(root, message.ID, *proof) {
			return errors.New("libp2p: legacy historical message is not in the conversion commitment")
		}
		return signing.VerifyMessage(message, author)
	case 2:
		if message.Author.MemberID == nil || message.RosterHead == nil {
			return fmt.Errorf("%w: incomplete historical member checkpoint", entmoot.ErrNotMember)
		}
		author, active, known := groupRoster.MemberInfoAtID(*message.Author.MemberID, *message.RosterHead)
		if !known {
			return fmt.Errorf("%w: historical head %s", entmoot.ErrRosterHeadUnknown, message.RosterHead)
		}
		if !active {
			return fmt.Errorf("%w: historical author %s", entmoot.ErrNotMember, message.Author.MemberID.String())
		}
		if err := verifyOperationalAuthor(message.Author, author); err != nil {
			return err
		}
		if err := signing.VerifyMessage(message, author); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("libp2p: unsupported historical message version %d", message.Version)
	}
}

func legacyHistoryCommitment(groupRoster *roster.RosterLog) ([32]byte, int, error) {
	var root [32]byte
	for _, entry := range groupRoster.Entries() {
		if entry.Op != "policy_change" || len(entry.Policy) == 0 {
			continue
		}
		var marker struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(entry.Policy, &marker); err != nil || marker.Type != "legacy_identity_upgrade" {
			continue
		}
		var policy entmoot.LegacyIdentityUpgradePolicy
		if err := json.Unmarshal(entry.Policy, &policy); err != nil {
			return root, 0, fmt.Errorf("libp2p: malformed legacy history commitment: %w", err)
		}
		decoded, err := hex.DecodeString(policy.LegacyHistoryRoot)
		if err != nil || len(decoded) != len(root) || policy.LegacyHistoryCount < 0 {
			return root, 0, errors.New("libp2p: malformed legacy history commitment")
		}
		copy(root[:], decoded)
		return root, policy.LegacyHistoryCount, nil
	}
	return root, 0, errors.New("libp2p: missing founder-signed legacy history commitment")
}

func verifyOperationalAuthor(claimed, rosterAuthor entmoot.NodeInfo) error {
	if claimed.MemberID == nil {
		return fmt.Errorf("%w: missing author member_id", entmoot.ErrNotMember)
	}
	rosterMemberID, err := entmoot.ResolvedMemberID(rosterAuthor)
	if err != nil || rosterMemberID != *claimed.MemberID || !bytes.Equal(claimed.EntmootPubKey, rosterAuthor.EntmootPubKey) {
		return fmt.Errorf("%w: message author does not match roster identity", entmoot.ErrNotMember)
	}
	rosterPeerID := rosterAuthor.PeerID
	if rosterPeerID == "" {
		rosterPeerID, err = entmoot.PeerIDFromPublicKey(rosterAuthor.EntmootPubKey)
		if err != nil {
			return err
		}
	}
	if claimed.PeerID != rosterPeerID {
		return fmt.Errorf("%w: message author peer_id does not match roster key", entmoot.ErrNotMember)
	}
	return nil
}
