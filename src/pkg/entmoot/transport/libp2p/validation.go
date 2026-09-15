package libp2ptransport

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/merkle"
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

// VerifyLiveAuthor validates an author-signed live message against the
// group's membership: shape, that the author is a member now, identity
// binding, and the author signature.
func VerifyLiveAuthor(group *membership.Group, message entmoot.Message, now time.Time) error {
	if group == nil {
		return fmt.Errorf("%w: missing group", entmoot.ErrNotMember)
	}
	if err := ValidateMessageShape(message, now); err != nil {
		return err
	}
	if message.Version != 2 || message.Author.MemberID == nil || message.RosterHead == nil {
		return fmt.Errorf("%w: live message must name a full-width member and the checkpoint it held", entmoot.ErrNotMember)
	}
	if *message.RosterHead != group.Canonical().ID {
		// A publisher whose membership is ahead of ours names a checkpoint we
		// have not seen. That is a synchronization gap, not a bad message: the
		// caller may hold it briefly and retry after a membership sync.
		//
		// A checkpoint we do know but that is no longer canonical is not an
		// error either: membership is a set, so a member at an older
		// checkpoint is still a member unless a record says otherwise. That
		// is checked below, by looking the author up in current state.
		if !group.HasCheckpoint(*message.RosterHead) && !group.HasRecord(*message.RosterHead) {
			// Only report a gap for a message that is at least internally
			// authentic. Without this, unsigned junk naming a fabricated head
			// would be indistinguishable from a real race and would occupy a
			// caller's retry buffer. Membership cannot be checked here: the
			// author may be a member only at the head we are missing.
			if err := signing.VerifyMessage(message, message.Author); err != nil {
				return fmt.Errorf("%w: unknown head with an invalid author signature: %v", entmoot.ErrSigInvalid, err)
			}
			return fmt.Errorf("%w: live head %s", entmoot.ErrRosterHeadUnknown, message.RosterHead)
		}
	}
	author, ok := group.MemberInfoByID(*message.Author.MemberID)
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

// VerifyLiveMessage authenticates a live message. Current membership is the
// whole authority: there is no per-message admission certificate, so a group
// keeps working when any particular member, founder included, is offline.
func VerifyLiveMessage(group *membership.Group, message entmoot.Message, now time.Time) error {
	return VerifyLiveAuthor(group, message, now)
}

// VerifyHistoricalMessage accepts version-2 messages at a known roster
// checkpoint. Version-0 messages require VerifyHistoricalMessageWithProof.
func VerifyHistoricalMessage(group *membership.Group, message entmoot.Message, now time.Time) error {
	return VerifyHistoricalMessageWithProof(group, message, now, nil)
}

// VerifyHistoricalMessageWithProof accepts immutable legacy messages only when
// their ID is included in the founder-signed conversion commitment.
func VerifyHistoricalMessageWithProof(group *membership.Group, message entmoot.Message, now time.Time, proof *merkle.Proof) error {
	if group == nil {
		return fmt.Errorf("%w: missing group", entmoot.ErrNotMember)
	}
	if err := ValidateMessageShape(message, now); err != nil {
		return err
	}
	switch message.Version {
	case 0:
		var author entmoot.NodeInfo
		found := false
		// A version-0 message predates member ids, so its author is matched
		// by Pilot node id and key against the linear chain the group
		// upgraded from. Without that chain on disk there is nothing to match.
		legacy := group.Legacy()
		if legacy != nil {
			for _, entry := range legacy.Entries() {
				if entry.Op == "add" && entry.Subject.PilotNodeID == message.Author.PilotNodeID &&
					bytes.Equal(entry.Subject.EntmootPubKey, message.Author.EntmootPubKey) {
					author, found = entry.Subject, true
					break
				}
			}
		}
		if !found {
			return fmt.Errorf("%w: legacy historical author %d", entmoot.ErrNotMember, message.Author.PilotNodeID)
		}
		if proof == nil {
			return errors.New("libp2p: legacy historical message lacks conversion proof")
		}
		root, count, err := legacyHistoryCommitment(group)
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
		author, active, known := group.MemberAt(*message.Author.MemberID, *message.RosterHead)
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

// legacyHistoryCommitment reads the founder-signed commitment that fixes the
// set of version-0 messages carried over at conversion. It lives on the linear
// chain, which is where those messages' authority also lives.
func legacyHistoryCommitment(group *membership.Group) ([32]byte, int, error) {
	var root [32]byte
	legacy := group.Legacy()
	if legacy == nil {
		return root, 0, errors.New("libp2p: group holds no legacy chain to verify version-0 messages against")
	}
	root, count, ok := legacy.LegacyHistoryCommitment()
	if !ok {
		return root, 0, errors.New("libp2p: missing founder-signed legacy history commitment")
	}
	return root, count, nil
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
