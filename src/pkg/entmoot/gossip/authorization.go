package gossip

import (
	"bytes"
	"fmt"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
)

// VerifyLiveMessage validates a current version-2 message for live forwarding.
// New transports require a full-width member identity and a founder acceptance
// certificate so later history remains authorized after membership changes.
func VerifyLiveMessage(r *roster.RosterLog, msg entmoot.Message, now time.Time) error {
	if r == nil {
		return fmt.Errorf("%w: missing roster", entmoot.ErrNotMember)
	}
	if err := ValidateMessageShape(msg, now); err != nil {
		return err
	}
	if msg.Version != 2 || msg.Author.MemberID == nil || msg.RosterHead == nil || *msg.RosterHead != r.Head() {
		return fmt.Errorf("%w: live message must name the current full-width member and roster head", entmoot.ErrNotMember)
	}
	author, ok := r.MemberInfoByID(*msg.Author.MemberID)
	if !ok {
		return fmt.Errorf("%w: live author %s", entmoot.ErrNotMember, msg.Author.MemberID.String())
	}
	if err := entmoot.ValidateMemberInfo(author); err != nil {
		return err
	}
	if err := signing.VerifyMessage(msg, author); err != nil {
		return err
	}
	return VerifyMessageAcceptance(r, msg)
}

// VerifyHistoricalMessage validates an exact stored or fetched message under
// the roster checkpoint and acceptance rules selected for #94.
func VerifyHistoricalMessage(r *roster.RosterLog, msg entmoot.Message, now time.Time) error {
	if r == nil {
		return fmt.Errorf("%w: missing roster", entmoot.ErrNotMember)
	}
	if err := ValidateMessageShape(msg, now); err != nil {
		return err
	}
	head, certified := messageAuthorizationHead(msg)
	if !certified {
		author, ok := r.MemberInfo(msg.Author.PilotNodeID)
		if !ok {
			return fmt.Errorf("%w: legacy author %d", entmoot.ErrNotMember, msg.Author.PilotNodeID)
		}
		return signing.VerifyMessage(msg, author)
	}
	var author entmoot.NodeInfo
	var member, known bool
	if msg.Author.MemberID != nil {
		author, member, known = r.MemberInfoAtID(*msg.Author.MemberID, head)
	} else {
		author, member, known = r.MemberInfoAt(msg.Author.PilotNodeID, head)
	}
	if !known {
		return fmt.Errorf("%w: %s", entmoot.ErrRosterHeadUnknown, head)
	}
	if !member {
		return fmt.Errorf("%w: author at roster head %s", entmoot.ErrNotMember, head)
	}
	if err := signing.VerifyMessage(msg, author); err != nil {
		return err
	}
	return VerifyMessageAcceptance(r, msg)
}

func VerifyMessageAcceptance(r *roster.RosterLog, msg entmoot.Message) error {
	if msg.Acceptance == nil {
		return fmt.Errorf("%w: message has no roster acceptance certificate", entmoot.ErrSigInvalid)
	}
	acceptance := msg.Acceptance
	if acceptance.Version != 1 || acceptance.GroupID != msg.GroupID || acceptance.MessageID != msg.ID ||
		(msg.RosterHead != nil && acceptance.RosterHead != *msg.RosterHead) {
		return fmt.Errorf("%w: acceptance certificate does not match message", entmoot.ErrSigInvalid)
	}
	founder, ok := r.Founder()
	if !ok || acceptance.Authority.PilotNodeID != founder.PilotNodeID ||
		!bytes.Equal(acceptance.Authority.EntmootPubKey, founder.EntmootPubKey) ||
		!memberIDEqual(acceptance.Authority.MemberID, founder.MemberID) {
		return fmt.Errorf("%w: acceptance authority is not the founder", entmoot.ErrSigInvalid)
	}
	var active, known bool
	if founder.MemberID != nil {
		_, active, known = r.MemberInfoAtID(*founder.MemberID, acceptance.RosterHead)
	} else {
		_, active, known = r.MemberInfoAt(founder.PilotNodeID, acceptance.RosterHead)
	}
	if !known {
		return fmt.Errorf("%w: acceptance head %s", entmoot.ErrRosterHeadUnknown, acceptance.RosterHead)
	}
	if !active {
		return fmt.Errorf("%w: founder inactive at acceptance head", entmoot.ErrSigInvalid)
	}
	signingBytes, err := canonical.MessageAcceptanceSigningBytes(*acceptance)
	if err != nil {
		return fmt.Errorf("%w: canonical acceptance: %v", entmoot.ErrSigInvalid, err)
	}
	if !keystore.Verify(founder.EntmootPubKey, signingBytes, acceptance.Signature) {
		return fmt.Errorf("%w: acceptance signature does not verify", entmoot.ErrSigInvalid)
	}
	return nil
}

func memberIDEqual(left, right *entmoot.MemberID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
