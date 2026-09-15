package libp2ptransport

import (
	"bytes"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

const (
	// MembershipProtocol serves checkpoints and membership records. A member
	// reads it under its membership; a joiner reads it with an invite, because
	// it must see the checkpoint before it can sign itself into the group.
	MembershipProtocol protocol.ID = "/entmoot/membership/1"
	// MembershipPushProtocol accepts one signed record. A joiner uses it to
	// hand over its own join.
	MembershipPushProtocol protocol.ID = "/entmoot/membership-push/1"
	HistoryProtocol        protocol.ID = "/entmoot/history/2"
	// PeerRecordProtocol serves members the signed peer records this node holds
	// for other members. It is members-only and never a bootstrap target.
	PeerRecordProtocol protocol.ID = "/entmoot/peer-records/1"
)

// MaxCapabilityRelays bounds the relay hints one invite may carry.
const MaxCapabilityRelays = membership.MaxCapabilityRelays

// ErrBootstrapDenied means the grant itself was refused: expired, revoked,
// exhausted, or bound to another identity. It is safe to report.
var ErrBootstrapDenied = membership.ErrInviteDenied

type BootstrapCapability = entmoot.BootstrapCapability

// SignBootstrapCapability binds the grant to the issuing identity.
func SignBootstrapCapability(issuer *keystore.Identity, capability *BootstrapCapability) error {
	return membership.SignInvite(issuer, capability)
}

// VerifyBootstrapCapability validates the invite's signature and shape, and
// binds a target-bound invite to the peer redeeming it. Whether the invite is
// still usable — revoked, exhausted, or issued by a demoted admin — is a
// question about group state, answered by Group.CheckInvite.
func VerifyBootstrapCapability(capability BootstrapCapability, remotePeer peer.ID, now time.Time) error {
	if err := membership.VerifyInviteSignature(capability); err != nil {
		return err
	}
	if err := membership.InviteValidAt(capability, now.UnixMilli()); err != nil {
		return err
	}
	if !capability.IsOpenInvite() {
		target, err := BindingFromPublicKey(capability.TargetPublicKey)
		if err != nil {
			return fmt.Errorf("%w: invalid target key: %v", ErrBootstrapDenied, err)
		}
		if target.PeerID != remotePeer {
			return fmt.Errorf("%w: target identity binding mismatch", ErrBootstrapDenied)
		}
	}
	return nil
}

// AuthorizedIssuer requires the identity that signed a capability to be a
// member who may currently administer the group, with the key the group
// records for it. The signing authority travels in the capability, so binding
// it to membership state is what makes the signature mean anything.
func AuthorizedIssuer(group *membership.Group, issuer entmoot.NodeInfo) error {
	if group == nil {
		return fmt.Errorf("%w: missing group", ErrBootstrapDenied)
	}
	issuerMemberID, err := entmoot.ResolvedMemberID(issuer)
	if err != nil {
		return fmt.Errorf("%w: issuer identity is incomplete", ErrBootstrapDenied)
	}
	if !group.CanAdminister(issuerMemberID) {
		return fmt.Errorf("%w: issuer cannot administer this group", ErrBootstrapDenied)
	}
	if known, found := group.MemberInfoByID(issuerMemberID); found {
		if !bytes.Equal(known.EntmootPubKey, issuer.EntmootPubKey) {
			return fmt.Errorf("%w: issuer key does not match its member record", ErrBootstrapDenied)
		}
		return nil
	}
	founder := group.Founder()
	if !bytes.Equal(founder.EntmootPubKey, issuer.EntmootPubKey) {
		return fmt.Errorf("%w: issuer is not a member of this group", ErrBootstrapDenied)
	}
	return nil
}
