package membership

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// bootstrapCapabilityDomain and the payload shape below must stay byte-exact:
// invites already in circulation were signed over these bytes, and changing
// them would invalidate every outstanding invite.
const bootstrapCapabilityDomain = "entmoot/bootstrap-capability/v1\x00"

// MaxCapabilityRelays bounds the relay hints one invite may carry, so an
// invite cannot fan a joiner out across an unbounded relay set.
const MaxCapabilityRelays = 8

// ErrInviteDenied means the invite itself was refused: bad signature, outside
// its validity window, malformed, or bound to another identity.
var ErrInviteDenied = errors.New("membership: invite denied")

// InviteSigningBytes returns the exact bytes an invite signature covers. It
// uses encoding/json rather than the canonical encoder because that is what
// the existing signatures were made over.
func InviteSigningBytes(capability entmoot.BootstrapCapability) ([]byte, error) {
	capability.Signature = nil
	payload, err := json.Marshal(capability)
	if err != nil {
		return nil, err
	}
	return append([]byte(bootstrapCapabilityDomain), payload...), nil
}

// SignInvite binds the grant to the issuing identity: the founder, or the
// delegated admin named in Issuer. An open invite carries no target identity
// and is redeemable by any holder while uses remain.
func SignInvite(issuer *keystore.Identity, capability *entmoot.BootstrapCapability) error {
	if issuer == nil || capability == nil {
		return errors.New("membership: issuer and capability are required")
	}
	authority := capability.SigningAuthority()
	if !bytes.Equal(issuer.PublicKey, authority.EntmootPubKey) {
		return errors.New("membership: capability signing authority does not match signing key")
	}
	if err := entmoot.ValidateOperationalMemberInfo(capability.Founder); err != nil {
		return fmt.Errorf("membership: invalid capability founder: %w", err)
	}
	if capability.Issuer != nil {
		if err := entmoot.ValidateOperationalMemberInfo(*capability.Issuer); err != nil {
			return fmt.Errorf("membership: invalid capability issuer: %w", err)
		}
	}
	if capability.MaxUses < 0 {
		return errors.New("membership: capability max uses cannot be negative")
	}
	if capability.IsOpenInvite() {
		if capability.TargetMemberID != (entmoot.MemberID{}) || capability.TargetPeerID != "" {
			return errors.New("membership: open invite must not carry a target identity")
		}
	} else {
		targetMemberID, err := entmoot.MemberIDFromPublicKey(capability.TargetPublicKey)
		if err != nil {
			return fmt.Errorf("membership: derive target member id: %w", err)
		}
		targetPeerID, err := entmoot.PeerIDFromPublicKey(capability.TargetPublicKey)
		if err != nil {
			return fmt.Errorf("membership: derive target peer id: %w", err)
		}
		if capability.TargetMemberID != targetMemberID || capability.TargetPeerID != targetPeerID {
			return errors.New("membership: capability target identity binding mismatch")
		}
	}
	payload, err := InviteSigningBytes(*capability)
	if err != nil {
		return err
	}
	capability.Signature = issuer.Sign(payload)
	return nil
}

// VerifyInviteSignature checks an invite's shape and issuer signature without
// needing a host, a peer or group state. Callers add the checks that need
// those: that the redeeming peer is the target, and that the issuer held
// authority at the checkpoint the invite names.
func VerifyInviteSignature(capability entmoot.BootstrapCapability) error {
	if capability.Nonce == ([32]byte{}) {
		return fmt.Errorf("%w: zero nonce", ErrInviteDenied)
	}
	if capability.MaxUses < 0 {
		return fmt.Errorf("%w: negative max uses", ErrInviteDenied)
	}
	if len(capability.Relays) > MaxCapabilityRelays {
		return fmt.Errorf("%w: capability advertises %d relays, cap is %d",
			ErrInviteDenied, len(capability.Relays), MaxCapabilityRelays)
	}
	if err := entmoot.ValidateMemberInfo(capability.Founder); err != nil {
		return fmt.Errorf("%w: invalid founder: %v", ErrInviteDenied, err)
	}
	if capability.Issuer != nil {
		if err := entmoot.ValidateMemberInfo(*capability.Issuer); err != nil {
			return fmt.Errorf("%w: invalid issuer: %v", ErrInviteDenied, err)
		}
	}
	if capability.IsOpenInvite() {
		if capability.TargetMemberID != (entmoot.MemberID{}) || capability.TargetPeerID != "" {
			return fmt.Errorf("%w: open invite carries a partial target identity", ErrInviteDenied)
		}
	} else {
		targetMemberID, err := entmoot.MemberIDFromPublicKey(capability.TargetPublicKey)
		if err != nil {
			return fmt.Errorf("%w: invalid target key: %v", ErrInviteDenied, err)
		}
		targetPeerID, err := entmoot.PeerIDFromPublicKey(capability.TargetPublicKey)
		if err != nil {
			return fmt.Errorf("%w: invalid target key: %v", ErrInviteDenied, err)
		}
		if capability.TargetMemberID != targetMemberID || capability.TargetPeerID != targetPeerID {
			return fmt.Errorf("%w: target identity binding mismatch", ErrInviteDenied)
		}
	}
	payload, err := InviteSigningBytes(capability)
	if err != nil {
		return fmt.Errorf("%w: encode capability: %v", ErrInviteDenied, err)
	}
	authority := capability.SigningAuthority()
	if len(authority.EntmootPubKey) != ed25519.PublicKeySize || !keystore.Verify(authority.EntmootPubKey, payload, capability.Signature) {
		return fmt.Errorf("%w: invalid issuer signature", ErrInviteDenied)
	}
	return nil
}

// InviteValidAt reports whether the invite's validity window contains the
// given time in unix milliseconds. An invite with no expiry never ages out;
// the issuer chooses.
func InviteValidAt(capability entmoot.BootstrapCapability, atMS int64) error {
	if capability.IssuedAtMS > atMS {
		return fmt.Errorf("%w: invite is not yet valid", ErrInviteDenied)
	}
	if capability.ExpiresAtMS != 0 && atMS > capability.ExpiresAtMS {
		return fmt.Errorf("%w: invite has expired", ErrInviteDenied)
	}
	return nil
}
