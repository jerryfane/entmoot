package entmoot

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const memberIDV2Domain = "entmoot/member/v2\x00"

// MemberIDFromPublicKey derives the stable application identity from the
// complete Ed25519 public key.
func MemberIDFromPublicKey(publicKey []byte) (MemberID, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return MemberID{}, fmt.Errorf("entmoot: member public key length %d, want %d", len(publicKey), ed25519.PublicKeySize)
	}
	h := sha256.New()
	_, _ = h.Write([]byte(memberIDV2Domain))
	_, _ = h.Write(publicKey)
	var id MemberID
	copy(id[:], h.Sum(nil))
	return id, nil
}

// PeerIDFromPublicKey derives the secure transport identity from the same
// Ed25519 key used for Entmoot signatures.
func PeerIDFromPublicKey(publicKey []byte) (string, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("entmoot: member public key length %d, want %d", len(publicKey), ed25519.PublicKeySize)
	}
	key, err := libp2pcrypto.UnmarshalEd25519PublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("entmoot: decode member public key for libp2p: %w", err)
	}
	id, err := peer.IDFromPublicKey(key)
	if err != nil {
		return "", fmt.Errorf("entmoot: derive libp2p peer id: %w", err)
	}
	return id.String(), nil
}

// ValidateMemberInfo verifies the full-width identity binding when MemberID is
// present. Legacy NodeInfo values omit MemberID and remain byte-compatible.
func ValidateMemberInfo(info NodeInfo) error {
	if info.MemberID == nil {
		return nil
	}
	want, err := MemberIDFromPublicKey(info.EntmootPubKey)
	if err != nil {
		return err
	}
	if want != *info.MemberID {
		return fmt.Errorf("entmoot: member id does not match public key")
	}
	if info.PeerID != "" {
		wantPeerID, err := PeerIDFromPublicKey(info.EntmootPubKey)
		if err != nil {
			return err
		}
		if wantPeerID != info.PeerID {
			return fmt.Errorf("entmoot: peer id does not match public key")
		}
	}
	return nil
}

// ValidateOperationalMemberInfo requires the full same-key application and
// transport identity used by newly written records and APIs.
func ValidateOperationalMemberInfo(info NodeInfo) error {
	if info.PilotNodeID != 0 || info.MemberID == nil || info.PeerID == "" {
		return fmt.Errorf("entmoot: operational identity requires member_id and peer_id")
	}
	return ValidateMemberInfo(info)
}

// ResolvedMemberID returns the full-width identity bound to info. Version-2
// records must carry MemberID; immutable legacy records are resolved from the
// same Ed25519 public key without changing their signed bytes.
func ResolvedMemberID(info NodeInfo) (MemberID, error) {
	if err := ValidateMemberInfo(info); err != nil {
		return MemberID{}, err
	}
	if info.MemberID != nil {
		return *info.MemberID, nil
	}
	return MemberIDFromPublicKey(info.EntmootPubKey)
}
