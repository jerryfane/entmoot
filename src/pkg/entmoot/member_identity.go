package entmoot

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
)

const memberIDV2Domain = "entmoot/member/v2\x00"

// MemberIDFromPublicKey derives the stable Pilot-independent application
// identity from the complete Ed25519 public key.
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
	return nil
}
