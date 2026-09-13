package entmoot

import (
	"testing"

	"entmoot/pkg/entmoot/keystore"
)

func TestFounderSignedLegacyIdentityMapping(t *testing.T) {
	founder := mustTransitionIdentity(t)
	member := mustTransitionIdentity(t)
	memberID, err := MemberIDFromPublicKey(member.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	mapping := LegacyIdentityMapping{
		LegacyNodeID: 42,
		MemberID:     memberID,
		MemberPubKey: member.PublicKey,
		Founder:      NodeInfo{EntmootPubKey: founder.PublicKey},
	}
	mapping.GroupID[0] = 1
	mapping.RosterHead[0] = 2
	if err := SignLegacyIdentityMapping(founder, &mapping); err != nil {
		t.Fatal(err)
	}
	if err := VerifyLegacyIdentityMapping(mapping); err != nil {
		t.Fatalf("valid mapping rejected: %v", err)
	}
	mapping.GroupID[0] ^= 1
	if err := VerifyLegacyIdentityMapping(mapping); err == nil {
		t.Fatal("mapping replayed into another group")
	}
}

func TestKeyRotationRequiresOldKeyAndAuthority(t *testing.T) {
	oldKey := mustTransitionIdentity(t)
	newKey := mustTransitionIdentity(t)
	authority := mustTransitionIdentity(t)
	oldID, _ := MemberIDFromPublicKey(oldKey.PublicKey)
	newID, _ := MemberIDFromPublicKey(newKey.PublicKey)
	rotation := KeyRotation{
		OldMemberID:  oldID,
		OldPublicKey: oldKey.PublicKey,
		NewMemberID:  newID,
		NewPublicKey: newKey.PublicKey,
		Authority:    NodeInfo{EntmootPubKey: authority.PublicKey},
	}
	rotation.GroupID[0] = 1
	if err := SignKeyRotation(oldKey, authority, &rotation); err != nil {
		t.Fatal(err)
	}
	if err := VerifyKeyRotation(rotation, nil); err != nil {
		t.Fatalf("valid rotation rejected: %v", err)
	}
	rotation.OldKeySignature[0] ^= 1
	if err := VerifyKeyRotation(rotation, nil); err == nil {
		t.Fatal("rotation without a valid old-key signature accepted")
	}
}

func TestEmergencyKeyRotationRequiresFounder(t *testing.T) {
	oldKey := mustTransitionIdentity(t)
	newKey := mustTransitionIdentity(t)
	founder := mustTransitionIdentity(t)
	oldID, _ := MemberIDFromPublicKey(oldKey.PublicKey)
	newID, _ := MemberIDFromPublicKey(newKey.PublicKey)
	rotation := KeyRotation{
		OldMemberID:  oldID,
		OldPublicKey: oldKey.PublicKey,
		NewMemberID:  newID,
		NewPublicKey: newKey.PublicKey,
		Authority:    NodeInfo{EntmootPubKey: founder.PublicKey},
		Emergency:    true,
	}
	if err := SignKeyRotation(nil, founder, &rotation); err != nil {
		t.Fatal(err)
	}
	if err := VerifyKeyRotation(rotation, founder.PublicKey); err != nil {
		t.Fatalf("valid emergency rotation rejected: %v", err)
	}
	stranger := mustTransitionIdentity(t)
	if err := VerifyKeyRotation(rotation, stranger.PublicKey); err == nil {
		t.Fatal("non-founder emergency authority accepted")
	}
}

func mustTransitionIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}
