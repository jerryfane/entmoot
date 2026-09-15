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

func mustTransitionIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}
