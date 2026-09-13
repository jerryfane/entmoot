package entmoot

import (
	"crypto/ed25519"
	"testing"
)

func TestMemberIDBinding(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	publicKey := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	first, err := MemberIDFromPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MemberIDFromPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("member id changed for the same key")
	}
	info := NodeInfo{EntmootPubKey: publicKey, MemberID: &first}
	if err := ValidateMemberInfo(info); err != nil {
		t.Fatalf("valid binding rejected: %v", err)
	}
	wrong := first
	wrong[0] ^= 1
	info.MemberID = &wrong
	if err := ValidateMemberInfo(info); err == nil {
		t.Fatal("invalid member-id/public-key association accepted")
	}
}

func TestLegacyNodeInfoStillOmitsMemberID(t *testing.T) {
	info := NodeInfo{PilotNodeID: 42, EntmootPubKey: make([]byte, ed25519.PublicKeySize)}
	if err := ValidateMemberInfo(info); err != nil {
		t.Fatalf("legacy identity rejected: %v", err)
	}
}
