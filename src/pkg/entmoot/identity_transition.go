package entmoot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"entmoot/pkg/entmoot/keystore"
)

const (
	legacyIdentityMappingDomain = "entmoot/legacy-identity-map/v1\x00"
	keyRotationDomain           = "entmoot/key-rotation/v1\x00"
)

// LegacyIdentityMapping is the only bridge from a legacy Pilot NodeID to a
// full-width MemberID. The current founder signs it for one group/checkpoint.
type LegacyIdentityMapping struct {
	GroupID      GroupID       `json:"group_id"`
	LegacyNodeID NodeID        `json:"legacy_pilot_node_id"`
	MemberID     MemberID      `json:"member_id"`
	MemberPubKey []byte        `json:"member_pubkey"`
	RosterHead   RosterEntryID `json:"roster_head"`
	Founder      NodeInfo      `json:"founder"`
	Signature    []byte        `json:"signature,omitempty"`
}

func legacyMappingSigningBytes(mapping LegacyIdentityMapping) ([]byte, error) {
	mapping.Signature = nil
	payload, err := json.Marshal(mapping)
	if err != nil {
		return nil, err
	}
	return append([]byte(legacyIdentityMappingDomain), payload...), nil
}

func SignLegacyIdentityMapping(founder *keystore.Identity, mapping *LegacyIdentityMapping) error {
	if founder == nil || mapping == nil {
		return errors.New("entmoot: founder and legacy mapping are required")
	}
	if !bytes.Equal(founder.PublicKey, mapping.Founder.EntmootPubKey) {
		return errors.New("entmoot: legacy mapping founder does not match signing key")
	}
	payload, err := legacyMappingSigningBytes(*mapping)
	if err != nil {
		return err
	}
	mapping.Signature = founder.Sign(payload)
	return nil
}

func VerifyLegacyIdentityMapping(mapping LegacyIdentityMapping) error {
	if mapping.LegacyNodeID == 0 {
		return errors.New("entmoot: legacy mapping has zero Pilot NodeID")
	}
	want, err := MemberIDFromPublicKey(mapping.MemberPubKey)
	if err != nil {
		return err
	}
	if want != mapping.MemberID {
		return errors.New("entmoot: legacy mapping MemberID does not match public key")
	}
	payload, err := legacyMappingSigningBytes(mapping)
	if err != nil {
		return err
	}
	if !keystore.Verify(mapping.Founder.EntmootPubKey, payload, mapping.Signature) {
		return errors.New("entmoot: invalid legacy identity mapping signature")
	}
	return nil
}

// KeyRotation binds a replacement key to an old MemberID. Normal rotation
// requires both the old key and current roster authority; emergency rotation
// is explicitly marked and requires the founder authority signature.
type KeyRotation struct {
	GroupID            GroupID  `json:"group_id"`
	OldMemberID        MemberID `json:"old_member_id"`
	OldPublicKey       []byte   `json:"old_public_key"`
	NewMemberID        MemberID `json:"new_member_id"`
	NewPublicKey       []byte   `json:"new_public_key"`
	Authority          NodeInfo `json:"authority"`
	Emergency          bool     `json:"emergency"`
	OldKeySignature    []byte   `json:"old_key_signature,omitempty"`
	AuthoritySignature []byte   `json:"authority_signature,omitempty"`
}

func keyRotationSigningBytes(rotation KeyRotation) ([]byte, error) {
	rotation.OldKeySignature = nil
	rotation.AuthoritySignature = nil
	payload, err := json.Marshal(rotation)
	if err != nil {
		return nil, err
	}
	return append([]byte(keyRotationDomain), payload...), nil
}

func SignKeyRotation(oldKey, authority *keystore.Identity, rotation *KeyRotation) error {
	if authority == nil || rotation == nil {
		return errors.New("entmoot: authority and rotation are required")
	}
	if !bytes.Equal(authority.PublicKey, rotation.Authority.EntmootPubKey) {
		return errors.New("entmoot: rotation authority does not match signing key")
	}
	payload, err := keyRotationSigningBytes(*rotation)
	if err != nil {
		return err
	}
	if rotation.Emergency {
		rotation.OldKeySignature = nil
	} else {
		if oldKey == nil || !bytes.Equal(oldKey.PublicKey, rotation.OldPublicKey) {
			return errors.New("entmoot: old rotation key is required")
		}
		rotation.OldKeySignature = oldKey.Sign(payload)
	}
	rotation.AuthoritySignature = authority.Sign(payload)
	return nil
}

func VerifyKeyRotation(rotation KeyRotation, founderPublicKey []byte) error {
	oldID, err := MemberIDFromPublicKey(rotation.OldPublicKey)
	if err != nil || oldID != rotation.OldMemberID {
		return errors.New("entmoot: invalid old rotation identity")
	}
	newID, err := MemberIDFromPublicKey(rotation.NewPublicKey)
	if err != nil || newID != rotation.NewMemberID {
		return errors.New("entmoot: invalid new rotation identity")
	}
	if oldID == newID {
		return errors.New("entmoot: key rotation does not change identity")
	}
	payload, err := keyRotationSigningBytes(rotation)
	if err != nil {
		return err
	}
	if !keystore.Verify(rotation.Authority.EntmootPubKey, payload, rotation.AuthoritySignature) {
		return errors.New("entmoot: invalid rotation authority signature")
	}
	if rotation.Emergency {
		if !bytes.Equal(rotation.Authority.EntmootPubKey, founderPublicKey) || len(rotation.OldKeySignature) != 0 {
			return errors.New("entmoot: emergency rotation requires founder authority only")
		}
		return nil
	}
	if !keystore.Verify(rotation.OldPublicKey, payload, rotation.OldKeySignature) {
		return errors.New("entmoot: invalid old-key rotation signature")
	}
	return nil
}

func (m LegacyIdentityMapping) String() string {
	return fmt.Sprintf("%s:%d->%s", m.GroupID.String(), m.LegacyNodeID, m.MemberID.String())
}
