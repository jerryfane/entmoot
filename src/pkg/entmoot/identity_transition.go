package entmoot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"entmoot/pkg/entmoot/keystore"
)

const legacyIdentityMappingDomain = "entmoot/legacy-identity-map/v1\x00"

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

// LegacyIdentityUpgradePolicy is founder-authorized by the roster entry that
// carries it. Its Merkle commitment limits accepted version-0 history to the
// exact messages present when the group crossed into the current protocol.
type LegacyIdentityUpgradePolicy struct {
	Type               string `json:"type"`
	MappingsSHA256     string `json:"mappings_sha256"`
	LegacyHistoryRoot  string `json:"legacy_history_root"`
	LegacyHistoryCount int    `json:"legacy_history_count"`
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

func (m LegacyIdentityMapping) String() string {
	return fmt.Sprintf("%s:%d->%s", m.GroupID.String(), m.LegacyNodeID, m.MemberID.String())
}
