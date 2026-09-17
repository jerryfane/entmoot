package roster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"entmoot/pkg/entmoot"
)

// AdminPolicyType is the value of the shared "type" discriminator that marks a
// policy_change payload as an admin-set change. Other policy types (such as
// the legacy identity upgrade checkpoint) travel through the same entry op and
// leave the admin set alone.
const AdminPolicyType = "admins/v1"

// adminPolicyFamily prefixes every admin-set policy version. A payload in this
// family that this build cannot read is refused rather than ignored: treating
// it as an unrelated policy would leave the current admins standing while the
// founder believed the set had changed.
const adminPolicyFamily = "admins/"

// MaxAdmins bounds the delegated-admin set. Roster changes stay strictly
// linear, so every admin is a concurrent writer competing for the same head;
// a small ceiling keeps that contention survivable.
const MaxAdmins = 16

// AdminPolicy is the payload of a founder-signed "policy_change" entry. It
// replaces the delegated-admin set wholesale: the entry states the complete
// set, so reading one entry is enough to know who could sign after it.
//
// Admins may add members and remove members that are not admins. Only the
// founder changes this set, removes an admin, or removes itself.
type AdminPolicy struct {
	Type   string             `json:"type"`
	Admins []entmoot.MemberID `json:"admins"`
}

// policyDiscriminator reads just the shared type tag from a policy payload.
type policyDiscriminator struct {
	Type string `json:"type"`
}

// IsAdminPolicy reports whether a policy_change payload is an admin-set change
// rather than some other policy this build does not interpret.
func IsAdminPolicy(payload []byte) bool {
	var probe policyDiscriminator
	if err := json.Unmarshal(payload, &probe); err != nil {
		return false
	}
	return probe.Type == AdminPolicyType
}

// IsUnknownAdminPolicy reports whether a payload claims to change the admin
// set in a version this build does not implement. Such an entry must not be
// accepted: peers would disagree about who can sign next, and the node that
// cannot read it would keep honouring admins the payload may have removed.
func IsUnknownAdminPolicy(payload []byte) bool {
	var probe policyDiscriminator
	if err := json.Unmarshal(payload, &probe); err != nil {
		return false
	}
	return strings.HasPrefix(probe.Type, adminPolicyFamily) && probe.Type != AdminPolicyType
}

// ParseAdminPolicy decodes an admin-set policy_change payload. Unknown fields
// and unknown types are refused: a payload that claims to set admins but that
// this build cannot read must not be mistaken for "no admins".
func ParseAdminPolicy(payload []byte) (AdminPolicy, error) {
	if len(payload) == 0 {
		return AdminPolicy{}, fmt.Errorf("roster: empty policy payload")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var policy AdminPolicy
	if err := decoder.Decode(&policy); err != nil {
		return AdminPolicy{}, fmt.Errorf("roster: decode admin policy: %w", err)
	}
	if policy.Type != AdminPolicyType {
		return AdminPolicy{}, fmt.Errorf("roster: unsupported admin policy type %q", policy.Type)
	}
	if len(policy.Admins) > MaxAdmins {
		return AdminPolicy{}, fmt.Errorf("roster: %d admins exceeds the ceiling of %d", len(policy.Admins), MaxAdmins)
	}
	for _, admin := range policy.Admins {
		if admin == (entmoot.MemberID{}) {
			return AdminPolicy{}, fmt.Errorf("roster: admin set contains the zero member id")
		}
	}
	return policy, nil
}
