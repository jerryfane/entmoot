package esphttp

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"entmoot/pkg/entmoot"
)

const (
	FleetRoleCoordinator = "coordinator"
	FleetRoleAgent       = "agent"

	FleetMemberInvited = "invited"
	FleetMemberActive  = "active"
	FleetMemberRemoved = "removed"

	FleetStatusActive   = "active"
	FleetStatusArchived = "archived"
	FleetStatusDeleted  = "deleted"
)

type FleetRecord struct {
	FleetID             string           `json:"fleet_id"`
	Name                string           `json:"name"`
	ControlGroupID      entmoot.GroupID  `json:"control_group_id,omitempty"`
	Coordinator         entmoot.NodeInfo `json:"coordinator"`
	CoordinatorDeviceID string           `json:"coordinator_device_id,omitempty"`
	Status              string           `json:"status"`
	CreatedAtMS         int64            `json:"created_at_ms"`
	UpdatedAtMS         int64            `json:"updated_at_ms"`
	ArchivedAtMS        int64            `json:"archived_at_ms,omitempty"`
	DeletedAtMS         int64            `json:"deleted_at_ms,omitempty"`
}

type FleetMemberRecord struct {
	FleetID       string         `json:"fleet_id"`
	NodeID        entmoot.NodeID `json:"node_id"`
	EntmootPubKey string         `json:"entmoot_pubkey"`
	Hostname      string         `json:"hostname,omitempty"`
	Role          string         `json:"role"`
	Status        string         `json:"status"`
	InvitedAtMS   int64          `json:"invited_at_ms,omitempty"`
	AcceptedAtMS  int64          `json:"accepted_at_ms,omitempty"`
	RemovedAtMS   int64          `json:"removed_at_ms,omitempty"`
	UpdatedAtMS   int64          `json:"updated_at_ms"`
}

type FleetInviteRecord struct {
	InviteID      string          `json:"invite_id"`
	FleetID       string          `json:"fleet_id"`
	NodeID        entmoot.NodeID  `json:"node_id"`
	EntmootPubKey string          `json:"entmoot_pubkey"`
	Hostname      string          `json:"hostname,omitempty"`
	Status        string          `json:"status"`
	Invite        json.RawMessage `json:"invite,omitempty"`
	CreatedAtMS   int64           `json:"created_at_ms"`
	UpdatedAtMS   int64           `json:"updated_at_ms"`
	ExpiresAtMS   int64           `json:"expires_at_ms,omitempty"`
}

type FleetActivityRecord struct {
	EventID     string            `json:"event_id"`
	FleetID     string            `json:"fleet_id"`
	Type        string            `json:"type"`
	Actor       entmoot.NodeInfo  `json:"actor"`
	Subject     *entmoot.NodeInfo `json:"subject,omitempty"`
	Summary     string            `json:"summary"`
	Metadata    json.RawMessage   `json:"metadata,omitempty"`
	CreatedAtMS int64             `json:"created_at_ms"`
}

func NewFleetID() (string, error) {
	return randomBase64URL(24)
}

func NewFleetInviteID() (string, error) {
	return randomBase64URL(18)
}

func NewFleetActivityID() (string, error) {
	return randomBase64URL(18)
}

func NormalizeFleetName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("fleet name is required")
	}
	if len(name) > 120 {
		return "", fmt.Errorf("fleet name is too long")
	}
	return name, nil
}

func NormalizeFleetMemberStatus(status string) string {
	switch strings.TrimSpace(status) {
	case FleetMemberInvited, FleetMemberActive, FleetMemberRemoved:
		return status
	default:
		return FleetMemberInvited
	}
}

func NormalizeFleetMemberRole(role string) string {
	switch strings.TrimSpace(role) {
	case FleetRoleCoordinator, FleetRoleAgent:
		return role
	default:
		return FleetRoleAgent
	}
}

func NormalizeFleetStatus(status string) string {
	switch strings.TrimSpace(status) {
	case FleetStatusActive, FleetStatusArchived, FleetStatusDeleted:
		return status
	default:
		return FleetStatusActive
	}
}

func ActiveFleetRecords(fleets []FleetRecord) []FleetRecord {
	active := make([]FleetRecord, 0, len(fleets))
	for _, fleet := range fleets {
		if NormalizeFleetStatus(fleet.Status) == FleetStatusActive {
			active = append(active, fleet)
		}
	}
	if active == nil {
		return []FleetRecord{}
	}
	return active
}

func cloneFleetRecord(in FleetRecord) FleetRecord {
	out := in
	out.Coordinator.EntmootPubKey = append([]byte(nil), in.Coordinator.EntmootPubKey...)
	return out
}

func cloneFleetMemberRecord(in FleetMemberRecord) FleetMemberRecord {
	return in
}

func cloneFleetInviteRecord(in FleetInviteRecord) FleetInviteRecord {
	in.Invite = append(json.RawMessage(nil), in.Invite...)
	return in
}

func cloneFleetActivityRecord(in FleetActivityRecord) FleetActivityRecord {
	out := in
	out.Actor.EntmootPubKey = append([]byte(nil), in.Actor.EntmootPubKey...)
	if in.Subject != nil {
		subj := *in.Subject
		subj.EntmootPubKey = append([]byte(nil), in.Subject.EntmootPubKey...)
		out.Subject = &subj
	}
	out.Metadata = append(json.RawMessage(nil), in.Metadata...)
	return out
}

func randomBase64URL(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
