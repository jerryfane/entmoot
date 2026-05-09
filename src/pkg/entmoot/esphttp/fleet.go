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

	FleetTaskModeDirectAssignment = "direct_assignee"
	FleetTaskModeFirstClaim       = "first_claim"
	FleetTaskModeOpenSubmission   = "open_submission"

	FleetTaskStatusProposed   = "proposed"
	FleetTaskStatusOpen       = "open"
	FleetTaskStatusAssigned   = "assigned"
	FleetTaskStatusInProgress = "in_progress"
	FleetTaskStatusSubmitted  = "submitted"
	FleetTaskStatusCompleted  = "completed"
	FleetTaskStatusRejected   = "rejected"
	FleetTaskStatusCanceled   = "canceled"

	FleetTaskSubmissionPending  = "pending"
	FleetTaskSubmissionAccepted = "accepted"
	FleetTaskSubmissionRejected = "rejected"
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

type FleetTaskRecord struct {
	TaskID        string            `json:"task_id"`
	FleetID       string            `json:"fleet_id"`
	Title         string            `json:"title"`
	Description   string            `json:"description,omitempty"`
	Mode          string            `json:"mode"`
	Status        string            `json:"status"`
	Creator       entmoot.NodeInfo  `json:"creator"`
	Assignee      *entmoot.NodeInfo `json:"assignee,omitempty"`
	CreatedAtMS   int64             `json:"created_at_ms"`
	UpdatedAtMS   int64             `json:"updated_at_ms"`
	CompletedAtMS int64             `json:"completed_at_ms,omitempty"`
	RejectedAtMS  int64             `json:"rejected_at_ms,omitempty"`
	CanceledAtMS  int64             `json:"canceled_at_ms,omitempty"`
}

type FleetTaskSubmissionRecord struct {
	SubmissionID string           `json:"submission_id"`
	FleetID      string           `json:"fleet_id"`
	TaskID       string           `json:"task_id"`
	Author       entmoot.NodeInfo `json:"author"`
	Content      string           `json:"content"`
	Status       string           `json:"status"`
	CreatedAtMS  int64            `json:"created_at_ms"`
	UpdatedAtMS  int64            `json:"updated_at_ms"`
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

func NewFleetTaskID() (string, error) {
	return randomBase64URL(18)
}

func NewFleetTaskSubmissionID() (string, error) {
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

func NormalizeFleetTaskMode(mode string) string {
	switch strings.TrimSpace(mode) {
	case FleetTaskModeDirectAssignment, FleetTaskModeFirstClaim, FleetTaskModeOpenSubmission:
		return strings.TrimSpace(mode)
	default:
		return FleetTaskModeOpenSubmission
	}
}

func IsValidFleetTaskMode(mode string) bool {
	switch strings.TrimSpace(mode) {
	case FleetTaskModeDirectAssignment, FleetTaskModeFirstClaim, FleetTaskModeOpenSubmission:
		return true
	default:
		return false
	}
}

func NormalizeFleetTaskStatus(status string) string {
	switch strings.TrimSpace(status) {
	case FleetTaskStatusProposed, FleetTaskStatusOpen, FleetTaskStatusAssigned, FleetTaskStatusInProgress, FleetTaskStatusSubmitted, FleetTaskStatusCompleted, FleetTaskStatusRejected, FleetTaskStatusCanceled:
		return status
	default:
		return FleetTaskStatusProposed
	}
}

func NormalizeFleetTaskSubmissionStatus(status string) string {
	switch strings.TrimSpace(status) {
	case FleetTaskSubmissionPending, FleetTaskSubmissionAccepted, FleetTaskSubmissionRejected:
		return status
	default:
		return FleetTaskSubmissionPending
	}
}

func NormalizeFleetTaskTitle(title string) (string, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return "", fmt.Errorf("task title is required")
	}
	if len(title) > 160 {
		return "", fmt.Errorf("task title is too long")
	}
	return title, nil
}

func NormalizeFleetTaskDescription(description string) (string, error) {
	description = strings.TrimSpace(description)
	if len(description) > 8000 {
		return "", fmt.Errorf("task description is too long")
	}
	return description, nil
}

func NormalizeFleetTaskSubmissionContent(content string) (string, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "", fmt.Errorf("submission content is required")
	}
	if len(content) > 16000 {
		return "", fmt.Errorf("submission content is too long")
	}
	return content, nil
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

func VisibleFleetRecords(fleets []FleetRecord, includeArchived bool) []FleetRecord {
	visible := make([]FleetRecord, 0, len(fleets))
	for _, fleet := range fleets {
		switch NormalizeFleetStatus(fleet.Status) {
		case FleetStatusActive:
			visible = append(visible, fleet)
		case FleetStatusArchived:
			if includeArchived {
				visible = append(visible, fleet)
			}
		}
	}
	if visible == nil {
		return []FleetRecord{}
	}
	return visible
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

func cloneFleetTaskRecord(in FleetTaskRecord) FleetTaskRecord {
	out := in
	out.Creator.EntmootPubKey = append([]byte(nil), in.Creator.EntmootPubKey...)
	if in.Assignee != nil {
		assignee := *in.Assignee
		assignee.EntmootPubKey = append([]byte(nil), in.Assignee.EntmootPubKey...)
		out.Assignee = &assignee
	}
	return out
}

func cloneFleetTaskSubmissionRecord(in FleetTaskSubmissionRecord) FleetTaskSubmissionRecord {
	out := in
	out.Author.EntmootPubKey = append([]byte(nil), in.Author.EntmootPubKey...)
	return out
}

func randomBase64URL(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
