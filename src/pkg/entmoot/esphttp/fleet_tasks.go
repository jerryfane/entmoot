package esphttp

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"entmoot/pkg/entmoot"
)

var (
	ErrFleetTaskInvalidTransition = errors.New("esphttp: invalid fleet task transition")
	ErrFleetTaskUnauthorized      = errors.New("esphttp: actor cannot mutate fleet task")
)

type FleetTaskMutation struct {
	Action              string
	Task                FleetTaskRecord
	ExpectedUpdatedAtMS int64
	Submission          FleetTaskSubmissionRecord
	ActivityType        string
	Summary             string
	Subject             *entmoot.NodeInfo
}

const (
	FleetTaskActionCreate           = "create"
	FleetTaskActionApprove          = "approve"
	FleetTaskActionAssign           = "assign"
	FleetTaskActionClaim            = "claim"
	FleetTaskActionSubmit           = "submit"
	FleetTaskActionComplete         = "complete"
	FleetTaskActionReject           = "reject"
	FleetTaskActionCancel           = "cancel"
	FleetTaskActionAcceptSubmission = "accept_submission"
	FleetTaskActionRejectSubmission = "reject_submission"
)

func normalizeFleetTaskRecord(rec FleetTaskRecord, nowMS int64) (FleetTaskRecord, error) {
	title, err := NormalizeFleetTaskTitle(rec.Title)
	if err != nil {
		return FleetTaskRecord{}, err
	}
	description, err := NormalizeFleetTaskDescription(rec.Description)
	if err != nil {
		return FleetTaskRecord{}, err
	}
	if rec.TaskID == "" {
		rec.TaskID, err = NewFleetTaskID()
		if err != nil {
			return FleetTaskRecord{}, err
		}
	}
	if rec.FleetID == "" {
		return FleetTaskRecord{}, fmt.Errorf("fleet id is required")
	}
	if rec.Creator.PilotNodeID == 0 || len(rec.Creator.EntmootPubKey) == 0 {
		return FleetTaskRecord{}, fmt.Errorf("task creator identity is required")
	}
	rec.Title = title
	rec.Description = description
	rec.Mode = NormalizeFleetTaskMode(rec.Mode)
	rec.Status = NormalizeFleetTaskStatus(rec.Status)
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = nowMS
	}
	if rec.UpdatedAtMS < nowMS {
		rec.UpdatedAtMS = nowMS
	}
	return rec, nil
}

func normalizeFleetTaskSubmissionRecord(rec FleetTaskSubmissionRecord, nowMS int64) (FleetTaskSubmissionRecord, error) {
	content, err := NormalizeFleetTaskSubmissionContent(rec.Content)
	if err != nil {
		return FleetTaskSubmissionRecord{}, err
	}
	if rec.SubmissionID == "" {
		rec.SubmissionID, err = NewFleetTaskSubmissionID()
		if err != nil {
			return FleetTaskSubmissionRecord{}, err
		}
	}
	if rec.FleetID == "" || rec.TaskID == "" {
		return FleetTaskSubmissionRecord{}, fmt.Errorf("fleet id and task id are required")
	}
	if rec.Author.PilotNodeID == 0 || len(rec.Author.EntmootPubKey) == 0 {
		return FleetTaskSubmissionRecord{}, fmt.Errorf("submission author identity is required")
	}
	rec.Content = content
	rec.Status = NormalizeFleetTaskSubmissionStatus(rec.Status)
	if rec.CreatedAtMS == 0 {
		rec.CreatedAtMS = nowMS
	}
	rec.UpdatedAtMS = nowMS
	return rec, nil
}

func FleetTaskActorFromMember(member FleetMemberRecord) entmoot.NodeInfo {
	pub, _ := base64.StdEncoding.DecodeString(strings.TrimSpace(member.EntmootPubKey))
	return entmoot.NodeInfo{PilotNodeID: member.NodeID, EntmootPubKey: pub}
}

func FleetTaskCanMutate(member FleetMemberRecord) bool {
	return member.Status == FleetMemberActive
}

func FleetTaskIsCoordinator(member FleetMemberRecord) bool {
	return member.Role == FleetRoleCoordinator && member.Status == FleetMemberActive
}

func FleetTaskHTTPError(err error) (int, string, string, bool) {
	switch {
	case errors.Is(err, ErrFleetTaskInvalidTransition):
		return http.StatusConflict, "invalid_task_transition", err.Error(), true
	case errors.Is(err, ErrFleetTaskUnauthorized):
		return http.StatusForbidden, "forbidden", err.Error(), true
	case errors.Is(err, ErrFleetNotActive):
		return http.StatusConflict, "fleet_archived", "fleet is archived", true
	default:
		return 0, "", "", false
	}
}

func ApplyFleetTaskMutation(task FleetTaskRecord, action string, actor FleetMemberRecord, nowMS int64, assignee *FleetMemberRecord, submission *FleetTaskSubmissionRecord) (FleetTaskMutation, error) {
	if !FleetTaskCanMutate(actor) {
		return FleetTaskMutation{}, ErrFleetTaskUnauthorized
	}
	actorInfo := FleetTaskActorFromMember(actor)
	out := FleetTaskMutation{Action: action, Task: cloneFleetTaskRecord(task), ExpectedUpdatedAtMS: task.UpdatedAtMS}
	switch strings.TrimSpace(action) {
	case FleetTaskActionCreate:
		out.Task.Status = FleetTaskStatusProposed
		out.ActivityType = "task.proposed"
		out.Summary = "Task proposed"
		if FleetTaskIsCoordinator(actor) {
			out.Task.Status = FleetTaskStatusOpen
			out.ActivityType = "task.opened"
			out.Summary = "Task opened"
		}
		out.Task.Creator = actorInfo
	case FleetTaskActionApprove:
		if !FleetTaskIsCoordinator(actor) {
			return FleetTaskMutation{}, ErrFleetTaskUnauthorized
		}
		if task.Status != FleetTaskStatusProposed {
			return FleetTaskMutation{}, fmt.Errorf("%w: task is not proposed", ErrFleetTaskInvalidTransition)
		}
		out.Task.Status = FleetTaskStatusOpen
		out.ActivityType = "task.opened"
		out.Summary = "Task opened"
	case FleetTaskActionAssign:
		if !FleetTaskIsCoordinator(actor) {
			return FleetTaskMutation{}, ErrFleetTaskUnauthorized
		}
		if task.Mode != FleetTaskModeDirectAssignment {
			return FleetTaskMutation{}, fmt.Errorf("%w: only direct-assignee tasks can be assigned", ErrFleetTaskInvalidTransition)
		}
		if assignee == nil || !FleetTaskCanMutate(*assignee) {
			return FleetTaskMutation{}, fmt.Errorf("%w: assignee must be an active fleet member", ErrFleetTaskInvalidTransition)
		}
		if task.Status != FleetTaskStatusOpen && task.Status != FleetTaskStatusProposed && task.Status != FleetTaskStatusAssigned {
			return FleetTaskMutation{}, fmt.Errorf("%w: task cannot be assigned", ErrFleetTaskInvalidTransition)
		}
		assigneeInfo := FleetTaskActorFromMember(*assignee)
		out.Task.Status = FleetTaskStatusAssigned
		out.Task.Assignee = &assigneeInfo
		out.Subject = &assigneeInfo
		out.ActivityType = "task.assigned"
		out.Summary = "Task assigned"
	case FleetTaskActionClaim:
		if task.Mode != FleetTaskModeFirstClaim {
			return FleetTaskMutation{}, fmt.Errorf("%w: task is not claimable", ErrFleetTaskInvalidTransition)
		}
		if task.Status != FleetTaskStatusOpen {
			return FleetTaskMutation{}, fmt.Errorf("%w: task is not open", ErrFleetTaskInvalidTransition)
		}
		out.Task.Status = FleetTaskStatusAssigned
		out.Task.Assignee = &actorInfo
		out.Subject = &actorInfo
		out.ActivityType = "task.claimed"
		out.Summary = "Task claimed"
	case FleetTaskActionSubmit:
		if !fleetTaskCanAcceptSubmission(task) {
			return FleetTaskMutation{}, fmt.Errorf("%w: task cannot accept submissions", ErrFleetTaskInvalidTransition)
		}
		if task.Mode == FleetTaskModeDirectAssignment && !nodeInfoMatchesPtr(task.Assignee, actor) {
			return FleetTaskMutation{}, ErrFleetTaskUnauthorized
		}
		if task.Mode == FleetTaskModeFirstClaim && !nodeInfoMatchesPtr(task.Assignee, actor) {
			return FleetTaskMutation{}, ErrFleetTaskUnauthorized
		}
		if submission == nil {
			return FleetTaskMutation{}, fmt.Errorf("%w: submission is required", ErrFleetTaskInvalidTransition)
		}
		sub := *submission
		sub.Author = actorInfo
		sub.Status = FleetTaskSubmissionPending
		out.Task.Status = FleetTaskStatusSubmitted
		if task.Mode == FleetTaskModeOpenSubmission {
			out.Task.Status = FleetTaskStatusOpen
		}
		out.Submission = sub
		out.Subject = &actorInfo
		out.ActivityType = "task.submitted"
		out.Summary = "Task submitted"
	case FleetTaskActionComplete:
		if !FleetTaskIsCoordinator(actor) {
			return FleetTaskMutation{}, ErrFleetTaskUnauthorized
		}
		if task.Status == FleetTaskStatusCompleted || task.Status == FleetTaskStatusCanceled || task.Status == FleetTaskStatusRejected {
			return FleetTaskMutation{}, fmt.Errorf("%w: task is already closed", ErrFleetTaskInvalidTransition)
		}
		out.Task.Status = FleetTaskStatusCompleted
		out.Task.CompletedAtMS = nowMS
		out.ActivityType = "task.completed"
		out.Summary = "Task completed"
	case FleetTaskActionReject:
		if !FleetTaskIsCoordinator(actor) {
			return FleetTaskMutation{}, ErrFleetTaskUnauthorized
		}
		if task.Status != FleetTaskStatusProposed && task.Status != FleetTaskStatusSubmitted {
			return FleetTaskMutation{}, fmt.Errorf("%w: task cannot be rejected", ErrFleetTaskInvalidTransition)
		}
		out.Task.Status = FleetTaskStatusRejected
		out.Task.RejectedAtMS = nowMS
		out.ActivityType = "task.rejected"
		out.Summary = "Task rejected"
	case FleetTaskActionCancel:
		if !FleetTaskIsCoordinator(actor) {
			return FleetTaskMutation{}, ErrFleetTaskUnauthorized
		}
		if task.Status == FleetTaskStatusCompleted || task.Status == FleetTaskStatusCanceled {
			return FleetTaskMutation{}, fmt.Errorf("%w: task is already closed", ErrFleetTaskInvalidTransition)
		}
		out.Task.Status = FleetTaskStatusCanceled
		out.Task.CanceledAtMS = nowMS
		out.ActivityType = "task.canceled"
		out.Summary = "Task canceled"
	default:
		return FleetTaskMutation{}, fmt.Errorf("%w: unknown action", ErrFleetTaskInvalidTransition)
	}
	out.Task.UpdatedAtMS = nowMS
	if action != FleetTaskActionCreate && out.Task.UpdatedAtMS <= out.ExpectedUpdatedAtMS {
		out.Task.UpdatedAtMS = out.ExpectedUpdatedAtMS + 1
	}
	return out, nil
}

func fleetTaskCanBeClaimed(task FleetTaskRecord) bool {
	return task.Mode == FleetTaskModeFirstClaim && task.Status == FleetTaskStatusOpen && task.Assignee == nil
}

func fleetTaskCanAcceptSubmission(task FleetTaskRecord) bool {
	return task.Status == FleetTaskStatusOpen || task.Status == FleetTaskStatusAssigned || task.Status == FleetTaskStatusInProgress
}

func nodeInfoMatchesPtr(node *entmoot.NodeInfo, member FleetMemberRecord) bool {
	if node == nil {
		return false
	}
	return node.PilotNodeID == member.NodeID && base64PubKey(*node) == strings.TrimSpace(member.EntmootPubKey)
}

func base64PubKey(node entmoot.NodeInfo) string {
	return base64.StdEncoding.EncodeToString(node.EntmootPubKey)
}
