package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

func TestApplyLiveAgentActionCreatesFleetTask(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(16)
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-live",
		MemberID:      nodeID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		PeerID:        coordinator.PeerID,
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		TopicFilters:   []string{"fleet/#"},
		AllowedActions: []string{liveActionTaskCreate},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cfg, nil, liveAgentAction{
		Kind:        liveActionTaskCreate,
		Title:       "Audit deploy",
		Description: "Check live agent rollout",
		Mode:        esphttp.FleetTaskModeOpenSubmission,
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	tasks, err := state.ListFleetTasks(ctx, "fleet-live", "")
	if err != nil {
		t.Fatalf("ListFleetTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Title != "Audit deploy" || tasks[0].Status != esphttp.FleetTaskStatusOpen {
		t.Fatalf("tasks = %+v, want one opened task", tasks)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "task.opened" || *activity[0].Actor.MemberID != nodeID {
		t.Fatalf("activity = %+v, want task.opened by live node", activity)
	}
	cappedCfg := cfg
	cappedCfg.MaxActionBytes = 10
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cappedCfg, nil, liveAgentAction{
		Kind:        liveActionTaskCreate,
		Title:       "Small",
		Description: "this description is too long",
	})
	if err == nil {
		t.Fatal("applyLiveAgentAction capped task err = nil, want max_action_bytes rejection")
	}
	if applied {
		t.Fatal("capped task applied = true, want false")
	}
	tasks, err = state.ListFleetTasks(ctx, "fleet-live", "")
	if err != nil {
		t.Fatalf("ListFleetTasks after capped task: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("tasks after capped task = %+v, want original task only", tasks)
	}
}

func TestApplyLiveAgentTaskActionRequiresFeatureFlags(t *testing.T) {
	t.Setenv("ENTMOOT_ENABLE_TASKS", "0")
	t.Setenv("ENTMOOT_ENABLE_FLEET", "0")
	ctx := context.Background()
	gid := testAgentLiveGroupID(0x40)
	coordinator := testAgentLiveAuthor(7)
	state := esphttp.NewMemoryStateStore()
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{FleetID: "fleet-gated", ControlGroupID: gid, Coordinator: coordinator, Status: esphttp.FleetStatusActive}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{FleetID: "fleet-gated", MemberID: *coordinator.MemberID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive}); err != nil {
		t.Fatal(err)
	}
	cfg := esphttp.LiveAgentConfig{GroupID: gid, MemberID: *coordinator.MemberID, Enabled: true, Mode: esphttp.LiveModeOperator, AllowedActions: []string{liveActionTaskCreate}}
	action := liveAgentAction{Kind: liveActionTaskCreate, Title: "Coordinate this", Mode: esphttp.FleetTaskModeOpenSubmission}
	flags := &globalFlags{data: t.TempDir()}
	applied, err := applyLiveAgentAction(ctx, flags, state, cfg, nil, action)
	if err == nil || applied {
		t.Fatalf("disabled task action: applied=%v err=%v", applied, err)
	}
	tasks, err := state.ListFleetTasks(ctx, "fleet-gated", "")
	if err != nil || len(tasks) != 0 {
		t.Fatalf("disabled action persisted tasks: %+v err=%v", tasks, err)
	}
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(flags), state, cfg, nil, action)
	if err != nil || !applied {
		t.Fatalf("enabled task action: applied=%v err=%v", applied, err)
	}
	tasks, err = state.ListFleetTasks(ctx, "fleet-gated", "")
	if err != nil || len(tasks) != 1 || tasks[0].Title != action.Title {
		t.Fatalf("enabled action did not persist task: %+v err=%v", tasks, err)
	}
}

func TestApplyLiveAgentActionAssignsFleetTasks(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(26)
	coordinatorNodeID := testAgentLiveMemberID(7)
	agentNodeID := testAgentLiveMemberID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	agent := testAgentLiveAuthor(8)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: coordinatorNodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: agentNodeID, PeerID: agent.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	for _, task := range []esphttp.FleetTaskRecord{
		{
			TaskID:      "task-claim",
			FleetID:     "fleet-live",
			Title:       "Claim me",
			Description: "First claim task",
			Mode:        esphttp.FleetTaskModeFirstClaim,
			Status:      esphttp.FleetTaskStatusOpen,
			Creator:     coordinator,
			CreatedAtMS: 10,
			UpdatedAtMS: 10,
		},
		{
			TaskID:      "task-assign",
			FleetID:     "fleet-live",
			Title:       "Assign me",
			Description: "Direct assignment task",
			Mode:        esphttp.FleetTaskModeDirectAssignment,
			Status:      esphttp.FleetTaskStatusOpen,
			Creator:     coordinator,
			CreatedAtMS: 11,
			UpdatedAtMS: 11,
		},
	} {
		if _, err := state.UpsertFleetTask(ctx, task); err != nil {
			t.Fatalf("UpsertFleetTask(%s): %v", task.TaskID, err)
		}
	}
	agentCfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskAssignSelf},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, agentCfg, nil, liveAgentAction{
		Kind:   liveActionTaskAssignSelf,
		TaskID: "task-claim",
	})
	if err != nil {
		t.Fatalf("assign_self applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("assign_self applied = false, want true")
	}
	claimed, found, err := state.GetFleetTask(ctx, "fleet-live", "task-claim")
	if err != nil || !found {
		t.Fatalf("GetFleetTask claim found/err = %v/%v", found, err)
	}
	if claimed.Status != esphttp.FleetTaskStatusAssigned || claimed.Assignee == nil || *claimed.Assignee.MemberID != agentNodeID {
		t.Fatalf("claimed task = %+v, want assigned to live agent", claimed)
	}
	coordinatorCfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       coordinatorNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskAssignOthers},
	}
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, coordinatorCfg, nil, liveAgentAction{
		Kind:             liveActionTaskAssignOthers,
		TaskID:           "task-assign",
		AssigneeMemberID: agentNodeID,
	})
	if err != nil {
		t.Fatalf("assign_others applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("assign_others applied = false, want true")
	}
	assigned, found, err := state.GetFleetTask(ctx, "fleet-live", "task-assign")
	if err != nil || !found {
		t.Fatalf("GetFleetTask assign found/err = %v/%v", found, err)
	}
	if assigned.Status != esphttp.FleetTaskStatusAssigned || assigned.Assignee == nil || *assigned.Assignee.MemberID != agentNodeID {
		t.Fatalf("assigned task = %+v, want assigned to target agent", assigned)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	activityTypes := map[string]bool{}
	for _, item := range activity {
		activityTypes[item.Type] = true
	}
	if len(activity) != 2 || !activityTypes["task.claimed"] || !activityTypes["task.assigned"] {
		t.Fatalf("activity = %+v, want task.claimed and task.assigned", activity)
	}
}

func TestApplyLiveAgentActionUpdatesOwnFleetTask(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(27)
	coordinatorNodeID := testAgentLiveMemberID(7)
	agentNodeID := testAgentLiveMemberID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	agent := testAgentLiveAuthor(8)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: coordinatorNodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: agentNodeID, PeerID: agent.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	if _, err := state.UpsertFleetTask(ctx, esphttp.FleetTaskRecord{
		TaskID:      "task-owned",
		FleetID:     "fleet-live",
		Title:       "Owned task",
		Description: "Direct task",
		Mode:        esphttp.FleetTaskModeDirectAssignment,
		Status:      esphttp.FleetTaskStatusAssigned,
		Creator:     coordinator,
		Assignee:    &agent,
		CreatedAtMS: 10,
		UpdatedAtMS: 10,
	}); err != nil {
		t.Fatalf("UpsertFleetTask: %v", err)
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskUpdateOwn},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cfg, nil, liveAgentAction{
		Kind:    liveActionTaskUpdateOwn,
		TaskID:  "task-owned",
		Content: "Finished the assigned check",
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	task, found, err := state.GetFleetTask(ctx, "fleet-live", "task-owned")
	if err != nil || !found {
		t.Fatalf("GetFleetTask found/err = %v/%v", found, err)
	}
	if task.Status != esphttp.FleetTaskStatusSubmitted {
		t.Fatalf("task status = %q, want submitted", task.Status)
	}
	submissions, err := state.ListFleetTaskSubmissions(ctx, "fleet-live", "task-owned")
	if err != nil {
		t.Fatalf("ListFleetTaskSubmissions: %v", err)
	}
	if len(submissions) != 1 || *submissions[0].Author.MemberID != agentNodeID || submissions[0].Content != "Finished the assigned check" {
		t.Fatalf("submissions = %+v, want one live agent submission", submissions)
	}
	cappedCfg := cfg
	cappedCfg.MaxActionBytes = 4
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cappedCfg, nil, liveAgentAction{
		Kind:    liveActionTaskUpdateOwn,
		TaskID:  "task-owned",
		Content: "too long",
	})
	if err == nil {
		t.Fatal("capped update err = nil, want max_action_bytes rejection")
	}
	if applied {
		t.Fatal("capped update applied = true, want false")
	}
}

func TestApplyLiveAgentActionCommentsOnFleetTask(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(34)
	coordinatorNodeID := testAgentLiveMemberID(7)
	agentNodeID := testAgentLiveMemberID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	agent := testAgentLiveAuthor(8)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: coordinatorNodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: agentNodeID, PeerID: agent.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	if _, err := state.UpsertFleetTask(ctx, esphttp.FleetTaskRecord{
		TaskID:      "task-comment",
		FleetID:     "fleet-live",
		Title:       "Commented task",
		Description: "Task with comment",
		Mode:        esphttp.FleetTaskModeOpenSubmission,
		Status:      esphttp.FleetTaskStatusOpen,
		Creator:     coordinator,
		CreatedAtMS: 10,
		UpdatedAtMS: 10,
	}); err != nil {
		t.Fatalf("UpsertFleetTask: %v", err)
	}
	before, found, err := state.GetFleetTask(ctx, "fleet-live", "task-comment")
	if err != nil || !found {
		t.Fatalf("GetFleetTask before found/err = %v/%v", found, err)
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskComment},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cfg, nil, liveAgentAction{
		Kind:    liveActionTaskComment,
		TaskID:  "task-comment",
		Content: "Blocked until the invite is accepted",
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	task, found, err := state.GetFleetTask(ctx, "fleet-live", "task-comment")
	if err != nil || !found {
		t.Fatalf("GetFleetTask found/err = %v/%v", found, err)
	}
	if task.UpdatedAtMS != before.UpdatedAtMS || task.Status != esphttp.FleetTaskStatusOpen {
		t.Fatalf("task = %+v, want comment without task mutation", task)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "task.comment" || *activity[0].Actor.MemberID != agentNodeID {
		t.Fatalf("activity = %+v, want one task.comment by live agent", activity)
	}
	var metadata struct {
		TaskID  string `json:"task_id"`
		Title   string `json:"task_title"`
		Comment string `json:"comment"`
	}
	if err := json.Unmarshal(activity[0].Metadata, &metadata); err != nil {
		t.Fatalf("Unmarshal activity metadata: %v", err)
	}
	if metadata.TaskID != "task-comment" || metadata.Title != "Commented task" || metadata.Comment != "Blocked until the invite is accepted" {
		t.Fatalf("activity metadata = %+v, want task comment metadata", metadata)
	}
	cappedCfg := cfg
	cappedCfg.MaxActionBytes = 4
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cappedCfg, nil, liveAgentAction{
		Kind:    liveActionTaskComment,
		TaskID:  "task-comment",
		Content: "too long",
	})
	if err == nil {
		t.Fatal("capped comment err = nil, want max_action_bytes rejection")
	}
	if applied {
		t.Fatal("capped comment applied = true, want false")
	}
}

func TestApplyLiveAgentActionUpdatesGroupMetadata(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(35)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	founder := testESPNodeInfo(t, id.PublicKey)
	nodeID := *founder.MemberID
	createLiveActionGroup(t, dataDir, gid, id, founder)
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *founder.MemberID,
		PeerID:        founder.PeerID,
		EntmootPubKey: founder.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMetadataUpdate},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:     liveActionMetadataUpdate,
		Metadata: json.RawMessage(`{"name":"Ops","tags":["live","fleet"],"custom":{"level":2}}`),
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	raw, ok, err := state.GetGroupMetadata(ctx, gid)
	if err != nil || !ok {
		t.Fatalf("GetGroupMetadata ok/err = %v/%v", ok, err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatalf("Unmarshal metadata: %v", err)
	}
	if metadata["name"] != "Ops" {
		t.Fatalf("metadata = %+v, want name Ops", metadata)
	}
	select {
	case topics := <-topicsCh:
		t.Fatalf("unexpected metadata update publish topics = %+v", topics)
	case <-time.After(50 * time.Millisecond):
	}
	cappedCfg := cfg
	cappedCfg.MaxActionBytes = 4
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cappedCfg, nil, liveAgentAction{
		Kind:     liveActionMetadataUpdate,
		Metadata: json.RawMessage(`{"name":"Too long"}`),
	})
	if err == nil {
		t.Fatal("capped metadata update err = nil, want max_action_bytes rejection")
	}
	if applied {
		t.Fatal("capped metadata update applied = true, want false")
	}
}

func TestApplyLiveAgentActionRejectsMetadataUpdateByNonFounder(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(36)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	founderID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate founder: %v", err)
	}
	agentID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate agent: %v", err)
	}
	founder := testESPNodeInfo(t, founderID.PublicKey)
	agent := testESPNodeInfo(t, agentID.PublicKey)
	agentNodeID := *agent.MemberID
	createLiveActionGroup(t, dataDir, gid, founderID, founder)
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *agent.MemberID,
		PeerID:        agent.PeerID,
		EntmootPubKey: agent.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMetadataUpdate},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:     liveActionMetadataUpdate,
		Metadata: json.RawMessage(`{"name":"Nope"}`),
	})
	if err == nil {
		t.Fatal("applyLiveAgentAction err = nil, want founder authorization error")
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
	if _, ok, err := state.GetGroupMetadata(ctx, gid); err != nil || ok {
		t.Fatalf("GetGroupMetadata ok/err = %v/%v, want no metadata", ok, err)
	}
}

func TestApplyLiveAgentActionRejectsMetadataUpdateWithoutCreatingGroupState(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(37)
	nodeID := testAgentLiveMemberID(7)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      nodeID,
		PeerID:        testESPNodeInfo(t, id.PublicKey).PeerID,
		EntmootPubKey: append([]byte(nil), id.PublicKey...),
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMetadataUpdate},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:     liveActionMetadataUpdate,
		Metadata: json.RawMessage(`{"name":"Missing group"}`),
	})
	if err == nil {
		t.Fatal("applyLiveAgentAction err = nil, want missing membership authorization error")
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
	if membership.Exists(dataDir, gid) {
		t.Fatal("rejected action created group membership state")
	}
}

func TestApplyLiveAgentActionUpdatesOpenSubmissionTaskUsesSubmissionTime(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(28)
	nodeID := testAgentLiveMemberID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	agent := testAgentLiveAuthor(8)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-live",
		MemberID:      nodeID,
		PeerID:        agent.PeerID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey),
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	if _, err := state.UpsertFleetTask(ctx, esphttp.FleetTaskRecord{
		TaskID:      "task-open",
		FleetID:     "fleet-live",
		Title:       "Open task",
		Description: "Open submission task",
		Mode:        esphttp.FleetTaskModeOpenSubmission,
		Status:      esphttp.FleetTaskStatusOpen,
		Creator:     coordinator,
		CreatedAtMS: 10,
		UpdatedAtMS: 10,
	}); err != nil {
		t.Fatalf("UpsertFleetTask: %v", err)
	}
	before, found, err := state.GetFleetTask(ctx, "fleet-live", "task-open")
	if err != nil || !found {
		t.Fatalf("GetFleetTask before found/err = %v/%v", found, err)
	}
	time.Sleep(2 * time.Millisecond)
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskUpdateOwn},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cfg, nil, liveAgentAction{
		Kind:    liveActionTaskUpdateOwn,
		TaskID:  "task-open",
		Content: "Open task submission",
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	task, found, err := state.GetFleetTask(ctx, "fleet-live", "task-open")
	if err != nil || !found {
		t.Fatalf("GetFleetTask found/err = %v/%v", found, err)
	}
	if task.Status != esphttp.FleetTaskStatusOpen || task.UpdatedAtMS != before.UpdatedAtMS {
		t.Fatalf("open task = %+v, want open with original updated_at_ms", task)
	}
	submissions, err := state.ListFleetTaskSubmissions(ctx, "fleet-live", "task-open")
	if err != nil {
		t.Fatalf("ListFleetTaskSubmissions: %v", err)
	}
	if len(submissions) != 1 || submissions[0].CreatedAtMS <= before.UpdatedAtMS {
		t.Fatalf("submissions = %+v, want created_at_ms after task updated_at_ms %d", submissions, before.UpdatedAtMS)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "task.submitted" || activity[0].CreatedAtMS != submissions[0].CreatedAtMS {
		t.Fatalf("activity = %+v, want task.submitted at submission time %d", activity, submissions[0].CreatedAtMS)
	}
}

func TestApplyLiveAgentActionRejectsForeignOrArchivedFleetTask(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(17)
	foreignGID := testAgentLiveGroupID(18)
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	for _, rec := range []esphttp.FleetRecord{
		{FleetID: "fleet-live", Name: "Live Fleet", ControlGroupID: gid, Coordinator: coordinator, CreatedAtMS: 1},
		{FleetID: "fleet-foreign", Name: "Foreign Fleet", ControlGroupID: foreignGID, Coordinator: coordinator, CreatedAtMS: 1},
		{FleetID: "fleet-archived", Name: "Archived Fleet", ControlGroupID: gid, Coordinator: coordinator, CreatedAtMS: 1, Status: esphttp.FleetStatusArchived},
	} {
		if _, err := state.CreateFleet(ctx, rec); err != nil {
			t.Fatalf("CreateFleet(%s): %v", rec.FleetID, err)
		}
		if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
			FleetID:       rec.FleetID,
			MemberID:      nodeID,
			PeerID:        coordinator.PeerID,
			EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
			Role:          esphttp.FleetRoleCoordinator,
			Status:        esphttp.FleetMemberActive,
		}); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", rec.FleetID, err)
		}
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskCreate},
	}
	for _, fleetID := range []string{"fleet-foreign", "fleet-archived"} {
		applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cfg, nil, liveAgentAction{
			Kind:    liveActionTaskCreate,
			FleetID: fleetID,
			Title:   "Should not persist",
		})
		if err == nil {
			t.Fatalf("applyLiveAgentAction(%s) err = nil, want rejection", fleetID)
		}
		if applied {
			t.Fatalf("applyLiveAgentAction(%s) applied = true, want false", fleetID)
		}
		tasks, err := state.ListFleetTasks(ctx, fleetID, "")
		if err != nil {
			t.Fatalf("ListFleetTasks(%s): %v", fleetID, err)
		}
		if len(tasks) != 0 {
			t.Fatalf("ListFleetTasks(%s) = %+v, want none", fleetID, tasks)
		}
	}
}

func TestApplyLiveAgentActionRejectsUnauthorizedDirectAssignmentBeforePersist(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(19)
	nodeID := testAgentLiveMemberID(7)
	assigneeNodeID := testAgentLiveMemberID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(1)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: *coordinator.MemberID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: nodeID, PeerID: testAgentLiveAuthor(7).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(testAgentLiveAuthor(7).EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: assigneeNodeID, PeerID: testAgentLiveAuthor(8).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(testAgentLiveAuthor(8).EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskCreate},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cfg, nil, liveAgentAction{
		Kind:             liveActionTaskCreate,
		Title:            "Unauthorized direct task",
		Mode:             esphttp.FleetTaskModeDirectAssignment,
		AssigneeMemberID: assigneeNodeID,
	})
	if !errors.Is(err, esphttp.ErrFleetTaskUnauthorized) {
		t.Fatalf("applyLiveAgentAction err = %v, want unauthorized", err)
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
	tasks, err := state.ListFleetTasks(ctx, "fleet-live", "")
	if err != nil {
		t.Fatalf("ListFleetTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("tasks = %+v, want none after rejected direct assignment", tasks)
	}
}

func TestApplyLiveAgentActionSendsFleetCommand(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(20)
	nodeID := testAgentLiveMemberID(7)
	targetNodeID := testAgentLiveMemberID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: nodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: targetNodeID, PeerID: testAgentLiveAuthor(8).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(testAgentLiveAuthor(8).EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionCommandSend},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionCommandSend,
		Action:         esphttp.FleetCommandActionEntmootVersion,
		Target:         esphttp.FleetCommandTargetNode,
		TargetMemberID: targetNodeID,
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	topics := <-topicsCh
	if len(topics) != 1 || topics[0] != "fleet/commands" {
		t.Fatalf("topics = %+v, want [fleet/commands]", topics)
	}
	var command esphttp.FleetCommandEnvelope
	if err := json.Unmarshal(<-contentCh, &command); err != nil {
		t.Fatalf("command JSON: %v", err)
	}
	if command.Action != esphttp.FleetCommandActionEntmootVersion || command.Target.Kind != esphttp.FleetCommandTargetNode || command.Target.MemberID != targetNodeID {
		t.Fatalf("command = %+v, want version command targeting node %d", command, targetNodeID)
	}
	detail, found, err := state.GetFleetCommandDetail(ctx, "fleet-live", command.CommandID)
	if err != nil || !found {
		t.Fatalf("GetFleetCommandDetail found/err = %v/%v", found, err)
	}
	if detail.Command.CommandID != command.CommandID || detail.Command.IssuerMemberID != nodeID {
		t.Fatalf("stored command = %+v, want sent command", detail.Command)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "command.sent" || activity[0].Subject == nil || *activity[0].Subject.MemberID != targetNodeID {
		t.Fatalf("activity = %+v, want command.sent for target", activity)
	}
}

func TestApplyLiveAgentActionRequestsFleetCommand(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(38)
	nodeID := testAgentLiveMemberID(7)
	targetNodeID := testAgentLiveMemberID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: nodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: targetNodeID, PeerID: testAgentLiveAuthor(8).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(testAgentLiveAuthor(8).EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionCommandRequest},
	}
	autoAccept := true
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionCommandRequest,
		Action:         esphttp.FleetCommandActionEntmootVersion,
		Target:         esphttp.FleetCommandTargetNode,
		TargetMemberID: targetNodeID,
		AutoAccept:     &autoAccept,
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	topics := <-topicsCh
	if len(topics) != 1 || topics[0] != "fleet/commands" {
		t.Fatalf("topics = %+v, want [fleet/commands]", topics)
	}
	var command esphttp.FleetCommandEnvelope
	if err := json.Unmarshal(<-contentCh, &command); err != nil {
		t.Fatalf("command JSON: %v", err)
	}
	if !command.AutoAccept {
		t.Fatalf("command.AutoAccept = false, want true for safe command.request")
	}
	if command.Action != esphttp.FleetCommandActionEntmootVersion || command.Target.Kind != esphttp.FleetCommandTargetNode || command.Target.MemberID != targetNodeID {
		t.Fatalf("command = %+v, want version request targeting node %d", command, targetNodeID)
	}
	detail, found, err := state.GetFleetCommandDetail(ctx, "fleet-live", command.CommandID)
	if err != nil || !found {
		t.Fatalf("GetFleetCommandDetail found/err = %v/%v", found, err)
	}
	if !detail.Command.AutoAccept {
		t.Fatalf("stored command AutoAccept = false, want true")
	}
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionCommandRequest,
		Action:         esphttp.FleetCommandActionAgentInstruction,
		Target:         esphttp.FleetCommandTargetNode,
		TargetMemberID: targetNodeID,
		Instruction:    "Summarize local status",
		AutoAccept:     &autoAccept,
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction agent instruction: %v", err)
	}
	if !applied {
		t.Fatal("agent instruction applied = false, want true")
	}
	topics = <-topicsCh
	if len(topics) != 1 || topics[0] != "fleet/commands" {
		t.Fatalf("agent instruction topics = %+v, want [fleet/commands]", topics)
	}
	if err := json.Unmarshal(<-contentCh, &command); err != nil {
		t.Fatalf("agent instruction command JSON: %v", err)
	}
	if command.AutoAccept {
		t.Fatalf("agent instruction AutoAccept = true, want false")
	}
	if command.Action != esphttp.FleetCommandActionAgentInstruction || command.Args["instruction"] != "Summarize local status" {
		t.Fatalf("agent instruction command = %+v, want manual agent instruction", command)
	}
	sendCfg := cfg
	sendCfg.AllowedActions = []string{liveActionCommandSend}
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, sendCfg, nil, liveAgentAction{
		Kind:           liveActionCommandSend,
		Action:         esphttp.FleetCommandActionAgentInstruction,
		Target:         esphttp.FleetCommandTargetNode,
		TargetMemberID: targetNodeID,
		Instruction:    "Summarize local status",
		AutoAccept:     &autoAccept,
	})
	if err == nil {
		t.Fatal("command.send agent instruction auto_accept=true err = nil, want unsafe auto-accept rejection")
	}
	if applied {
		t.Fatal("command.send agent instruction applied = true, want false")
	}
	select {
	case topics := <-topicsCh:
		t.Fatalf("unexpected command.send publish topics = %+v", topics)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestApplyLiveAgentActionQueuesExternalMessageSend(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(39)
	nodeID := testAgentLiveMemberID(7)
	targetNodeID := testAgentLiveMemberID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: nodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: targetNodeID, PeerID: testAgentLiveAuthor(8).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(testAgentLiveAuthor(8).EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionExternalMessage},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:             liveActionExternalMessage,
		FleetID:          "fleet-live",
		TargetMemberID:   targetNodeID,
		Channel:          "Telegram",
		ExternalTarget:   "owner",
		ExternalActionID: "notify-owner",
		Message:          "Live rollout finished.",
		TimeoutMS:        60_000,
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	topics := <-topicsCh
	if len(topics) != 1 || topics[0] != "fleet/commands" {
		t.Fatalf("topics = %+v, want [fleet/commands]", topics)
	}
	var command esphttp.FleetCommandEnvelope
	if err := json.Unmarshal(<-contentCh, &command); err != nil {
		t.Fatalf("command JSON: %v", err)
	}
	if command.Action != esphttp.FleetCommandActionAgentInstruction || command.AutoAccept {
		t.Fatalf("command action/auto_accept = %s/%v, want manual agent instruction", command.Action, command.AutoAccept)
	}
	if command.Target.Kind != esphttp.FleetCommandTargetNode || command.Target.MemberID != targetNodeID {
		t.Fatalf("command target = %+v, want node %d", command.Target, targetNodeID)
	}
	spec, err := esphttp.FleetCommandInstructionSpecFromArgs(command.Args)
	if err != nil {
		t.Fatalf("FleetCommandInstructionSpecFromArgs: %v", err)
	}
	if !strings.Contains(spec.Instruction, "Live rollout finished.") {
		t.Fatalf("instruction = %q, want message text", spec.Instruction)
	}
	if spec.TimeoutMS != 60_000 {
		t.Fatalf("timeout = %d, want 60000", spec.TimeoutMS)
	}
	if len(spec.Actions) != 1 {
		t.Fatalf("actions = %+v, want one required message action", spec.Actions)
	}
	action := spec.Actions[0]
	if action.ID != "notify-owner" || action.Kind != esphttp.FleetCommandExternalActionMessageSend || action.Channel != "telegram" || action.Target != "owner" || !action.Required || !action.DeliveryRequired {
		t.Fatalf("action = %+v, want required telegram owner message.send", action)
	}
}

func TestApplyLiveAgentActionRejectsExternalMessageBeforePublish(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(40)
	nodeID := testAgentLiveMemberID(7)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-live",
		MemberID:      nodeID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionExternalMessage},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionExternalMessage,
		FleetID:        "fleet-live",
		TargetMemberID: nodeID,
		Channel:        "telegram",
		ExternalTarget: "owner",
	})
	if err == nil {
		t.Fatal("applyLiveAgentAction err = nil, want missing message rejection")
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
	select {
	case topics := <-topicsCh:
		t.Fatalf("unexpected publish topics = %+v", topics)
	case <-time.After(50 * time.Millisecond):
	}
	commands, err := state.ListFleetCommands(ctx, "fleet-live", esphttp.FleetCommandListFilter{})
	if err != nil {
		t.Fatalf("ListFleetCommands: %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("commands = %+v, want none", commands)
	}
}

func TestApplyLiveAgentActionRejectsCommandSendBeforePublish(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(25)
	nodeID := testAgentLiveMemberID(7)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-live",
		MemberID:      nodeID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionCommandSend},
		MaxActionBytes: 10,
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:   liveActionCommandSend,
		Action: esphttp.FleetCommandActionEntmootVersion,
	})
	if err == nil {
		t.Fatal("applyLiveAgentAction err = nil, want max_action_bytes rejection")
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
	select {
	case topics := <-topicsCh:
		t.Fatalf("unexpected publish topics = %+v", topics)
	case <-time.After(50 * time.Millisecond):
	}
	applied, err = applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionCommandSend,
		Action:         esphttp.FleetCommandActionEntmootVersion,
		TargetMemberID: testAgentLiveMemberID(8),
	})
	if err == nil {
		t.Fatal("applyLiveAgentAction ambiguous target err = nil, want rejection")
	}
	if applied {
		t.Fatal("ambiguous target applied = true, want false")
	}
	select {
	case topics := <-topicsCh:
		t.Fatalf("unexpected ambiguous target publish topics = %+v", topics)
	case <-time.After(50 * time.Millisecond):
	}
	commands, err := state.ListFleetCommands(ctx, "fleet-live", esphttp.FleetCommandListFilter{})
	if err != nil {
		t.Fatalf("ListFleetCommands: %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("commands = %+v, want none", commands)
	}
}

func TestApplyLiveAgentActionCreatesFleetInvite(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(32)
	coordinatorNodeID := testAgentLiveMemberID(7)
	targetNodeID := testAgentLiveMemberID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	targetEntPub := testAgentLiveAuthor(8).EntmootPubKey
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		Status:         esphttp.FleetStatusActive,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-live",
		MemberID:      coordinatorNodeID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	inviteCh := make(chan *ipc.InviteCreateReq, 1)
	stop := serveLiveInfoInviteCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, gid, inviteCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       coordinatorNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionInviteCreate},
	}
	for _, wrongID := range []entmoot.MemberID{{}, testAgentLiveMemberID(9)} {
		applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
			Kind: liveActionInviteCreate, TargetMemberID: wrongID, TargetEntKey: base64.StdEncoding.EncodeToString(targetEntPub),
		})
		if err == nil || applied {
			t.Fatalf("mismatched recipient accepted: member=%s applied=%v err=%v", wrongID, applied, err)
		}
		select {
		case request := <-inviteCh:
			t.Fatalf("invalid recipient reached invitation IPC: %+v", request)
		default:
		}
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionInviteCreate,
		TargetMemberID: targetNodeID,
		TargetEntKey:   base64.StdEncoding.EncodeToString(targetEntPub),
		Hostname:       "deimos",
		ValidFor:       "1h",
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	select {
	case req := <-inviteCh:
		if req.GroupID != gid || !bytes.Equal(req.TargetPublicKey, targetEntPub) {
			t.Fatalf("invite request does not bind the requested recipient: %+v", req)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	members, err := state.ListFleetMembers(ctx, "fleet-live")
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	invited, ok := fleetMemberForNode(members, targetNodeID)
	if !ok || invited.Status != esphttp.FleetMemberInvited || invited.Hostname != "deimos" {
		t.Fatalf("invited member = %+v/%v, want invited deimos", invited, ok)
	}
	invites, err := state.ListFleetInvites(ctx, "fleet-live")
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].MemberID != targetNodeID || invites[0].Status != esphttp.FleetMemberInvited {
		t.Fatalf("invites = %+v, want one invited target", invites)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "member.invited" || activity[0].Subject == nil || *activity[0].Subject.MemberID != targetNodeID {
		t.Fatalf("activity = %+v, want member.invited for target", activity)
	}
}

func TestApplyLiveAgentActionRejectsInviteCreateByNonCoordinator(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(33)
	coordinatorNodeID := testAgentLiveMemberID(7)
	agentNodeID := testAgentLiveMemberID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		Status:         esphttp.FleetStatusActive,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: coordinatorNodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: agentNodeID, PeerID: testAgentLiveAuthor(8).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(testAgentLiveAuthor(8).EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionInviteCreate},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionInviteCreate,
		TargetMemberID: testAgentLiveMemberID(9),
		TargetEntKey:   base64.StdEncoding.EncodeToString(testAgentLiveAuthor(9).EntmootPubKey),
	})
	if !errors.Is(err, esphttp.ErrFleetTaskUnauthorized) {
		t.Fatalf("applyLiveAgentAction err = %v, want unauthorized", err)
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
	members, err := state.ListFleetMembers(ctx, "fleet-live")
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if _, ok := fleetMemberForNode(members, testAgentLiveMemberID(9)); ok {
		t.Fatal("target member exists, want no invite mutation")
	}
}

func TestApplyLiveAgentActionRemovesFleetMember(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(29)
	coordinatorNodeID := testAgentLiveMemberID(7)
	targetNodeID := testAgentLiveMemberID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	targetPub := testAgentLiveAuthor(8).EntmootPubKey
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: coordinatorNodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: targetNodeID, PeerID: testAgentLiveAuthor(8).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(targetPub), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberInvited},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-target",
		FleetID:       "fleet-live",
		MemberID:      targetNodeID,
		PeerID:        testAgentLiveAuthor(8).PeerID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(targetPub),
		Status:        esphttp.FleetMemberInvited,
		Capability:    json.RawMessage(`{"group_id":"test"}`),
		CreatedAtMS:   1,
	}); err != nil {
		t.Fatalf("CreateFleetInvite: %v", err)
	}
	removeCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveLiveInfoRemoveCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, removeCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       coordinatorNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMemberRemove},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionMemberRemove,
		TargetMemberID: targetNodeID,
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	select {
	case req := <-removeCh:
		if req.GroupID != gid || *req.Target.MemberID != targetNodeID || base64.StdEncoding.EncodeToString(req.Target.EntmootPubKey) != base64.StdEncoding.EncodeToString(targetPub) {
			t.Fatalf("member remove req = %+v, want target %d", req, targetNodeID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for member remove IPC request")
	}
	members, err := state.ListFleetMembers(ctx, "fleet-live")
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	removed, ok := fleetMemberForNode(members, targetNodeID)
	if !ok || removed.Status != esphttp.FleetMemberRemoved {
		t.Fatalf("removed member = %+v/%v, want removed", removed, ok)
	}
	invites, err := state.ListFleetInvites(ctx, "fleet-live")
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites = %+v, want removed invite cleared", invites)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "member.removed" || activity[0].Subject == nil || *activity[0].Subject.MemberID != targetNodeID {
		t.Fatalf("activity = %+v, want member.removed for target", activity)
	}
}

func TestApplyLiveAgentActionMemberRemoveRetriesDroppedIPC(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(31)
	coordinatorNodeID := testAgentLiveMemberID(7)
	targetNodeID := testAgentLiveMemberID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	targetPub := testAgentLiveAuthor(8).EntmootPubKey
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: coordinatorNodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: targetNodeID, PeerID: testAgentLiveAuthor(8).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(targetPub), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	removeCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveLiveInfoRemoveDrop(t, controlSocketPath(dataDir), &ipc.InfoResp{
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, removeCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       coordinatorNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMemberRemove},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: dataDir}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionMemberRemove,
		TargetMemberID: targetNodeID,
	})
	if !errors.Is(err, errLiveActionTransport) {
		t.Fatalf("applyLiveAgentAction err = %v, want live action transport", err)
	}
	if applied {
		t.Fatal("applied = true, want false for retryable IPC failure")
	}
	select {
	case <-removeCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for member remove IPC request")
	}
}

func TestApplyLiveAgentActionRejectsMemberRemoveByNonCoordinator(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(30)
	coordinatorNodeID := testAgentLiveMemberID(7)
	agentNodeID := testAgentLiveMemberID(8)
	targetNodeID := testAgentLiveMemberID(9)
	state := esphttp.NewMemoryStateStore()
	coordinator := testAgentLiveAuthor(7)
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:        "fleet-live",
		Name:           "Live Fleet",
		ControlGroupID: gid,
		Coordinator:    coordinator,
		CreatedAtMS:    1,
	}); err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	for _, member := range []esphttp.FleetMemberRecord{
		{FleetID: "fleet-live", MemberID: coordinatorNodeID, PeerID: coordinator.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: agentNodeID, PeerID: testAgentLiveAuthor(8).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(testAgentLiveAuthor(8).EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", MemberID: targetNodeID, PeerID: testAgentLiveAuthor(9).PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(testAgentLiveAuthor(9).EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", member.MemberID, err)
		}
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMemberRemove},
	}
	applied, err := applyLiveAgentAction(ctx, enableCoordinationFeatures(&globalFlags{data: t.TempDir()}), state, cfg, nil, liveAgentAction{
		Kind:           liveActionMemberRemove,
		TargetMemberID: targetNodeID,
	})
	if !errors.Is(err, esphttp.ErrFleetTaskUnauthorized) {
		t.Fatalf("applyLiveAgentAction err = %v, want unauthorized", err)
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
	members, err := state.ListFleetMembers(ctx, "fleet-live")
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	target, ok := fleetMemberForNode(members, targetNodeID)
	if !ok || target.Status != esphttp.FleetMemberActive {
		t.Fatalf("target member = %+v/%v, want still active", target, ok)
	}
}

func serveLiveInfoPublishCapture(t *testing.T, sock string, info *ipc.InfoResp, topicsCh chan<- []string, contentCh chan<- []byte) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch req := payload.(type) {
			case *ipc.InfoReq:
				_ = ipc.EncodeAndWrite(conn, info)
			case *ipc.PublishReq:
				topicsCh <- append([]string(nil), req.Topics...)
				contentCh <- append([]byte(nil), req.Content...)
				resp := &ipc.PublishResp{TimestampMS: time.Now().UnixMilli()}
				if req.GroupID != nil {
					resp.GroupID = *req.GroupID
				}
				_ = ipc.EncodeAndWrite(conn, resp)
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveLiveInfoInviteCapture(t *testing.T, sock string, info *ipc.InfoResp, gid entmoot.GroupID, inviteCh chan<- *ipc.InviteCreateReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch req := payload.(type) {
			case *ipc.InfoReq:
				_ = ipc.EncodeAndWrite(conn, info)
			case *ipc.InviteCreateReq:
				inviteCh <- req
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{
					Status:     "created",
					GroupID:    gid,
					Capability: entmoot.BootstrapCapability{GroupID: gid},
					RosterHead: entmoot.RosterEntryID{},
					Members:    2,
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveLiveInfoRemoveCapture(t *testing.T, sock string, info *ipc.InfoResp, removeCh chan<- *ipc.MemberRemoveReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch req := payload.(type) {
			case *ipc.InfoReq:
				_ = ipc.EncodeAndWrite(conn, info)
			case *ipc.MemberRemoveReq:
				removeCh <- req
				_ = ipc.EncodeAndWrite(conn, &ipc.MemberRemoveResp{
					Status:     "removed",
					GroupID:    req.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveLiveInfoRemoveDrop(t *testing.T, sock string, info *ipc.InfoResp, removeCh chan<- *ipc.MemberRemoveReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch req := payload.(type) {
			case *ipc.InfoReq:
				_ = ipc.EncodeAndWrite(conn, info)
			case *ipc.MemberRemoveReq:
				removeCh <- req
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func createLiveActionGroup(t *testing.T, dataDir string, gid entmoot.GroupID, founderID *keystore.Identity, founder entmoot.NodeInfo) {
	t.Helper()
	group, err := membership.Create(dataDir, founderID, founder, gid, membership.DefaultPolicy(), 1_700_000_000_000)
	if err != nil {
		t.Fatalf("membership.Create: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("membership close: %v", err)
	}
}
