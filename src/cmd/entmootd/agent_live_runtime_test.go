package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/store"
)

func TestRunAgentLiveScanListenAdvancesCursorWithoutRunner(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(1)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if err := msgStore.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeListen,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	result, err := runAgentLiveScan(ctx, &globalFlags{}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		timeout: time.Second,
		limit:   10,
	})
	if err != nil {
		t.Fatalf("runAgentLiveScan: %v", err)
	}
	if result.Seen != 1 || result.Matched != 1 || result.Proposed != 0 || result.Applied != 0 {
		t.Fatalf("result = %+v, want one matched listen event and no action", result)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v", ok, err)
	}
	if cursor.LastSeenAtMS != msg.Timestamp {
		t.Fatalf("cursor.LastSeenAtMS = %d, want %d", cursor.LastSeenAtMS, msg.Timestamp)
	}
	if cursor.LastSeenAuthorNodeID != msg.Author.PilotNodeID || cursor.LastSeenMessageID != msg.ID {
		t.Fatalf("cursor tie-breaker = %d/%s, want %d/%s", cursor.LastSeenAuthorNodeID, cursor.LastSeenMessageID, msg.Author.PilotNodeID, msg.ID)
	}
}

func TestRunAgentLiveScanFirstRunUsesConfigUpdatedAtFloor(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(2)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "old")
	if err := msgStore.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeListen,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  200,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	result, err := runAgentLiveScan(ctx, &globalFlags{}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		timeout: time.Second,
		limit:   10,
	})
	if err != nil {
		t.Fatalf("runAgentLiveScan: %v", err)
	}
	if result.Seen != 0 || result.Matched != 0 {
		t.Fatalf("result = %+v, want no replayed history", result)
	}
	if _, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID); err != nil || ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v, want no cursor", ok, err)
	}
}

func TestRunAgentLiveScanCursorTieBreakerProcessesSameTimestamp(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(3)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	first := testAgentLiveMessage(gid, 11, 100, "chat", "first")
	second := testAgentLiveMessage(gid, 12, 100, "chat", "second")
	for _, msg := range []entmoot.Message{first, second} {
		if err := msgStore.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeListen,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	if _, err := state.UpsertLiveAgentCursor(ctx, cursorFromMessage(gid, nodeID, first)); err != nil {
		t.Fatalf("UpsertLiveAgentCursor: %v", err)
	}
	result, err := runAgentLiveScan(ctx, &globalFlags{}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		timeout: time.Second,
		limit:   10,
	})
	if err != nil {
		t.Fatalf("runAgentLiveScan: %v", err)
	}
	if result.Seen != 1 || result.Matched != 1 {
		t.Fatalf("result = %+v, want only later same-timestamp message", result)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v", ok, err)
	}
	if cursor.LastSeenMessageID != second.ID {
		t.Fatalf("cursor.LastSeenMessageID = %s, want %s", cursor.LastSeenMessageID, second.ID)
	}
}

func TestRunAgentLiveScanSeenIDsCatchDelayedOlderTimestamp(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(8)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	alreadySeen := testAgentLiveMessage(gid, 11, 200, "chat", "seen")
	delayed := testAgentLiveMessage(gid, 12, 150, "chat", "delayed")
	if err := msgStore.Put(ctx, delayed); err != nil {
		t.Fatalf("Put delayed: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeListen,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	if _, err := state.UpsertLiveAgentCursor(ctx, cursorFromMessage(gid, nodeID, alreadySeen)); err != nil {
		t.Fatalf("UpsertLiveAgentCursor: %v", err)
	}
	result, err := runAgentLiveScan(ctx, &globalFlags{}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		timeout: time.Second,
		limit:   10,
	})
	if err != nil {
		t.Fatalf("runAgentLiveScan: %v", err)
	}
	if result.Seen != 1 || result.Matched != 1 {
		t.Fatalf("result = %+v, want delayed older message processed", result)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v", ok, err)
	}
	if cursor.LastSeenMessageID != alreadySeen.ID {
		t.Fatalf("cursor.LastSeenMessageID = %s, want latest key %s", cursor.LastSeenMessageID, alreadySeen.ID)
	}
	if len(cursor.SeenMessageIDs) != 2 || cursor.SeenMessageIDs[1] != delayed.ID {
		t.Fatalf("cursor.SeenMessageIDs = %+v, want delayed message retained", cursor.SeenMessageIDs)
	}
}

func TestRunAgentLiveScanSeenIDEvictionDoesNotReplayOldMessages(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(10)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	old := testAgentLiveMessage(gid, 11, 900, "chat", "old")
	fresh := testAgentLiveMessage(gid, 13, 1001, "chat", "fresh")
	for _, msg := range []entmoot.Message{old, fresh} {
		if err := msgStore.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	var highWater entmoot.MessageID
	highWater[0] = 12
	highWater[1] = 232
	seen := make([]entmoot.MessageID, liveCursorMaxSeenIDs)
	for i := range seen {
		seen[i][0] = byte(i)
		seen[i][1] = byte(i >> 8)
	}
	if _, err := state.UpsertLiveAgentCursor(ctx, esphttp.LiveAgentCursor{
		GroupID:              gid,
		NodeID:               nodeID,
		ScanFloorAtMS:        400,
		LastSeenAtMS:         1000,
		LastSeenAuthorNodeID: 12,
		LastSeenMessageID:    highWater,
		SeenMessageIDs:       seen,
		UpdatedAtMS:          1000,
	}); err != nil {
		t.Fatalf("UpsertLiveAgentCursor: %v", err)
	}
	runner := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nprintf '{\"actions\":[]}'\n"), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	result, err := runAgentLiveScan(ctx, &globalFlags{}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		runner:  runner,
		timeout: time.Second,
		limit:   1,
	})
	if err != nil {
		t.Fatalf("runAgentLiveScan: %v", err)
	}
	if result.Seen != 1 || result.Matched != 1 {
		t.Fatalf("result = %+v, want only fresh message after saturated replay window", result)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v", ok, err)
	}
	if cursor.LastSeenMessageID != fresh.ID {
		t.Fatalf("cursor.LastSeenMessageID = %s, want %s", cursor.LastSeenMessageID, fresh.ID)
	}
	if cursor.ScanFloorAtMS != fresh.Timestamp {
		t.Fatalf("cursor.ScanFloorAtMS = %d, want %d", cursor.ScanFloorAtMS, fresh.Timestamp)
	}
}

func TestLiveScanFloorSlidesPastEnableTime(t *testing.T) {
	gid := testAgentLiveGroupID(13)
	cfg := esphttp.LiveAgentConfig{
		GroupID:     gid,
		NodeID:      7,
		UpdatedAtMS: 100,
	}
	cursor := esphttp.LiveAgentCursor{
		GroupID:       gid,
		NodeID:        7,
		ScanFloorAtMS: 100,
		LastSeenAtMS:  100 + liveCursorOverlapWindow.Milliseconds() + 50,
	}
	want := cursor.LastSeenAtMS - liveCursorOverlapWindow.Milliseconds()
	if got := liveScanFloor(cfg, cursor, true); got != want {
		t.Fatalf("liveScanFloor = %d, want %d", got, want)
	}
}

func TestRunAgentLiveScanPublishFailureKeepsCursor(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(4)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if err := msgStore.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	runner := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nprintf '{\"actions\":[{\"kind\":\"reply\",\"message\":\"ok\"}]}'\n"), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	_, err = runAgentLiveScan(ctx, &globalFlags{data: t.TempDir()}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		runner:  runner,
		timeout: time.Second,
		limit:   10,
	})
	if !errors.Is(err, errLiveActionTransport) {
		t.Fatalf("runAgentLiveScan err = %v, want live action transport", err)
	}
	if _, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID); err != nil || ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v, want no consumed cursor", ok, err)
	}
}

func TestRunAgentLiveScanPartialPublishFailurePersistsCursor(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(9)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if err := msgStore.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	runner := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nprintf '{\"actions\":[{\"kind\":\"reply\",\"message\":\"one\"},{\"kind\":\"reply\",\"message\":\"two\"}]}'\n"), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	stop := serveLivePublishOnceThenFail(t, controlSocketPath(dataDir))
	defer stop()
	result, err := runAgentLiveScan(ctx, &globalFlags{data: dataDir}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		runner:  runner,
		timeout: time.Second,
		limit:   10,
	})
	if !errors.Is(err, errLiveActionTransport) {
		t.Fatalf("runAgentLiveScan err = %v, want live action transport", err)
	}
	if result.Applied != 1 {
		t.Fatalf("result.Applied = %d, want 1", result.Applied)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v", ok, err)
	}
	if cursor.LastSeenMessageID != msg.ID {
		t.Fatalf("cursor.LastSeenMessageID = %s, want %s", cursor.LastSeenMessageID, msg.ID)
	}
}

func TestRunAgentLiveScanHonorsMaxActionsPerScan(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(14)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if err := msgStore.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:           gid,
		NodeID:            nodeID,
		Enabled:           true,
		Mode:              esphttp.LiveModeConverse,
		TopicFilters:      []string{"chat"},
		MaxActionsPerScan: 1,
		UpdatedAtMS:       1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	runner := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nprintf '{\"actions\":[{\"kind\":\"reply\",\"message\":\"one\"},{\"kind\":\"reply\",\"message\":\"two\"}]}'\n"), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	topicsCh := make(chan []string, 1)
	stop := serveLivePublishCapture(t, controlSocketPath(dataDir), topicsCh)
	defer stop()
	result, err := runAgentLiveScan(ctx, &globalFlags{data: dataDir}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		runner:  runner,
		timeout: time.Second,
		limit:   10,
	})
	if err != nil {
		t.Fatalf("runAgentLiveScan: %v", err)
	}
	if result.Proposed != 2 || result.Applied != 1 || result.Rejected != 1 {
		t.Fatalf("result = %+v, want two proposed, one applied, one rejected by budget", result)
	}
	<-topicsCh
}

func TestRunAgentLiveScanLimitAdvancesOnlySentBatch(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(5)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msgs := []entmoot.Message{
		testAgentLiveMessage(gid, 11, 100, "chat", "one"),
		testAgentLiveMessage(gid, 12, 101, "chat", "two"),
		testAgentLiveMessage(gid, 13, 102, "chat", "three"),
	}
	for _, msg := range msgs {
		if err := msgStore.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	runner := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nprintf '{\"actions\":[]}'\n"), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	result, err := runAgentLiveScan(ctx, &globalFlags{}, state, msgStore, cfg, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  nodeID,
		runner:  runner,
		timeout: time.Second,
		limit:   2,
	})
	if err != nil {
		t.Fatalf("runAgentLiveScan: %v", err)
	}
	if result.Seen != 2 || result.Matched != 2 {
		t.Fatalf("result = %+v, want only limited sent batch", result)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v", ok, err)
	}
	if cursor.LastSeenMessageID != msgs[1].ID {
		t.Fatalf("cursor.LastSeenMessageID = %s, want %s", cursor.LastSeenMessageID, msgs[1].ID)
	}
}

func TestApplyLiveAgentActionAllowsOwnerAlertOutsideFilters(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(6)
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         7,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		TopicFilters:   []string{"tasks/#"},
		AllowedActions: []string{liveActionAlertOwner},
	}
	_, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, cfg, nil, liveAgentAction{
		Kind:    liveActionAlertOwner,
		Message: "owner check",
	})
	if !errors.Is(err, errLiveActionTransport) {
		t.Fatalf("applyLiveAgentAction err = %v, want transport attempt after authorization", err)
	}
}

func TestApplyLiveAgentActionDefaultsWhitespaceOwnerAlertTopic(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(11)
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         7,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		TopicFilters:   []string{"tasks/#"},
		AllowedActions: []string{liveActionAlertOwner},
	}
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	topicsCh := make(chan []string, 1)
	stop := serveLivePublishCapture(t, controlSocketPath(dataDir), topicsCh)
	defer stop()
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, cfg, []liveAgentRunnerMessage{{
		Topics: []string{"tasks/incident"},
	}}, liveAgentAction{
		Kind:    liveActionAlertOwner,
		Message: "owner check",
		Topic:   "   ",
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatalf("applied = false, want true")
	}
	topics := <-topicsCh
	if len(topics) != 1 || topics[0] != "alerts/owner" {
		t.Fatalf("topics = %+v, want [alerts/owner]", topics)
	}
}

func TestApplyLiveAgentActionDefaultsToMatchedReplyTopic(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(12)
	cfg := esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       7,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"ops"},
	}
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	topicsCh := make(chan []string, 1)
	stop := serveLivePublishCapture(t, controlSocketPath(dataDir), topicsCh)
	defer stop()
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, cfg, []liveAgentRunnerMessage{{
		Topics: []string{"noise", "ops"},
	}}, liveAgentAction{
		Kind:    liveActionReply,
		Message: "ack",
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatalf("applied = false, want true")
	}
	topics := <-topicsCh
	if len(topics) != 1 || topics[0] != "ops" {
		t.Fatalf("topics = %+v, want [ops]", topics)
	}
}

func TestApplyLiveAgentActionRejectsOversizedMessage(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(15)
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         7,
		Enabled:        true,
		Mode:           esphttp.LiveModeConverse,
		TopicFilters:   []string{"chat"},
		MaxActionBytes: 3,
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, cfg, []liveAgentRunnerMessage{{
		Topics: []string{"chat"},
	}}, liveAgentAction{
		Kind:    liveActionReply,
		Message: "four",
	})
	if err == nil {
		t.Fatal("applyLiveAgentAction err = nil, want max_action_bytes rejection")
	}
	if errors.Is(err, errLiveActionTransport) {
		t.Fatalf("applyLiveAgentAction err = %v, want validation rejection before transport", err)
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
}

func TestLiveMessageMentionsAgentRequiresExactToken(t *testing.T) {
	gid := testAgentLiveGroupID(7)
	matches := []string{
		"hello @7",
		"hello @agent-7,",
		"please check node:7.",
	}
	for _, content := range matches {
		if !liveMessageMentionsAgent(testAgentLiveMessage(gid, 11, 100, "chat", content), 7) {
			t.Fatalf("liveMessageMentionsAgent(%q, 7) = false, want true", content)
		}
	}
	nonMatches := []string{
		"hello @71",
		"hello @agent-70",
		"please check node:72",
	}
	for _, content := range nonMatches {
		if liveMessageMentionsAgent(testAgentLiveMessage(gid, 11, 100, "chat", content), 7) {
			t.Fatalf("liveMessageMentionsAgent(%q, 7) = true, want false", content)
		}
	}
}

func TestParseLiveRunnerOutputExtractsJSON(t *testing.T) {
	output, err := parseLiveRunnerOutput("text\n{\"actions\":[{\"kind\":\"reply\",\"message\":\"ok\"}]}\n")
	if err != nil {
		t.Fatalf("parseLiveRunnerOutput: %v", err)
	}
	if len(output.Actions) != 1 || output.Actions[0].Kind != "reply" || output.Actions[0].Message != "ok" {
		t.Fatalf("output = %+v", output)
	}
}

func testAgentLiveGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}

func testAgentLiveMessage(gid entmoot.GroupID, author entmoot.NodeID, ts int64, topic, content string) entmoot.Message {
	var id entmoot.MessageID
	id[0] = byte(author)
	id[1] = byte(ts)
	return entmoot.Message{
		ID:        id,
		GroupID:   gid,
		Author:    entmoot.NodeInfo{PilotNodeID: author, EntmootPubKey: []byte{1, 2, 3}},
		Timestamp: ts,
		Topics:    []string{topic},
		Content:   []byte(content),
	}
}

func serveLivePublishOnceThenFail(t *testing.T, sock string) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for i := 0; i < 2; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			if i == 0 {
				_, payload, err := ipc.ReadAndDecode(conn)
				if err == nil {
					if _, ok := payload.(*ipc.PublishReq); ok {
						_ = ipc.EncodeAndWrite(conn, &ipc.PublishResp{MessageID: testAgentLiveMessageID(1), GroupID: testAgentLiveGroupID(9), TimestampMS: 123})
					}
				}
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveLivePublishCapture(t *testing.T, sock string, topicsCh chan<- []string) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, payload, err := ipc.ReadAndDecode(conn)
		if err != nil {
			return
		}
		req, ok := payload.(*ipc.PublishReq)
		if !ok {
			return
		}
		topicsCh <- append([]string(nil), req.Topics...)
		resp := &ipc.PublishResp{TimestampMS: time.Now().UnixMilli()}
		if req.GroupID != nil {
			resp.GroupID = *req.GroupID
		}
		_ = ipc.EncodeAndWrite(conn, resp)
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func testAgentLiveMessageID(seed byte) entmoot.MessageID {
	var id entmoot.MessageID
	id[0] = seed
	return id
}
