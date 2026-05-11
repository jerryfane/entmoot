package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
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

func TestAgentLiveRunGroupsFiltersByTags(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	ops := testAgentLiveGroupID(21)
	docs := testAgentLiveGroupID(22)
	untagged := testAgentLiveGroupID(23)
	disabled := testAgentLiveGroupID(24)
	nodeID := entmoot.NodeID(7)
	for _, gid := range []entmoot.GroupID{ops, docs, untagged, disabled} {
		enabled := gid != disabled
		if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
			GroupID:     gid,
			NodeID:      nodeID,
			Enabled:     enabled,
			Mode:        esphttp.LiveModeListen,
			UpdatedAtMS: 1,
		}); err != nil {
			t.Fatalf("UpsertLiveAgentConfig: %v", err)
		}
	}
	if err := state.SetGroupMetadata(ctx, ops, json.RawMessage(`{"tags":["ops","ios"]}`)); err != nil {
		t.Fatalf("SetGroupMetadata ops: %v", err)
	}
	if err := state.SetGroupMetadata(ctx, docs, json.RawMessage(`{"tags":["docs"]}`)); err != nil {
		t.Fatalf("SetGroupMetadata docs: %v", err)
	}
	if err := state.SetGroupMetadata(ctx, disabled, json.RawMessage(`{"tags":["ops"]}`)); err != nil {
		t.Fatalf("SetGroupMetadata disabled: %v", err)
	}
	got, err := agentLiveRunGroups(ctx, state, "", true, nodeID, []string{"ops"})
	if err != nil {
		t.Fatalf("agentLiveRunGroups ops: %v", err)
	}
	if len(got) != 1 || got[0].GroupID != ops || got[0].Mode != esphttp.LiveModeListen {
		t.Fatalf("ops groups = %v, want only %s", got, ops)
	}
	got, err = agentLiveRunGroups(ctx, state, "", true, nodeID, []string{"ops", "ios"})
	if err != nil {
		t.Fatalf("agentLiveRunGroups ops+ios: %v", err)
	}
	if len(got) != 1 || got[0].GroupID != ops {
		t.Fatalf("ops+ios groups = %v, want only %s", got, ops)
	}
	got, err = agentLiveRunGroups(ctx, state, "", true, nodeID, nil)
	if err != nil {
		t.Fatalf("agentLiveRunGroups all: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("all groups = %v, want 3", got)
	}
}

func TestValidateAgentLiveAllGroupsLeaseRequiresAggregateBudget(t *testing.T) {
	adapterGroups := []agentLiveRunGroup{
		{GroupID: testAgentLiveGroupID(31), Mode: esphttp.LiveModeConverse},
		{GroupID: testAgentLiveGroupID(32), Mode: esphttp.LiveModeConverse},
		{GroupID: testAgentLiveGroupID(33), Mode: esphttp.LiveModeConverse},
	}
	err := validateAgentLiveAllGroupsLease(true, false, adapterGroups, 10*time.Second, 30*time.Second, 45*time.Second)
	if err == nil {
		t.Fatal("validateAgentLiveAllGroupsLease accepted lease shorter than aggregate scan budget")
	}
	if err := validateAgentLiveAllGroupsLease(true, false, adapterGroups, 10*time.Second, 30*time.Second, 2*time.Minute); err != nil {
		t.Fatalf("validateAgentLiveAllGroupsLease rejected sufficient lease: %v", err)
	}
	twoAdapterGroups := []agentLiveRunGroup{
		{GroupID: testAgentLiveGroupID(34), Mode: esphttp.LiveModeConverse},
		{GroupID: testAgentLiveGroupID(35), Mode: esphttp.LiveModeConverse},
	}
	if err := validateAgentLiveAllGroupsLease(true, false, twoAdapterGroups, 10*time.Second, 30*time.Second, 45*time.Second); err != nil {
		t.Fatalf("validateAgentLiveAllGroupsLease rejected default two-group budget: %v", err)
	}
	listenGroups := []agentLiveRunGroup{
		{GroupID: testAgentLiveGroupID(36), Mode: esphttp.LiveModeListen},
		{GroupID: testAgentLiveGroupID(37), Mode: esphttp.LiveModeListen},
		{GroupID: testAgentLiveGroupID(38), Mode: esphttp.LiveModeListen},
	}
	if err := validateAgentLiveAllGroupsLease(true, false, listenGroups, 10*time.Second, 30*time.Second, 45*time.Second); err != nil {
		t.Fatalf("validateAgentLiveAllGroupsLease rejected listen-only defaults: %v", err)
	}
	mixedGroups := []agentLiveRunGroup{
		{GroupID: testAgentLiveGroupID(39), Mode: esphttp.LiveModeListen},
		{GroupID: testAgentLiveGroupID(40), Mode: esphttp.LiveModeConverse},
		{GroupID: testAgentLiveGroupID(41), Mode: esphttp.LiveModeOperator},
	}
	if err := validateAgentLiveAllGroupsLease(true, false, mixedGroups, 10*time.Second, 30*time.Second, 45*time.Second); err == nil {
		t.Fatal("validateAgentLiveAllGroupsLease accepted listen group followed by two adapter groups with default lease")
	}
	adapterBeforeListen := []agentLiveRunGroup{
		{GroupID: testAgentLiveGroupID(42), Mode: esphttp.LiveModeConverse},
		{GroupID: testAgentLiveGroupID(43), Mode: esphttp.LiveModeListen},
	}
	if err := validateAgentLiveAllGroupsLease(true, false, adapterBeforeListen, 10*time.Second, 30*time.Second, 35*time.Second); err == nil {
		t.Fatal("validateAgentLiveAllGroupsLease ignored earlier adapter group before listen renewal")
	}
	if err := validateAgentLiveAllGroupsLease(true, true, adapterGroups, 10*time.Second, 30*time.Second, 45*time.Second); err != nil {
		t.Fatalf("validateAgentLiveAllGroupsLease rejected once mode: %v", err)
	}
}

func TestRenewAgentLiveRunBindingsSkipsMissingOnlyInAllGroupsMode(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(31)
	missing := testAgentLiveGroupID(32)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeListen,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	}); err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	bindings, err := renewAgentLiveRunBindings(ctx, state, []agentLiveRunGroup{{GroupID: gid}, {GroupID: missing}}, nodeID, time.Second, false)
	if err != nil {
		t.Fatalf("renewAgentLiveRunBindings all groups: %v", err)
	}
	if len(bindings) != 1 || bindings[0].Config.GroupID != gid || bindings[0].Presence.GroupID != gid {
		t.Fatalf("bindings = %+v, want only configured group", bindings)
	}
	if _, err := renewAgentLiveRunBindings(ctx, state, []agentLiveRunGroup{{GroupID: missing}}, nodeID, time.Second, true); err == nil {
		t.Fatal("renewAgentLiveRunBindings single group missing config succeeded, want error")
	}
}

func TestPrintAgentLiveRunJSONPreservesSingleGroupShape(t *testing.T) {
	gid := testAgentLiveGroupID(41)
	binding := agentLiveRunBinding{Presence: esphttp.LiveAgentPresence{GroupID: gid, NodeID: 7, Status: esphttp.LiveStatusOnline}}
	scan := agentLiveRunGroupScan{GroupID: gid, Scan: agentLiveScanResult{Seen: 1, Matched: 1}}
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return printAgentLiveRunJSON(false, []agentLiveRunBinding{binding}, []agentLiveRunGroupScan{scan})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("printAgentLiveRunJSON single code/stderr = %d/%q", code, stderr)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("single JSON: %v; stdout=%s", err, stdout)
	}
	if _, ok := got["presence"]; !ok {
		t.Fatalf("single JSON = %s, missing presence", stdout)
	}
	if _, ok := got["scan"]; !ok {
		t.Fatalf("single JSON = %s, missing scan", stdout)
	}
	if _, ok := got["scans"]; ok {
		t.Fatalf("single JSON = %s, must not use plural scans", stdout)
	}
	code, stdout, stderr = captureCommandOutput(t, func() int {
		return printAgentLiveRunJSON(true, []agentLiveRunBinding{binding}, []agentLiveRunGroupScan{scan})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("printAgentLiveRunJSON all-groups code/stderr = %d/%q", code, stderr)
	}
	got = nil
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("all-groups JSON: %v; stdout=%s", err, stdout)
	}
	if _, ok := got["scans"]; !ok {
		t.Fatalf("all-groups JSON = %s, missing plural scans", stdout)
	}
	if _, ok := got["scan"]; ok {
		t.Fatalf("all-groups JSON = %s, must not use singular scan", stdout)
	}
}

func TestCmdAgentLiveRunMalformedGroupIsInvalidArgument(t *testing.T) {
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdAgentLiveRun(&globalFlags{data: t.TempDir()}, []string{"-group", "not-a-group", "-node", "7", "-once"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("cmdAgentLiveRun code = %d, want %d; stderr=%s", code, exitInvalidArgument, stderr)
	}
	if !strings.Contains(stderr, "agent-live run:") {
		t.Fatalf("stderr = %q, want agent-live run error", stderr)
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
