package main

import (
	"bytes"
	"context"
	"encoding/base64"
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
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
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

func TestRunAgentLiveScanUnwrapsCommandRunnerOutput(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(55)
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
	if err := os.WriteFile(runner, []byte(`#!/bin/sh
cat <<'JSON'
{"status":"completed","summary":"done","output":"{\"actions\":[{\"kind\":\"reply\",\"message\":\"wrapped\"}]}"}
JSON
`), 0o700); err != nil {
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
	if result.Proposed != 1 || result.Applied != 1 || result.Rejected != 0 {
		t.Fatalf("result = %+v, want one applied unwrapped action", result)
	}
	if topics := <-topicsCh; len(topics) != 1 || topics[0] != "chat" {
		t.Fatalf("topics = %+v, want chat", topics)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v", ok, err)
	}
	if cursor.LastSeenMessageID != msg.ID {
		t.Fatalf("cursor.LastSeenMessageID = %s, want %s", cursor.LastSeenMessageID, msg.ID)
	}
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
	_, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, esphttp.NewMemoryStateStore(), cfg, nil, liveAgentAction{
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
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, esphttp.NewMemoryStateStore(), cfg, []liveAgentRunnerMessage{{
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
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, esphttp.NewMemoryStateStore(), cfg, []liveAgentRunnerMessage{{
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
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, esphttp.NewMemoryStateStore(), cfg, []liveAgentRunnerMessage{{
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

func TestApplyLiveAgentActionCreatesFleetTask(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(16)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte("agent-key")}
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
		NodeID:        nodeID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		TopicFilters:   []string{"fleet/#"},
		AllowedActions: []string{liveActionTaskCreate},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cfg, nil, liveAgentAction{
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
	if len(activity) != 1 || activity[0].Type != "task.opened" || activity[0].Actor.PilotNodeID != nodeID {
		t.Fatalf("activity = %+v, want task.opened by live node", activity)
	}
	cappedCfg := cfg
	cappedCfg.MaxActionBytes = 10
	applied, err = applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cappedCfg, nil, liveAgentAction{
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

func TestApplyLiveAgentActionAssignsFleetTasks(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(26)
	coordinatorNodeID := entmoot.NodeID(7)
	agentNodeID := entmoot.NodeID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: coordinatorNodeID, EntmootPubKey: []byte("coordinator-key")}
	agent := entmoot.NodeInfo{PilotNodeID: agentNodeID, EntmootPubKey: []byte("agent-key")}
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
		{FleetID: "fleet-live", NodeID: coordinatorNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: agentNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
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
		NodeID:         agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskAssignSelf},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, agentCfg, nil, liveAgentAction{
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
	if claimed.Status != esphttp.FleetTaskStatusAssigned || claimed.Assignee == nil || claimed.Assignee.PilotNodeID != agentNodeID {
		t.Fatalf("claimed task = %+v, want assigned to live agent", claimed)
	}
	coordinatorCfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         coordinatorNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskAssignOthers},
	}
	applied, err = applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, coordinatorCfg, nil, liveAgentAction{
		Kind:           liveActionTaskAssignOthers,
		TaskID:         "task-assign",
		AssigneeNodeID: uint64(agentNodeID),
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
	if assigned.Status != esphttp.FleetTaskStatusAssigned || assigned.Assignee == nil || assigned.Assignee.PilotNodeID != agentNodeID {
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
	coordinatorNodeID := entmoot.NodeID(7)
	agentNodeID := entmoot.NodeID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: coordinatorNodeID, EntmootPubKey: []byte("coordinator-key")}
	agent := entmoot.NodeInfo{PilotNodeID: agentNodeID, EntmootPubKey: []byte("agent-key")}
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
		{FleetID: "fleet-live", NodeID: coordinatorNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: agentNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
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
		NodeID:         agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskUpdateOwn},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cfg, nil, liveAgentAction{
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
	if len(submissions) != 1 || submissions[0].Author.PilotNodeID != agentNodeID || submissions[0].Content != "Finished the assigned check" {
		t.Fatalf("submissions = %+v, want one live agent submission", submissions)
	}
	cappedCfg := cfg
	cappedCfg.MaxActionBytes = 4
	applied, err = applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cappedCfg, nil, liveAgentAction{
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
	coordinatorNodeID := entmoot.NodeID(7)
	agentNodeID := entmoot.NodeID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: coordinatorNodeID, EntmootPubKey: []byte("coordinator-key")}
	agent := entmoot.NodeInfo{PilotNodeID: agentNodeID, EntmootPubKey: []byte("agent-key")}
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
		{FleetID: "fleet-live", NodeID: coordinatorNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: agentNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(agent.EntmootPubKey), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
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
		NodeID:         agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskComment},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cfg, nil, liveAgentAction{
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
	if len(activity) != 1 || activity[0].Type != "task.comment" || activity[0].Actor.PilotNodeID != agentNodeID {
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
	applied, err = applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cappedCfg, nil, liveAgentAction{
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
	nodeID := entmoot.NodeID(7)
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
	founder := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: append([]byte(nil), id.PublicKey...)}
	createLiveActionRoster(t, dataDir, gid, id, founder)
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   founder.PilotNodeID,
		EntmootPubKey: founder.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMetadataUpdate},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
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
	applied, err = applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cappedCfg, nil, liveAgentAction{
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
	founderNodeID := entmoot.NodeID(7)
	agentNodeID := entmoot.NodeID(8)
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
	founder := entmoot.NodeInfo{PilotNodeID: founderNodeID, EntmootPubKey: append([]byte(nil), founderID.PublicKey...)}
	agent := entmoot.NodeInfo{PilotNodeID: agentNodeID, EntmootPubKey: append([]byte(nil), agentID.PublicKey...)}
	createLiveActionRoster(t, dataDir, gid, founderID, founder)
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   agent.PilotNodeID,
		EntmootPubKey: agent.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMetadataUpdate},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
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

func TestApplyLiveAgentActionRejectsMetadataUpdateWithoutCreatingRoster(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(37)
	nodeID := entmoot.NodeID(7)
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
		PilotNodeID:   nodeID,
		EntmootPubKey: append([]byte(nil), id.PublicKey...),
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMetadataUpdate},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:     liveActionMetadataUpdate,
		Metadata: json.RawMessage(`{"name":"Missing roster"}`),
	})
	if err == nil {
		t.Fatal("applyLiveAgentAction err = nil, want missing roster authorization error")
	}
	if applied {
		t.Fatal("applied = true, want false")
	}
	if _, err := os.Stat(groupRosterPath(dataDir, gid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("roster stat err = %v, want not exist", err)
	}
}

func TestApplyLiveAgentActionUpdatesOpenSubmissionTaskUsesSubmissionTime(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(28)
	nodeID := entmoot.NodeID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: 7, EntmootPubKey: []byte("coordinator-key")}
	agent := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte("agent-key")}
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
		NodeID:        nodeID,
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
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskUpdateOwn},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cfg, nil, liveAgentAction{
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
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte("agent-key")}
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
			NodeID:        nodeID,
			EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
			Role:          esphttp.FleetRoleCoordinator,
			Status:        esphttp.FleetMemberActive,
		}); err != nil {
			t.Fatalf("UpsertFleetMember(%s): %v", rec.FleetID, err)
		}
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskCreate},
	}
	for _, fleetID := range []string{"fleet-foreign", "fleet-archived"} {
		applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cfg, nil, liveAgentAction{
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
	nodeID := entmoot.NodeID(7)
	assigneeNodeID := entmoot.NodeID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: []byte("coordinator-key")}
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
		{FleetID: "fleet-live", NodeID: 1, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: nodeID, EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("agent-key")), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: assigneeNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("assignee-key")), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
		}
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionTaskCreate},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cfg, nil, liveAgentAction{
		Kind:           liveActionTaskCreate,
		Title:          "Unauthorized direct task",
		Mode:           esphttp.FleetTaskModeDirectAssignment,
		AssigneeNodeID: uint64(assigneeNodeID),
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
	nodeID := entmoot.NodeID(7)
	targetNodeID := entmoot.NodeID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte("coordinator-key")}
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
		{FleetID: "fleet-live", NodeID: nodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: targetNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("target-key")), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
		}
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   coordinator.PilotNodeID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionCommandSend},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:         liveActionCommandSend,
		Action:       esphttp.FleetCommandActionEntmootVersion,
		Target:       esphttp.FleetCommandTargetNode,
		TargetNodeID: uint64(targetNodeID),
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
	if command.Action != esphttp.FleetCommandActionEntmootVersion || command.Target.Kind != esphttp.FleetCommandTargetNode || command.Target.PilotNodeID != targetNodeID {
		t.Fatalf("command = %+v, want version command targeting node %d", command, targetNodeID)
	}
	detail, found, err := state.GetFleetCommandDetail(ctx, "fleet-live", command.CommandID)
	if err != nil || !found {
		t.Fatalf("GetFleetCommandDetail found/err = %v/%v", found, err)
	}
	if detail.Command.CommandID != command.CommandID || detail.Command.IssuerNodeID != nodeID {
		t.Fatalf("stored command = %+v, want sent command", detail.Command)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "command.sent" || activity[0].Subject == nil || activity[0].Subject.PilotNodeID != targetNodeID {
		t.Fatalf("activity = %+v, want command.sent for target", activity)
	}
}

func TestApplyLiveAgentActionRequestsFleetCommand(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(38)
	nodeID := entmoot.NodeID(7)
	targetNodeID := entmoot.NodeID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte("coordinator-key")}
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
		{FleetID: "fleet-live", NodeID: nodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: targetNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("target-key")), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
		}
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   coordinator.PilotNodeID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionCommandRequest},
	}
	autoAccept := true
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:         liveActionCommandRequest,
		Action:       esphttp.FleetCommandActionEntmootVersion,
		Target:       esphttp.FleetCommandTargetNode,
		TargetNodeID: uint64(targetNodeID),
		AutoAccept:   &autoAccept,
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
	if command.Action != esphttp.FleetCommandActionEntmootVersion || command.Target.Kind != esphttp.FleetCommandTargetNode || command.Target.PilotNodeID != targetNodeID {
		t.Fatalf("command = %+v, want version request targeting node %d", command, targetNodeID)
	}
	detail, found, err := state.GetFleetCommandDetail(ctx, "fleet-live", command.CommandID)
	if err != nil || !found {
		t.Fatalf("GetFleetCommandDetail found/err = %v/%v", found, err)
	}
	if !detail.Command.AutoAccept {
		t.Fatalf("stored command AutoAccept = false, want true")
	}
	applied, err = applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:         liveActionCommandRequest,
		Action:       esphttp.FleetCommandActionAgentInstruction,
		Target:       esphttp.FleetCommandTargetNode,
		TargetNodeID: uint64(targetNodeID),
		Instruction:  "Summarize local status",
		AutoAccept:   &autoAccept,
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
	applied, err = applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, sendCfg, nil, liveAgentAction{
		Kind:         liveActionCommandSend,
		Action:       esphttp.FleetCommandActionAgentInstruction,
		Target:       esphttp.FleetCommandTargetNode,
		TargetNodeID: uint64(targetNodeID),
		Instruction:  "Summarize local status",
		AutoAccept:   &autoAccept,
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
	nodeID := entmoot.NodeID(7)
	targetNodeID := entmoot.NodeID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte("coordinator-key")}
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
		{FleetID: "fleet-live", NodeID: nodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: targetNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("target-key")), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
		}
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   coordinator.PilotNodeID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionExternalMessage},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:             liveActionExternalMessage,
		FleetID:          "fleet-live",
		TargetNodeID:     uint64(targetNodeID),
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
	if command.Target.Kind != esphttp.FleetCommandTargetNode || command.Target.PilotNodeID != targetNodeID {
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
	nodeID := entmoot.NodeID(7)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte("coordinator-key")}
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
		NodeID:        nodeID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   coordinator.PilotNodeID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionExternalMessage},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:           liveActionExternalMessage,
		FleetID:        "fleet-live",
		TargetNodeID:   uint64(nodeID),
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
	nodeID := entmoot.NodeID(7)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: []byte("coordinator-key")}
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
		NodeID:        nodeID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	topicsCh := make(chan []string, 1)
	contentCh := make(chan []byte, 1)
	stop := serveLiveInfoPublishCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   coordinator.PilotNodeID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, topicsCh, contentCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         nodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionCommandSend},
		MaxActionBytes: 10,
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
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
	applied, err = applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:         liveActionCommandSend,
		Action:       esphttp.FleetCommandActionEntmootVersion,
		TargetNodeID: 8,
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
	coordinatorNodeID := entmoot.NodeID(7)
	targetNodeID := entmoot.NodeID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: coordinatorNodeID, EntmootPubKey: []byte(strings.Repeat("c", 32))}
	targetPilotPub := []byte(strings.Repeat("p", 32))
	targetEntPub := []byte(strings.Repeat("t", 32))
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
		NodeID:        coordinatorNodeID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	inviteCh := make(chan *ipc.InviteCreateReq, 1)
	stop := serveLiveInfoInviteCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   coordinator.PilotNodeID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, gid, inviteCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         coordinatorNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionInviteCreate},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:           liveActionInviteCreate,
		TargetNodeID:   uint64(targetNodeID),
		TargetPilotKey: base64.StdEncoding.EncodeToString(targetPilotPub),
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
		if req.GroupID != gid || req.Target.PilotNodeID != targetNodeID {
			t.Fatalf("invite req = %+v, want target node %d", req, targetNodeID)
		}
		if !bytes.Equal(req.TargetPilotPubKey, targetPilotPub) || !bytes.Equal(req.Target.EntmootPubKey, targetEntPub) {
			t.Fatalf("invite target keys = %+v, want supplied keys", req.Target)
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
	if len(invites) != 1 || invites[0].NodeID != targetNodeID || invites[0].Status != esphttp.FleetMemberInvited {
		t.Fatalf("invites = %+v, want one invited target", invites)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-live", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "member.invited" || activity[0].Subject == nil || activity[0].Subject.PilotNodeID != targetNodeID {
		t.Fatalf("activity = %+v, want member.invited for target", activity)
	}
}

func TestApplyLiveAgentActionRejectsInviteCreateByNonCoordinator(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(33)
	coordinatorNodeID := entmoot.NodeID(7)
	agentNodeID := entmoot.NodeID(8)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: coordinatorNodeID, EntmootPubKey: []byte(strings.Repeat("c", 32))}
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
		{FleetID: "fleet-live", NodeID: coordinatorNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: agentNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 32))), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
		}
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionInviteCreate},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cfg, nil, liveAgentAction{
		Kind:           liveActionInviteCreate,
		TargetNodeID:   9,
		TargetPilotKey: base64.StdEncoding.EncodeToString([]byte(strings.Repeat("p", 32))),
		TargetEntKey:   base64.StdEncoding.EncodeToString([]byte(strings.Repeat("t", 32))),
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
	if _, ok := fleetMemberForNode(members, 9); ok {
		t.Fatal("target member exists, want no invite mutation")
	}
}

func TestApplyLiveAgentActionRemovesFleetMember(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(29)
	coordinatorNodeID := entmoot.NodeID(7)
	targetNodeID := entmoot.NodeID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: coordinatorNodeID, EntmootPubKey: []byte(strings.Repeat("c", 32))}
	targetPub := []byte(strings.Repeat("t", 32))
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
		{FleetID: "fleet-live", NodeID: coordinatorNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: targetNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(targetPub), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberInvited},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
		}
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-target",
		FleetID:       "fleet-live",
		NodeID:        targetNodeID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(targetPub),
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"test"}`),
		CreatedAtMS:   1,
	}); err != nil {
		t.Fatalf("CreateFleetInvite: %v", err)
	}
	removeCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveLiveInfoRemoveCapture(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   coordinator.PilotNodeID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, removeCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         coordinatorNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMemberRemove},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:         liveActionMemberRemove,
		TargetNodeID: uint64(targetNodeID),
	})
	if err != nil {
		t.Fatalf("applyLiveAgentAction: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	select {
	case req := <-removeCh:
		if req.GroupID != gid || req.Target.PilotNodeID != targetNodeID || base64.StdEncoding.EncodeToString(req.Target.EntmootPubKey) != base64.StdEncoding.EncodeToString(targetPub) {
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
	if len(activity) != 1 || activity[0].Type != "member.removed" || activity[0].Subject == nil || activity[0].Subject.PilotNodeID != targetNodeID {
		t.Fatalf("activity = %+v, want member.removed for target", activity)
	}
}

func TestApplyLiveAgentActionMemberRemoveRetriesDroppedIPC(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(31)
	coordinatorNodeID := entmoot.NodeID(7)
	targetNodeID := entmoot.NodeID(8)
	dataDir, err := os.MkdirTemp("/tmp", "entmoot-live-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: coordinatorNodeID, EntmootPubKey: []byte(strings.Repeat("c", 32))}
	targetPub := []byte(strings.Repeat("t", 32))
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
		{FleetID: "fleet-live", NodeID: coordinatorNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: targetNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(targetPub), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
		}
	}
	removeCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveLiveInfoRemoveDrop(t, controlSocketPath(dataDir), &ipc.InfoResp{
		PilotNodeID:   coordinator.PilotNodeID,
		EntmootPubKey: coordinator.EntmootPubKey,
		Running:       true,
	}, removeCh)
	defer stop()
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         coordinatorNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMemberRemove},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:         liveActionMemberRemove,
		TargetNodeID: uint64(targetNodeID),
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
	coordinatorNodeID := entmoot.NodeID(7)
	agentNodeID := entmoot.NodeID(8)
	targetNodeID := entmoot.NodeID(9)
	state := esphttp.NewMemoryStateStore()
	coordinator := entmoot.NodeInfo{PilotNodeID: coordinatorNodeID, EntmootPubKey: []byte(strings.Repeat("c", 32))}
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
		{FleetID: "fleet-live", NodeID: coordinatorNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString(coordinator.EntmootPubKey), Role: esphttp.FleetRoleCoordinator, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: agentNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 32))), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
		{FleetID: "fleet-live", NodeID: targetNodeID, EntmootPubKey: base64.StdEncoding.EncodeToString([]byte(strings.Repeat("t", 32))), Role: esphttp.FleetRoleAgent, Status: esphttp.FleetMemberActive},
	} {
		if _, err := state.UpsertFleetMember(ctx, member); err != nil {
			t.Fatalf("UpsertFleetMember(%d): %v", member.NodeID, err)
		}
	}
	cfg := esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         agentNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		AllowedActions: []string{liveActionMemberRemove},
	}
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: t.TempDir()}, state, cfg, nil, liveAgentAction{
		Kind:         liveActionMemberRemove,
		TargetNodeID: uint64(targetNodeID),
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

func TestParseLiveRunnerOutputAllowsExplicitNoActions(t *testing.T) {
	output, err := parseLiveRunnerOutput(`{"actions":[]}`)
	if err != nil {
		t.Fatalf("parseLiveRunnerOutput: %v", err)
	}
	if len(output.Actions) != 0 {
		t.Fatalf("actions = %+v, want none", output.Actions)
	}
}

func TestParseLiveRunnerOutputUnwrapsCommandRunnerOutput(t *testing.T) {
	output, err := parseLiveRunnerOutput(`{"status":"completed","summary":"done","output":"{\"actions\":[{\"kind\":\"reply\",\"message\":\"wrapped\"}]}"}`)
	if err != nil {
		t.Fatalf("parseLiveRunnerOutput: %v", err)
	}
	if len(output.Actions) != 1 || output.Actions[0].Kind != "reply" || output.Actions[0].Message != "wrapped" {
		t.Fatalf("output = %+v", output)
	}
}

func TestParseLiveRunnerOutputUnwrapsNoisyCommandRunnerOutput(t *testing.T) {
	output, err := parseLiveRunnerOutput(`{"status":"completed","summary":"done","output":"thinking\n{\"actions\":[{\"kind\":\"reply\",\"message\":\"wrapped noisy\"}]}\n"}`)
	if err != nil {
		t.Fatalf("parseLiveRunnerOutput: %v", err)
	}
	if len(output.Actions) != 1 || output.Actions[0].Kind != "reply" || output.Actions[0].Message != "wrapped noisy" {
		t.Fatalf("output = %+v", output)
	}
}

func TestParseLiveRunnerOutputClassifiesInvalidJSON(t *testing.T) {
	tests := []string{
		"",
		"not-json",
		`{"status":"completed","summary":"done"}`,
		`{"status":"completed","output":"not-json"}`,
		`{"status":"failed","output":"{\"actions\":[{\"kind\":\"reply\",\"message\":\"bad\"}]}"}`,
		`{"status":"running","output":"{\"actions\":[{\"kind\":\"reply\",\"message\":\"bad\"}]}"}`,
		`{"actions":null}`,
		`{"actions":{"kind":"reply","message":"bad"}}`,
		`{"status":"completed","output":"{\"runner\":\"openclaw\",\"actions\":[{\"kind\":\"message.send\",\"confirmed\":true}]}"}`,
	}
	for _, stdout := range tests {
		_, err := parseLiveRunnerOutput(stdout)
		if !errors.Is(err, errLiveRunnerInvalidJSON) {
			t.Fatalf("parseLiveRunnerOutput(%q) err = %v, want invalid JSON classification", stdout, err)
		}
	}
}

func TestRunLiveAgentRunnerCustomRuntimeReceivesContextAndEnv(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	t.Setenv("ENTMOOT_TEST_DIR", tempDir)
	gid := testAgentLiveGroupID(51)
	runner := filepath.Join(tempDir, "runner.sh")
	if err := os.WriteFile(runner, []byte(`#!/bin/sh
cat > "$ENTMOOT_TEST_DIR/stdin.json"
{
  printf '%s\n' "$ENTMOOT_LIVE_GROUP_ID"
  printf '%s\n' "$ENTMOOT_LIVE_NODE_ID"
  printf '%s\n' "$ENTMOOT_LIVE_MODE"
} > "$ENTMOOT_TEST_DIR/env.txt"
printf '{"actions":[]}'
`), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	output, err := runLiveAgentRunner(ctx, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  7,
		runner:  runner,
		timeout: time.Second,
		limit:   10,
	}, liveAgentRunnerContext{
		GroupID: gid,
		NodeID:  7,
		Mode:    esphttp.LiveModeConverse,
		Events:  []liveAgentRunnerMessage{{Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("runLiveAgentRunner: %v", err)
	}
	if len(output.Actions) != 0 {
		t.Fatalf("actions = %+v, want none", output.Actions)
	}
	envData, err := os.ReadFile(filepath.Join(tempDir, "env.txt"))
	if err != nil {
		t.Fatalf("ReadFile env: %v", err)
	}
	wantEnv := gid.String() + "\n7\n" + esphttp.LiveModeConverse + "\n"
	if string(envData) != wantEnv {
		t.Fatalf("env = %q, want %q", envData, wantEnv)
	}
	stdinData, err := os.ReadFile(filepath.Join(tempDir, "stdin.json"))
	if err != nil {
		t.Fatalf("ReadFile stdin: %v", err)
	}
	if !bytes.Contains(stdinData, []byte(`"group_id":"`+gid.String()+`"`)) || !bytes.Contains(stdinData, []byte(`"mode":"converse"`)) {
		t.Fatalf("stdin context = %s, missing live context", stdinData)
	}
}

func TestRunLiveAgentRunnerTimeoutIsRecoverableDeadline(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	runner := filepath.Join(tempDir, "runner.sh")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\necho slow >&2\nsleep 1\n"), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	_, err := runLiveAgentRunner(ctx, agentLiveRuntimeConfig{
		groupID: testAgentLiveGroupID(52),
		nodeID:  7,
		runner:  runner,
		timeout: 20 * time.Millisecond,
		limit:   10,
	}, liveAgentRunnerContext{
		GroupID: testAgentLiveGroupID(52),
		NodeID:  7,
		Mode:    esphttp.LiveModeConverse,
		Events:  []liveAgentRunnerMessage{{Content: "hello"}},
	})
	if !errors.Is(err, errLiveRunnerTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runLiveAgentRunner err = %v, want runner timeout and deadline", err)
	}
}

func TestOpenClawLiveAgentArgsPreserveCommandShape(t *testing.T) {
	clearOpenClawRunnerEnv(t)
	t.Setenv("ENTMOOT_OPENCLAW_AGENT", "mars")
	args := openClawLiveAgentArgs(agentLiveRuntimeConfig{timeout: 90 * time.Second}, `{"ok":true}`)
	want := []string{
		"agent",
		"--agent", "mars",
		"--message", "Entmoot live interaction context JSON:\n" + `{"ok":true}`,
		"--json",
		"--timeout", "90",
	}
	if strings.Join(args, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
}

func TestScanAgentLiveRunGroupsTimeoutDegradesAndBacksOff(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	t.Setenv("ENTMOOT_TEST_DIR", tempDir)
	gid := testAgentLiveGroupID(53)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if err := msgStore.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	}); err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	slowRunner := filepath.Join(tempDir, "slow.sh")
	if err := os.WriteFile(slowRunner, []byte("#!/bin/sh\necho slow >&2\nsleep 1\n"), 0o700); err != nil {
		t.Fatalf("WriteFile slow runner: %v", err)
	}
	backoffs := newAgentLiveBackoffTracker(50*time.Millisecond, time.Second)
	bindings, scans, err := scanAgentLiveRunGroups(ctx, &globalFlags{}, state, msgStore, []agentLiveRunGroup{{GroupID: gid, Mode: esphttp.LiveModeConverse}}, nodeID, time.Second, true, agentLiveRuntimeConfig{
		nodeID:  nodeID,
		runner:  slowRunner,
		timeout: 20 * time.Millisecond,
		limit:   10,
	}, backoffs)
	if err != nil {
		t.Fatalf("scanAgentLiveRunGroups: %v", err)
	}
	if len(bindings) != 1 || bindings[0].Presence.Status != esphttp.LiveStatusDegraded {
		t.Fatalf("bindings = %+v, want degraded presence", bindings)
	}
	if len(scans) != 1 || scans[0].Scan.Status != agentLiveScanStatusError || scans[0].Scan.ErrorKind != liveRecoverableRunnerTimeout || scans[0].Scan.NextAttemptAtMS == 0 {
		t.Fatalf("scans = %+v, want timeout degraded scan", scans)
	}
	if _, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID); err != nil || ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v, want no consumed cursor", ok, err)
	}

	fastRunner := filepath.Join(tempDir, "fast.sh")
	if err := os.WriteFile(fastRunner, []byte(`#!/bin/sh
printf ran > "$ENTMOOT_TEST_DIR/ran"
printf '{"actions":[]}'
`), 0o700); err != nil {
		t.Fatalf("WriteFile fast runner: %v", err)
	}
	bindings, scans, err = scanAgentLiveRunGroups(ctx, &globalFlags{}, state, msgStore, []agentLiveRunGroup{{GroupID: gid, Mode: esphttp.LiveModeConverse}}, nodeID, time.Second, true, agentLiveRuntimeConfig{
		nodeID:  nodeID,
		runner:  fastRunner,
		timeout: time.Second,
		limit:   10,
	}, backoffs)
	if err != nil {
		t.Fatalf("scanAgentLiveRunGroups during backoff: %v", err)
	}
	if len(scans) != 1 || scans[0].Scan.Status != agentLiveScanStatusBackoff || scans[0].Scan.ErrorKind != liveRecoverableRunnerTimeout {
		t.Fatalf("backoff scans = %+v, want backoff scan", scans)
	}
	if _, err := os.Stat(filepath.Join(tempDir, "ran")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runner executed during backoff, stat err = %v", err)
	}

	key := agentLiveBackoffKey{groupID: gid, nodeID: nodeID}
	entry := backoffs.entries[key]
	entry.nextAttemptAt = time.Now().Add(-time.Millisecond)
	backoffs.entries[key] = entry
	bindings, scans, err = scanAgentLiveRunGroups(ctx, &globalFlags{}, state, msgStore, []agentLiveRunGroup{{GroupID: gid, Mode: esphttp.LiveModeConverse}}, nodeID, time.Second, true, agentLiveRuntimeConfig{
		nodeID:  nodeID,
		runner:  fastRunner,
		timeout: time.Second,
		limit:   10,
	}, backoffs)
	if err != nil {
		t.Fatalf("scanAgentLiveRunGroups after backoff: %v", err)
	}
	if len(bindings) != 1 || bindings[0].Presence.Status != esphttp.LiveStatusOnline {
		t.Fatalf("bindings after success = %+v, want online presence", bindings)
	}
	if len(scans) != 1 || scans[0].Scan.Status != agentLiveScanStatusOK {
		t.Fatalf("success scans = %+v, want ok scan", scans)
	}
	if _, ok := backoffs.entries[key]; ok {
		t.Fatalf("backoff entry still present after success: %+v", backoffs.entries[key])
	}
	if _, err := os.Stat(filepath.Join(tempDir, "ran")); err != nil {
		t.Fatalf("runner did not execute after backoff: %v", err)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v, want consumed cursor", ok, err)
	}
	if cursor.LastSeenMessageID != msg.ID {
		t.Fatalf("cursor.LastSeenMessageID = %s, want %s", cursor.LastSeenMessageID, msg.ID)
	}
}

func TestScanAgentLiveRunGroupsActionTransportDegrades(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(54)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if err := msgStore.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	}); err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	runner := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nprintf '{\"actions\":[{\"kind\":\"reply\",\"message\":\"ok\"}]}'\n"), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	bindings, scans, err := scanAgentLiveRunGroups(ctx, &globalFlags{data: t.TempDir()}, state, msgStore, []agentLiveRunGroup{{GroupID: gid, Mode: esphttp.LiveModeConverse}}, nodeID, time.Second, true, agentLiveRuntimeConfig{
		nodeID:  nodeID,
		runner:  runner,
		timeout: time.Second,
		limit:   10,
	}, newAgentLiveBackoffTracker(50*time.Millisecond, time.Second))
	if err != nil {
		t.Fatalf("scanAgentLiveRunGroups: %v", err)
	}
	if len(bindings) != 1 || bindings[0].Presence.Status != esphttp.LiveStatusDegraded {
		t.Fatalf("bindings = %+v, want degraded presence", bindings)
	}
	if len(scans) != 1 || scans[0].Scan.Status != agentLiveScanStatusError || scans[0].Scan.ErrorKind != liveRecoverableActionTransport {
		t.Fatalf("scans = %+v, want action transport degraded scan", scans)
	}
}

func TestScanAgentLiveRunGroupsInvalidWrapperDoesNotAdvanceCursor(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(56)
	nodeID := entmoot.NodeID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := store.NewMemory()
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if err := msgStore.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		NodeID:       nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	}); err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	runner := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(runner, []byte(`#!/bin/sh
printf '%s' '{"status":"completed","summary":"done"}'
`), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	bindings, scans, err := scanAgentLiveRunGroups(ctx, &globalFlags{}, state, msgStore, []agentLiveRunGroup{{GroupID: gid, Mode: esphttp.LiveModeConverse}}, nodeID, time.Second, true, agentLiveRuntimeConfig{
		nodeID:  nodeID,
		runner:  runner,
		timeout: time.Second,
		limit:   10,
	}, newAgentLiveBackoffTracker(50*time.Millisecond, time.Second))
	if err != nil {
		t.Fatalf("scanAgentLiveRunGroups: %v", err)
	}
	if len(bindings) != 1 || bindings[0].Presence.Status != esphttp.LiveStatusDegraded {
		t.Fatalf("bindings = %+v, want degraded presence", bindings)
	}
	if len(scans) != 1 || scans[0].Scan.Status != agentLiveScanStatusError || scans[0].Scan.ErrorKind != liveRecoverableRunnerInvalidJSON {
		t.Fatalf("scans = %+v, want invalid-json degraded scan", scans)
	}
	if _, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID); err != nil || ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v, want no consumed cursor", ok, err)
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
					Invite:     entmoot.Invite{GroupID: gid},
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

func createLiveActionRoster(t *testing.T, dataDir string, gid entmoot.GroupID, founderID *keystore.Identity, founder entmoot.NodeInfo) {
	t.Helper()
	rlog, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	if err := rlog.Genesis(founderID, founder, 1_700_000_000_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
}

func testAgentLiveMessageID(seed byte) entmoot.MessageID {
	var id entmoot.MessageID
	id[0] = seed
	return id
}
