package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
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
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/store/storetest"
)

type testLivePolicyStore struct {
	policies map[entmoot.GroupID]entpolicy.Policy
}

func (s testLivePolicyStore) Get(_ context.Context, groupID entmoot.GroupID) (entpolicy.Policy, bool, error) {
	p, ok := s.policies[groupID]
	return p, ok, nil
}

func TestRunAgentLiveScanListenAdvancesCursorWithoutRunner(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(1)
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
	if cursor.LastSeenAuthorMemberID != *msg.Author.MemberID || cursor.LastSeenMessageID != msg.ID {
		t.Fatalf("cursor tie-breaker = %s/%s, want %s/%s", cursor.LastSeenAuthorMemberID, cursor.LastSeenMessageID, msg.Author.MemberID, msg.ID)
	}
}

func TestRunAgentLiveScanFirstRunUsesConfigUpdatedAtFloor(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(2)
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "old")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	first := testAgentLiveMessage(gid, 11, 100, "chat", "first")
	second := testAgentLiveMessage(gid, 12, 100, "chat", "second")
	if bytes.Compare(first.Author.MemberID[:], second.Author.MemberID[:]) > 0 {
		first, second = second, first
	}
	for _, msg := range []entmoot.Message{first, second} {
		if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
		timeout: 5 * time.Second,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	alreadySeen := testAgentLiveMessage(gid, 11, 200, "chat", "seen")
	delayed := testAgentLiveMessage(gid, 12, 150, "chat", "delayed")
	if _, err := msgStore.Put(ctx, delayed.GroupID, delayed); err != nil {
		t.Fatalf("Put delayed: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
		timeout: 5 * time.Second,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	old := testAgentLiveMessage(gid, 11, 900, "chat", "old")
	fresh := testAgentLiveMessage(gid, 13, 1001, "chat", "fresh")
	for _, msg := range []entmoot.Message{old, fresh} {
		if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
		GroupID:                gid,
		MemberID:               nodeID,
		ScanFloorAtMS:          400,
		LastSeenAtMS:           1000,
		LastSeenAuthorMemberID: testAgentLiveMemberID(12),
		LastSeenMessageID:      highWater,
		SeenMessageIDs:         seen,
		UpdatedAtMS:            1000,
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
		MemberID:    testAgentLiveMemberID(7),
		UpdatedAtMS: 100,
	}
	cursor := esphttp.LiveAgentCursor{
		GroupID:       gid,
		MemberID:      testAgentLiveMemberID(7),
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:           gid,
		MemberID:          nodeID,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msgs := []entmoot.Message{
		testAgentLiveMessage(gid, 11, 100, "chat", "one"),
		testAgentLiveMessage(gid, 12, 101, "chat", "two"),
		testAgentLiveMessage(gid, 13, 102, "chat", "three"),
	}
	for _, msg := range msgs {
		if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
		MemberID:       testAgentLiveMemberID(7),
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
		MemberID:       testAgentLiveMemberID(7),
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
		MemberID:     testAgentLiveMemberID(7),
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
		MemberID:       testAgentLiveMemberID(7),
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
  printf '%s\n' "$ENTMOOT_LIVE_MEMBER_ID"
  printf '%s\n' "$ENTMOOT_LIVE_MODE"
} > "$ENTMOOT_TEST_DIR/env.txt"
printf '{"actions":[]}'
`), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	output, err := runLiveAgentRunner(ctx, agentLiveRuntimeConfig{
		groupID: gid,
		nodeID:  testAgentLiveMemberID(7),
		runner:  runner,
		timeout: time.Second,
		limit:   10,
	}, liveAgentRunnerContext{
		GroupID:  gid,
		MemberID: testAgentLiveMemberID(7),
		Mode:     esphttp.LiveModeConverse,
		Events:   []liveAgentRunnerMessage{{Content: "hello"}},
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
	wantEnv := gid.String() + "\n" + testAgentLiveMemberID(7).String() + "\n" + esphttp.LiveModeConverse + "\n"
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
		nodeID:  testAgentLiveMemberID(7),
		runner:  runner,
		timeout: 20 * time.Millisecond,
		limit:   10,
	}, liveAgentRunnerContext{
		GroupID:  testAgentLiveGroupID(52),
		MemberID: testAgentLiveMemberID(7),
		Mode:     esphttp.LiveModeConverse,
		Events:   []liveAgentRunnerMessage{{Content: "hello"}},
	})
	if !errors.Is(err, errLiveRunnerTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runLiveAgentRunner err = %v, want runner timeout and deadline", err)
	}
}

func TestScanAgentLiveRunGroupsTimeoutDegradesAndBacksOff(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	t.Setenv("ENTMOOT_TEST_DIR", tempDir)
	gid := testAgentLiveGroupID(53)
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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

	key := agentLiveBackoffKey{groupID: gid, memberID: nodeID}
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	msg := testAgentLiveMessage(gid, 11, 100, "chat", "hello")
	if _, err := msgStore.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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

func TestRunAgentLiveScanPolicyTriggerLimiterSkipsRunnerWithoutAdvancingCursor(t *testing.T) {
	ctx := context.Background()
	gid := testAgentLiveGroupID(57)
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	msgStore := storetest.New(t)
	first := testAgentLiveMessage(gid, 11, 100, "chat", "first")
	second := testAgentLiveMessage(gid, 12, 200, "chat", "second")
	if _, err := msgStore.Put(ctx, first.GroupID, first); err != nil {
		t.Fatalf("Put first: %v", err)
	}
	cfg, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
		Enabled:      true,
		Mode:         esphttp.LiveModeConverse,
		TopicFilters: []string{"chat"},
		UpdatedAtMS:  1,
	})
	if err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	dir := t.TempDir()
	runner := filepath.Join(dir, "runner.sh")
	if err := os.WriteFile(runner, []byte(`#!/bin/sh
count="$ENTMOOT_TEST_DIR/count"
if [ -f "$count" ]; then n=$(cat "$count"); else n=0; fi
n=$((n + 1))
printf '%s' "$n" > "$count"
printf '{"actions":[]}'
`), 0o700); err != nil {
		t.Fatalf("WriteFile runner: %v", err)
	}
	p := entpolicy.TheEntMootDefault()
	p.LiveTriggerRate = "1/hour"
	p.LiveTriggerBurst = 1
	runCfg := agentLiveRuntimeConfig{
		nodeID:         nodeID,
		runner:         runner,
		timeout:        time.Second,
		limit:          10,
		policies:       testLivePolicyStore{policies: map[entmoot.GroupID]entpolicy.Policy{gid: p}},
		triggerLimiter: newAgentLiveTriggerLimiter(nil),
	}
	t.Setenv("ENTMOOT_TEST_DIR", dir)

	firstResult, err := runAgentLiveScan(ctx, &globalFlags{}, state, msgStore, cfg, runCfg)
	if err != nil {
		t.Fatalf("runAgentLiveScan first: %v", err)
	}
	if firstResult.Status != agentLiveScanStatusOK || firstResult.Matched != 1 {
		t.Fatalf("first result = %+v, want ok matched", firstResult)
	}
	if _, err := msgStore.Put(ctx, second.GroupID, second); err != nil {
		t.Fatalf("Put second: %v", err)
	}
	secondResult, err := runAgentLiveScan(ctx, &globalFlags{}, state, msgStore, cfg, runCfg)
	if err != nil {
		t.Fatalf("runAgentLiveScan second: %v", err)
	}
	if secondResult.Status != agentLiveScanStatusError || secondResult.ErrorKind != liveRecoverableTriggerRateLimit {
		t.Fatalf("second result = %+v, want trigger rate limit", secondResult)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "count"))
	if err != nil {
		t.Fatalf("ReadFile count: %v", err)
	}
	if strings.TrimSpace(string(raw)) != "1" {
		t.Fatalf("runner count = %q, want 1", raw)
	}
	cursor, ok, err := state.GetLiveAgentCursor(ctx, gid, nodeID)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentCursor ok/err = %v/%v, want cursor", ok, err)
	}
	if cursor.LastSeenMessageID != first.ID {
		t.Fatalf("cursor.LastSeenMessageID = %s, want first %s", cursor.LastSeenMessageID, first.ID)
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
	nodeID := testAgentLiveMemberID(7)
	for _, gid := range []entmoot.GroupID{ops, docs, untagged, disabled} {
		enabled := gid != disabled
		if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
			GroupID:     gid,
			MemberID:    nodeID,
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
	nodeID := testAgentLiveMemberID(7)
	state := esphttp.NewMemoryStateStore()
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:      gid,
		MemberID:     nodeID,
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

func testAgentLiveGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}

func testAgentLiveAuthor(seed byte) entmoot.NodeInfo {
	var keySeed [ed25519.SeedSize]byte
	keySeed[0] = seed
	publicKey := ed25519.NewKeyFromSeed(keySeed[:]).Public().(ed25519.PublicKey)
	memberID, _ := entmoot.MemberIDFromPublicKey(publicKey)
	peerID, _ := entmoot.PeerIDFromPublicKey(publicKey)
	return entmoot.NodeInfo{MemberID: &memberID, PeerID: peerID, EntmootPubKey: publicKey}
}

func testAgentLiveMemberID(seed byte) entmoot.MemberID {
	return *testAgentLiveAuthor(seed).MemberID
}

func testAgentLiveMessage(gid entmoot.GroupID, author byte, ts int64, topic, content string) entmoot.Message {
	var id entmoot.MessageID
	id[0] = byte(author)
	id[1] = byte(ts)
	return entmoot.Message{
		Version:   2,
		ID:        id,
		GroupID:   gid,
		Author:    testAgentLiveAuthor(author),
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
		for i := range 2 {
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

func TestLiveMessageMentionsAgentRequiresExactToken(t *testing.T) {
	member := testAgentLiveMemberID(7)
	id := member.String()
	for _, content := range []string{"hello @" + id, "hello @agent-" + id + ",", "please check member:" + id + "."} {
		if !liveMessageMentionsAgent(entmoot.Message{Content: []byte(content)}, member) {
			t.Fatalf("full member mention was missed: %q", content)
		}
	}
	for _, content := range []string{"hello @" + id + "a", "hello @agent-" + id + "0", "please check member:" + id + "_extra", "hello @" + testAgentLiveMemberID(8).String()} {
		if liveMessageMentionsAgent(entmoot.Message{Content: []byte(content)}, member) {
			t.Fatalf("different or extended member token matched: %q", content)
		}
	}
}

func TestCmdAgentLiveRunMalformedGroupIsInvalidArgument(t *testing.T) {
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdAgentLiveRun(&globalFlags{data: t.TempDir()}, []string{"-group", "not-a-group", "-member", testAgentLiveMemberID(7).String(), "-once"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("malformed group exit=%d; stderr=%s", code, stderr)
	}
}
