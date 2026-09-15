package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

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
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
		Kind:     liveActionMetadataUpdate,
		Metadata: json.RawMessage(`{"name":"Ops","tags":["live","ops"],"custom":{"level":2}}`),
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
	applied, err := applyLiveAgentAction(ctx, &globalFlags{data: dataDir}, state, cfg, nil, liveAgentAction{
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
