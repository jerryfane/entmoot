package main

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

type fakeHandshakeApprovalPilot struct {
	info     ipcclient.Info
	pending  []ipcclient.PendingHandshake
	identity map[uint32]ipcclient.NodeIdentity

	lookupCalls  []uint32
	approveCalls []uint32
}

func (f *fakeHandshakeApprovalPilot) InfoStruct(context.Context) (ipcclient.Info, error) {
	return f.info, nil
}

func (f *fakeHandshakeApprovalPilot) Subscribe(context.Context, string) ([]byte, *ipcclient.Subscription, error) {
	return nil, nil, ipcclient.ErrSubscribeUnsupported
}

func (f *fakeHandshakeApprovalPilot) PendingHandshakes(context.Context) ([]ipcclient.PendingHandshake, error) {
	return append([]ipcclient.PendingHandshake(nil), f.pending...), nil
}

func (f *fakeHandshakeApprovalPilot) LookupNode(_ context.Context, nodeID uint32) (ipcclient.NodeIdentity, error) {
	f.lookupCalls = append(f.lookupCalls, nodeID)
	return f.identity[nodeID], nil
}

func (f *fakeHandshakeApprovalPilot) ApproveHandshake(_ context.Context, nodeID uint32) (map[string]interface{}, error) {
	f.approveCalls = append(f.approveCalls, nodeID)
	return map[string]interface{}{"ok": true}, nil
}

func TestHandshakeApprovalApprovesOnlyCurrentRosterMembersWithMatchingPilotKey(t *testing.T) {
	memberA := entmoot.NodeID(200)
	memberB := entmoot.NodeID(201)
	nonMember := entmoot.NodeID(300)
	pubA := "pilot-pub-a"
	pubB := "pilot-pub-b"

	rt := &groupRuntime{
		nodeID:                100,
		sessions:              map[entmoot.GroupID]*groupSession{},
		handshakeApprovalWake: make(chan struct{}, 1),
		logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	rt.sessions[testRuntimeGroupID(0xA1)] = &groupSession{
		groupID: testRuntimeGroupID(0xA1),
		roster:  testHandshakeApprovalRoster(t, memberA),
	}
	rt.sessions[testRuntimeGroupID(0xA2)] = &groupSession{
		groupID: testRuntimeGroupID(0xA2),
		roster:  testHandshakeApprovalRoster(t, memberB),
	}
	pilot := &fakeHandshakeApprovalPilot{
		identity: map[uint32]ipcclient.NodeIdentity{
			uint32(memberA): {NodeID: uint32(memberA), PublicKey: pubA, Source: "trusted"},
			uint32(memberB): {NodeID: uint32(memberB), PublicKey: "different", Source: "trusted"},
		},
	}
	rt.pilotDriver = pilot

	pending := []ipcclient.PendingHandshake{
		{NodeID: uint32(nonMember), PublicKey: "pilot-pub-non-member"},
		{NodeID: uint32(memberB), PublicKey: pubB},
		{NodeID: uint32(memberA), PublicKey: pubA},
	}
	rt.evaluatePendingHandshakes(context.Background(), map[string]handshakeApprovalCacheEntry{}, pending, "test")

	if !reflect.DeepEqual(pilot.approveCalls, []uint32{uint32(memberA)}) {
		t.Fatalf("approve calls = %v, want [%d]", pilot.approveCalls, memberA)
	}
	if !reflect.DeepEqual(pilot.lookupCalls, []uint32{uint32(memberA), uint32(memberB)}) {
		t.Fatalf("lookup calls = %v, want [%d %d]", pilot.lookupCalls, memberA, memberB)
	}
}

func testHandshakeApprovalRoster(t *testing.T, nodeID entmoot.NodeID) *roster.RosterLog {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	rlog := roster.New(testRuntimeGroupID(byte(nodeID)))
	info := entmoot.NodeInfo{
		PilotNodeID:   nodeID,
		EntmootPubKey: append([]byte(nil), identity.PublicKey...),
	}
	if err := rlog.Genesis(identity, info, time.Now().UnixMilli()); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	return rlog
}
