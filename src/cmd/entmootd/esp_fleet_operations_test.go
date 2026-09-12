package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
)

func TestESPFleetInviteRejectsCoordinatorTarget(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	coordID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate coordinator: %v", err)
	}
	coordinator := testESPNodeInfo(t, coordID.PublicKey)
	fleet, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:             "fleet-a",
		Name:                "Ops Fleet",
		ControlGroupID:      testESPGroupID(23),
		Coordinator:         coordinator,
		CoordinatorDeviceID: "ios-1",
		CreatedAtMS:         1_700_000_000_000,
	})
	if err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: encodeBase64(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	exec := espOperationExecutor{stateStore: state, socketPath: testUnixSocketPath(t), timeout: time.Second}

	_, err = exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				MemberID:      *coordinator.MemberID,
				PeerID:        coordinator.PeerID,
				EntmootPubKey: append([]byte(nil), coordinator.EntmootPubKey...),
			},
		}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("ExecuteSignRequest err = %v, want 400 bad_request", err)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after rejected self-invite = %+v, want active coordinator", members)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after rejected self-invite = %+v, want none", invites)
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after rejected self-invite = %+v, want none", activity)
	}
	// The Fleet's coordinator identity remains authoritative even when its
	// derived member row is absent.
	if err := state.DeleteFleetMember(ctx, fleet.FleetID, *coordinator.MemberID); err != nil {
		t.Fatal(err)
	}
	_, err = exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind: "fleet_invite_create",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target:  &inviteTargetPayload{MemberID: *coordinator.MemberID, PeerID: coordinator.PeerID, EntmootPubKey: coordinator.EntmootPubKey},
		}),
	}, nil)
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("coordinator self-invite without a member row: %v", err)
	}
	members, err = state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil || len(members) != 0 {
		t.Fatalf("self-invite recreated coordinator as an agent: %+v err=%v", members, err)
	}
}

func TestESPFleetInviteRollsBackLocalStateOnActivityFailure(t *testing.T) {
	ctx := context.Background()
	state := &failingFleetStateStore{
		MemoryStateStore:   esphttp.NewMemoryStateStore(),
		failAppendActivity: true,
	}
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	target := testESPNodeInfo(t, targetPub)
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteAndRemoveIPC(t, sock, fleet.ControlGroupID, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				MemberID:      *target.MemberID,
				PeerID:        target.PeerID,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
			Hostname: "phobos",
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want activity failure")
	}
	select {
	case req := <-inviteReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("invite create group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	select {
	case req := <-removeReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("rollback remove group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rollback member remove IPC request")
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after invite rollback = %+v, want active coordinator only", members)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after invite rollback = %+v, want none", invites)
	}
	_ = coordinator
}

func TestESPFleetInviteRollbackTreatsControlNotMemberAsRevoked(t *testing.T) {
	ctx := context.Background()
	state := &failingFleetStateStore{
		MemoryStateStore:   esphttp.NewMemoryStateStore(),
		failAppendActivity: true,
	}
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	target := testESPNodeInfo(t, targetPub)
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteRemoveNotMemberIPC(t, sock, fleet.ControlGroupID, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				MemberID:      *target.MemberID,
				PeerID:        target.PeerID,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
			Hostname: "phobos",
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want activity failure")
	}
	select {
	case req := <-inviteReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("invite create group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	select {
	case req := <-removeReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("rollback remove group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rollback member remove IPC request")
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after invite rollback = %+v, want active coordinator only", members)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after invite rollback = %+v, want none", invites)
	}
	_ = coordinator
}

func TestESPFleetRemoveRollsBackLocalStateOnActivityFailure(t *testing.T) {
	ctx := context.Background()
	state := &failingFleetStateStore{
		MemoryStateStore:   esphttp.NewMemoryStateStore(),
		failAppendActivity: true,
	}
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	target := testESPNodeInfo(t, targetPub)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		MemberID:      *target.MemberID,
		PeerID:        target.PeerID,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-phobos",
		FleetID:       fleet.FleetID,
		MemberID:      *target.MemberID,
		PeerID:        target.PeerID,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Capability:    json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_500,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveIPC(t, sock, testESPGroupID(26), removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				MemberID:      *target.MemberID,
				PeerID:        target.PeerID,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want activity failure")
	}
	select {
	case req := <-removeReqCh:
		t.Fatalf("unexpected member remove IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("members after remove rollback = %+v, want coordinator plus target", members)
	}
	for _, member := range members {
		if member.MemberID == *target.MemberID && member.Status != esphttp.FleetMemberActive {
			t.Fatalf("target member after rollback = %+v, want active", member)
		}
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after remove rollback = %+v, want none", activity)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].InviteID != "invite-phobos" {
		t.Fatalf("invites after remove rollback = %+v, want restored invite", invites)
	}
	_ = coordinator
}

func mustMarshalJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return raw
}

func serveESPFleetInviteAndRemoveIPC(t *testing.T, sock string, gid entmoot.GroupID, inviteReqCh chan<- *ipc.InviteCreateReq, removeReqCh chan<- *ipc.MemberRemoveReq) func() {
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
			switch v := payload.(type) {
			case *ipc.InviteAuthorityCheckReq:
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
					Status:     "ok",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			case *ipc.InviteCreateReq:
				if inviteReqCh != nil {
					inviteReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{
					Status:     "created",
					GroupID:    gid,
					Capability: entmoot.BootstrapCapability{GroupID: gid},
					RosterHead: entmoot.RosterEntryID{},
					Members:    2,
				})
			case *ipc.MemberRemoveReq:
				if removeReqCh != nil {
					removeReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.MemberRemoveResp{
					Status:     "removed",
					GroupID:    gid,
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

func serveESPFleetInviteRemoveNotMemberIPC(t *testing.T, sock string, gid entmoot.GroupID, inviteReqCh chan<- *ipc.InviteCreateReq, removeReqCh chan<- *ipc.MemberRemoveReq) func() {
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
			switch v := payload.(type) {
			case *ipc.InviteAuthorityCheckReq:
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
					Status:     "ok",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			case *ipc.InviteCreateReq:
				if inviteReqCh != nil {
					inviteReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{
					Status:     "created",
					GroupID:    gid,
					Capability: entmoot.BootstrapCapability{GroupID: gid},
					RosterHead: entmoot.RosterEntryID{},
					Members:    2,
				})
			case *ipc.MemberRemoveReq:
				if removeReqCh != nil {
					removeReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{
					Code:    ipc.CodeNotMember,
					GroupID: &v.GroupID,
					Message: "target is not a member",
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

type failingFleetStateStore struct {
	*esphttp.MemoryStateStore
	failAppendActivity bool
}

func (s *failingFleetStateStore) AppendFleetActivity(ctx context.Context, rec esphttp.FleetActivityRecord) (esphttp.FleetActivityRecord, error) {
	if s.failAppendActivity {
		return esphttp.FleetActivityRecord{}, errors.New("fleet activity store unavailable")
	}
	return s.MemoryStateStore.AppendFleetActivity(ctx, rec)
}

func createFleetForTest(t *testing.T, state esphttp.StateStore, fleetID, name string) (esphttp.FleetRecord, entmoot.NodeInfo) {
	t.Helper()
	coordinatorID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate coordinator: %v", err)
	}
	coordinator := testESPNodeInfo(t, coordinatorID.PublicKey)
	fleet, err := state.CreateFleet(context.Background(), esphttp.FleetRecord{
		FleetID:             fleetID,
		Name:                name,
		ControlGroupID:      testESPGroupID(90),
		Coordinator:         coordinator,
		CoordinatorDeviceID: "ios-1",
		CreatedAtMS:         1_700_000_000_000,
	})
	if err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(context.Background(), esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		MemberID:      *coordinator.MemberID,
		PeerID:        coordinator.PeerID,
		EntmootPubKey: encodeBase64(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	return fleet, coordinator
}

func serveESPFleetMemberRemoveIPC(t *testing.T, sock string, gid entmoot.GroupID, reqCh chan<- *ipc.MemberRemoveReq) func() {
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
			if req, ok := payload.(*ipc.MemberRemoveReq); ok {
				if reqCh != nil {
					reqCh <- req
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.MemberRemoveResp{
					Status:     "removed",
					GroupID:    gid,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
				_ = req
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}
