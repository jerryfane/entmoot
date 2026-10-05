package main

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"testing"

	"github.com/libp2p/go-libp2p"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// The running daemon removes members over IPC, which is also the path ESP's
// member_remove takes. Removing an admin there must seal the removal with a
// checkpoint like the CLI does, or a join through the admin's invite dated
// before the removal is still admitted by the daemon that signed it.
func TestMemberRemoveOverIPCRefusesBackdatedJoinsThroughTheAdmin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	admin, adminInfo := mustDaemonIdentity(t)
	outsider, outsiderInfo := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	mustJoinWithInvite(t, group, admin, mustDaemonInvite(t, group, founder, adminInfo, 1))
	policy := group.Policy()
	policy.Admins = []entmoot.MemberID{*adminInfo.MemberID}
	grant, err := group.SignRecord(founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy})
	if err != nil {
		t.Fatalf("grant admin: %v", err)
	}
	invite := mustDaemonInvite(t, group, admin, outsiderInfo, 1)
	mustCloseGroup(t, group)

	host, binding, err := libp2ptransport.NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close()
	messages, err := store.OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer messages.Close()
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: founder, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: host, Binding: binding, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer runtime.Close()
	session, _, err := runtime.AddLocalGroup(ctx, gid)
	if err != nil {
		t.Fatalf("AddLocalGroup: %v", err)
	}
	server := &ipcServer{
		memberID: *founderInfo.MemberID,
		peerID:   founderInfo.PeerID,
		identity: founder,
		dataDir:  root,
		runtime:  runtime,
	}

	client, daemon := net.Pipe()
	defer client.Close()
	go func() {
		defer daemon.Close()
		server.handleMemberRemove(ctx, daemon, &ipc.MemberRemoveReq{GroupID: gid, Target: adminInfo})
	}()
	_, decoded, err := ipc.ReadAndDecode(client)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if frame, ok := decoded.(*ipc.ErrorFrame); ok {
		t.Fatalf("member_remove refused: %s: %s", frame.Code, frame.Message)
	}
	resp, ok := decoded.(*ipc.MemberRemoveResp)
	if !ok {
		t.Fatalf("response is %T, want a member_remove response", decoded)
	}
	if resp.RosterHead != session.group.Canonical().ID {
		t.Fatalf("member_remove reported roster head %s, the group is at %s", resp.RosterHead, session.group.Canonical().ID)
	}

	backdated := mustBackdatedJoin(t, gid, outsider, invite, grant.Timestamp+1)
	if _, err := session.group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
		t.Fatalf("the daemon applied a join through the removed admin's invite dated before the removal: %v (member=%t)",
			err, session.group.IsMemberID(*outsiderInfo.MemberID))
	}
}
