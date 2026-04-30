package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/events"
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"
)

func TestGroupRuntimeAddsMultipleSelfGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	rt, err := newGroupRuntime(groupRuntimeConfig{
		NodeID:    45491,
		Identity:  identity,
		DataDir:   dataDir,
		Store:     st,
		Notify:    newNotifyingStore(st, events.NewBus()),
		Transport: newRuntimeFakeTransport(),
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer rt.Close()

	inviteA := selfInvite(t, dataDir, st, identity, 45491, testRuntimeGroupID(0xA1))
	inviteB := selfInvite(t, dataDir, st, identity, 45491, testRuntimeGroupID(0xB2))
	if _, created, err := rt.AddInvite(ctx, inviteA); err != nil || !created {
		t.Fatalf("AddInvite A created/err = %v/%v, want true/nil", created, err)
	}
	if _, created, err := rt.AddInvite(ctx, inviteB); err != nil || !created {
		t.Fatalf("AddInvite B created/err = %v/%v, want true/nil", created, err)
	}
	if rt.Count() != 2 {
		t.Fatalf("Count = %d, want 2", rt.Count())
	}
	if _, ok := rt.SingleGroup(); ok {
		t.Fatal("SingleGroup returned ok with two active groups")
	}
}

func TestGroupRuntimeAddLocalGroupStartsPersistedGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	rt, err := newGroupRuntime(groupRuntimeConfig{
		NodeID:    45491,
		Identity:  identity,
		DataDir:   dataDir,
		Store:     st,
		Notify:    newNotifyingStore(st, events.NewBus()),
		Transport: newRuntimeFakeTransport(),
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer rt.Close()

	gid := testRuntimeGroupID(0xA3)
	_ = selfInvite(t, dataDir, st, identity, 45491, gid)
	if _, created, err := rt.AddLocalGroup(ctx, gid); err != nil || !created {
		t.Fatalf("AddLocalGroup created/err = %v/%v, want true/nil", created, err)
	}
	if rt.Count() != 1 {
		t.Fatalf("Count = %d, want 1", rt.Count())
	}
}

func TestGroupRuntimeAddLocalGroupRejectsNonMember(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	rt, err := newGroupRuntime(groupRuntimeConfig{
		NodeID:    45491,
		Identity:  identity,
		DataDir:   dataDir,
		Store:     st,
		Notify:    newNotifyingStore(st, events.NewBus()),
		Transport: newRuntimeFakeTransport(),
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer rt.Close()

	gid := testRuntimeGroupID(0xA4)
	_ = selfInvite(t, dataDir, st, identity, 99999, gid)
	if _, _, err := rt.AddLocalGroup(ctx, gid); !errors.Is(err, errLocalGroupNotMember) {
		t.Fatalf("AddLocalGroup err = %v, want errLocalGroupNotMember", err)
	}
}

func TestGroupRuntimeAddLocalGroupRejectsIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	runtimeIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate runtime identity: %v", err)
	}
	rosterIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate roster identity: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	rt, err := newGroupRuntime(groupRuntimeConfig{
		NodeID:    45491,
		Identity:  runtimeIdentity,
		DataDir:   dataDir,
		Store:     st,
		Notify:    newNotifyingStore(st, events.NewBus()),
		Transport: newRuntimeFakeTransport(),
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer rt.Close()

	gid := testRuntimeGroupID(0xA5)
	_ = selfInvite(t, dataDir, st, rosterIdentity, 45491, gid)
	if _, _, err := rt.AddLocalGroup(ctx, gid); !errors.Is(err, errLocalGroupIdentityMismatch) {
		t.Fatalf("AddLocalGroup err = %v, want errLocalGroupIdentityMismatch", err)
	}
}

func TestGroupRuntimeRejectedJoinRollsBackNewGroupDir(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	runtimeIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate runtime identity: %v", err)
	}
	rosterIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate roster identity: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	rt, err := newGroupRuntime(groupRuntimeConfig{
		NodeID:    45491,
		Identity:  runtimeIdentity,
		DataDir:   dataDir,
		Store:     st,
		Notify:    newNotifyingStore(st, events.NewBus()),
		Transport: newRuntimeFakeTransport(),
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer rt.Close()

	gid := testRuntimeGroupID(0xA9)
	if _, _, err := rt.addGroup(ctx, gid, persistForeignRosterBootstrap(t, dataDir, rosterIdentity, 99999, gid)); !errors.Is(err, errLocalGroupNotMember) {
		t.Fatalf("addGroup err = %v, want errLocalGroupNotMember", err)
	}
	if _, err := os.Stat(groupDirPath(dataDir, gid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("group dir still exists after rejected join: %v", err)
	}
	gids, err := selectServeGroupIDs(dataDir, nil, nil)
	if !errors.Is(err, errServeNoGroups) {
		t.Fatalf("selectServeGroupIDs err = %v, gids = %v; want errServeNoGroups", err, gids)
	}
}

func TestGroupRuntimeRejectedJoinPreservesExistingGroupDir(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	runtimeIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate runtime identity: %v", err)
	}
	rosterIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate roster identity: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	rt, err := newGroupRuntime(groupRuntimeConfig{
		NodeID:    45491,
		Identity:  runtimeIdentity,
		DataDir:   dataDir,
		Store:     st,
		Notify:    newNotifyingStore(st, events.NewBus()),
		Transport: newRuntimeFakeTransport(),
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer rt.Close()

	gid := testRuntimeGroupID(0xAA)
	groupDir := groupDirPath(dataDir, gid)
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatalf("mkdir group dir: %v", err)
	}
	sentinel := filepath.Join(groupDir, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	if _, _, err := rt.addGroup(ctx, gid, persistForeignRosterBootstrap(t, dataDir, rosterIdentity, 99999, gid)); !errors.Is(err, errLocalGroupNotMember) {
		t.Fatalf("addGroup err = %v, want errLocalGroupNotMember", err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("sentinel was not preserved: %v", err)
	}
	if _, err := os.Stat(groupRosterPath(dataDir, gid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created roster still exists after rejected join: %v", err)
	}
}

func TestSelectServeGroupIDsFiltersGroupsWithoutRoster(t *testing.T) {
	dataDir := t.TempDir()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	valid := testRuntimeGroupID(0xA6)
	missingRoster := testRuntimeGroupID(0xA7)
	_ = selfInvite(t, dataDir, st, identity, 45491, valid)
	if err := os.MkdirAll(groupDirPath(dataDir, missingRoster), 0o700); err != nil {
		t.Fatalf("mkdir missing roster group: %v", err)
	}

	gids, err := selectServeGroupIDs(dataDir, nil, nil)
	if err != nil {
		t.Fatalf("selectServeGroupIDs: %v", err)
	}
	if len(gids) != 1 || gids[0] != valid {
		t.Fatalf("gids = %v, want [%s]", gids, valid.String())
	}
}

func TestSelectServeGroupIDsRequiresSelectedRoster(t *testing.T) {
	dataDir := t.TempDir()
	missing := testRuntimeGroupID(0xA8)
	if _, err := selectServeGroupIDs(dataDir, []string{missing.String()}, nil); !errors.Is(err, errServeGroupMissing) {
		t.Fatalf("selectServeGroupIDs err = %v, want errServeGroupMissing", err)
	}
}

func TestSelectServeGroupIDsRejectsMalformedSelectedGroup(t *testing.T) {
	if _, err := selectServeGroupIDs(t.TempDir(), []string{"not-a-group-id"}, nil); !errors.Is(err, errServeInvalidGroupID) {
		t.Fatalf("selectServeGroupIDs err = %v, want errServeInvalidGroupID", err)
	}
}

func TestCmdServeMalformedGroupReturnsInvalidArgumentBeforePilotSetup(t *testing.T) {
	if code := cmdServe(&globalFlags{}, []string{"-group", "not-a-group-id"}); code != exitInvalidArgument {
		t.Fatalf("cmdServe code = %d, want %d", code, exitInvalidArgument)
	}
}

func TestCmdServeNoGroupsReturnsGroupNotFoundBeforePilotSetup(t *testing.T) {
	code := cmdServe(&globalFlags{
		data:   t.TempDir(),
		socket: filepath.Join(t.TempDir(), "missing-pilot.sock"),
	}, nil)
	if code != exitGroupNotFound {
		t.Fatalf("cmdServe code = %d, want %d", code, exitGroupNotFound)
	}
}

func TestCmdServeMissingSelectedGroupReturnsGroupNotFoundBeforePilotSetup(t *testing.T) {
	missing := testRuntimeGroupID(0xAB)
	code := cmdServe(&globalFlags{
		data:   t.TempDir(),
		socket: filepath.Join(t.TempDir(), "missing-pilot.sock"),
	}, []string{"-group", missing.String()})
	if code != exitGroupNotFound {
		t.Fatalf("cmdServe code = %d, want %d", code, exitGroupNotFound)
	}
}

func TestClassifyJoinAddInviteMembershipFailures(t *testing.T) {
	for _, err := range []error{errLocalGroupNotMember, errLocalGroupIdentityMismatch} {
		if code := classifyJoinAddInviteError(err); code != exitNotMember {
			t.Fatalf("classifyJoinAddInviteError(%v) = %d, want %d", err, code, exitNotMember)
		}
	}
}

func TestGroupMuxRoutesByFirstFrameGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := newRuntimeFakeTransport()
	mux := newGroupMuxTransport(base, nil)
	gidA := testRuntimeGroupID(0xA1)
	gidB := testRuntimeGroupID(0xB2)
	groupA, _ := mux.Group(gidA)
	groupB, _ := mux.Group(gidB)
	tunnelA := make(chan entmoot.NodeID, 1)
	tunnelB := make(chan entmoot.NodeID, 1)
	groupA.SetOnTunnelUp(func(peer entmoot.NodeID) { tunnelA <- peer })
	groupB.SetOnTunnelUp(func(peer entmoot.NodeID) { tunnelB <- peer })
	go func() { _ = mux.AcceptLoop(ctx) }()

	client, server := net.Pipe()
	defer client.Close()
	base.acceptCh <- runtimeAccept{conn: server, remote: 45981}
	go func() {
		_ = wire.EncodeAndWrite(client, &wire.MerkleReq{GroupID: gidB})
	}()

	acceptCtx, acceptCancel := context.WithTimeout(ctx, time.Second)
	defer acceptCancel()
	gotConn, remote, err := groupB.Accept(acceptCtx)
	if err != nil {
		t.Fatalf("groupB.Accept: %v", err)
	}
	defer gotConn.Close()
	if remote != 45981 {
		t.Fatalf("remote = %d, want 45981", remote)
	}
	_, payload, err := wire.ReadAndDecode(gotConn)
	if err != nil {
		t.Fatalf("ReadAndDecode: %v", err)
	}
	req, ok := payload.(*wire.MerkleReq)
	if !ok || req.GroupID != gidB {
		t.Fatalf("payload = %#v, want MerkleReq for gidB", payload)
	}
	expectTunnelUp(t, tunnelB, 45981, "groupB")
	expectNoTunnelUp(t, tunnelA, "groupA")
}

func TestGroupMuxRoutesMemberProfileFramesByGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := newRuntimeFakeTransport()
	mux := newGroupMuxTransport(base, nil)
	gidA := testRuntimeGroupID(0xA1)
	gidB := testRuntimeGroupID(0xB2)
	groupA, _ := mux.Group(gidA)
	groupB, _ := mux.Group(gidB)
	tunnelA := make(chan entmoot.NodeID, 1)
	tunnelB := make(chan entmoot.NodeID, 1)
	groupA.SetOnTunnelUp(func(peer entmoot.NodeID) { tunnelA <- peer })
	groupB.SetOnTunnelUp(func(peer entmoot.NodeID) { tunnelB <- peer })
	go func() { _ = mux.AcceptLoop(ctx) }()

	for name, payload := range map[string]any{
		"ad":   &wire.MemberProfileAd{GroupID: gidB},
		"req":  &wire.MemberProfileSnapshotReq{GroupID: gidB},
		"resp": &wire.MemberProfileSnapshotResp{GroupID: gidB},
	} {
		client, server := net.Pipe()
		base.acceptCh <- runtimeAccept{conn: server, remote: 45981}
		go func() {
			_ = wire.EncodeAndWrite(client, payload)
		}()

		acceptCtx, acceptCancel := context.WithTimeout(ctx, time.Second)
		gotConn, remote, err := groupB.Accept(acceptCtx)
		acceptCancel()
		if err != nil {
			_ = client.Close()
			t.Fatalf("%s: groupB.Accept: %v", name, err)
		}
		if remote != 45981 {
			_ = gotConn.Close()
			_ = client.Close()
			t.Fatalf("%s: remote = %d, want 45981", name, remote)
		}
		_, gotPayload, err := wire.ReadAndDecode(gotConn)
		_ = gotConn.Close()
		_ = client.Close()
		if err != nil {
			t.Fatalf("%s: ReadAndDecode: %v", name, err)
		}
		if gotGroupID, ok := mux.groupForPayload(gotPayload); !ok || gotGroupID != gidB {
			t.Fatalf("%s: payload = %#v, want routable gidB", name, gotPayload)
		}
		expectTunnelUp(t, tunnelB, 45981, "groupB")
		expectNoTunnelUp(t, tunnelA, "groupA")
	}
}

func TestGroupMuxDoesNotFireTunnelUpBeforeFirstFrame(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := newRuntimeFakeTransport()
	mux := newGroupMuxTransport(base, nil)
	groupB, _ := mux.Group(testRuntimeGroupID(0xB2))
	tunnelB := make(chan entmoot.NodeID, 1)
	groupB.SetOnTunnelUp(func(peer entmoot.NodeID) { tunnelB <- peer })
	go func() { _ = mux.AcceptLoop(ctx) }()

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	base.acceptCh <- runtimeAccept{conn: server, remote: 45981}

	expectNoTunnelUp(t, tunnelB, "groupB")
}

func TestGroupTransportOutboundDialFiresOnlyItsGroupTunnelUp(t *testing.T) {
	ctx := context.Background()

	base := newRuntimeFakeTransport()
	mux := newGroupMuxTransport(base, nil)
	groupA, _ := mux.Group(testRuntimeGroupID(0xA1))
	groupB, _ := mux.Group(testRuntimeGroupID(0xB2))
	tunnelA := make(chan entmoot.NodeID, 1)
	tunnelB := make(chan entmoot.NodeID, 1)
	groupA.SetOnTunnelUp(func(peer entmoot.NodeID) { tunnelA <- peer })
	groupB.SetOnTunnelUp(func(peer entmoot.NodeID) { tunnelB <- peer })

	base.dialConn = &bufferConn{}
	conn, err := groupB.Dial(ctx, 45981)
	if err != nil {
		t.Fatalf("groupB.Dial: %v", err)
	}
	defer conn.Close()

	expectTunnelUp(t, tunnelB, 45981, "groupB")
	expectNoTunnelUp(t, tunnelA, "groupA")
}

func TestGroupTransportOutboundDialFailureDoesNotFireTunnelUp(t *testing.T) {
	ctx := context.Background()

	base := newRuntimeFakeTransport()
	mux := newGroupMuxTransport(base, nil)
	groupB, _ := mux.Group(testRuntimeGroupID(0xB2))
	tunnelB := make(chan entmoot.NodeID, 1)
	groupB.SetOnTunnelUp(func(peer entmoot.NodeID) { tunnelB <- peer })

	base.dialErr = errors.New("dial failed")
	if _, err := groupB.Dial(ctx, 45981); err == nil {
		t.Fatal("groupB.Dial succeeded, want error")
	}

	expectNoTunnelUp(t, tunnelB, "groupB")
}

func TestGroupTransportForwardsOptionalTransportCapabilities(t *testing.T) {
	base := newRuntimeFakeTransport()
	base.dialBudget = 47 * time.Second
	base.dropPeerSession = true
	base.classification = gossip.StreamErrorClassification{
		Retryable: true,
		Timeout:   true,
	}
	mux := newGroupMuxTransport(base, nil)
	group, _ := mux.Group(testRuntimeGroupID(0xB2))

	if got := group.DialBudget(); got != base.dialBudget {
		t.Fatalf("DialBudget = %v, want %v", got, base.dialBudget)
	}
	if !group.DropPeerSession(45981) {
		t.Fatal("DropPeerSession = false, want true")
	}
	got := group.ClassifyStreamError(errors.New("dial timeout"))
	if got != base.classification {
		t.Fatalf("ClassifyStreamError = %+v, want %+v", got, base.classification)
	}
}

func TestGroupRuntimeConcurrentDuplicateJoinKeepsMuxGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	rt, err := newGroupRuntime(groupRuntimeConfig{
		NodeID:    45491,
		Identity:  identity,
		DataDir:   dataDir,
		Store:     st,
		Notify:    newNotifyingStore(st, events.NewBus()),
		Transport: newRuntimeFakeTransport(),
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer rt.Close()

	invite := selfInvite(t, dataDir, st, identity, 45491, testRuntimeGroupID(0xC3))
	start := make(chan struct{})
	const workers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	createdCount := 0
	errs := make([]error, 0)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, created, err := rt.AddInvite(ctx, invite)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			if created {
				createdCount++
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("AddInvite errors: %v", errs)
	}
	if createdCount != 1 {
		t.Fatalf("createdCount = %d, want 1", createdCount)
	}
	if rt.Count() != 1 {
		t.Fatalf("Count = %d, want 1", rt.Count())
	}
	if _, ok := rt.mux.lookupGroup(invite.GroupID); !ok {
		t.Fatal("mux group was removed after duplicate joins")
	}
}

func TestGroupMuxSlowFirstFrameDoesNotBlockAcceptLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := newRuntimeFakeTransport()
	mux := newGroupMuxTransport(base, nil)
	groupB, _ := mux.Group(testRuntimeGroupID(0xB2))
	go func() { _ = mux.AcceptLoop(ctx) }()

	slowClient, slowServer := net.Pipe()
	defer slowClient.Close()
	defer slowServer.Close()
	base.acceptCh <- runtimeAccept{conn: slowServer, remote: 45981}
	time.Sleep(50 * time.Millisecond)

	client, server := net.Pipe()
	defer client.Close()
	base.acceptCh <- runtimeAccept{conn: server, remote: 45981}
	go func() {
		_ = wire.EncodeAndWrite(client, &wire.MerkleReq{GroupID: testRuntimeGroupID(0xB2)})
	}()

	acceptCtx, acceptCancel := context.WithTimeout(ctx, time.Second)
	defer acceptCancel()
	gotConn, _, err := groupB.Accept(acceptCtx)
	if err != nil {
		t.Fatalf("groupB.Accept: %v", err)
	}
	defer gotConn.Close()
}

func TestEndpointChangeFanoutBroadcastsToAllSubscribers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upstream := make(chan struct{})
	fanout := newEndpointChangeFanout(upstream)
	subA, unsubA := fanout.Subscribe()
	defer unsubA()
	subB, unsubB := fanout.Subscribe()
	defer unsubB()
	go fanout.Run(ctx)

	upstream <- struct{}{}

	expectEndpointTick(t, subA, "subA")
	expectEndpointTick(t, subB, "subB")
}

func TestEndpointChangeFanoutCollapsesBursts(t *testing.T) {
	fanout := newEndpointChangeFanout(make(chan struct{}))
	subA, unsubA := fanout.Subscribe()
	defer unsubA()
	subB, unsubB := fanout.Subscribe()
	defer unsubB()

	fanout.broadcast()
	fanout.broadcast()
	fanout.broadcast()

	expectEndpointTick(t, subA, "subA")
	expectEndpointTick(t, subB, "subB")
	expectNoEndpointTick(t, subA, "subA")
	expectNoEndpointTick(t, subB, "subB")
}

func TestEndpointChangeFanoutUnsubscribeKeepsRemainingSubscribers(t *testing.T) {
	fanout := newEndpointChangeFanout(make(chan struct{}))
	subA, unsubA := fanout.Subscribe()
	subB, unsubB := fanout.Subscribe()
	defer unsubB()

	unsubA()
	fanout.broadcast()

	if _, ok := <-subA; ok {
		t.Fatal("subA is still open after unsubscribe")
	}
	expectEndpointTick(t, subB, "subB")
}

func TestIPCPublishRequiresGroupWhenMultipleSessionsActive(t *testing.T) {
	gidA := testRuntimeGroupID(0xA1)
	gidB := testRuntimeGroupID(0xB2)
	srv := &ipcServer{
		runtime: &groupRuntime{
			sessions: map[entmoot.GroupID]*groupSession{
				gidA: {groupID: gidA},
				gidB: {groupID: gidB},
			},
		},
	}
	conn := &bufferConn{}
	srv.handlePublish(context.Background(), conn, &ipc.PublishReq{Topics: []string{"x"}, Content: []byte("body")})
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		t.Fatalf("ReadAndDecode: %v", err)
	}
	frame, ok := payload.(*ipc.ErrorFrame)
	if !ok {
		t.Fatalf("payload = %#v, want ErrorFrame", payload)
	}
	if frame.Code != ipc.CodeInvalidArgument {
		t.Fatalf("code = %s, want INVALID_ARGUMENT", frame.Code)
	}
}

func expectEndpointTick(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if !ok {
			t.Fatalf("%s closed before endpoint tick", name)
		}
	case <-time.After(time.Second):
		t.Fatalf("%s did not receive endpoint tick", name)
	}
}

func expectNoEndpointTick(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s received duplicate endpoint tick", name)
	default:
	}
}

func expectTunnelUp(t *testing.T, ch <-chan entmoot.NodeID, want entmoot.NodeID, name string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("%s tunnel peer = %d, want %d", name, got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("%s did not receive tunnel-up", name)
	}
}

func expectNoTunnelUp(t *testing.T, ch <-chan entmoot.NodeID, name string) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("%s received unexpected tunnel-up from %d", name, got)
	case <-time.After(100 * time.Millisecond):
	}
}

func selfInvite(t *testing.T, dataDir string, st *store.SQLite, identity *keystore.Identity, nodeID entmoot.NodeID, gid entmoot.GroupID) entmoot.Invite {
	t.Helper()
	r, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer r.Close()
	info := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: append([]byte(nil), identity.PublicKey...)}
	if err := r.Genesis(identity, info, time.Now().UnixMilli()); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	root, err := st.MerkleRoot(context.Background(), gid)
	if err != nil {
		t.Fatalf("MerkleRoot: %v", err)
	}
	now := time.Now().UnixMilli()
	invite := entmoot.Invite{
		GroupID:    gid,
		Founder:    info,
		RosterHead: r.Head(),
		MerkleRoot: root,
		IssuedAt:   now,
		ValidUntil: now + int64(time.Hour/time.Millisecond),
		Issuer:     info,
	}
	signing := invite
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		t.Fatalf("canonical invite: %v", err)
	}
	invite.Signature = identity.Sign(sigInput)
	return invite
}

func persistForeignRosterBootstrap(t *testing.T, dataDir string, identity *keystore.Identity, nodeID entmoot.NodeID, gid entmoot.GroupID) func(context.Context, *gossip.Gossiper) error {
	t.Helper()
	return func(context.Context, *gossip.Gossiper) error {
		r, err := roster.OpenJSONL(dataDir, gid)
		if err != nil {
			return err
		}
		defer r.Close()
		info := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: append([]byte(nil), identity.PublicKey...)}
		return r.Genesis(identity, info, time.Now().UnixMilli())
	}
}

func testRuntimeGroupID(fill byte) entmoot.GroupID {
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = fill
	}
	return gid
}

type runtimeAccept struct {
	conn   net.Conn
	remote entmoot.NodeID
}

type runtimeFakeTransport struct {
	acceptCh        chan runtimeAccept
	closed          chan struct{}
	dialConn        net.Conn
	dialErr         error
	dialBudget      time.Duration
	dropPeerSession bool
	classification  gossip.StreamErrorClassification
}

func newRuntimeFakeTransport() *runtimeFakeTransport {
	return &runtimeFakeTransport{
		acceptCh: make(chan runtimeAccept, 16),
		closed:   make(chan struct{}),
	}
}

func (t *runtimeFakeTransport) Dial(context.Context, entmoot.NodeID) (net.Conn, error) {
	if t.dialErr != nil {
		return nil, t.dialErr
	}
	if t.dialConn != nil {
		return t.dialConn, nil
	}
	return nil, errors.New("dial not implemented")
}

func (t *runtimeFakeTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	select {
	case item := <-t.acceptCh:
		return item.conn, item.remote, nil
	case <-t.closed:
		return nil, 0, net.ErrClosed
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func (t *runtimeFakeTransport) TrustedPeers(context.Context) ([]entmoot.NodeID, error) {
	return nil, nil
}

func (t *runtimeFakeTransport) SetPeerEndpoints(context.Context, entmoot.NodeID, []entmoot.NodeEndpoint) error {
	return nil
}

func (t *runtimeFakeTransport) SetOnTunnelUp(func(entmoot.NodeID)) {}

func (t *runtimeFakeTransport) DialBudget() time.Duration {
	return t.dialBudget
}

func (t *runtimeFakeTransport) DropPeerSession(entmoot.NodeID) bool {
	return t.dropPeerSession
}

func (t *runtimeFakeTransport) ClassifyStreamError(error) gossip.StreamErrorClassification {
	return t.classification
}

func (t *runtimeFakeTransport) Close() error {
	select {
	case <-t.closed:
	default:
		close(t.closed)
	}
	return nil
}

type bufferConn struct {
	bytes []byte
}

func (c *bufferConn) Read(p []byte) (int, error) {
	if len(c.bytes) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.bytes)
	c.bytes = c.bytes[n:]
	return n, nil
}

func (c *bufferConn) Write(p []byte) (int, error) {
	c.bytes = append(c.bytes, p...)
	return len(p), nil
}

func (c *bufferConn) Close() error                     { return nil }
func (c *bufferConn) LocalAddr() net.Addr              { return fakeAddr("local") }
func (c *bufferConn) RemoteAddr() net.Addr             { return fakeAddr("remote") }
func (c *bufferConn) SetDeadline(time.Time) error      { return nil }
func (c *bufferConn) SetReadDeadline(time.Time) error  { return nil }
func (c *bufferConn) SetWriteDeadline(time.Time) error { return nil }

type fakeAddr string

func (a fakeAddr) Network() string { return string(a) }
func (a fakeAddr) String() string  { return string(a) }

var _ gossip.Transport = (*runtimeFakeTransport)(nil)
