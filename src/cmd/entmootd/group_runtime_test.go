package main

import (
	"context"
	"errors"
	"io"
	"net"
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

func TestGroupMuxRoutesByFirstFrameGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := newRuntimeFakeTransport()
	mux := newGroupMuxTransport(base, nil)
	gidA := testRuntimeGroupID(0xA1)
	gidB := testRuntimeGroupID(0xB2)
	_, _ = mux.Group(gidA)
	groupB, _ := mux.Group(gidB)
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
	acceptCh chan runtimeAccept
	closed   chan struct{}
}

func newRuntimeFakeTransport() *runtimeFakeTransport {
	return &runtimeFakeTransport{
		acceptCh: make(chan runtimeAccept, 16),
		closed:   make(chan struct{}),
	}
}

func (t *runtimeFakeTransport) Dial(context.Context, entmoot.NodeID) (net.Conn, error) {
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
