package gossip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"
)

type scriptedTransport struct {
	mu        sync.Mutex
	handlers  []func(net.Conn)
	dialErrs  []error
	dials     int
	drops     int
	dialDelay time.Duration
}

type staleClassifyingTransport struct {
	scriptedTransport
}

func (t *staleClassifyingTransport) ClassifyStreamError(err error) StreamErrorClassification {
	return StreamErrorClassification{Retryable: true, StaleSession: true}
}

func (t *scriptedTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	if t.dialDelay > 0 {
		select {
		case <-time.After(t.dialDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	t.mu.Lock()
	t.dials++
	if len(t.dialErrs) > 0 {
		err := t.dialErrs[0]
		t.dialErrs = t.dialErrs[1:]
		t.mu.Unlock()
		return nil, err
	}
	if len(t.handlers) == 0 {
		t.mu.Unlock()
		return nil, fmt.Errorf("no scripted dial handler for %d", peer)
	}
	h := t.handlers[0]
	t.handlers = t.handlers[1:]
	t.mu.Unlock()

	client, server := net.Pipe()
	go h(server)
	return client, nil
}

func (t *scriptedTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	<-ctx.Done()
	return nil, 0, ctx.Err()
}

func (t *scriptedTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return nil, nil
}

func (t *scriptedTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	return nil
}

func (t *scriptedTransport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {}

func (t *scriptedTransport) DropPeerSession(peer entmoot.NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.drops++
	return true
}

func (t *scriptedTransport) Close() error {
	return nil
}

func (t *scriptedTransport) counts() (dials, drops int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dials, t.drops
}

func waitForCondition(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func eofAfterRequest(c net.Conn) {
	defer c.Close()
	_, _, _ = wire.ReadAndDecode(c)
}

func respondAfterRequest(resp any) func(net.Conn) {
	return func(c net.Conn) {
		defer c.Close()
		_, _, err := wire.ReadAndDecode(c)
		if err != nil {
			return
		}
		_ = wire.EncodeAndWrite(c, resp)
	}
}

func newScriptedRecoveryGossiper(gid entmoot.GroupID, tr Transport) *Gossiper {
	return &Gossiper{
		cfg: Config{
			GroupID:   gid,
			Transport: tr,
		},
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		clk:               clock.System{},
		pending:           make(map[retryKey]*retryState),
		lastReconciled:    make(map[entmoot.NodeID]reconcileState),
		reconcileInFlight: make(map[entmoot.NodeID]struct{}),
		reconcileFailures: make(map[entmoot.NodeID]reconcileState),
		dialBackoffs:      make(map[entmoot.NodeID]*peerDialState),
	}
}

type tombstoneMessageStore struct {
	*store.Memory
	id entmoot.MessageID
}

func (s *tombstoneMessageStore) HasTombstone(_ context.Context, _ entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	return id == s.id, nil
}

type coverageMessageStore struct {
	*store.Memory
	floor int64
}

func (s *coverageMessageStore) CoverageFloor(context.Context, entmoot.GroupID) (int64, error) {
	return s.floor, nil
}

func TestTransportClassifiedStaleStreamErrorsAreRetryable(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	g := newScriptedRecoveryGossiper(gid, &staleClassifyingTransport{})
	err := fmt.Errorf("transport-specific stale stream")
	classification := g.classifyStreamError(context.Background(), err)
	if !classification.Retryable {
		t.Fatal("transport-classified stale stream was not retryable")
	}
	if !classification.StaleSession {
		t.Fatal("transport-classified stale stream was not stale-session")
	}
	if !shouldDropPeerSessionAfterStreamFailure(classification, 0, context.Background()) {
		t.Fatal("transport-classified stale stream did not request cached session drop")
	}
}

func TestFetchPeerRootDropsSessionAndRetriesOnEOF(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	var root wire.MerkleRoot
	root[0] = 42
	tr := &scriptedTransport{
		handlers: []func(net.Conn){
			eofAfterRequest,
			respondAfterRequest(&wire.MerkleResp{GroupID: gid, Root: root, MessageCount: 1}),
		},
	}
	g := newScriptedRecoveryGossiper(gid, tr)

	got, ok := g.fetchPeerRoot(context.Background(), 20, 0)
	if !ok {
		t.Fatalf("fetchPeerRoot failed after retry")
	}
	if got.Root != root {
		t.Fatalf("root mismatch: got %x want %x", got, root)
	}
	dials, drops := tr.counts()
	if dials != 2 {
		t.Fatalf("dials = %d, want 2", dials)
	}
	if drops != 1 {
		t.Fatalf("drops = %d, want 1", drops)
	}
}

func TestFetchPeerRootCarriesCoverageWindow(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	var root wire.MerkleRoot
	root[0] = 43
	seenSince := make(chan int64, 1)
	tr := &scriptedTransport{handlers: []func(net.Conn){
		func(c net.Conn) {
			defer c.Close()
			_, payload, err := wire.ReadAndDecode(c)
			if err != nil {
				return
			}
			req, ok := payload.(*wire.MerkleReq)
			if !ok {
				return
			}
			seenSince <- req.SinceMillis
			_ = wire.EncodeAndWrite(c, &wire.MerkleResp{
				GroupID:             gid,
				Root:                root,
				CoverageFloorMS:     20,
				ComparedSinceMillis: req.SinceMillis,
			})
		},
	}}
	g := newScriptedRecoveryGossiper(gid, tr)

	got, ok := g.fetchPeerRoot(context.Background(), 20, 42)
	if !ok {
		t.Fatal("fetchPeerRoot failed")
	}
	if got.Root != root || got.CoverageFloorMS != 20 || got.ComparedSinceMillis != 42 {
		t.Fatalf("response = %+v, want root/floor/window %x/20/42", got, root)
	}
	select {
	case since := <-seenSince:
		if since != 42 {
			t.Fatalf("request since = %d, want 42", since)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe Merkle request")
	}
}

func TestMerkleResponseReportsPartialCoverageWindow(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	st := &coverageMessageStore{Memory: store.NewMemory(), floor: 20}
	old := entmoot.Message{GroupID: gid, Timestamp: 10}
	old.ID[0] = 1
	recent := entmoot.Message{GroupID: gid, Timestamp: 30}
	recent.ID[0] = 2
	if _, err := st.Put(context.Background(), gid, old); err != nil {
		t.Fatalf("Put old: %v", err)
	}
	if _, err := st.Put(context.Background(), gid, recent); err != nil {
		t.Fatalf("Put recent: %v", err)
	}
	g := newScriptedRecoveryGossiper(gid, &scriptedTransport{})
	g.cfg.Store = st
	server, client := net.Pipe()
	defer client.Close()
	go func() {
		defer server.Close()
		g.onMerkleReq(context.Background(), server, 20, &wire.MerkleReq{GroupID: gid})
	}()

	_, payload, err := wire.ReadAndDecode(client)
	if err != nil {
		t.Fatalf("ReadAndDecode: %v", err)
	}
	resp, ok := payload.(*wire.MerkleResp)
	if !ok {
		t.Fatalf("payload type = %T, want *wire.MerkleResp", payload)
	}
	want, err := store.MerkleRootSince(context.Background(), st, gid, 20)
	if err != nil {
		t.Fatalf("MerkleRootSince: %v", err)
	}
	if resp.Root != wire.MerkleRoot(want) || resp.CoverageFloorMS != 20 || resp.ComparedSinceMillis != 20 {
		t.Fatalf("response = %+v, want root %x at coverage floor 20", resp, want)
	}
	if inserted, err := g.acceptInboundMessage(context.Background(), 20, old); inserted || !errors.Is(err, store.ErrPruned) {
		t.Fatalf("accept below coverage floor = (%v, %v), want (false, ErrPruned)", inserted, err)
	}
}

func TestFetchPeerRootDropsSessionAndRetriesPilotConnectionNotFoundDial(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	var root wire.MerkleRoot
	root[0] = 77
	tr := &staleClassifyingTransport{scriptedTransport: scriptedTransport{
		dialErrs: []error{fmt.Errorf("pilot open stream: connection not found")},
		handlers: []func(net.Conn){
			respondAfterRequest(&wire.MerkleResp{GroupID: gid, Root: root, MessageCount: 1}),
		},
	}}
	g := newScriptedRecoveryGossiper(gid, tr)

	got, ok := g.fetchPeerRoot(context.Background(), 20, 0)
	if !ok {
		t.Fatalf("fetchPeerRoot failed after stale Pilot session retry")
	}
	if got.Root != root {
		t.Fatalf("root mismatch: got %x want %x", got, root)
	}
	dials, drops := tr.counts()
	if dials != 2 {
		t.Fatalf("dials = %d, want 2", dials)
	}
	if drops != 1 {
		t.Fatalf("drops = %d, want 1", drops)
	}
	if !g.canDial(20) {
		t.Fatal("stale Pilot connection error armed dial backoff")
	}
}

func TestFetchPeerRootRetriesPilotConnectionClosingDial(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	var root wire.MerkleRoot
	root[0] = 78
	tr := &staleClassifyingTransport{scriptedTransport: scriptedTransport{
		dialErrs: []error{fmt.Errorf("pilot open stream: connection closing")},
		handlers: []func(net.Conn){
			respondAfterRequest(&wire.MerkleResp{GroupID: gid, Root: root, MessageCount: 1}),
		},
	}}
	g := newScriptedRecoveryGossiper(gid, tr)

	got, ok := g.fetchPeerRoot(context.Background(), 20, 0)
	if !ok {
		t.Fatalf("fetchPeerRoot failed after stale Pilot closing retry")
	}
	if got.Root != root {
		t.Fatalf("root mismatch: got %x want %x", got, root)
	}
	dials, drops := tr.counts()
	if dials != 2 {
		t.Fatalf("dials = %d, want 2", dials)
	}
	if drops != 1 {
		t.Fatalf("drops = %d, want 1", drops)
	}
	if !g.canDial(20) {
		t.Fatal("Pilot connection closing error armed dial backoff")
	}
}

func TestFetchPeerRootDropsSessionOnFirstReadDeadline(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	var root wire.MerkleRoot
	root[0] = 99
	tr := &scriptedTransport{
		handlers: []func(net.Conn){
			func(c net.Conn) {
				defer c.Close()
				_, _, _ = wire.ReadAndDecode(c)
				time.Sleep(50 * time.Millisecond)
			},
			respondAfterRequest(&wire.MerkleResp{GroupID: gid, Root: root}),
		},
	}
	g := newScriptedRecoveryGossiper(gid, tr)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	payload, err := g.requestResponseWithAttemptTimeout(ctx, 20, &wire.MerkleReq{GroupID: gid}, wire.MsgMerkleResp, "merkle_req", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("requestResponseWithAttemptTimeout failed after retry: %v", err)
	}
	resp, ok := payload.(*wire.MerkleResp)
	if !ok {
		t.Fatalf("payload type = %T, want *wire.MerkleResp", payload)
	}
	if resp.Root != root {
		t.Fatalf("root mismatch: got %x want %x", resp.Root, root)
	}
	dials, drops := tr.counts()
	if dials != 2 {
		t.Fatalf("dials = %d, want 2", dials)
	}
	if drops != 1 {
		t.Fatalf("drops = %d, want 1 for first read deadline", drops)
	}
}

func TestRequestResponseReadDeadlineArmsDialBackoff(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	tr := &scriptedTransport{
		handlers: []func(net.Conn){
			func(c net.Conn) {
				defer c.Close()
				_, _, _ = wire.ReadAndDecode(c)
				time.Sleep(50 * time.Millisecond)
			},
			func(c net.Conn) {
				defer c.Close()
				_, _, _ = wire.ReadAndDecode(c)
				time.Sleep(50 * time.Millisecond)
			},
		},
	}
	g := newScriptedRecoveryGossiper(gid, tr)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := g.requestResponseWithAttemptTimeout(ctx, 20, &wire.MerkleReq{GroupID: gid}, wire.MsgMerkleResp, "merkle_req", 10*time.Millisecond)
	if err == nil {
		t.Fatal("requestResponseWithAttemptTimeout unexpectedly succeeded")
	}
	if g.canDial(20) {
		t.Fatal("read deadline did not arm dial backoff")
	}
}

func TestRequestResponseDialBudgetDoesNotUseAttemptTimeout(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	var root wire.MerkleRoot
	root[0] = 123
	tr := &scriptedTransport{
		dialDelay: 30 * time.Millisecond,
		handlers: []func(net.Conn){
			respondAfterRequest(&wire.MerkleResp{GroupID: gid, Root: root}),
		},
	}
	g := newScriptedRecoveryGossiper(gid, tr)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	payload, err := g.requestResponseWithAttemptTimeout(ctx, 20, &wire.MerkleReq{GroupID: gid}, wire.MsgMerkleResp, "merkle_req", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("requestResponseWithAttemptTimeout failed: %v", err)
	}
	resp, ok := payload.(*wire.MerkleResp)
	if !ok {
		t.Fatalf("payload type = %T, want *wire.MerkleResp", payload)
	}
	if resp.Root != root {
		t.Fatalf("root mismatch: got %x want %x", resp.Root, root)
	}
	dials, drops := tr.counts()
	if dials != 1 {
		t.Fatalf("dials = %d, want 1", dials)
	}
	if drops != 0 {
		t.Fatalf("drops = %d, want 0", drops)
	}
}

func TestMaybeReconcileUsesLifetimeContextForCanceledTrigger(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	tr := &scriptedTransport{}
	g := newScriptedRecoveryGossiper(gid, tr)
	g.lifeMu.Lock()
	g.lifeCtx = context.Background()
	g.lifeMu.Unlock()

	triggerCtx, cancel := context.WithCancel(context.Background())
	cancel()
	g.maybeReconcile(triggerCtx, 20)

	waitForCondition(t, func() bool {
		dials, _ := tr.counts()
		return dials > 0
	})
}

type localCancelTransport struct{}

func (localCancelTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	return nil, ctx.Err()
}

func (localCancelTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	<-ctx.Done()
	return nil, 0, ctx.Err()
}

func (localCancelTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return nil, nil
}

func (localCancelTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	return nil
}

func (localCancelTransport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {}

func (localCancelTransport) Close() error { return nil }

func TestLocalContextCancellationDoesNotArmDialBackoff(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	g := newScriptedRecoveryGossiper(gid, localCancelTransport{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.requestResponse(ctx, 20, &wire.MerkleReq{GroupID: gid}, wire.MsgMerkleResp, "merkle_req")
	if err == nil {
		t.Fatal("requestResponse unexpectedly succeeded")
	}
	if !g.canDial(20) {
		t.Fatal("local cancellation armed dial backoff")
	}
}

func TestRequestResponseCancelsBlockedRead(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	tr := &scriptedTransport{
		handlers: []func(net.Conn){
			func(c net.Conn) {
				defer c.Close()
				_, _, _ = wire.ReadAndDecode(c)
				_, _ = io.Copy(io.Discard, c)
			},
		},
	}
	g := newScriptedRecoveryGossiper(gid, tr)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := g.requestResponse(ctx, 20, &wire.MerkleReq{GroupID: gid}, wire.MsgMerkleResp, "merkle_req")
		done <- err
	}()

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("requestResponse unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("requestResponse did not return after context cancellation")
	}
	if !g.canDial(20) {
		t.Fatal("local cancellation armed dial backoff")
	}
	if _, drops := tr.counts(); drops != 0 {
		t.Fatalf("drops = %d, want 0 for local cancellation", drops)
	}
}

type staticAddr string

func (a staticAddr) Network() string { return "test" }
func (a staticAddr) String() string  { return string(a) }

type scriptedWriteConn struct {
	writeErr error
	closed   bool
}

func (c *scriptedWriteConn) Read(b []byte) (int, error)       { return 0, io.EOF }
func (c *scriptedWriteConn) Write(b []byte) (int, error)      { return len(b), c.writeErr }
func (c *scriptedWriteConn) Close() error                     { c.closed = true; return nil }
func (c *scriptedWriteConn) LocalAddr() net.Addr              { return staticAddr("local") }
func (c *scriptedWriteConn) RemoteAddr() net.Addr             { return staticAddr("remote") }
func (c *scriptedWriteConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedWriteConn) SetWriteDeadline(time.Time) error { return nil }

type scriptedWriteTransport struct {
	mu    sync.Mutex
	conns []net.Conn
	dials int
	drops int
}

func (t *scriptedWriteTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dials++
	if len(t.conns) == 0 {
		return nil, fmt.Errorf("no scripted conn")
	}
	c := t.conns[0]
	t.conns = t.conns[1:]
	return c, nil
}

func (t *scriptedWriteTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	<-ctx.Done()
	return nil, 0, ctx.Err()
}

func (t *scriptedWriteTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return nil, nil
}

func (t *scriptedWriteTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	return nil
}

func (t *scriptedWriteTransport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {}

func (t *scriptedWriteTransport) DropPeerSession(peer entmoot.NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.drops++
	return true
}

func (t *scriptedWriteTransport) Close() error { return nil }

func (t *scriptedWriteTransport) counts() (dials, drops int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dials, t.drops
}

type deadlineBudgetTransport struct {
	mu            sync.Mutex
	minDialBudget time.Duration
	dialBudget    time.Duration
	dials         int
	writes        int
	writeDeadline time.Time
}

func (t *deadlineBudgetTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	t.mu.Lock()
	t.dials++
	t.mu.Unlock()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) < t.minDialBudget {
		return nil, context.DeadlineExceeded
	}
	return &deadlineBudgetConn{owner: t}, nil
}

func (t *deadlineBudgetTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	<-ctx.Done()
	return nil, 0, ctx.Err()
}

func (t *deadlineBudgetTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return nil, nil
}

func (t *deadlineBudgetTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	return nil
}

func (t *deadlineBudgetTransport) DialBudget() time.Duration                  { return t.dialBudget }
func (t *deadlineBudgetTransport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {}
func (t *deadlineBudgetTransport) Close() error                               { return nil }

type deadlineBudgetConn struct {
	owner *deadlineBudgetTransport
}

func (c *deadlineBudgetConn) Read(b []byte) (int, error) { return 0, io.EOF }
func (c *deadlineBudgetConn) Write(b []byte) (int, error) {
	c.owner.mu.Lock()
	defer c.owner.mu.Unlock()
	c.owner.writes++
	return len(b), nil
}
func (c *deadlineBudgetConn) Close() error                    { return nil }
func (c *deadlineBudgetConn) LocalAddr() net.Addr             { return staticAddr("local") }
func (c *deadlineBudgetConn) RemoteAddr() net.Addr            { return staticAddr("remote") }
func (c *deadlineBudgetConn) SetDeadline(time.Time) error     { return nil }
func (c *deadlineBudgetConn) SetReadDeadline(time.Time) error { return nil }
func (c *deadlineBudgetConn) SetWriteDeadline(deadline time.Time) error {
	c.owner.mu.Lock()
	defer c.owner.mu.Unlock()
	c.owner.writeDeadline = deadline
	return nil
}

type recordingWriteDeadlineConn struct {
	writeDeadline time.Time
	writes        int
}

func (c *recordingWriteDeadlineConn) Read(b []byte) (int, error) { return 0, io.EOF }
func (c *recordingWriteDeadlineConn) Write(b []byte) (int, error) {
	c.writes++
	return len(b), nil
}
func (c *recordingWriteDeadlineConn) Close() error                    { return nil }
func (c *recordingWriteDeadlineConn) LocalAddr() net.Addr             { return staticAddr("local") }
func (c *recordingWriteDeadlineConn) RemoteAddr() net.Addr            { return staticAddr("remote") }
func (c *recordingWriteDeadlineConn) SetDeadline(time.Time) error     { return nil }
func (c *recordingWriteDeadlineConn) SetReadDeadline(time.Time) error { return nil }
func (c *recordingWriteDeadlineConn) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		c.writeDeadline = deadline
	}
	return nil
}

type recordingDeadlineTransport struct {
	scriptedTransport
	mu       sync.Mutex
	deadline time.Time
}

func (t *recordingDeadlineTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	conn, err := t.scriptedTransport.Dial(ctx, peer)
	if err != nil {
		return nil, err
	}
	return &recordingDeadlineConn{Conn: conn, owner: t}, nil
}

func (t *recordingDeadlineTransport) deadlineSnapshot() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.deadline
}

type recordingDeadlineConn struct {
	net.Conn
	owner *recordingDeadlineTransport
}

func (c *recordingDeadlineConn) SetDeadline(deadline time.Time) error {
	c.owner.mu.Lock()
	if !deadline.IsZero() {
		c.owner.deadline = deadline
	}
	c.owner.mu.Unlock()
	return c.Conn.SetDeadline(deadline)
}

func TestOneWaySendDropsStaleSessionAndRetriesWrite(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	tr := &scriptedWriteTransport{
		conns: []net.Conn{
			&scriptedWriteConn{writeErr: io.ErrClosedPipe},
			&scriptedWriteConn{},
		},
	}
	g := newScriptedRecoveryGossiper(gid, tr)

	err := g.sendIHave(context.Background(), 20, &wire.IHave{GroupID: gid})
	if err != nil {
		t.Fatalf("sendIHave failed after retry: %v", err)
	}
	dials, drops := tr.counts()
	if dials != 2 {
		t.Fatalf("dials = %d, want 2", dials)
	}
	if drops != 1 {
		t.Fatalf("drops = %d, want 1", drops)
	}
}

func TestFanoutPushUsesLongDialBudgetAndFreshWriteBudget(t *testing.T) {
	t.Parallel()

	var gid entmoot.GroupID
	gid[0] = 1
	tr := &deadlineBudgetTransport{
		minDialBudget: 25 * time.Second,
		dialBudget:    30 * time.Second,
	}
	g := newScriptedRecoveryGossiper(gid, tr)

	g.fanoutPush(context.Background(), []entmoot.NodeID{20}, &wire.Gossip{GroupID: gid}, entmoot.MessageID{})

	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.dials != 1 {
		t.Fatalf("dials = %d, want 1", tr.dials)
	}
	if tr.writes != 1 {
		t.Fatalf("writes = %d, want 1", tr.writes)
	}
	if tr.writeDeadline.IsZero() {
		t.Fatal("write deadline was not set")
	}
	if until := time.Until(tr.writeDeadline); until < 4*time.Second {
		t.Fatalf("write deadline budget = %v, want roughly %v", until, fanoutWriteTimeout)
	}
}

func TestInboundFetchResponseUsesLargeFrameWriteBudget(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	msg := f.buildMessage(10, "fetch body deadline", 2_000)
	if _, err := f.nodes[10].storeM.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("seed message: %v", err)
	}

	conn := &recordingWriteDeadlineConn{}
	req := &wire.FetchReq{GroupID: f.groupID, ID: msg.ID}
	reqCtx, cancel := f.nodes[10].gossip.inboundLargeFrameResponseContext(ctx)
	defer cancel()
	f.nodes[10].gossip.onFetchReq(reqCtx, conn, 20, req)

	if conn.writes != 1 {
		t.Fatalf("writes = %d, want 1", conn.writes)
	}
	if conn.writeDeadline.IsZero() {
		t.Fatal("fetch response write deadline was not set")
	}
	if until := time.Until(conn.writeDeadline); until <= reconcileSessionTimeout {
		t.Fatalf("fetch response write budget = %v, want > %v", until, reconcileSessionTimeout)
	}
}

func TestInboundMemberProfileSnapshotResponseUsesLargeFrameWriteBudget(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	conn := &recordingWriteDeadlineConn{}
	req := &wire.MemberProfileSnapshotReq{GroupID: f.groupID}
	reqCtx, cancel := f.nodes[10].gossip.inboundLargeFrameResponseContext(ctx)
	defer cancel()
	f.nodes[10].gossip.onMemberProfileSnapshotReq(reqCtx, conn, 20, req)

	if conn.writes != 1 {
		t.Fatalf("writes = %d, want 1", conn.writes)
	}
	if conn.writeDeadline.IsZero() {
		t.Fatal("member profile snapshot response write deadline was not set")
	}
	if until := time.Until(conn.writeDeadline); until <= reconcileSessionTimeout {
		t.Fatalf("member profile snapshot response write budget = %v, want > %v", until, reconcileSessionTimeout)
	}
}

func TestInboundTransportSnapshotResponseUsesLargeFrameWriteBudget(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	conn := &recordingWriteDeadlineConn{}
	req := &wire.TransportSnapshotReq{GroupID: f.groupID}
	reqCtx, cancel := f.nodes[10].gossip.inboundLargeFrameResponseContext(ctx)
	defer cancel()
	f.nodes[10].gossip.onTransportSnapshotReq(reqCtx, conn, 20, req)

	if conn.writes != 1 {
		t.Fatalf("writes = %d, want 1", conn.writes)
	}
	if conn.writeDeadline.IsZero() {
		t.Fatal("transport snapshot response write deadline was not set")
	}
	if until := time.Until(conn.writeDeadline); until <= reconcileSessionTimeout {
		t.Fatalf("transport snapshot response write budget = %v, want > %v", until, reconcileSessionTimeout)
	}
}

func TestInboundMerkleResponseUsesControlWriteBudget(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	conn := &recordingWriteDeadlineConn{}
	req := &wire.MerkleReq{GroupID: f.groupID}
	reqCtx, cancel := f.nodes[10].gossip.inboundOneShotContext(ctx)
	defer cancel()
	f.nodes[10].gossip.onMerkleReq(reqCtx, conn, 20, req)

	if conn.writes != 1 {
		t.Fatalf("writes = %d, want 1", conn.writes)
	}
	if conn.writeDeadline.IsZero() {
		t.Fatal("merkle response write deadline was not set")
	}
	until := time.Until(conn.writeDeadline)
	if until <= 0 || until > reconcileSessionTimeout+time.Second {
		t.Fatalf("merkle response write budget = %v, want roughly %v", until, reconcileSessionTimeout)
	}
}

func TestFetchFromUsesLargeFrameAttemptBudget(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	msg := f.buildMessage(10, "fetch body requester deadline", 2_000)
	tr := &recordingDeadlineTransport{
		scriptedTransport: scriptedTransport{
			handlers: []func(net.Conn){
				respondAfterRequest(&wire.FetchResp{GroupID: f.groupID, ID: msg.ID, Message: &msg}),
			},
		},
	}
	f.nodes[20].gossip.cfg.Transport = tr

	if _, err := f.nodes[20].gossip.fetchFrom(ctx, 10, msg.ID); err != nil {
		t.Fatalf("fetchFrom failed: %v", err)
	}
	deadline := tr.deadlineSnapshot()
	if deadline.IsZero() {
		t.Fatal("fetch request deadline was not set")
	}
	if until := time.Until(deadline); until <= reconcileSessionTimeout {
		t.Fatalf("fetch request budget = %v, want > %v", until, reconcileSessionTimeout)
	}
}

func TestPullMemberProfileSnapshotUsesLargeFrameAttemptBudget(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	profileStore, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = profileStore.Close() })

	tr := &recordingDeadlineTransport{
		scriptedTransport: scriptedTransport{
			handlers: []func(net.Conn){
				respondAfterRequest(&wire.MemberProfileSnapshotResp{GroupID: f.groupID}),
			},
		},
	}
	g := f.nodes[20].gossip
	g.cfg.Transport = tr
	g.cfg.MemberProfileStore = profileStore

	if _, err := g.pullMemberProfileSnapshot(ctx, 10); err != nil {
		t.Fatalf("pullMemberProfileSnapshot failed: %v", err)
	}
	deadline := tr.deadlineSnapshot()
	if deadline.IsZero() {
		t.Fatal("member profile snapshot request deadline was not set")
	}
	if until := time.Until(deadline); until <= reconcileSessionTimeout {
		t.Fatalf("member profile snapshot request budget = %v, want > %v", until, reconcileSessionTimeout)
	}
}

func TestScheduledMemberProfileSnapshotUsesLargeFrameAttemptBudget(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	profileStore, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = profileStore.Close() })

	tr := &recordingDeadlineTransport{
		scriptedTransport: scriptedTransport{
			handlers: []func(net.Conn){
				respondAfterRequest(&wire.MemberProfileSnapshotResp{GroupID: f.groupID}),
			},
		},
	}
	g := f.nodes[20].gossip
	g.cfg.Transport = tr
	g.cfg.MemberProfileStore = profileStore

	if !g.scheduleMemberProfileSnapshotPull(ctx, 10, "test") {
		t.Fatal("snapshot pull was not scheduled")
	}
	waitUntil(t, time.Second, "scheduled member profile snapshot records deadline", func() bool {
		return !tr.deadlineSnapshot().IsZero()
	})
	waitUntil(t, time.Second, "scheduled member profile snapshot leaves in-flight set", func() bool {
		g.pendMu.Lock()
		defer g.pendMu.Unlock()
		_, ok := g.profilePulls[10]
		return !ok
	})
	deadline := tr.deadlineSnapshot()
	if until := time.Until(deadline); until <= g.fanoutAttemptBudget() {
		t.Fatalf("scheduled member profile snapshot budget = %v, want > fanout budget %v", until, g.fanoutAttemptBudget())
	}
}

func TestFetchMissingSkipsTombstonedID(t *testing.T) {
	t.Parallel()
	var gid entmoot.GroupID
	gid[0] = 1
	var id entmoot.MessageID
	id[0] = 9
	tr := &scriptedTransport{}
	g := newScriptedRecoveryGossiper(gid, tr)
	g.cfg.Store = &tombstoneMessageStore{Memory: store.NewMemory(), id: id}

	g.fetchMissingFrom(context.Background(), 20, []entmoot.MessageID{id})

	dials, _ := tr.counts()
	if dials != 0 {
		t.Fatalf("tombstoned id triggered %d fetch dials", dials)
	}
	msg := entmoot.Message{ID: id, GroupID: gid}
	if inserted, err := g.acceptInboundMessage(context.Background(), 20, msg); inserted || !errors.Is(err, store.ErrPruned) {
		t.Fatalf("accept tombstoned message = (%v, %v), want (false, ErrPruned)", inserted, err)
	}
}
func TestFullRangeFallbackFetchesMissingIDs(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	msg := f.buildMessage(20, "peer-only", 2_000)
	if _, err := f.nodes[20].storeM.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("seed peer message: %v", err)
	}
	peerRootBytes, err := f.nodes[20].storeM.MerkleRoot(ctx, f.groupID)
	if err != nil {
		t.Fatalf("peer MerkleRoot: %v", err)
	}
	peerRoot := wire.MerkleRoot(peerRootBytes)

	tr := &scriptedTransport{
		handlers: []func(net.Conn){
			respondAfterRequest(&wire.RangeResp{GroupID: f.groupID, IDs: []entmoot.MessageID{msg.ID}}),
			respondAfterRequest(&wire.FetchResp{GroupID: f.groupID, Message: &msg}),
		},
	}
	aG := f.nodes[10].gossip
	aG.cfg.Transport = tr

	if ok := aG.fetchFullRangeFallback(ctx, 20, peerRoot, 0); !ok {
		t.Fatalf("full-range fallback failed")
	}
	has, err := f.nodes[10].storeM.Has(ctx, f.groupID, msg.ID)
	if err != nil {
		t.Fatalf("local Has: %v", err)
	}
	if !has {
		t.Fatalf("fallback did not fetch missing peer message")
	}
}

func TestFullRangeFallbackContinuesAcrossPages(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	first := f.buildMessage(20, "peer-page-1", 2_000)
	second := f.buildMessage(20, "peer-page-2", 3_000)
	for _, msg := range []entmoot.Message{first, second} {
		if _, err := f.nodes[20].storeM.Put(ctx, msg.GroupID, msg); err != nil {
			t.Fatalf("seed peer message: %v", err)
		}
		if _, err := f.nodes[10].storeM.Put(ctx, msg.GroupID, msg); err != nil {
			t.Fatalf("seed local message: %v", err)
		}
	}
	peerRootBytes, err := f.nodes[20].storeM.MerkleRoot(ctx, f.groupID)
	if err != nil {
		t.Fatalf("peer MerkleRoot: %v", err)
	}
	nextID := first.ID
	continuation := make(chan *wire.RangeReq, 1)
	tr := &scriptedTransport{
		handlers: []func(net.Conn){
			respondAfterRequest(&wire.RangeResp{
				GroupID:         f.groupID,
				IDs:             []entmoot.MessageID{first.ID},
				Generation:      7,
				NextTimestampMS: first.Timestamp,
				NextAuthor:      first.Author.PilotNodeID,
				NextID:          &nextID,
				HasMore:         true,
			}),
			func(c net.Conn) {
				defer c.Close()
				_, payload, err := wire.ReadAndDecode(c)
				if err != nil {
					return
				}
				req, ok := payload.(*wire.RangeReq)
				if !ok {
					return
				}
				continuation <- req
				_ = wire.EncodeAndWrite(c, &wire.RangeResp{
					GroupID:    f.groupID,
					IDs:        []entmoot.MessageID{second.ID},
					Generation: 7,
				})
			},
		},
	}
	aG := f.nodes[10].gossip
	aG.cfg.Transport = tr

	if ok := aG.fetchFullRangeFallback(ctx, 20, wire.MerkleRoot(peerRootBytes), 0); !ok {
		t.Fatal("paged full-range fallback failed")
	}
	for _, msg := range []entmoot.Message{first, second} {
		has, err := f.nodes[10].storeM.Has(ctx, f.groupID, msg.ID)
		if err != nil || !has {
			t.Fatalf("local Has(%s) = (%v, %v), want true", msg.ID, has, err)
		}
	}
	select {
	case req := <-continuation:
		if req.Generation != 7 || req.AfterID == nil || *req.AfterID != first.ID ||
			req.AfterTimestampMS != first.Timestamp || req.AfterAuthor != first.Author.PilotNodeID {
			t.Fatalf("continuation request = %+v, want generation 7 after first message", req)
		}
	case <-time.After(time.Second):
		t.Fatal("second range page was not requested")
	}
}

func TestFullRangeFallbackRestartsChangedSnapshot(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	msg := f.buildMessage(20, "peer-restart", 2_000)
	for _, nodeID := range []entmoot.NodeID{10, 20} {
		if _, err := f.nodes[nodeID].storeM.Put(ctx, msg.GroupID, msg); err != nil {
			t.Fatalf("seed node %d: %v", nodeID, err)
		}
	}
	peerRoot, err := f.nodes[20].storeM.MerkleRoot(ctx, f.groupID)
	if err != nil {
		t.Fatalf("peer MerkleRoot: %v", err)
	}
	tr := &scriptedTransport{handlers: []func(net.Conn){
		respondAfterRequest(&wire.RangeResp{
			GroupID:         f.groupID,
			Generation:      8,
			SnapshotChanged: true,
		}),
		respondAfterRequest(&wire.RangeResp{
			GroupID:    f.groupID,
			IDs:        []entmoot.MessageID{msg.ID},
			Generation: 8,
		}),
	}}
	g := f.nodes[10].gossip
	g.cfg.Transport = tr

	if ok := g.fetchFullRangeFallback(ctx, 20, wire.MerkleRoot(peerRoot), 0); !ok {
		t.Fatal("fallback did not recover from changed snapshot")
	}
	dials, _ := tr.counts()
	if dials != 2 {
		t.Fatalf("range dials = %d, want 2 after one restart", dials)
	}
}
