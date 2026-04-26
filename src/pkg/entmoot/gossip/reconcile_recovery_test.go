package gossip

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/wire"
)

type scriptedTransport struct {
	mu        sync.Mutex
	handlers  []func(net.Conn)
	dials     int
	drops     int
	dialDelay time.Duration
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

	got, ok := g.fetchPeerRoot(context.Background(), 20)
	if !ok {
		t.Fatalf("fetchPeerRoot failed after retry")
	}
	if got != root {
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

func TestFullRangeFallbackFetchesMissingIDs(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx := context.Background()
	msg := f.buildMessage(20, "peer-only", 2_000)
	if err := f.nodes[20].storeM.Put(ctx, msg); err != nil {
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

	if ok := aG.fetchFullRangeFallback(ctx, 20, peerRoot); !ok {
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
