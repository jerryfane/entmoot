package gossip

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/wire"
)

type scriptedTransport struct {
	mu       sync.Mutex
	handlers []func(net.Conn)
	dials    int
	drops    int
}

func (t *scriptedTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
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
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		clk:          clock.System{},
		dialBackoffs: make(map[entmoot.NodeID]*peerDialState),
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
