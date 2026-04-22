package gossip

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

// TestMemTransport_OnTunnelUpFiresBothSides asserts that SetOnTunnelUp
// installs a callback that fires on both the dialer and acceptor sides
// of a memTransport pair once the net.Pipe is wired. Each side's
// callback must see the OTHER peer's NodeID and fire exactly once for
// a single Dial/Accept pair.
//
// Callback dispatch is async (fireOnTunnelUp spawns its own goroutine
// so the hot path stays non-blocking), so the test polls up to 1 s
// before failing.
func TestMemTransport_OnTunnelUpFiresBothSides(t *testing.T) {
	t.Parallel()
	const (
		aID entmoot.NodeID = 101
		bID entmoot.NodeID = 202
	)
	trs := NewMemTransports([]entmoot.NodeID{aID, bID})
	a := trs[aID]
	b := trs[bID]
	defer a.Close()
	defer b.Close()

	var (
		aCount, bCount atomic.Int32
		aPeer, bPeer   atomic.Uint32
	)
	a.SetOnTunnelUp(func(p entmoot.NodeID) {
		aCount.Add(1)
		aPeer.Store(uint32(p))
	})
	b.SetOnTunnelUp(func(p entmoot.NodeID) {
		bCount.Add(1)
		bPeer.Store(uint32(p))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Accept runs in a goroutine; the Dial below feeds it.
	acceptErr := make(chan error, 1)
	go func() {
		_, remote, err := b.Accept(ctx)
		if err != nil {
			acceptErr <- err
			return
		}
		if remote != aID {
			acceptErr <- &unexpectedRemoteErr{got: remote, want: aID}
			return
		}
		acceptErr <- nil
	}()

	conn, err := a.Dial(ctx, bID)
	if err != nil {
		t.Fatalf("a.Dial(b): %v", err)
	}
	defer conn.Close()

	if err := <-acceptErr; err != nil {
		t.Fatalf("b.Accept: %v", err)
	}

	// Poll for the async callback deliveries.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if aCount.Load() == 1 && bCount.Load() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := aCount.Load(); got != 1 {
		t.Fatalf("aCount = %d, want 1", got)
	}
	if got := bCount.Load(); got != 1 {
		t.Fatalf("bCount = %d, want 1", got)
	}
	if got := entmoot.NodeID(aPeer.Load()); got != bID {
		t.Fatalf("a callback saw peer=%d, want %d", got, bID)
	}
	if got := entmoot.NodeID(bPeer.Load()); got != aID {
		t.Fatalf("b callback saw peer=%d, want %d", got, aID)
	}
}

// TestMemTransport_SetOnTunnelUpNilClears asserts that passing nil to
// SetOnTunnelUp clears a previously-installed callback: subsequent
// Dial/Accept pairs must not invoke the old callback.
func TestMemTransport_SetOnTunnelUpNilClears(t *testing.T) {
	t.Parallel()
	const (
		aID entmoot.NodeID = 11
		bID entmoot.NodeID = 22
	)
	trs := NewMemTransports([]entmoot.NodeID{aID, bID})
	a := trs[aID]
	b := trs[bID]
	defer a.Close()
	defer b.Close()

	var fired atomic.Int32
	a.SetOnTunnelUp(func(entmoot.NodeID) { fired.Add(1) })
	a.SetOnTunnelUp(nil) // clear
	b.SetOnTunnelUp(nil) // never set, but clearing should be safe

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_, _, _ = b.Accept(ctx)
	}()
	conn, err := a.Dial(ctx, bID)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Give any stray goroutine a chance to run, then assert.
	time.Sleep(150 * time.Millisecond)
	if got := fired.Load(); got != 0 {
		t.Fatalf("cleared callback fired %d times, want 0", got)
	}
}

type unexpectedRemoteErr struct {
	got, want entmoot.NodeID
}

func (e *unexpectedRemoteErr) Error() string {
	return "unexpected remote NodeID"
}
