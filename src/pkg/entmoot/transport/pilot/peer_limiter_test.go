package pilot

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

func TestPeerDialLimiterHoldsSlotUntilRelease(t *testing.T) {
	t.Parallel()

	const peer entmoot.NodeID = 45981
	limiter := newPeerDialLimiter(1)
	if err := limiter.acquire(context.Background(), peer, nil); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := limiter.acquire(ctx, peer, nil); err == nil {
		t.Fatal("second acquire succeeded before release")
	}

	limiter.release(peer)
	if err := limiter.acquire(context.Background(), peer, nil); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	limiter.release(peer)
}

func TestPeerDialLimiterIsPerPeer(t *testing.T) {
	t.Parallel()

	limiter := newPeerDialLimiter(1)
	if err := limiter.acquire(context.Background(), 1, nil); err != nil {
		t.Fatalf("first peer acquire: %v", err)
	}
	if err := limiter.acquire(context.Background(), 2, nil); err != nil {
		t.Fatalf("second peer acquire: %v", err)
	}
	limiter.release(1)
	limiter.release(2)
}

func TestPeerDialLimiterAcquireReturnsClosedWhenTransportCloses(t *testing.T) {
	t.Parallel()

	const peer entmoot.NodeID = 45982
	limiter := newPeerDialLimiter(1)
	if err := limiter.acquire(context.Background(), peer, nil); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer limiter.release(peer)

	closed := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- limiter.acquire(context.Background(), peer, closed)
	}()

	select {
	case err := <-errCh:
		t.Fatalf("acquire returned before transport close: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(closed)

	select {
	case err := <-errCh:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("acquire error = %v, want %v", err, net.ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after transport close")
	}
}
