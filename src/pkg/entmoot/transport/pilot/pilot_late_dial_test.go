package pilot

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

func TestGetOrCreateSessionKeepsInternalDialContextAfterCallerCancel(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	callerReturned := make(chan struct{})
	internalCtxErr := make(chan error, 1)
	tp := &Transport{
		cfg:    Config{ListenPort: 1004},
		closed: make(chan struct{}),
		limits: newPeerDialLimiter(pilotMaxConcurrentPeerDials),
		dialAddr: func(ctx context.Context, addr Addr, port uint16) (*ipcclient.Conn, error) {
			close(started)
			<-callerReturned
			internalCtxErr <- ctx.Err()
			return nil, errors.New("late dial result")
		},
	}

	callerCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, _, err := tp.dialPilotStream(callerCtx, 45981, nil)
		errCh <- err
	}()

	<-started
	cancel()
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
			t.Fatalf("dialPilotStream error = %v, want caller cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dialPilotStream did not return after caller cancellation")
	}

	close(callerReturned)
	select {
	case err := <-internalCtxErr:
		if err != nil {
			t.Fatalf("internal dial context was canceled by caller context: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dialAddr was not drained after caller cancellation")
	}
}

func TestTransportDialHoldsPeerLimiterSlotUntilLateDrainCompletes(t *testing.T) {
	t.Parallel()

	const peer = 45981
	started := make(chan struct{}, pilotMaxConcurrentPeerDials+4)
	releaseDrains := make(chan struct{})
	var activeDialAddr int64
	var maxActiveDialAddr int64

	tp := &Transport{
		cfg:    Config{ListenPort: 1004},
		closed: make(chan struct{}),
		limits: newPeerDialLimiter(pilotMaxConcurrentPeerDials),
		dialAddr: func(ctx context.Context, addr Addr, port uint16) (*ipcclient.Conn, error) {
			n := atomic.AddInt64(&activeDialAddr, 1)
			for {
				max := atomic.LoadInt64(&maxActiveDialAddr)
				if n <= max || atomic.CompareAndSwapInt64(&maxActiveDialAddr, max, n) {
					break
				}
			}
			defer atomic.AddInt64(&activeDialAddr, -1)

			started <- struct{}{}
			select {
			case <-releaseDrains:
				return nil, errors.New("late dial result")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}

	dialWithTimeout := func(timeout time.Duration) <-chan error {
		errCh := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		go func() {
			defer cancel()
			_, err := tp.Dial(ctx, peer)
			errCh <- err
		}()
		return errCh
	}

	first := make([]<-chan error, 0, pilotMaxConcurrentPeerDials)
	for i := 0; i < pilotMaxConcurrentPeerDials; i++ {
		first = append(first, dialWithTimeout(50*time.Millisecond))
	}
	for i := 0; i < pilotMaxConcurrentPeerDials; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("DialAddr call %d did not start", i+1)
		}
	}
	for i, errCh := range first {
		select {
		case err := <-errCh:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("initial Dial %d err = %v, want deadline exceeded", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("initial Dial %d did not return after caller deadline", i)
		}
	}

	const extraDials = pilotMaxConcurrentPeerDials + 4
	extra := make([]<-chan error, 0, extraDials)
	for i := 0; i < extraDials; i++ {
		extra = append(extra, dialWithTimeout(15*time.Millisecond))
	}
	for i, errCh := range extra {
		select {
		case err := <-errCh:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("extra Dial %d err = %v, want deadline exceeded", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("extra Dial %d did not return after caller deadline", i)
		}
	}

	select {
	case <-started:
		t.Fatalf("started more than %d DialAddr calls while late dials were draining", pilotMaxConcurrentPeerDials)
	default:
	}
	if got := atomic.LoadInt64(&maxActiveDialAddr); got > pilotMaxConcurrentPeerDials {
		t.Fatalf("max active DialAddr calls = %d, want <= %d", got, pilotMaxConcurrentPeerDials)
	}

	close(releaseDrains)
	deadline := time.After(time.Second)
	for atomic.LoadInt64(&activeDialAddr) != 0 {
		select {
		case <-deadline:
			t.Fatalf("late DialAddr drains still active: %d", atomic.LoadInt64(&activeDialAddr))
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestTransportDialBlockedOnPeerLimiterWakesOnClose(t *testing.T) {
	t.Parallel()

	const peer = 45981
	tp := &Transport{
		cfg:    Config{ListenPort: 1004},
		closed: make(chan struct{}),
		limits: newPeerDialLimiter(pilotMaxConcurrentPeerDials),
		dialAddr: func(context.Context, Addr, uint16) (*ipcclient.Conn, error) {
			t.Fatal("DialAddr should not run while peer limiter is saturated")
			return nil, nil
		},
	}
	neverClosed := make(chan struct{})
	for i := 0; i < pilotMaxConcurrentPeerDials; i++ {
		if err := tp.limits.acquire(context.Background(), peer, neverClosed); err != nil {
			t.Fatalf("pre-acquire slot %d: %v", i, err)
		}
	}
	defer func() {
		for i := 0; i < pilotMaxConcurrentPeerDials; i++ {
			tp.limits.release(peer)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := tp.Dial(context.Background(), peer)
		errCh <- err
	}()

	time.Sleep(25 * time.Millisecond)
	if err := tp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Dial err = %v, want net.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dial blocked on saturated limiter after Transport.Close")
	}
}
