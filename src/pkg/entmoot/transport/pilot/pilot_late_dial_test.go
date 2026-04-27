package pilot

import (
	"context"
	"errors"
	"strings"
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
		cfg:      Config{ListenPort: 1004},
		closed:   make(chan struct{}),
		sessions: newSessionManager(nil, false),
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
		_, err := tp.getOrCreateSession(callerCtx, 45981)
		errCh <- err
	}()

	<-started
	cancel()
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
			t.Fatalf("getOrCreateSession error = %v, want caller cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("getOrCreateSession did not return after caller cancellation")
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
