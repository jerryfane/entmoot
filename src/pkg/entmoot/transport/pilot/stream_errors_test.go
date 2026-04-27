package pilot

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/yamux"

	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

func TestClassifyStreamErrorPilotStaleSession(t *testing.T) {
	t.Parallel()

	tests := []error{
		ipcclient.ErrConnectionNotFound,
		fmt.Errorf("wrapped: %w", ipcclient.ErrConnectionNotEstablished),
		fmt.Errorf("wrapped: %w", ipcclient.ErrConnectionClosing),
		errSessionNotActive,
		yamux.ErrSessionShutdown,
		yamux.ErrStreamClosed,
		yamux.ErrRemoteGoAway,
		yamux.ErrConnectionReset,
		yamux.ErrKeepAliveTimeout,
	}
	for _, err := range tests {
		err := err
		t.Run(err.Error(), func(t *testing.T) {
			t.Parallel()
			got := ClassifyStreamError(err)
			if !got.Retryable || !got.StaleSession {
				t.Fatalf("classification = %+v, want retryable stale-session", got)
			}
			if got.Timeout {
				t.Fatalf("classification = %+v, timeout should be false", got)
			}
		})
	}
}

func TestClassifyStreamErrorPilotTimeout(t *testing.T) {
	t.Parallel()

	for _, err := range []error{yamux.ErrTimeout, yamux.ErrConnectionWriteTimeout} {
		got := ClassifyStreamError(err)
		if !got.Retryable || !got.Timeout {
			t.Fatalf("%v classification = %+v, want retryable timeout", err, got)
		}
		if got.StaleSession {
			t.Fatalf("%v classification = %+v, stale-session should be false", err, got)
		}
	}
}

func TestClassifyStreamErrorPilotUnknown(t *testing.T) {
	t.Parallel()

	got := ClassifyStreamError(errors.New("application rejected frame"))
	if got.Retryable || got.StaleSession || got.Timeout || got.LocalContext {
		t.Fatalf("classification = %+v, want zero", got)
	}
}
