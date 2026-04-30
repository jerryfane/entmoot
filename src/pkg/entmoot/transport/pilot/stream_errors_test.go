package pilot

import (
	"errors"
	"fmt"
	"testing"

	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

func TestClassifyStreamErrorPilotStaleSession(t *testing.T) {
	t.Parallel()

	tests := []error{
		ipcclient.ErrClosed,
		ipcclient.ErrConnectionNotFound,
		fmt.Errorf("wrapped: %w", ipcclient.ErrConnectionNotEstablished),
		fmt.Errorf("wrapped: %w", ipcclient.ErrConnectionClosing),
		fmt.Errorf("wrapped: %w", ipcclient.ErrSendFailed),
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

func TestClassifyStreamErrorPilotUnknown(t *testing.T) {
	t.Parallel()

	got := ClassifyStreamError(errors.New("application rejected frame"))
	if got.Retryable || got.StaleSession || got.Timeout || got.LocalContext {
		t.Fatalf("classification = %+v, want zero", got)
	}
}

func TestClassifyStreamErrorPilotDialTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		localContext bool
	}{
		{
			name:         "limiter",
			err:          fmt.Errorf("wrapped: %w", ErrDialLimiterTimeout),
			localContext: true,
		},
		{
			name:         "caller",
			err:          fmt.Errorf("wrapped: %w", ErrDialCallerTimeout),
			localContext: true,
		},
		{
			name: "daemon",
			err:  fmt.Errorf("wrapped: %w", ErrDaemonDialTimeout),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ClassifyStreamError(tt.err)
			if !got.Retryable || !got.Timeout {
				t.Fatalf("classification = %+v, want retryable timeout", got)
			}
			if got.LocalContext != tt.localContext {
				t.Fatalf("LocalContext = %v, want %v", got.LocalContext, tt.localContext)
			}
			if got.StaleSession {
				t.Fatalf("classification = %+v, stale-session should be false", got)
			}
		})
	}
}
