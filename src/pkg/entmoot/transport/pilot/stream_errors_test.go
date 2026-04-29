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
