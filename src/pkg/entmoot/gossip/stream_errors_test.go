package gossip

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return false }

func TestGenericStreamErrorClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want StreamErrorClassification
	}{
		{
			name: "eof",
			err:  io.EOF,
			want: StreamErrorClassification{Retryable: true, StaleSession: true},
		},
		{
			name: "closed pipe",
			err:  io.ErrClosedPipe,
			want: StreamErrorClassification{Retryable: true, StaleSession: true},
		},
		{
			name: "net closed",
			err:  net.ErrClosed,
			want: StreamErrorClassification{Retryable: true, StaleSession: true},
		},
		{
			name: "deadline",
			err:  context.DeadlineExceeded,
			want: StreamErrorClassification{Retryable: true, Timeout: true},
		},
		{
			name: "net timeout",
			err:  timeoutErr{},
			want: StreamErrorClassification{Retryable: true, Timeout: true},
		},
		{
			name: "other",
			err:  errors.New("remote rejected request"),
			want: StreamErrorClassification{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := classifyGenericStreamError(context.Background(), tt.err)
			if got != tt.want {
				t.Fatalf("classification = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestGenericStreamErrorLocalDeadline(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	got := classifyGenericStreamError(ctx, context.DeadlineExceeded)
	want := StreamErrorClassification{Retryable: true, Timeout: true, LocalContext: true}
	if got != want {
		t.Fatalf("classification = %+v, want %+v", got, want)
	}
}

func TestStreamErrorDecisionHelpers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	retryable := StreamErrorClassification{Retryable: true}
	stale := StreamErrorClassification{Retryable: true, StaleSession: true}
	local := StreamErrorClassification{LocalContext: true}

	if !shouldRetryStreamFailure(retryable, 0, ctx, 0) {
		t.Fatal("retryable first attempt did not retry")
	}
	if shouldRetryStreamFailure(retryable, 1, ctx, 0) {
		t.Fatal("retryable second attempt retried")
	}
	if !shouldDropPeerSessionAfterStreamFailure(stale, 0) {
		t.Fatal("stale stream did not request session drop")
	}
	if shouldRecordDialFailure(local) {
		t.Fatal("local context would record dial failure")
	}
	if shouldRecordDialFailure(stale) {
		t.Fatal("stale session would record dial failure")
	}
}
