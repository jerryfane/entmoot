package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type fakePilotHandle struct{}

func (fakePilotHandle) Close() error { return nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestOpenPilotWithRetrySucceedsAfterTransientFailures(t *testing.T) {
	now := time.Unix(100, 0)
	var attempts int
	var slept []time.Duration

	got, err := openPilotWithRetry(context.Background(), pilotWaitOptions{
		Timeout:   10 * time.Second,
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  500 * time.Millisecond,
		Logger:    discardLogger(),
		Now:       func() time.Time { return now },
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			now = now.Add(d)
			return nil
		},
		Jitter: func(d time.Duration) time.Duration { return d },
	}, func() (fakePilotHandle, error) {
		attempts++
		if attempts < 3 {
			return fakePilotHandle{}, errors.New("pilot not listening yet")
		}
		return fakePilotHandle{}, nil
	})
	if err != nil {
		t.Fatalf("openPilotWithRetry returned error: %v", err)
	}
	if got != (fakePilotHandle{}) {
		t.Fatalf("unexpected handle: %#v", got)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	wantSleeps := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	if len(slept) != len(wantSleeps) {
		t.Fatalf("slept = %v, want %v", slept, wantSleeps)
	}
	for i := range slept {
		if slept[i] != wantSleeps[i] {
			t.Fatalf("slept[%d] = %s, want %s", i, slept[i], wantSleeps[i])
		}
	}
}

func TestOpenPilotWithRetryZeroTimeoutSingleAttempt(t *testing.T) {
	var attempts int
	_, err := openPilotWithRetry(context.Background(), pilotWaitOptions{
		Timeout: 0,
		Logger:  discardLogger(),
	}, func() (fakePilotHandle, error) {
		attempts++
		return fakePilotHandle{}, errors.New("pilot unavailable")
	})
	if err == nil {
		t.Fatal("openPilotWithRetry returned nil error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestOpenPilotWithRetryTimesOutWithLastError(t *testing.T) {
	now := time.Unix(100, 0)
	var attempts int

	_, err := openPilotWithRetry(context.Background(), pilotWaitOptions{
		Timeout:   250 * time.Millisecond,
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  100 * time.Millisecond,
		Logger:    discardLogger(),
		Now:       func() time.Time { return now },
		Sleep: func(_ context.Context, d time.Duration) error {
			now = now.Add(d)
			return nil
		},
		Jitter: func(d time.Duration) time.Duration { return d },
	}, func() (fakePilotHandle, error) {
		attempts++
		return fakePilotHandle{}, errors.New("listen refused")
	})
	if err == nil {
		t.Fatal("openPilotWithRetry returned nil error")
	}
	if !strings.Contains(err.Error(), "pilot not ready after 250ms") ||
		!strings.Contains(err.Error(), "listen refused") {
		t.Fatalf("error = %q, want timeout and last error", err.Error())
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestNextPilotWaitDelayCapsAtMax(t *testing.T) {
	if got := nextPilotWaitDelay(100*time.Millisecond, time.Second); got != 200*time.Millisecond {
		t.Fatalf("delay = %s, want 200ms", got)
	}
	if got := nextPilotWaitDelay(800*time.Millisecond, time.Second); got != time.Second {
		t.Fatalf("delay = %s, want 1s", got)
	}
	if got := nextPilotWaitDelay(time.Second, time.Second); got != time.Second {
		t.Fatalf("delay = %s, want 1s", got)
	}
}
