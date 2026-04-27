package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"entmoot/pkg/entmoot/transport/pilot"
)

type closeablePilot interface {
	Close() error
}

type pilotWaitOptions struct {
	Timeout   time.Duration
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Logger    *slog.Logger

	Now    func() time.Time
	Sleep  func(context.Context, time.Duration) error
	Jitter func(time.Duration) time.Duration
}

func openPilotForJoin(gf *globalFlags) (*pilot.Transport, error) {
	opts := pilotWaitOptions{
		Timeout:   gf.pilotWaitTimeout,
		BaseDelay: gf.pilotWaitBaseDelay,
		MaxDelay:  gf.pilotWaitMaxDelay,
		Logger:    slog.Default(),
	}
	return openPilotWithRetry(context.Background(), opts, func() (*pilot.Transport, error) {
		return openPilot(gf)
	})
}

func openPilotWithRetry[T closeablePilot](parent context.Context, opts pilotWaitOptions, open func() (T, error)) (T, error) {
	var zero T
	if open == nil {
		return zero, errors.New("pilot wait: nil opener")
	}
	if opts.Timeout <= 0 {
		return open()
	}
	if opts.BaseDelay <= 0 {
		opts.BaseDelay = 250 * time.Millisecond
	}
	if opts.MaxDelay <= 0 {
		opts.MaxDelay = opts.BaseDelay
	}
	if opts.MaxDelay < opts.BaseDelay {
		opts.MaxDelay = opts.BaseDelay
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepContext
	}
	if opts.Jitter == nil {
		opts.Jitter = defaultPilotWaitJitter
	}

	start := opts.Now()
	deadline := start.Add(opts.Timeout)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()

	delay := opts.BaseDelay
	var lastErr error
	for attempt := 1; ; attempt++ {
		if attempt > 1 && !opts.Now().Before(deadline) {
			break
		}
		tr, err := open()
		if err == nil {
			opts.Logger.Info("join: pilot ready",
				slog.Int("attempt", attempt),
				slog.Duration("waited", opts.Now().Sub(start)))
			return tr, nil
		}
		lastErr = err
		if !opts.Now().Before(deadline) {
			break
		}

		sleepFor := opts.Jitter(delay)
		if sleepFor < 0 {
			sleepFor = 0
		}
		if remaining := deadline.Sub(opts.Now()); remaining > 0 && sleepFor > remaining {
			sleepFor = remaining
		}
		opts.Logger.Debug("join: waiting for pilot",
			slog.Int("attempt", attempt),
			slog.String("err", err.Error()),
			slog.Duration("next_delay", sleepFor))
		if err := opts.Sleep(ctx, sleepFor); err != nil {
			if lastErr != nil {
				return zero, fmt.Errorf("pilot not ready after %s: last error: %w", opts.Now().Sub(start), lastErr)
			}
			return zero, err
		}
		delay = nextPilotWaitDelay(delay, opts.MaxDelay)
	}

	return zero, fmt.Errorf("pilot not ready after %s: last error: %w", opts.Timeout, lastErr)
}

func nextPilotWaitDelay(current, max time.Duration) time.Duration {
	if current <= 0 {
		current = 250 * time.Millisecond
	}
	if max <= 0 {
		return current
	}
	if current >= max {
		return max
	}
	next := current * 2
	if next < current || next > max {
		return max
	}
	return next
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func defaultPilotWaitJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return max / 2
	}
	return time.Duration(n.Int64())
}
