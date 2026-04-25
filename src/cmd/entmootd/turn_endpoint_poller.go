package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

// turnEndpointPollInterval is the legacy 30-second polling cadence
// retained as a fallback when pilot < v1.9.0-jf.11b doesn't support
// the Subscribe / Notify push primitives. Modern pilots subscribe
// once at startup and receive zero polling traffic — see Run().
const turnEndpointPollInterval = 30 * time.Second

// turnEndpointPollTimeout bounds a single pilot Info reply (legacy
// polling path only). Sized at half the interval so a stuck IPC
// can't cause pollOnce to outlive its window. 15 s is generous for
// low-power ARM boxes; tuned in v1.4.5 after phobos hit a 100%
// failure rate at the original 3 s.
const turnEndpointPollTimeout = 15 * time.Second

// turnEndpointFetcher returns the current TURN relay address as
// reported by the local pilot-daemon. Abstracted as a function so
// tests can inject a stub without a real daemon connection. Used by
// the legacy polling fallback in Run().
type turnEndpointFetcher func(ctx context.Context) (string, error)

// turnEndpointPoller observes pilot's TURN relay address and signals
// the gossip advertiser whenever the value changes.
//
// Modes (chosen at Run time):
//
//   - **Push (preferred)**: when drv is non-nil and Pilot >= jf.11b
//     supports Subscribe, we issue one Subscribe at startup and
//     react to opNotify frames. Zero polling traffic, immediate
//     detection of rotation events.
//
//   - **Polling fallback**: when drv is nil (test injection) OR
//     Pilot < jf.11b returns ErrSubscribeUnsupported, we fall back
//     to the v1.4.x polling loop (Info every 30 s, 15 s timeout).
//
// External API is unchanged from v1.4.x: CurrentTURN, Changed,
// Run. (v1.5.0)
type turnEndpointPoller struct {
	drv      *ipcclient.Driver  // nil only in tests via the fetcher constructor
	fetch    turnEndpointFetcher // legacy polling path
	interval time.Duration

	mu       sync.Mutex
	turnAddr string // last-observed TURN relay address

	// changed receives a non-blocking tick on every rotation.
	// Buffered 1; bursts are collapsed because the advertiser
	// re-reads LocalEndpoints freshly on each tick.
	changed chan struct{}
}

func newTURNEndpointPoller(d *ipcclient.Driver, interval time.Duration) *turnEndpointPoller {
	p := &turnEndpointPoller{
		drv:      d,
		interval: interval,
		changed:  make(chan struct{}, 1),
	}
	// Legacy fetcher reused by the polling-fallback path. Wraps the
	// same Driver.InfoStruct call the v1.4.x poller used.
	p.fetch = func(ctx context.Context) (string, error) {
		info, err := d.InfoStruct(ctx)
		if err != nil {
			return "", err
		}
		return info.TURNEndpoint, nil
	}
	return p
}

// newTURNEndpointPollerFromFetcher is the test-injection constructor.
// drv is left nil so Run() takes the legacy polling path directly,
// without attempting Subscribe — keeps the existing pollOnce-based
// stub-fetcher tests working unchanged.
func newTURNEndpointPollerFromFetcher(f turnEndpointFetcher, interval time.Duration) *turnEndpointPoller {
	return &turnEndpointPoller{
		fetch:    f,
		interval: interval,
		changed:  make(chan struct{}, 1),
	}
}

// CurrentTURN returns the last-observed TURN relay address. Empty if
// no value has been observed yet (initial cold start, or pilot has
// no TURN configured).
func (p *turnEndpointPoller) CurrentTURN() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.turnAddr
}

// Changed returns a receive-only channel that gets a tick whenever
// CurrentTURN's value changes. Pass to gossip.Config.EndpointsChanged.
func (p *turnEndpointPoller) Changed() <-chan struct{} {
	return p.changed
}

// applyTURN updates the cached value if it differs from the previous
// one and signals Changed. Returns true on actual change. Used by
// both the Subscribe path and the polling fallback.
func (p *turnEndpointPoller) applyTURN(newAddr string) bool {
	p.mu.Lock()
	prev := p.turnAddr
	p.turnAddr = newAddr
	p.mu.Unlock()
	if prev == newAddr {
		return false
	}
	slog.Info("turn-endpoint: pilot TURN relay rotated",
		slog.String("old", prev),
		slog.String("new", newAddr))
	select {
	case p.changed <- struct{}{}:
	default:
		// Buffer full — advertiser will pick up the latest
		// value next time it reads LocalEndpoints. Collapsing
		// bursts is the documented behaviour.
	}
	return true
}

// pollOnce queries pilot Info via the legacy fetcher and updates the
// cached TURN value. Retained so the v1.4.x stub-fetcher tests keep
// working. Errors log at Warn (rotation detection is offline for
// this cycle); the previous value is preserved so a transient IPC
// hiccup doesn't withdraw the advertisement.
func (p *turnEndpointPoller) pollOnce(ctx context.Context) bool {
	qctx, cancel := context.WithTimeout(ctx, turnEndpointPollTimeout)
	defer cancel()
	turn, err := p.fetch(qctx)
	if err != nil {
		slog.Warn("turn-endpoint poll: pilot Info query failed; "+
			"TURN rotation detection paused for this cycle",
			slog.String("err", err.Error()))
		return false
	}
	return p.applyTURN(turn)
}

// Run drives the poller for the lifetime of ctx. It first tries the
// Subscribe (push) path; if pilot doesn't support it, falls back to
// the polling loop.
//
// The Subscribe path: one Subscribe at startup → SubscribeOK delivers
// the current value → demuxer routes Notify frames into the
// Subscription's channel → applyTURN signals Changed on each
// distinct value. Zero polling traffic during steady state.
//
// The polling path: legacy v1.4.x behaviour (turnEndpointPollInterval
// ticker calling pollOnce). Activated only against pre-jf.11b
// pilots OR test fetchers (drv == nil).
func (p *turnEndpointPoller) Run(ctx context.Context) {
	if p.drv != nil {
		if p.runSubscribe(ctx) {
			return
		}
		// Subscribe unsupported / failed — fall through to polling.
	}
	p.runLegacyPolling(ctx)
}

// runSubscribe attempts the push path. Returns true if Subscribe
// succeeded and ran to completion (ctx cancelled or channel closed);
// returns false when Subscribe is unsupported by the connected pilot
// so the caller can fall back to polling.
func (p *turnEndpointPoller) runSubscribe(ctx context.Context) bool {
	subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
	initial, sub, err := p.drv.Subscribe(subCtx, "turn_endpoint")
	subCancel()
	if err != nil {
		if errors.Is(err, ipcclient.ErrSubscribeUnsupported) {
			slog.Info("turn-endpoint subscribe unsupported by " +
				"connected pilot; falling back to 30s polling " +
				"(upgrade pilot to v1.9.0-jf.11b for push notifications)")
			return false
		}
		slog.Warn("turn-endpoint subscribe failed; falling back to polling",
			slog.String("err", err.Error()))
		return false
	}
	defer sub.Close()

	slog.Info("turn-endpoint subscribed",
		slog.String("initial", string(initial)))
	p.applyTURN(string(initial))

	for {
		select {
		case <-ctx.Done():
			return true
		case n, ok := <-sub.Events():
			if !ok {
				// Channel closed — driver shutting down. Returning
				// true so the caller doesn't fall back to polling
				// against a closing driver.
				return true
			}
			p.applyTURN(string(n.Payload))
		}
	}
}

// runLegacyPolling is the v1.4.x polling loop. Used as a fallback
// against pre-jf.11b pilots and as the primary path in test
// fetchers.
func (p *turnEndpointPoller) runLegacyPolling(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}
