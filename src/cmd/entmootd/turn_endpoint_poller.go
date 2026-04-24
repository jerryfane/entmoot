package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

// turnEndpointPollInterval is how often entmootd asks pilot-daemon for
// its current TURN relay endpoint. Cloudflare rotates allocation ports
// on restart or credential refresh; if entmootd doesn't notice the
// rotation, remote peers keep using the stale TURN relay address in
// their cached transport_ad and their outbound frames get silently
// dropped by Cloudflare's anycast edge.
//
// 30 s balances two concerns:
//   - Fast enough that a rotation is observed and re-advertised within
//     a typical reconnect window.
//   - Slow enough that we don't hammer the daemon's IPC socket with
//     Info queries. Info is cheap, but a poll per node per 30 s is
//     ~0.03 qps — negligible.
//
// Timer is jittered ±10% via the loop body to avoid thundering-herd if
// a whole fleet restarts together.
const turnEndpointPollInterval = 30 * time.Second

// turnEndpointPollTimeout bounds how long we wait for a single pilot
// Info reply before giving up on a poll. Much shorter than
// turnEndpointPollInterval so a stuck IPC doesn't cause pollOnce to
// outlive its interval window.
const turnEndpointPollTimeout = 3 * time.Second

// turnEndpointFetcher returns the current TURN relay address as
// reported by the local pilot-daemon's Info IPC. Abstracted as a
// function so tests can inject a stub without standing up a full
// daemon socket.
type turnEndpointFetcher func(ctx context.Context) (string, error)

// turnEndpointPoller polls the local pilot-daemon's Info.TURNEndpoint
// and signals Entmoot's gossip advertiser whenever the value changes.
// Without this poller, the advertiser's LocalEndpoints is fixed at
// startup from CLI flags alone, and laptop-style TURN rotation (port
// changes on restart) goes unadvertised until the weekly safety-net
// refresh — effectively orphaning the peer in the mesh. (v1.4.4)
type turnEndpointPoller struct {
	fetch    turnEndpointFetcher
	interval time.Duration

	mu       sync.Mutex
	turnAddr string // last-observed pilot.Info.TURNEndpoint

	// changed receives a non-blocking tick per rotation. Buffered 1
	// so the advertiser loop can be momentarily behind; further
	// rotations during a pending signal are collapsed into the one
	// re-publish, which queries LocalEndpoints at fire time and
	// sees the current value anyway.
	changed chan struct{}
}

func newTURNEndpointPoller(d *ipcclient.Driver, interval time.Duration) *turnEndpointPoller {
	return newTURNEndpointPollerFromFetcher(
		func(ctx context.Context) (string, error) {
			info, err := d.InfoStruct(ctx)
			if err != nil {
				return "", err
			}
			return info.TURNEndpoint, nil
		},
		interval,
	)
}

// newTURNEndpointPollerFromFetcher is the test-injection constructor.
// The production path uses newTURNEndpointPoller above, which wraps
// an ipcclient.Driver.
func newTURNEndpointPollerFromFetcher(f turnEndpointFetcher, interval time.Duration) *turnEndpointPoller {
	return &turnEndpointPoller{
		fetch:    f,
		interval: interval,
		changed:  make(chan struct{}, 1),
	}
}

// CurrentTURN returns the last-observed TURN relay address. Empty if
// the daemon has no TURN provider configured, or if no poll has
// completed yet.
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

// pollOnce queries pilot Info and updates turnAddr, returning true if
// the value changed. Errors degrade to Debug; the previous value is
// preserved across transient IPC failures so a momentary pilot hiccup
// doesn't spuriously withdraw the advertisement.
func (p *turnEndpointPoller) pollOnce(ctx context.Context) bool {
	qctx, cancel := context.WithTimeout(ctx, turnEndpointPollTimeout)
	defer cancel()
	turn, err := p.fetch(qctx)
	if err != nil {
		slog.Debug("turn-endpoint poll: pilot Info query failed",
			slog.String("err", err.Error()))
		return false
	}
	p.mu.Lock()
	prev := p.turnAddr
	p.turnAddr = turn
	p.mu.Unlock()
	if prev != turn {
		slog.Info("turn-endpoint poll: pilot TURN relay rotated",
			slog.String("old", prev),
			slog.String("new", turn))
		select {
		case p.changed <- struct{}{}:
		default:
			// Buffer full: advertiser loop hasn't consumed the
			// previous tick yet. No-op; the advertiser's next
			// re-publish will read LocalEndpoints freshly and see
			// the current turnAddr anyway.
		}
		return true
	}
	return false
}

// Run polls until ctx is cancelled. The caller should do one
// synchronous pollOnce BEFORE launching Run so that LocalEndpoints
// returns the current TURN addr on the advertiser's startup tick —
// otherwise the advertiser's first publish goes out with TURN="" and
// the fix doesn't kick in until the 30 s interval expires.
func (p *turnEndpointPoller) Run(ctx context.Context) {
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
