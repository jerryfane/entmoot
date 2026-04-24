package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubFetcher is a turnEndpointFetcher whose return values can be
// rewritten between calls. It also counts how many times it was
// called so tests can assert poll cadence without timing
// flakiness.
type stubFetcher struct {
	mu      sync.Mutex
	current string
	err     error
	calls   atomic.Int32
}

func newStubFetcher(initial string) *stubFetcher {
	return &stubFetcher{current: initial}
}

func (s *stubFetcher) set(v string) {
	s.mu.Lock()
	s.current = v
	s.mu.Unlock()
}

func (s *stubFetcher) setErr(e error) {
	s.mu.Lock()
	s.err = e
	s.mu.Unlock()
}

func (s *stubFetcher) fetch(_ context.Context) (string, error) {
	s.calls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return "", s.err
	}
	return s.current, nil
}

// TestTURNEndpointPoller_DetectsRotationAndSignals locks the core
// contract: when pilot-daemon's TURN endpoint changes between
// polls, the poller updates CurrentTURN and sends a tick on
// Changed.
func TestTURNEndpointPoller_DetectsRotationAndSignals(t *testing.T) {
	stub := newStubFetcher("104.30.149.4:9587")
	p := newTURNEndpointPollerFromFetcher(stub.fetch, 10*time.Millisecond)

	// First poll seeds the value.
	if changed := p.pollOnce(context.Background()); !changed {
		t.Fatalf("first poll should report change (empty -> initial value)")
	}
	if got := p.CurrentTURN(); got != "104.30.149.4:9587" {
		t.Fatalf("CurrentTURN after first poll = %q; want initial", got)
	}
	// Drain the change signal.
	select {
	case <-p.Changed():
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("first poll should signal Changed")
	}

	// Same value again — no change, no signal.
	if changed := p.pollOnce(context.Background()); changed {
		t.Fatalf("second poll with same value should NOT report change")
	}
	select {
	case <-p.Changed():
		t.Fatalf("Changed must not signal when TURN endpoint unchanged")
	case <-time.After(20 * time.Millisecond):
	}

	// Rotation: new port. Must update + signal.
	stub.set("104.30.149.4:20414")
	if changed := p.pollOnce(context.Background()); !changed {
		t.Fatalf("rotation poll should report change")
	}
	if got := p.CurrentTURN(); got != "104.30.149.4:20414" {
		t.Fatalf("CurrentTURN after rotation = %q; want :20414", got)
	}
	select {
	case <-p.Changed():
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("rotation poll should signal Changed")
	}
}

// TestTURNEndpointPoller_ErrorPreservesPreviousValue ensures a
// transient pilot IPC failure does NOT clobber the cached TURN
// addr. Otherwise a single hiccup would withdraw the advertisement
// and force every peer through the empty-ad branch.
func TestTURNEndpointPoller_ErrorPreservesPreviousValue(t *testing.T) {
	stub := newStubFetcher("104.30.149.4:9587")
	p := newTURNEndpointPollerFromFetcher(stub.fetch, 10*time.Millisecond)
	p.pollOnce(context.Background())
	<-p.Changed()
	if got := p.CurrentTURN(); got != "104.30.149.4:9587" {
		t.Fatalf("seed CurrentTURN = %q; want initial", got)
	}

	stub.setErr(errors.New("EOF reading from pilot socket"))
	if changed := p.pollOnce(context.Background()); changed {
		t.Fatalf("pollOnce on fetch error must not report change")
	}
	if got := p.CurrentTURN(); got != "104.30.149.4:9587" {
		t.Fatalf("CurrentTURN after fetch error = %q; want preserved %q "+
			"(transient IPC errors must not withdraw the advertisement)",
			got, "104.30.149.4:9587")
	}
	select {
	case <-p.Changed():
		t.Fatalf("Changed must not signal on fetch error")
	case <-time.After(20 * time.Millisecond):
	}
}

// TestTURNEndpointPoller_SignalCoalesces verifies the buffered-1
// channel collapses bursts: if two rotations happen back-to-back
// before the consumer reads, the consumer sees one tick (and the
// second poll fills any drained slot). The advertiser loop reads
// LocalEndpoints on each tick anyway, so collapsing bursts is the
// intended behavior — never lose state, never spam.
func TestTURNEndpointPoller_SignalCoalesces(t *testing.T) {
	stub := newStubFetcher("a:1")
	p := newTURNEndpointPollerFromFetcher(stub.fetch, 10*time.Millisecond)

	// Two rotations without consuming Changed in between.
	p.pollOnce(context.Background())
	stub.set("b:2")
	p.pollOnce(context.Background())
	stub.set("c:3")
	p.pollOnce(context.Background())

	// First read drains the (single) buffered tick.
	select {
	case <-p.Changed():
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("expected at least one Changed tick after rotations")
	}
	// Second read should NOT block forever — but we also don't need
	// another tick. The contract: at least one signal per uninterrupted
	// burst; the loop re-reads LocalEndpoints freshly anyway.
	select {
	case <-p.Changed():
		// Acceptable: a later poll re-signalled after the buffer drained.
	case <-time.After(20 * time.Millisecond):
		// Also acceptable: bursts collapsed into the first tick.
	}

	if got := p.CurrentTURN(); got != "c:3" {
		t.Fatalf("CurrentTURN after burst = %q; want c:3 (last value wins)", got)
	}
}

// TestTURNEndpointPoller_RunStopsOnContextCancel: closing the
// supplied context must stop the polling goroutine. Otherwise a
// shutdown would leave the goroutine spinning and holding the
// pilot IPC connection open.
func TestTURNEndpointPoller_RunStopsOnContextCancel(t *testing.T) {
	stub := newStubFetcher("a:1")
	p := newTURNEndpointPollerFromFetcher(stub.fetch, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	// Let it tick a few times so we know it's running.
	time.Sleep(30 * time.Millisecond)
	if got := stub.calls.Load(); got < 2 {
		t.Fatalf("Run should have polled at least twice in 30 ms; got %d calls", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Run did not return within 200 ms of context cancel")
	}
}
