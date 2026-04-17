package ratelimit_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/ratelimit"

	"golang.org/x/time/rate"
)

// anchor is the reference time used by every test that wires a clock.Fake.
var anchor = time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)

// testLimits is the default v0 shape but with the clock injected via a Fake
// from the caller.
func testLimits() ratelimit.Limits { return ratelimit.DefaultLimits() }

func TestDefaultLimits_Values(t *testing.T) {
	l := ratelimit.DefaultLimits()
	if l.MsgRate != 100 {
		t.Errorf("MsgRate: got %v, want 100", l.MsgRate)
	}
	if l.MsgBurst != 200 {
		t.Errorf("MsgBurst: got %d, want 200", l.MsgBurst)
	}
	if l.BytesRate != 1<<20 {
		t.Errorf("BytesRate: got %v, want 1 MiB/s", l.BytesRate)
	}
	if l.BytesBurst != 4<<20 {
		t.Errorf("BytesBurst: got %d, want 4 MiB", l.BytesBurst)
	}
}

func TestAllow_MessageBurstThenRejectsUntilRefill(t *testing.T) {
	fk := clock.NewFake(anchor)
	// Disable the byte bucket so this test isolates the message bucket.
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:   100,
		MsgBurst:  200,
		BytesRate: 0,
	}, fk)

	peer := entmoot.NodeID(42)

	// Drain the burst of 200.
	for i := 0; i < 200; i++ {
		if err := lim.Allow(peer, 0); err != nil {
			t.Fatalf("message %d: unexpected rate-limit: %v", i, err)
		}
	}

	// 201st must reject without any time advance.
	if err := lim.Allow(peer, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited after burst, got %v", err)
	}

	// Advance 1 s: at 100/s the bucket refills by 100 tokens; another 100
	// messages should pass.
	fk.Advance(1 * time.Second)
	for i := 0; i < 100; i++ {
		if err := lim.Allow(peer, 0); err != nil {
			t.Fatalf("refill message %d: unexpected rate-limit: %v", i, err)
		}
	}
	if err := lim.Allow(peer, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited after refill drained, got %v", err)
	}
}

func TestAllow_ByteBurstAcceptsFourMiBRejectsFiveMiB(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)
	peer := entmoot.NodeID(7)

	// 4 MiB fits the burst exactly.
	if err := lim.Allow(peer, 4<<20); err != nil {
		t.Fatalf("4 MiB payload: unexpected rate-limit: %v", err)
	}

	// Reset peer so the next call is independent of remaining msg tokens.
	lim.Reset(peer)

	// 5 MiB exceeds the 4 MiB burst in a single call; must reject.
	if err := lim.Allow(peer, 5<<20); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("5 MiB payload: expected ErrRateLimited, got %v", err)
	}
}

func TestAllow_RejectionDoesNotBurnMsgToken(t *testing.T) {
	// This test pins down the "both-or-neither" commit contract: when the
	// byte bucket rejects, the message bucket must not have consumed a
	// token. We drain 199 msg tokens, then submit an oversized payload; a
	// final small Allow should still succeed (i.e. we still had 1 of 200).
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)
	peer := entmoot.NodeID(11)

	// Use 199 cheap messages first.
	for i := 0; i < 199; i++ {
		if err := lim.Allow(peer, 0); err != nil {
			t.Fatalf("priming message %d: unexpected rate-limit: %v", i, err)
		}
	}

	// Oversized byte payload: must reject and NOT consume a msg token.
	if err := lim.Allow(peer, 5<<20); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("oversized payload: expected ErrRateLimited, got %v", err)
	}

	// One msg token should still be available.
	if err := lim.Allow(peer, 0); err != nil {
		t.Fatalf("post-reject msg: expected success (token not burned), got %v", err)
	}

	// Now the 200-token burst is truly exhausted.
	if err := lim.Allow(peer, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("final msg: expected ErrRateLimited, got %v", err)
	}
}

func TestAllow_ClockAdvanceRestoresTokens(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:    10,
		MsgBurst:   10,
		BytesRate:  1000,
		BytesBurst: 1000,
	}, fk)
	peer := entmoot.NodeID(99)

	// Drain msg burst of 10.
	for i := 0; i < 10; i++ {
		if err := lim.Allow(peer, 0); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
	if err := lim.Allow(peer, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited after drain, got %v", err)
	}

	// 1 s at 10/s refills the full burst.
	fk.Advance(1 * time.Second)
	for i := 0; i < 10; i++ {
		if err := lim.Allow(peer, 0); err != nil {
			t.Fatalf("refilled %d: %v", i, err)
		}
	}
	if err := lim.Allow(peer, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited after refilled drain, got %v", err)
	}
}

func TestAllow_ConcurrentSamePeerIsRaceSafe(t *testing.T) {
	// Run with -race; the invariant is that exactly MsgBurst calls succeed
	// and no data race is reported on the shared peer entry.
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:    1, // slow enough that the window sees no refill
		MsgBurst:   50,
		BytesRate:  0,
		BytesBurst: 0,
	}, fk)
	peer := entmoot.NodeID(123)

	const goroutines = 10
	const perGoroutine = 20 // total attempts = 200, of which 50 should pass

	var ok, bad int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perGoroutine; i++ {
				err := lim.Allow(peer, 0)
				if err == nil {
					atomic.AddInt64(&ok, 1)
				} else if errors.Is(err, entmoot.ErrRateLimited) {
					atomic.AddInt64(&bad, 1)
				} else {
					t.Errorf("unexpected err: %v", err)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt64(&ok); got != 50 {
		t.Errorf("ok count: got %d, want 50", got)
	}
	if got := atomic.LoadInt64(&bad); got != goroutines*perGoroutine-50 {
		t.Errorf("bad count: got %d, want %d", got, goroutines*perGoroutine-50)
	}
}

func TestReset_RestoresFullBurst(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)
	peer := entmoot.NodeID(5)

	// Drain the msg burst.
	for i := 0; i < 200; i++ {
		if err := lim.Allow(peer, 0); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
	if err := lim.Allow(peer, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited after drain, got %v", err)
	}

	// Reset: next Allow should get full burst again without advancing time.
	lim.Reset(peer)
	for i := 0; i < 200; i++ {
		if err := lim.Allow(peer, 0); err != nil {
			t.Fatalf("post-reset %d: %v", i, err)
		}
	}
	if err := lim.Allow(peer, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited post-reset-drain, got %v", err)
	}
}

func TestAllow_UnlimitedWhenRateZero(t *testing.T) {
	fk := clock.NewFake(anchor)
	// Both rates zero -> both buckets disabled.
	lim := ratelimit.New(ratelimit.Limits{}, fk)
	peer := entmoot.NodeID(3)

	for i := 0; i < 10_000; i++ {
		if err := lim.Allow(peer, 1<<30); err != nil {
			t.Fatalf("iter %d: unexpected rate-limit with zero rates: %v", i, err)
		}
	}
}

func TestAllow_UnlimitedSingleBucket(t *testing.T) {
	// Bytes unlimited, messages enforced: exhausting the msg bucket still
	// rejects even though huge payloads are allowed.
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:   rate.Limit(1),
		MsgBurst:  3,
		BytesRate: 0, // disabled
	}, fk)
	peer := entmoot.NodeID(17)

	for i := 0; i < 3; i++ {
		if err := lim.Allow(peer, 1<<30); err != nil {
			t.Fatalf("iter %d: unexpected rate-limit: %v", i, err)
		}
	}
	if err := lim.Allow(peer, 1); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("expected msg-bucket rejection, got %v", err)
	}
}

func TestAllow_UnknownPeerStartsWithFullBurst(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)

	// Peer A drains its burst.
	a := entmoot.NodeID(1)
	for i := 0; i < 200; i++ {
		if err := lim.Allow(a, 0); err != nil {
			t.Fatalf("A drain %d: %v", i, err)
		}
	}
	if err := lim.Allow(a, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("A post-drain: expected ErrRateLimited, got %v", err)
	}

	// Peer B, seen for the first time, must have a full burst independent
	// of A's state.
	b := entmoot.NodeID(2)
	for i := 0; i < 200; i++ {
		if err := lim.Allow(b, 0); err != nil {
			t.Fatalf("B %d: unexpected rate-limit for fresh peer: %v", i, err)
		}
	}
}

func TestNew_NilClockUsesSystem(t *testing.T) {
	// Smoke test: a nil clock should not panic and should still apply
	// limits. We can't usefully assert timing without racing real time,
	// so just confirm the burst is honored.
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:  1,
		MsgBurst: 2,
	}, nil)
	peer := entmoot.NodeID(1)
	if err := lim.Allow(peer, 0); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := lim.Allow(peer, 0); err != nil {
		t.Fatalf("second: %v", err)
	}
	if err := lim.Allow(peer, 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("third: expected ErrRateLimited, got %v", err)
	}
}
