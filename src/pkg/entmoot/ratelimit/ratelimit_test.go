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

// TestDefaultTopicLimits_TransportV1 asserts the v1.2.0 default
// per-(peer, topic) limit for "_pilot/transport/v1" matches the plan:
// burst of 10, refill 10/hour (rate.Every(6*time.Minute)).
func TestDefaultTopicLimits_TransportV1(t *testing.T) {
	tl, ok := ratelimit.DefaultTopicLimits()["_pilot/transport/v1"]
	if !ok {
		t.Fatalf("DefaultTopicLimits missing _pilot/transport/v1 entry")
	}
	if tl.MsgBurst != 10 {
		t.Errorf("_pilot/transport/v1 MsgBurst = %d, want 10", tl.MsgBurst)
	}
	if tl.MsgRate != rate.Every(6*time.Minute) {
		t.Errorf("_pilot/transport/v1 MsgRate = %v, want rate.Every(6m)", tl.MsgRate)
	}
}

// TestAllowTopic_DrainThenReject bounds the per-(peer, topic) bucket:
// burst calls all pass, the burst+1 call rejects, a long clock advance
// refills fully, and the cycle repeats. Exercises the transport-ad
// bucket at its plan defaults.
func TestAllowTopic_DrainThenReject(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)
	peer := entmoot.NodeID(42)
	topic := "_pilot/transport/v1"

	// Drain the 10-token burst. The global per-peer bucket is far more
	// generous, so these always reach the topic check.
	for i := 0; i < 10; i++ {
		if err := lim.AllowTopic(peer, topic, 64); err != nil {
			t.Fatalf("drain %d: unexpected err: %v", i, err)
		}
	}
	if err := lim.AllowTopic(peer, topic, 64); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("post-drain: expected ErrRateLimited, got %v", err)
	}

	// Advance 1 hour -> at 10/hour the bucket refills to full.
	fk.Advance(1 * time.Hour)
	for i := 0; i < 10; i++ {
		if err := lim.AllowTopic(peer, topic, 64); err != nil {
			t.Fatalf("refilled %d: unexpected err: %v", i, err)
		}
	}
	if err := lim.AllowTopic(peer, topic, 64); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("after refill drain: expected ErrRateLimited, got %v", err)
	}
}

// TestAllowTopic_TopicBucketIndependentPerPeer verifies one abusive
// peer can drain its topic bucket without affecting another peer's
// quota.
func TestAllowTopic_TopicBucketIndependentPerPeer(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)
	a := entmoot.NodeID(1)
	b := entmoot.NodeID(2)
	topic := "_pilot/transport/v1"

	for i := 0; i < 10; i++ {
		if err := lim.AllowTopic(a, topic, 64); err != nil {
			t.Fatalf("A drain %d: %v", i, err)
		}
	}
	if err := lim.AllowTopic(a, topic, 64); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("A post-drain: expected ErrRateLimited, got %v", err)
	}
	// B is untouched — full burst available.
	for i := 0; i < 10; i++ {
		if err := lim.AllowTopic(b, topic, 64); err != nil {
			t.Fatalf("B independent %d: %v", i, err)
		}
	}
}

// TestAllowTopic_UnconfiguredTopicDelegatesToGlobal asserts that a
// topic with no explicit TopicLimit entry falls back to the global
// per-peer bucket; the topic-level bucket is a no-op.
func TestAllowTopic_UnconfiguredTopicDelegatesToGlobal(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:  1,
		MsgBurst: 3,
	}, fk)
	peer := entmoot.NodeID(7)

	// Burst of 3 — from the global bucket — then reject.
	for i := 0; i < 3; i++ {
		if err := lim.AllowTopic(peer, "chat/messages", 0); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
	}
	if err := lim.AllowTopic(peer, "chat/messages", 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited from global bucket, got %v", err)
	}
}

// TestAllowTopic_TopicRejectDoesNotBurnGlobalTokens asserts the
// both-or-neither contract: a topic-bucket rejection must leave the
// global per-peer message bucket untouched so legitimate non-topic
// traffic isn't starved by noisy system-topic spam.
func TestAllowTopic_TopicRejectDoesNotBurnGlobalTokens(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)
	peer := entmoot.NodeID(9)
	topic := "_pilot/transport/v1"

	// Drain the topic bucket.
	for i := 0; i < 10; i++ {
		if err := lim.AllowTopic(peer, topic, 64); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
	// Further topic calls reject.
	for i := 0; i < 50; i++ {
		if err := lim.AllowTopic(peer, topic, 64); !errors.Is(err, entmoot.ErrRateLimited) {
			t.Fatalf("iter %d: expected ErrRateLimited, got %v", i, err)
		}
	}
	// The 10 passing topic calls each also charged the global per-peer
	// bucket (by design — they're on top of the global quota), so 190
	// global tokens should remain. Topic-rejected calls MUST NOT have
	// burned additional global tokens — if they did, fewer than 190
	// calls would pass here.
	for i := 0; i < 190; i++ {
		if err := lim.Allow(peer, 0); err != nil {
			t.Fatalf("global iter %d: topic reject burned global tokens: %v", i, err)
		}
	}
}

// TestAllowTopic_GlobalRejectCancelsTopicReservation asserts the
// reverse: when the global per-peer bucket rejects, the per-(peer,
// topic) bucket must not be charged.
func TestAllowTopic_GlobalRejectCancelsTopicReservation(t *testing.T) {
	fk := clock.NewFake(anchor)
	// Tight global bucket, loose topic bucket so we can isolate the
	// cancel-on-global-reject path.
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:  1,
		MsgBurst: 1,
		TopicLimits: map[string]ratelimit.TopicLimit{
			"t": {MsgRate: 1000, MsgBurst: 1000},
		},
	}, fk)
	peer := entmoot.NodeID(11)

	if err := lim.AllowTopic(peer, "t", 0); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := lim.AllowTopic(peer, "t", 0); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("second (global bucket empty): expected ErrRateLimited, got %v", err)
	}
	// 999 topic tokens should still be available; assert that a single
	// clock advance (restoring 1 global token) lets the next call through.
	fk.Advance(1 * time.Second)
	if err := lim.AllowTopic(peer, "t", 0); err != nil {
		t.Fatalf("post-refill: topic bucket appears to have been charged anyway: %v", err)
	}
}

// TestAllowTopic_ResetDropsBuckets asserts that Reset(peer) drops both
// the global and per-topic state for peer.
func TestAllowTopic_ResetDropsBuckets(t *testing.T) {
	fk := clock.NewFake(anchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)
	peer := entmoot.NodeID(3)
	topic := "_pilot/transport/v1"

	for i := 0; i < 10; i++ {
		if err := lim.AllowTopic(peer, topic, 64); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
	if err := lim.AllowTopic(peer, topic, 64); !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("post-drain: expected ErrRateLimited, got %v", err)
	}
	lim.Reset(peer)
	// Fresh burst available again.
	for i := 0; i < 10; i++ {
		if err := lim.AllowTopic(peer, topic, 64); err != nil {
			t.Fatalf("post-reset %d: %v", i, err)
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
