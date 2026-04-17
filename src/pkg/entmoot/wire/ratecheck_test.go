package wire_test

import (
	"errors"
	"testing"
	"time"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/ratelimit"
	"entmoot/pkg/entmoot/wire"
)

// rateAnchor is the reference time used by the rate-check tests.
var rateAnchor = time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)

func TestCheckFrameRate_BelowBurstPasses(t *testing.T) {
	fk := clock.NewFake(rateAnchor)
	lim := ratelimit.New(ratelimit.DefaultLimits(), fk)
	peer := entmoot.NodeID(7)

	// 1 KiB fits comfortably in the default byte burst (4 MiB).
	if err := wire.CheckFrameRate(peer, 1024, lim); err != nil {
		t.Fatalf("below-burst frame rejected: %v", err)
	}
}

func TestCheckFrameRate_AboveBurstReturnsRateLimited(t *testing.T) {
	fk := clock.NewFake(rateAnchor)
	// Constrain the byte bucket so a single call can clearly exceed it.
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:    100,
		MsgBurst:   200,
		BytesRate:  1024,
		BytesBurst: 2048,
	}, fk)
	peer := entmoot.NodeID(7)

	// 3 KiB > 2 KiB burst → immediate rejection.
	err := wire.CheckFrameRate(peer, 3*1024, lim)
	if err == nil {
		t.Fatal("expected rate-limit rejection, got nil")
	}
	if !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("errors.Is(ErrRateLimited): got false, err=%v", err)
	}
}

func TestCheckFrameRate_PassesAfterClockAdvance(t *testing.T) {
	fk := clock.NewFake(rateAnchor)
	// Tight msg bucket: burst 1, refill 1/sec. First call drains it, second
	// immediate call would be rejected; but after advancing the fake clock
	// by a full second the bucket has refilled and the call succeeds.
	lim := ratelimit.New(ratelimit.Limits{
		MsgRate:   1,
		MsgBurst:  1,
		BytesRate: 0, // disabled so we isolate msg-bucket behavior
	}, fk)
	peer := entmoot.NodeID(7)

	if err := wire.CheckFrameRate(peer, 64, lim); err != nil {
		t.Fatalf("first call rejected: %v", err)
	}
	// Second immediate call: msg bucket empty.
	err := wire.CheckFrameRate(peer, 64, lim)
	if err == nil {
		t.Fatal("expected rate-limit on second immediate call")
	}
	if !errors.Is(err, entmoot.ErrRateLimited) {
		t.Fatalf("errors.Is(ErrRateLimited): got false, err=%v", err)
	}

	// Advance one second: bucket refills by one token.
	fk.Advance(1 * time.Second)

	if err := wire.CheckFrameRate(peer, 64, lim); err != nil {
		t.Fatalf("post-advance call rejected: %v", err)
	}
}
