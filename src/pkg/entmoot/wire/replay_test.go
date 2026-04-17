package wire_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/wire"
)

// replayAnchor is the reference wall-clock time used by every test that
// wires a clock.Fake into a ReplayChecker.
var replayAnchor = time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)

// helloBody returns a JSON-encoded Hello body with the given timestamp and
// an opaque marker byte so otherwise-identical timestamps hash differently.
func helloBody(t *testing.T, tsMillis int64, marker byte) []byte {
	t.Helper()
	h := wire.Hello{
		NodeID:    entmoot.NodeID(1),
		PubKey:    []byte{marker},
		Timestamp: tsMillis,
	}
	body, err := json.Marshal(&h)
	if err != nil {
		t.Fatalf("marshal Hello: %v", err)
	}
	return body
}

// fetchReqBody returns a JSON-encoded FetchReq body. FetchReq has no
// Timestamp field; VerifyFresh should skip the window check for it but
// still dedupe on body hash.
func fetchReqBody(t *testing.T, idFill byte) []byte {
	t.Helper()
	var gid entmoot.GroupID
	var mid entmoot.MessageID
	for i := range mid {
		mid[i] = idFill
	}
	req := wire.FetchReq{GroupID: gid, ID: mid}
	body, err := json.Marshal(&req)
	if err != nil {
		t.Fatalf("marshal FetchReq: %v", err)
	}
	return body
}

func TestVerifyFresh_AcceptsInWindow(t *testing.T) {
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	body := helloBody(t, replayAnchor.UnixMilli(), 1)
	if err := rc.VerifyFresh(wire.MsgHello, body); err != nil {
		t.Fatalf("fresh frame rejected: %v", err)
	}
	if got := rc.Size(); got != 1 {
		t.Errorf("Size after first insert: got %d, want 1", got)
	}
}

func TestVerifyFresh_RejectsOldTimestamp(t *testing.T) {
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	// 6 minutes old -> well past ReplayMaxAge.
	ts := replayAnchor.Add(-6 * time.Minute).UnixMilli()
	body := helloBody(t, ts, 1)

	err := rc.VerifyFresh(wire.MsgHello, body)
	if err == nil {
		t.Fatal("expected replay rejection, got nil")
	}
	if !errors.Is(err, entmoot.ErrReplay) {
		t.Fatalf("errors.Is(ErrReplay): got false, err=%v", err)
	}
}

func TestVerifyFresh_RejectsFarFuture(t *testing.T) {
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	// 31 seconds into the future -> just past ReplayMaxFuture.
	ts := replayAnchor.Add(31 * time.Second).UnixMilli()
	body := helloBody(t, ts, 1)

	err := rc.VerifyFresh(wire.MsgHello, body)
	if err == nil {
		t.Fatal("expected future-clock rejection, got nil")
	}
	if !errors.Is(err, entmoot.ErrReplay) {
		t.Fatalf("errors.Is(ErrReplay): got false, err=%v", err)
	}
}

func TestVerifyFresh_PastBoundaryIsStrict(t *testing.T) {
	// Exactly -5 minutes: past bound is strict, so this must be rejected.
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	ts := replayAnchor.Add(-wire.ReplayMaxAge).UnixMilli()
	body := helloBody(t, ts, 1)

	err := rc.VerifyFresh(wire.MsgHello, body)
	if err == nil {
		t.Fatal("expected strict past-boundary rejection, got nil")
	}
	if !errors.Is(err, entmoot.ErrReplay) {
		t.Fatalf("errors.Is(ErrReplay): got false, err=%v", err)
	}
}

func TestVerifyFresh_FutureBoundaryIsInclusive(t *testing.T) {
	// Exactly +30s: future bound is inclusive, so this must be accepted.
	// Documents the asymmetric boundary choice (strict past, inclusive
	// future) that the replay.go package comment calls out.
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	ts := replayAnchor.Add(wire.ReplayMaxFuture).UnixMilli()
	body := helloBody(t, ts, 1)

	if err := rc.VerifyFresh(wire.MsgHello, body); err != nil {
		t.Fatalf("inclusive future boundary rejected: %v", err)
	}
}

func TestVerifyFresh_DuplicateBodyRejected(t *testing.T) {
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	body := helloBody(t, replayAnchor.UnixMilli(), 7)

	if err := rc.VerifyFresh(wire.MsgHello, body); err != nil {
		t.Fatalf("first insert rejected: %v", err)
	}
	err := rc.VerifyFresh(wire.MsgHello, body)
	if err == nil {
		t.Fatal("expected dedupe rejection on duplicate body, got nil")
	}
	if !errors.Is(err, entmoot.ErrReplay) {
		t.Fatalf("errors.Is(ErrReplay): got false, err=%v", err)
	}
}

func TestVerifyFresh_DifferentBodiesSameTimestampAccepted(t *testing.T) {
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	ts := replayAnchor.UnixMilli()
	a := helloBody(t, ts, 1)
	b := helloBody(t, ts, 2)

	if err := rc.VerifyFresh(wire.MsgHello, a); err != nil {
		t.Fatalf("body A rejected: %v", err)
	}
	if err := rc.VerifyFresh(wire.MsgHello, b); err != nil {
		t.Fatalf("body B rejected: %v", err)
	}
	if got := rc.Size(); got != 2 {
		t.Errorf("Size: got %d, want 2", got)
	}
}

func TestVerifyFresh_NoTimestampFieldSkipsWindowCheckButDedupes(t *testing.T) {
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	// FetchReq has no Timestamp field. Even without a window check, the
	// body should still hit the dedupe set on the second insert.
	body := fetchReqBody(t, 0xAB)

	if err := rc.VerifyFresh(wire.MsgFetchReq, body); err != nil {
		t.Fatalf("first FetchReq rejected: %v", err)
	}
	err := rc.VerifyFresh(wire.MsgFetchReq, body)
	if err == nil {
		t.Fatal("expected dedupe rejection on duplicate FetchReq, got nil")
	}
	if !errors.Is(err, entmoot.ErrReplay) {
		t.Fatalf("errors.Is(ErrReplay): got false, err=%v", err)
	}
}

func TestVerifyFresh_LRUEviction(t *testing.T) {
	// With maxDedup = 4, inserting 5 unique bodies must evict the oldest
	// (body 0); re-inserting body 0 afterwards must succeed as fresh.
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 4)

	bodies := make([][]byte, 5)
	for i := range bodies {
		bodies[i] = fetchReqBody(t, byte(0xC0+i))
		if err := rc.VerifyFresh(wire.MsgFetchReq, bodies[i]); err != nil {
			t.Fatalf("insert %d rejected: %v", i, err)
		}
	}
	if got := rc.Size(); got != 4 {
		t.Errorf("Size after eviction: got %d, want 4", got)
	}
	// bodies[0] should have been evicted. Re-inserting must succeed.
	if err := rc.VerifyFresh(wire.MsgFetchReq, bodies[0]); err != nil {
		t.Fatalf("re-insert of evicted body rejected: %v", err)
	}
	// bodies[4] was just the newest; it should still be a duplicate.
	err := rc.VerifyFresh(wire.MsgFetchReq, bodies[4])
	if err == nil {
		t.Fatal("expected newest body to still dedupe, got nil")
	}
	if !errors.Is(err, entmoot.ErrReplay) {
		t.Fatalf("errors.Is(ErrReplay): got false, err=%v", err)
	}
}

func TestVerifyFresh_ConcurrentRaceSafe(t *testing.T) {
	// Hammer VerifyFresh from 8 goroutines. This test is primarily an
	// assertion target for `go test -race`; functional result is just "no
	// panics, no data races".
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 1024)

	const workers = 8
	const perWorker = 200

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				// Unique body per (worker, i) so most inserts succeed; that
				// keeps both the happy and the dedupe paths exercised.
				marker := byte((id*perWorker + i) & 0xff)
				body := helloBody(t, replayAnchor.UnixMilli(), marker)
				_ = rc.VerifyFresh(wire.MsgHello, body)
			}
		}(w)
	}
	wg.Wait()

	if rc.Size() <= 0 {
		t.Errorf("Size: got %d, want > 0", rc.Size())
	}
}

func TestVerifyFresh_FakeClockDrivesWindow(t *testing.T) {
	// A timestamp that is borderline-fresh at anchor becomes stale once
	// we advance the fake clock past the ReplayMaxAge boundary. Uses a
	// fresh body per call to keep this test focused on the time window
	// rather than on dedupe.
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	ts := replayAnchor.UnixMilli()
	body1 := helloBody(t, ts, 1)
	if err := rc.VerifyFresh(wire.MsgHello, body1); err != nil {
		t.Fatalf("borderline-fresh frame rejected at anchor: %v", err)
	}

	// Advance 6 minutes; the same timestamp is now older than ReplayMaxAge.
	fk.Advance(6 * time.Minute)
	body2 := helloBody(t, ts, 2) // different hash so we're not hitting dedupe
	err := rc.VerifyFresh(wire.MsgHello, body2)
	if err == nil {
		t.Fatal("expected stale rejection after clock advance, got nil")
	}
	if !errors.Is(err, entmoot.ErrReplay) {
		t.Fatalf("errors.Is(ErrReplay): got false, err=%v", err)
	}
}

// TestVerifyFresh_MalformedTimestampBodyErrors ensures a malformed JSON body
// on a timestamped message type surfaces a non-replay error, so callers can
// distinguish malformed-frame rejection from legitimate replay hits.
func TestVerifyFresh_MalformedTimestampBodyErrors(t *testing.T) {
	fk := clock.NewFake(replayAnchor)
	rc := wire.NewReplayChecker(fk, 0)

	err := rc.VerifyFresh(wire.MsgHello, []byte("not json"))
	if err == nil {
		t.Fatal("expected error for malformed Hello body, got nil")
	}
	if errors.Is(err, entmoot.ErrReplay) {
		t.Fatalf("malformed frame should not wrap ErrReplay: %v", err)
	}
	if !errors.Is(err, entmoot.ErrMalformedFrame) {
		t.Fatalf("expected ErrMalformedFrame, got: %v", err)
	}
}

// Sanity check that our helloBody produces something Decode is happy with
// — otherwise the replay tests could silently mask a real framing bug.
func TestHelloBodyDecodesCleanly(t *testing.T) {
	body := helloBody(t, replayAnchor.UnixMilli(), 0x42)
	v, err := wire.Decode(wire.MsgHello, body)
	if err != nil {
		t.Fatalf("Decode Hello: %v", err)
	}
	h, ok := v.(*wire.Hello)
	if !ok {
		t.Fatalf("Decode returned %T, want *wire.Hello", v)
	}
	if h.Timestamp != replayAnchor.UnixMilli() {
		t.Errorf("Timestamp: got %d, want %d", h.Timestamp, replayAnchor.UnixMilli())
	}
	// keep fmt import used if we ever print diagnostics; touching it here
	// avoids a surprise later if a maintainer adds %v formatting.
	_ = fmt.Sprintf("%v", h)
}
