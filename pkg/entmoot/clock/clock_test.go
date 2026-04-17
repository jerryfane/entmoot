package clock

import (
	"sync"
	"testing"
	"time"
)

func TestSystemNowIsMonotonicWithinCall(t *testing.T) {
	var s System
	a := s.Now()
	b := s.Now()
	if b.Before(a) {
		t.Fatalf("System.Now went backwards: a=%v b=%v", a, b)
	}
}

func TestFakeNowAndAdvance(t *testing.T) {
	anchor := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	f := NewFake(anchor)
	if got := f.Now(); !got.Equal(anchor) {
		t.Fatalf("Now: got %v want %v", got, anchor)
	}
	f.Advance(2 * time.Second)
	if got := f.Now(); !got.Equal(anchor.Add(2 * time.Second)) {
		t.Fatalf("after Advance(2s): got %v", got)
	}
	f.Advance(-time.Second)
	if got := f.Now(); !got.Equal(anchor.Add(time.Second)) {
		t.Fatalf("after Advance(-1s): got %v", got)
	}
}

// TestFakeConcurrent exercises Advance + Now from many goroutines. The
// sync.Mutex inside Fake should keep both operations race-free and the final
// time should equal the anchor plus the sum of all advances.
func TestFakeConcurrent(t *testing.T) {
	anchor := time.Unix(0, 0)
	f := NewFake(anchor)

	const writers = 16
	const readers = 16
	const iters = 1000

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				f.Advance(time.Millisecond)
			}
		}()
	}
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_ = f.Now()
			}
		}()
	}

	wg.Wait()

	expected := anchor.Add(time.Duration(writers*iters) * time.Millisecond)
	if got := f.Now(); !got.Equal(expected) {
		t.Fatalf("final Now: got %v want %v", got, expected)
	}
}
