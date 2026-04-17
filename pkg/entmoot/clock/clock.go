// Package clock provides a small time abstraction so code that consults the
// wall clock (replay windows, rate limiters, timestamp validation) can be
// tested deterministically.
//
// Production code uses System (a thin wrapper around time.Now). Tests use
// Fake, which is safe to advance and read concurrently.
package clock

import (
	"sync"
	"time"
)

// Clock is the minimal time source. Sub-packages that need the wall clock
// depend on this interface rather than on time.Now directly.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

// System implements Clock by delegating to time.Now.
type System struct{}

// Now returns the current wall-clock time.
func (System) Now() time.Time { return time.Now() }

// Fake is a thread-safe, manually-advanced Clock for tests.
type Fake struct {
	mu sync.Mutex
	t  time.Time
}

// NewFake returns a Fake clock anchored at t.
func NewFake(t time.Time) *Fake {
	return &Fake{t: t}
}

// Now returns the Fake's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

// Advance moves the Fake's time forward by d. Negative durations are
// permitted; tests occasionally need to move backwards.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}
