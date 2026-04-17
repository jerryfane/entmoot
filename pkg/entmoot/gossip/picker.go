package gossip

import (
	"math/rand/v2"
	"sync"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/roster"
)

// PeerPicker samples NodeIDs from a roster. v0 is random-only: no near-peer
// (interest-overlap) pool. The second pool is tracked in the plan's "Deferred
// from v0" list and will slot in alongside the random pool when v1 needs it.
//
// PeerPicker is safe for concurrent use by any number of goroutines once
// constructed. Every Sample takes an internal mutex so the underlying
// *rand.Rand (which is NOT safe for concurrent use) is accessed serially.
type PeerPicker struct {
	roster *roster.RosterLog
	local  entmoot.NodeID

	mu  sync.Mutex
	rnd *rand.Rand
}

// NewPicker constructs a PeerPicker over the given roster. local is the
// NodeID that Sample always excludes. rnd MUST be non-nil — callers pass a
// *rand.Rand constructed from their chosen deterministic source in tests or
// from a crypto/rand-seeded source in production.
//
// Passing a nil rnd panics; silently falling back to a global source would
// erode the deterministic-tests promise.
func NewPicker(rl *roster.RosterLog, local entmoot.NodeID, rnd *rand.Rand) *PeerPicker {
	if rnd == nil {
		panic("gossip: NewPicker requires a non-nil *rand.Rand")
	}
	return &PeerPicker{roster: rl, local: local, rnd: rnd}
}

// Sample returns up to n random peers from the current roster membership,
// excluding local. Duplicates are never produced. The result is deterministic
// when the underlying *rand.Rand is seeded.
//
// n ≤ 0 yields a nil slice. If the roster has fewer than n+1 members
// (including local), the returned slice is shorter than n — Sample never
// blocks, never panics, and never pads with zero NodeIDs.
func (p *PeerPicker) Sample(n int) []entmoot.NodeID {
	if n <= 0 {
		return nil
	}

	// Build the candidate list: current members minus local. Members() is
	// already sorted ascending, so seeded runs produce identical orderings
	// before the shuffle.
	all := p.roster.Members()
	candidates := make([]entmoot.NodeID, 0, len(all))
	for _, m := range all {
		if m == p.local {
			continue
		}
		candidates = append(candidates, m)
	}
	if len(candidates) == 0 {
		return nil
	}

	k := n
	if k > len(candidates) {
		k = len(candidates)
	}

	// Partial Fisher-Yates: shuffle only the first k positions. Deterministic
	// when the caller supplies a seeded *rand.Rand.
	p.mu.Lock()
	for i := 0; i < k; i++ {
		j := i + p.rnd.IntN(len(candidates)-i)
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}
	p.mu.Unlock()
	return candidates[:k]
}
