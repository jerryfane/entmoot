// Package wire — replay protection.
//
// ReplayChecker rejects frames that either fall outside the allowed
// timestamp window or whose bodies have already been seen recently. It
// mirrors Pilot's HandshakeManager replay design
// (repos/pilotprotocol/pkg/daemon/handshake.go:55-63) so an Entmoot operator
// familiar with Pilot's trust dial sees the same acceptance window here:
//
//   - ReplayMaxAge    = 5 * time.Minute    (past bound, strict)
//   - ReplayMaxFuture = 30 * time.Second   (future bound, inclusive)
//
// Algorithm, per call to VerifyFresh(t, body):
//
//  1. If t is a message type that carries a Timestamp field (MsgHello and
//     MsgGossip are the only v0 types that do — see wire/types.go), pull the
//     int64 Timestamp out with a minimal anonymous-struct decode. If the
//     timestamp is older than now-ReplayMaxAge, or further ahead than
//     now+ReplayMaxFuture, reject. Other message types skip the window check
//     (they are point-to-point requests and responses whose freshness is
//     enforced by the request/response correlation rather than a wall-clock
//     window).
//  2. Compute sha256(body) and consult a bounded LRU dedup set. If present,
//     reject as a replay. Otherwise insert and accept.
//
// The dedup set is capped at maxDedup entries (default 8192, matching
// Pilot's maxReplaySetEntries). When the cap is reached, the oldest entry is
// evicted to make room. Eviction uses a container/list-backed LRU keyed by
// the 32-byte sha256; freshly-accepted bodies move to the front, stale ones
// fall off the back.
//
// All exported methods are safe for concurrent use.
//
// Sharing across groups: a single ReplayChecker is intended to be shared
// across every group on a connection in v0. That is intentional — the hash
// set is bounded per-connection rather than per-group, and a duplicate
// body is a duplicate regardless of which group-id field it carries. If a
// future phase introduces per-group isolation (e.g. to prevent a loud group
// from evicting entries from a quiet one), it would shard by group id in
// the caller and hold one ReplayChecker per shard.
package wire

import (
	"container/list"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"
)

// Replay-window constants mirrored from Pilot's handshake protocol
// (repos/pilotprotocol/pkg/daemon/handshake.go:57-58).
const (
	// ReplayMaxAge is the maximum age of an accepted message. A timestamp at
	// or beyond this bound in the past is rejected.
	ReplayMaxAge = 5 * time.Minute
	// ReplayMaxFuture is the maximum allowed clock skew into the future. A
	// timestamp at or within this bound is accepted; beyond it is rejected.
	ReplayMaxFuture = 30 * time.Second
)

// defaultMaxDedup is the default cap on the dedup LRU when NewReplayChecker
// is called with maxDedup <= 0. Matches Pilot's maxReplaySetEntries.
const defaultMaxDedup = 8192

// ReplayChecker rejects stale or duplicate frames. The zero value is not
// usable; construct with NewReplayChecker. Methods are safe for concurrent
// use by multiple goroutines.
type ReplayChecker struct {
	clk      clock.Clock
	maxDedup int

	mu    sync.Mutex
	// lru holds dedup entries in insertion (= recency) order. Front = newest.
	lru *list.List
	// index maps sha256(body) to the element in lru holding that key.
	index map[[32]byte]*list.Element
}

// dedupEntry is the value stored in each list element. The key is kept
// alongside so eviction can remove the matching index entry without a
// second lookup.
type dedupEntry struct {
	key  [32]byte
	seen time.Time
}

// NewReplayChecker builds a checker with the given clock and max dedup
// entries. A nil clk defaults to clock.System. A maxDedup of 0 or less
// defaults to 8192 (matches Pilot's maxReplaySetEntries).
func NewReplayChecker(clk clock.Clock, maxDedup int) *ReplayChecker {
	if clk == nil {
		clk = clock.System{}
	}
	if maxDedup <= 0 {
		maxDedup = defaultMaxDedup
	}
	return &ReplayChecker{
		clk:      clk,
		maxDedup: maxDedup,
		lru:      list.New(),
		index:    make(map[[32]byte]*list.Element),
	}
}

// timestampedBody is a minimal decode target: we only care about the
// Timestamp field when doing the window check, so we avoid touching any
// other field and avoid the cost of a full Decode roundtrip before dedupe.
type timestampedBody struct {
	Timestamp int64 `json:"timestamp"`
}

// carriesTimestamp reports whether message type t has a Timestamp field in
// its v0 wire payload. See wire/types.go: only MsgHello and MsgGossip do.
func carriesTimestamp(t MsgType) bool {
	switch t {
	case MsgHello, MsgGossip:
		return true
	default:
		return false
	}
}

// VerifyFresh reports whether the given frame passes replay checks.
//
// For message types that carry a Timestamp (MsgHello, MsgGossip in v0),
// the timestamp is extracted from the JSON body and must fall within
// (now - ReplayMaxAge, now + ReplayMaxFuture]. The past bound is strict
// (exactly -ReplayMaxAge is rejected) and the future bound is inclusive
// (exactly +ReplayMaxFuture is accepted). Other message types skip this
// check.
//
// The body's sha256 is then looked up in a bounded LRU dedup set. On hit,
// VerifyFresh returns an error wrapping entmoot.ErrReplay. On miss, the
// hash is inserted; when the set exceeds its cap, the oldest entry is
// evicted. A miss is accepted and nil is returned.
//
// Callers should use errors.Is(err, entmoot.ErrReplay) to detect replay
// rejections. Malformed bodies (undecodable timestamps on a timestamped
// type) produce a non-replay error so callers can distinguish from
// legitimate replay attempts.
func (rc *ReplayChecker) VerifyFresh(t MsgType, body []byte) error {
	now := rc.clk.Now()

	if carriesTimestamp(t) {
		var tb timestampedBody
		if err := json.Unmarshal(body, &tb); err != nil {
			return fmt.Errorf("replay: decode timestamp for %s: %w: %v", t, entmoot.ErrMalformedFrame, err)
		}
		ts := time.UnixMilli(tb.Timestamp)
		// Past bound: strict. A timestamp exactly at or beyond -ReplayMaxAge
		// is rejected. Matches Pilot's `now.Sub(ts) > handshakeMaxAge` flip;
		// we use >= so the boundary case is deterministically stale.
		if now.Sub(ts) >= ReplayMaxAge {
			return fmt.Errorf("replay: timestamp %s older than %s: %w",
				ts.Format(time.RFC3339Nano), ReplayMaxAge, entmoot.ErrReplay)
		}
		// Future bound: inclusive. A timestamp exactly at +ReplayMaxFuture is
		// accepted; strictly beyond it is rejected. Documented here because
		// asymmetric boundary choices tend to surprise maintainers.
		if ts.Sub(now) > ReplayMaxFuture {
			return fmt.Errorf("replay: timestamp %s further than %s in the future: %w",
				ts.Format(time.RFC3339Nano), ReplayMaxFuture, entmoot.ErrReplay)
		}
	}

	key := sha256.Sum256(body)

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if elt, ok := rc.index[key]; ok {
		// Already seen. Touch recency then reject: a hit still counts as
		// activity against this body, so we keep it fresh in the LRU to
		// continue dedupe-ing near-in-time duplicates.
		rc.lru.MoveToFront(elt)
		return fmt.Errorf("replay: duplicate body sha256: %w", entmoot.ErrReplay)
	}

	// Insert at the front.
	elt := rc.lru.PushFront(&dedupEntry{key: key, seen: now})
	rc.index[key] = elt

	// Evict from the back until under cap.
	for rc.lru.Len() > rc.maxDedup {
		oldest := rc.lru.Back()
		if oldest == nil {
			break
		}
		entry := oldest.Value.(*dedupEntry)
		delete(rc.index, entry.key)
		rc.lru.Remove(oldest)
	}

	return nil
}

// Size returns the current number of entries in the dedup set. Useful for
// tests and basic observability; not intended to drive production logic.
func (rc *ReplayChecker) Size() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.lru.Len()
}
