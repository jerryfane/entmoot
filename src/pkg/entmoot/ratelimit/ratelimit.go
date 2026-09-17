// Package ratelimit implements token-bucket rate limiting over a pair of
// buckets — messages and bytes — keyed by MemberID.
//
// This package holds no defaults. The limits actually enforced come from the
// group's enforcement policy: cmd/entmootd builds them with
// entpolicy.ContentLimits and passes them to New, and the policy presets live
// in pkg/entmoot/policy (DefaultMessageRatePerAuthor, DefaultByteRatePerAuthor
// and their bursts). Numbers written here would be a second, unread set.
//
// This package exposes the Allow path only: it decides whether a given
// message + payload pair is within the author's current budget. It has no
// opinion about what a caller does with a refusal, and nothing in the tree
// stalls reads or disconnects a peer for exceeding a budget: the publish and
// history paths in cmd/entmootd turn a refusal into an error to the caller.
//
// A Limiter tracks one pair of buckets per peer, keyed by MemberID. Buckets
// are created lazily on first contact and live for the process.
//
// Clock injection: golang.org/x/time/rate consults time.Now internally only
// through its Allow / Reserve shorthands. The *At / *N variants accept an
// explicit time.Time, which is how tests drive deterministic advances.
// This package routes every bucket call through AllowN / ReserveN /
// CancelAt with t = clk.Now(), so a clock.Fake in tests fully controls
// token refill.
package ratelimit

import (
	"sync"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"

	"golang.org/x/time/rate"
)

// Limits configure per-peer token-bucket limits.
//
// A zero rate (MsgRate == 0 or BytesRate == 0) disables that bucket: Allow
// never rejects because of it, regardless of the accompanying burst value.
// This is primarily a test / relaxed-deployment affordance.
type Limits struct {
	// MsgRate is the refill rate of the message bucket, in messages/second.
	MsgRate rate.Limit
	// MsgBurst is the burst capacity of the message bucket.
	MsgBurst int
	// BytesRate is the refill rate of the byte bucket, in bytes/second.
	BytesRate rate.Limit
	// BytesBurst is the burst capacity of the byte bucket.
	BytesBurst int
}

// peerLimiter holds the pair of buckets for a single peer. A nil bucket
// means the corresponding Limits.*Rate was zero, i.e. that dimension is
// unlimited for this peer.
type peerLimiter struct {
	msg   *rate.Limiter
	bytes *rate.Limiter
}

// Limiter tracks per-peer token buckets. The zero value is not usable;
// construct one with New. Limiter is safe for concurrent use by multiple
// goroutines.
type Limiter struct {
	limits Limits
	clk    clock.Clock

	mu    sync.Mutex
	peers map[entmoot.MemberID]*peerLimiter
}

// New returns a Limiter that applies the given Limits to every peer.
//
// clk is used for all token-bucket time reads; pass nil to use
// clock.System (time.Now under the hood).
func New(limits Limits, clk clock.Clock) *Limiter {
	if clk == nil {
		clk = clock.System{}
	}
	return &Limiter{
		limits: limits,
		clk:    clk,
		peers:  make(map[entmoot.MemberID]*peerLimiter),
	}
}

// bucketFor returns the peerLimiter for peer, creating it on first sight.
// Called with l.mu held.
func (l *Limiter) bucketFor(peer entmoot.MemberID) *peerLimiter {
	if pl, ok := l.peers[peer]; ok {
		return pl
	}
	pl := &peerLimiter{}
	if l.limits.MsgRate > 0 {
		pl.msg = rate.NewLimiter(l.limits.MsgRate, l.limits.MsgBurst)
	}
	if l.limits.BytesRate > 0 {
		pl.bytes = rate.NewLimiter(l.limits.BytesRate, l.limits.BytesBurst)
	}
	l.peers[peer] = pl
	return pl
}

// Allow consumes 1 message token and nbytes byte tokens for peer. It
// returns nil if both buckets accepted the charge, or entmoot.ErrRateLimited
// if either bucket is exhausted.
//
// Atomicity: Allow first reserves from the message bucket via ReserveN,
// then from the byte bucket. If the byte reservation fails, the message
// reservation is canceled with CancelAt so the rejection does not burn a
// token. A zero nbytes skips the byte bucket entirely; a zero-rate bucket
// is treated as unlimited and never rejects.
//
// Callers that disable a bucket by passing rate 0 in Limits still get the
// other bucket enforced.
func (l *Limiter) Allow(peer entmoot.MemberID, nbytes int) error {
	now := l.clk.Now()

	l.mu.Lock()
	pl := l.bucketFor(peer)
	l.mu.Unlock()

	// Message bucket: reserve 1 token.
	var msgRes *rate.Reservation
	if pl.msg != nil {
		msgRes = pl.msg.ReserveN(now, 1)
		// ReserveN returns !ok only when n > burst (or limit is Inf+0). A
		// non-ok reservation means the caller can never satisfy this in a
		// single call, so treat it as rate-limited.
		if !msgRes.OK() {
			return entmoot.ErrRateLimited
		}
		if msgRes.DelayFrom(now) > 0 {
			// Tokens not yet available. Cancel the reservation so the
			// future-reserved token is released back, and reject.
			msgRes.CancelAt(now)
			return entmoot.ErrRateLimited
		}
	}

	// Byte bucket: reserve nbytes tokens (if applicable).
	if pl.bytes != nil && nbytes > 0 {
		byteRes := pl.bytes.ReserveN(now, nbytes)
		if !byteRes.OK() || byteRes.DelayFrom(now) > 0 {
			if byteRes.OK() {
				byteRes.CancelAt(now)
			}
			if msgRes != nil {
				msgRes.CancelAt(now)
			}
			return entmoot.ErrRateLimited
		}
	}

	return nil
}
