// Package ratelimit implements per-peer token-bucket rate limiting for
// Entmoot connections.
//
// Per ARCHITECTURE.md §10 (Denial of service), every :1004 connection is
// governed by two buckets:
//
//   - a message-rate bucket (v0 default: 100 msg/s, burst 200)
//   - a byte-rate bucket    (v0 default: 1 MiB/s, burst 4 MiB)
//
// This package exposes the Allow path only: it decides whether a given
// message + payload pair is within the peer's current budget. Backpressure
// (reads stall) and the 30 s sustained-violation hard disconnect are the
// connection layer's responsibility (Phase C) and live outside this package.
//
// A Limiter tracks one pair of buckets per peer, keyed by NodeID. Buckets
// are created lazily on first contact: an unknown peer starts with a full
// burst. Call Reset on disconnect to drop the per-peer state.
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
	"time"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/clock"

	"golang.org/x/time/rate"
)

// V0 default limits, mirrored from ARCHITECTURE.md §10.
const (
	// DefaultMsgRate is the steady-state messages-per-second allowance.
	DefaultMsgRate rate.Limit = 100
	// DefaultMsgBurst is the burst capacity of the message bucket.
	DefaultMsgBurst = 200
	// DefaultBytesRate is the steady-state bytes-per-second allowance (1 MiB/s).
	DefaultBytesRate rate.Limit = 1 << 20
	// DefaultBytesBurst is the burst capacity of the byte bucket (4 MiB).
	DefaultBytesBurst = 4 << 20
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
	// TopicLimits is a map from topic -> quota. Unspecified topics fall
	// through to the per-peer global limit with no additional constraint.
	// Topic-scoped buckets are applied on top of the global per-peer
	// bucket by AllowTopic — both must pass for the call to succeed.
	// (v1.2.0)
	TopicLimits map[string]TopicLimit
}

// TopicLimit is the per-(peer, topic) quota applied by AllowTopic in
// addition to the global per-peer limit. Intended for system topics
// such as "_pilot/transport/v1" that legitimate publishers should emit
// rarely and abusers would otherwise blast through the generous
// per-peer Msg/Bytes limits. (v1.2.0)
type TopicLimit struct {
	// MsgRate is the refill rate, in messages/second, for the (peer,
	// topic) bucket. Zero means "no topic-specific bucket".
	MsgRate rate.Limit
	// MsgBurst is the burst capacity of the (peer, topic) bucket.
	MsgBurst int
}

// DefaultLimits returns the v0 ARCHITECTURE.md §10 defaults: 100 msg/s
// burst 200, 1 MiB/s burst 4 MiB. TopicLimits is populated via
// DefaultTopicLimits so callers that want both the global quota and the
// v1.2.0 per-(peer, topic) caps need only take this default.
func DefaultLimits() Limits {
	return Limits{
		MsgRate:     DefaultMsgRate,
		MsgBurst:    DefaultMsgBurst,
		BytesRate:   DefaultBytesRate,
		BytesBurst:  DefaultBytesBurst,
		TopicLimits: DefaultTopicLimits(),
	}
}

// DefaultTopicLimits returns the v1.2.0 per-(peer, topic) defaults for
// system topics. Currently only "_pilot/transport/v1" — transport-ad
// gossip: 10-token burst refilled at 1/6-minute (10/hour). A legitimate
// publisher emits roughly 1/week (weekly safety-net refresh); the
// default leaves 1680x headroom before rate-limiting kicks in, so only
// genuinely abusive peers hit the cap.
func DefaultTopicLimits() map[string]TopicLimit {
	return map[string]TopicLimit{
		"_pilot/transport/v1": {
			MsgRate:  rate.Every(6 * time.Minute),
			MsgBurst: 10,
		},
	}
}

// peerLimiter holds the pair of buckets for a single peer. A nil bucket
// means the corresponding Limits.*Rate was zero, i.e. that dimension is
// unlimited for this peer.
type peerLimiter struct {
	msg   *rate.Limiter
	bytes *rate.Limiter
}

// topicPeerKey identifies a single per-(peer, topic) bucket. (v1.2.0)
type topicPeerKey struct {
	peer  entmoot.NodeID
	topic string
}

// Limiter tracks per-peer token buckets. The zero value is not usable;
// construct one with New. Limiter is safe for concurrent use by multiple
// goroutines.
type Limiter struct {
	limits Limits
	clk    clock.Clock

	mu         sync.Mutex
	peers      map[entmoot.NodeID]*peerLimiter
	topicPeers map[topicPeerKey]*rate.Limiter
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
		limits:     limits,
		clk:        clk,
		peers:      make(map[entmoot.NodeID]*peerLimiter),
		topicPeers: make(map[topicPeerKey]*rate.Limiter),
	}
}

// bucketFor returns the peerLimiter for peer, creating it on first sight.
// Called with l.mu held.
func (l *Limiter) bucketFor(peer entmoot.NodeID) *peerLimiter {
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
func (l *Limiter) Allow(peer entmoot.NodeID, nbytes int) error {
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

// Reset discards the per-peer bucket state for peer. The next Allow call
// for that peer will allocate a fresh pair of buckets with full burst.
// Per-(peer, topic) buckets created via AllowTopic are also dropped so
// the next AllowTopic call re-allocates a fresh bucket. Call on
// disconnect.
func (l *Limiter) Reset(peer entmoot.NodeID) {
	l.mu.Lock()
	delete(l.peers, peer)
	for k := range l.topicPeers {
		if k.peer == peer {
			delete(l.topicPeers, k)
		}
	}
	l.mu.Unlock()
}

// topicBucketFor returns the *rate.Limiter for (peer, topic), creating
// it on first sight from l.limits.TopicLimits[topic]. Returns nil if
// the topic has no configured limit (or its rate is zero), meaning
// the topic-specific bucket is disabled and only the global per-peer
// bucket applies. Caller must hold l.mu.
func (l *Limiter) topicBucketFor(peer entmoot.NodeID, topic string) *rate.Limiter {
	key := topicPeerKey{peer: peer, topic: topic}
	if b, ok := l.topicPeers[key]; ok {
		return b
	}
	tl, ok := l.limits.TopicLimits[topic]
	if !ok || tl.MsgRate <= 0 || tl.MsgBurst <= 0 {
		return nil
	}
	b := rate.NewLimiter(tl.MsgRate, tl.MsgBurst)
	l.topicPeers[key] = b
	return b
}

// AllowTopic is like Allow but additionally enforces a per-(peer,
// topic) bucket if one is configured via Limits.TopicLimits. Returns
// nil only when BOTH the topic bucket (if any) AND the global per-peer
// bucket allow the message. On rejection from either bucket, returns
// entmoot.ErrRateLimited and neither bucket has a token consumed
// (failed reservations are canceled so rejections don't burn tokens).
//
// Topics without an explicit TopicLimit entry behave exactly like
// Allow — the topic dimension is simply skipped. Intended for system
// topics like "_pilot/transport/v1" that need a tighter cap than the
// generous per-peer default. (v1.2.0)
func (l *Limiter) AllowTopic(peer entmoot.NodeID, topic string, nbytes int) error {
	now := l.clk.Now()

	l.mu.Lock()
	pl := l.bucketFor(peer)
	tb := l.topicBucketFor(peer, topic)
	l.mu.Unlock()

	// Topic bucket: reserve 1 token first, so a topic-level reject doesn't
	// consume anything from the global per-peer bucket.
	var topicRes *rate.Reservation
	if tb != nil {
		topicRes = tb.ReserveN(now, 1)
		if !topicRes.OK() {
			return entmoot.ErrRateLimited
		}
		if topicRes.DelayFrom(now) > 0 {
			topicRes.CancelAt(now)
			return entmoot.ErrRateLimited
		}
	}

	// Global per-peer message bucket.
	var msgRes *rate.Reservation
	if pl.msg != nil {
		msgRes = pl.msg.ReserveN(now, 1)
		if !msgRes.OK() {
			if topicRes != nil {
				topicRes.CancelAt(now)
			}
			return entmoot.ErrRateLimited
		}
		if msgRes.DelayFrom(now) > 0 {
			msgRes.CancelAt(now)
			if topicRes != nil {
				topicRes.CancelAt(now)
			}
			return entmoot.ErrRateLimited
		}
	}

	// Global per-peer byte bucket.
	if pl.bytes != nil && nbytes > 0 {
		byteRes := pl.bytes.ReserveN(now, nbytes)
		if !byteRes.OK() || byteRes.DelayFrom(now) > 0 {
			if byteRes.OK() {
				byteRes.CancelAt(now)
			}
			if msgRes != nil {
				msgRes.CancelAt(now)
			}
			if topicRes != nil {
				topicRes.CancelAt(now)
			}
			return entmoot.ErrRateLimited
		}
	}
	return nil
}
