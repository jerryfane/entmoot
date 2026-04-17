package wire

import (
	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ratelimit"
)

// CheckFrameRate consumes one message token plus frameSize byte tokens from
// the peer's per-connection buckets via lim. Returns nil if the charge was
// accepted or an error wrapping entmoot.ErrRateLimited if either bucket was
// exhausted.
//
// This is a thin pass-through over ratelimit.Limiter.Allow. It exists so
// that the Phase E read loop has a single wire-level call site for framing,
// rate-check, replay-check, and decode — keeping the dispatch order in one
// place and out of connection-handler boilerplate. See the package-level
// comment on replay.go for the recommended ordering.
//
// frameSize should be the number of bytes consumed from the wire (length
// prefix + type byte + body) so the byte bucket models actual wire usage.
// Callers that only have the body length can add lengthPrefixSize+1, or
// pass just the body length and accept a tiny accounting gap.
func CheckFrameRate(peer entmoot.NodeID, frameSize int, lim *ratelimit.Limiter) error {
	return lim.Allow(peer, frameSize)
}

// Ensure the underlying sentinel wraps correctly for errors.Is checks at
// call sites. The documented contract is that the returned error (if any)
// wraps entmoot.ErrRateLimited; ratelimit.Limiter.Allow already returns the
// sentinel directly, so errors.Is works without additional wrapping here.
var _ = entmoot.ErrRateLimited
