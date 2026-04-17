package entmoot

import "errors"

// ErrSigInvalid is returned by roster, message, and invite verification when
// the Ed25519 signature does not match the signed payload.
var ErrSigInvalid = errors.New("entmoot: signature invalid")

// ErrReplay is returned by the wire layer when a received message falls
// outside the 5m-past/30s-future timestamp window or its hash has already
// been seen (dedupe set).
var ErrReplay = errors.New("entmoot: replay rejected")

// ErrRosterReject is returned by the roster layer when an Apply is rejected
// (non-founder signature in v0).
var ErrRosterReject = errors.New("entmoot: roster apply rejected")

// ErrRateLimited is returned by the rate-limit layer when a peer exceeds its
// per-peer token bucket (msg/s or bytes/s).
var ErrRateLimited = errors.New("entmoot: rate limited")

// ErrUnknownMessage is returned by the wire codec when it receives a frame
// whose 1-byte message type is not one of the registered types.
var ErrUnknownMessage = errors.New("entmoot: unknown message type")

// ErrMalformedFrame is returned by the wire codec when a frame fails length
// parsing or JSON unmarshaling.
var ErrMalformedFrame = errors.New("entmoot: malformed frame")

// ErrOversized is returned by the wire codec when a frame's declared length
// exceeds the 16 MiB hard cap.
var ErrOversized = errors.New("entmoot: frame exceeds 16 MiB cap")

// ErrNotMember is returned by the delivery layer when a message is authored
// by a node that is not a current roster member.
var ErrNotMember = errors.New("entmoot: author not a group member")

// ErrInviteExpired is returned by gossip.Join when the invite's ValidUntil
// timestamp is in the past relative to the local clock.
var ErrInviteExpired = errors.New("entmoot: invite has expired")
