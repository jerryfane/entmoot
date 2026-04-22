package reconcile

import (
	"context"
	"fmt"
	"math/big"

	"entmoot/pkg/entmoot"
)

// Role identifies which end of a reconciliation session a Session instance
// represents. It is informational only; the protocol is symmetric after
// the initial frame.
type Role uint8

const (
	// RoleInitiator is the side that sends the first frame (a single
	// KindFingerprint over the full keyspace).
	RoleInitiator Role = iota
	// RoleResponder is the side that waits for the initiator's first
	// frame and then alternates with it.
	RoleResponder
)

// Session is the per-peer RBSR state machine. A Session is single-threaded:
// callers alternate Next() invocations against one peer, feeding each side's
// outbound frames into the other side's next Next() call.
//
// A Session is NOT safe for concurrent use. Create one per reconciliation
// exchange and discard it when done (or when Next returns an error).
type Session struct {
	cfg     Config
	storage Storage
	role    Role
	round   int
	done    bool
	missing []entmoot.MessageID
	seen    map[[32]byte]struct{}
	// resolved tracks ranges for which we have already sent a KindIDList
	// in a prior round. When the peer replies with their own KindIDList
	// on a resolved range, we treat it as the closing half of the
	// exchange and emit KindEmpty rather than looping our own diff back.
	resolved map[[64]byte]struct{}
}

// zeroID is the "no lower bound" / "max upper bound" sentinel value for the
// keyspace. We treat MessageID{} as Lo = 0 and as Hi = 2^256 (max).
var zeroID entmoot.MessageID

// NewInitiator constructs an initiator Session and returns its opening
// frame: a single KindFingerprint over the full keyspace [0, max). The
// caller sends the frame to the peer and drives subsequent rounds via
// Next.
func NewInitiator(cfg Config, storage Storage) (*Session, []Range, error) {
	if storage == nil {
		return nil, nil, fmt.Errorf("reconcile: nil storage")
	}
	cfg = withDefaults(cfg)
	s := &Session{
		cfg:     cfg,
		storage: storage,
		role:    RoleInitiator,
		seen:     make(map[[32]byte]struct{}),
		resolved: make(map[[64]byte]struct{}),
	}
	fp, _, err := s.fingerprint(context.Background(), zeroID, zeroID)
	if err != nil {
		return nil, nil, fmt.Errorf("reconcile: initial fingerprint: %w", err)
	}
	return s, []Range{{
		Lo:          zeroID,
		Hi:          zeroID,
		Kind:        KindFingerprint,
		Fingerprint: fp,
	}}, nil
}

// NewResponder constructs a responder Session. The caller drives it by
// calling Next with the incoming frame from the initiator.
func NewResponder(cfg Config, storage Storage) *Session {
	cfg = withDefaults(cfg)
	return &Session{
		cfg:      cfg,
		storage:  storage,
		role:     RoleResponder,
		seen:     make(map[[32]byte]struct{}),
		resolved: make(map[[64]byte]struct{}),
	}
}

// Done reports whether the session has reached a terminal state: both
// sides sent only KindEmpty frames in the last exchange, so no further
// information needs to flow.
func (s *Session) Done() bool {
	return s.done
}

// Missing returns the union, across all rounds so far, of ids the peer has
// advertised that local storage does not hold. Callers use this list to
// drive follow-up body fetches. The slice is owned by the Session; copy
// if you need to retain it past the next Next() call.
func (s *Session) Missing() []entmoot.MessageID {
	return s.missing
}

// Next processes one frame of incoming ranges from the peer and returns
// the outgoing frame, any ids newly discovered as missing this round, and
// whether the session has converged.
//
// The newlyMissing slice contains only ids discovered in this particular
// round; Missing() returns the cumulative set.
//
// ErrMaxRounds is returned if the session has already performed
// cfg.MaxRounds steps.
func (s *Session) Next(ctx context.Context, incoming []Range) ([]Range, []entmoot.MessageID, bool, error) {
	s.round++
	if s.round > s.cfg.MaxRounds {
		return nil, nil, false, ErrMaxRounds
	}

	var outgoing []Range
	var newlyMissing []entmoot.MessageID
	sawFingerprint := false
	sawUnresolvedIDList := false

	for _, r := range incoming {
		switch r.Kind {
		case KindFingerprint:
			sawFingerprint = true
			localFP, localCount, err := s.fingerprint(ctx, r.Lo, r.Hi)
			if err != nil {
				return nil, nil, false, fmt.Errorf("reconcile: local fingerprint: %w", err)
			}
			if localFP == r.Fingerprint {
				outgoing = append(outgoing, Range{
					Lo:   r.Lo,
					Hi:   r.Hi,
					Kind: KindEmpty,
				})
				continue
			}
			if localCount <= s.cfg.LeafThreshold {
				ids, err := s.storage.IterIDs(ctx, r.Lo, r.Hi)
				if err != nil {
					return nil, nil, false, fmt.Errorf("reconcile: iter leaf: %w", err)
				}
				// Remember we emitted our full local contents for
				// this range, so when the peer replies with their
				// own diff-IDList we know to close (not re-loop).
				s.resolved[rangeKey(r.Lo, r.Hi)] = struct{}{}
				outgoing = append(outgoing, Range{
					Lo:   r.Lo,
					Hi:   r.Hi,
					Kind: KindIDList,
					IDs:  ids,
				})
				continue
			}
			// Split and emit a fingerprint per sub-range.
			subs, err := s.split(ctx, r.Lo, r.Hi)
			if err != nil {
				return nil, nil, false, err
			}
			outgoing = append(outgoing, subs...)

		case KindIDList:
			localIDs, err := s.storage.IterIDs(ctx, r.Lo, r.Hi)
			if err != nil {
				return nil, nil, false, fmt.Errorf("reconcile: iter leaf: %w", err)
			}
			localSet := make(map[entmoot.MessageID]struct{}, len(localIDs))
			for _, id := range localIDs {
				localSet[id] = struct{}{}
			}

			// diffA: peer has, we don't. Dedupe against s.seen so the
			// same id isn't reported twice across rounds.
			for _, id := range r.IDs {
				if _, ok := localSet[id]; ok {
					continue
				}
				key := [32]byte(id)
				if _, already := s.seen[key]; already {
					continue
				}
				s.seen[key] = struct{}{}
				newlyMissing = append(newlyMissing, id)
				s.missing = append(s.missing, id)
			}

			rKey := rangeKey(r.Lo, r.Hi)
			if _, already := s.resolved[rKey]; already {
				// The peer's incoming KindIDList is the second half
				// of an exchange we initiated: we already sent our
				// diff for this range and the peer has now replied
				// with theirs (above, absorbed into missing). The
				// range is fully resolved; send an ack.
				outgoing = append(outgoing, Range{
					Lo:   r.Lo,
					Hi:   r.Hi,
					Kind: KindEmpty,
				})
				continue
			}

			// diffB: we have, peer doesn't.
			peerSet := make(map[entmoot.MessageID]struct{}, len(r.IDs))
			for _, id := range r.IDs {
				peerSet[id] = struct{}{}
			}
			var diffB []entmoot.MessageID
			for _, id := range localIDs {
				if _, ok := peerSet[id]; ok {
					continue
				}
				diffB = append(diffB, id)
			}
			if len(diffB) == 0 {
				// We add nothing new for the peer; ack with Empty.
				s.resolved[rKey] = struct{}{}
				outgoing = append(outgoing, Range{
					Lo:   r.Lo,
					Hi:   r.Hi,
					Kind: KindEmpty,
				})
			} else {
				sawUnresolvedIDList = true
				s.resolved[rKey] = struct{}{}
				outgoing = append(outgoing, Range{
					Lo:   r.Lo,
					Hi:   r.Hi,
					Kind: KindIDList,
					IDs:  diffB,
				})
			}

		case KindEmpty:
			// No action.

		default:
			return nil, nil, false, fmt.Errorf("reconcile: unknown range kind %s", r.Kind)
		}
	}

	// Convergence rule: we converge on a round where the peer sent no
	// fingerprint requiring further narrowing and we have nothing of our
	// own left to tell them (no unresolved KindIDList containing ids the
	// peer is missing). Our outgoing frame may still contain KindEmpty
	// entries — those are harmless acks.
	done := !sawFingerprint && !sawUnresolvedIDList
	if done {
		allEmpty := true
		for _, r := range outgoing {
			if r.Kind != KindEmpty {
				allEmpty = false
				break
			}
		}
		if !allEmpty {
			done = false
		}
	}
	s.done = done
	return outgoing, newlyMissing, done, nil
}

// fingerprint computes the local fingerprint for [lo, hi) along with the
// count of ids contributing to it.
func (s *Session) fingerprint(ctx context.Context, lo, hi entmoot.MessageID) (Fingerprint, int, error) {
	ids, err := s.storage.IterIDs(ctx, lo, hi)
	if err != nil {
		return Fingerprint{}, 0, err
	}
	var acc Accumulator
	for _, id := range ids {
		acc.Add(id)
	}
	return acc.Finalize(), len(ids), nil
}

// split partitions [lo, hi) into cfg.FanoutPerRound equal-width
// sub-ranges and emits a KindFingerprint frame for each. Sub-ranges with
// zero ids still emit a frame so the peer can observe the "peer has none
// here" signal and converge.
//
// If the 256-bit width of [lo, hi) is smaller than FanoutPerRound we fall
// back to a single KindIDList frame because the range is already narrow
// enough that splitting further cannot reduce it meaningfully.
func (s *Session) split(ctx context.Context, lo, hi entmoot.MessageID) ([]Range, error) {
	loInt := new(big.Int).SetBytes(lo[:])
	var hiInt *big.Int
	if hi == zeroID {
		// Upper bound is 2^256.
		hiInt = new(big.Int).Lsh(big.NewInt(1), 256)
	} else {
		hiInt = new(big.Int).SetBytes(hi[:])
	}
	width := new(big.Int).Sub(hiInt, loInt)
	fanout := int64(s.cfg.FanoutPerRound)
	if fanout < 2 {
		fanout = 2
	}
	if width.Cmp(big.NewInt(fanout)) < 0 {
		// Too narrow to split meaningfully — emit a KindIDList leaf
		// instead. CountInRange was > LeafThreshold to get here, but
		// the keyspace is so narrow we cannot split further; the peer
		// will still learn the ids from this leaf.
		ids, err := s.storage.IterIDs(ctx, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("reconcile: narrow-split iter: %w", err)
		}
		s.resolved[rangeKey(lo, hi)] = struct{}{}
		return []Range{{
			Lo:   lo,
			Hi:   hi,
			Kind: KindIDList,
			IDs:  ids,
		}}, nil
	}

	// Equal-width chunks. The last chunk absorbs any remainder so the
	// union exactly covers [lo, hi).
	chunk := new(big.Int).Div(width, big.NewInt(fanout))
	out := make([]Range, 0, fanout)
	for i := int64(0); i < fanout; i++ {
		subLo := new(big.Int).Add(loInt, new(big.Int).Mul(chunk, big.NewInt(i)))
		var subHi *big.Int
		if i == fanout-1 {
			subHi = hiInt
		} else {
			subHi = new(big.Int).Add(loInt, new(big.Int).Mul(chunk, big.NewInt(i+1)))
		}
		r := Range{
			Lo:   bigToID(subLo),
			Hi:   bigToIDHi(subHi, hiInt),
			Kind: KindFingerprint,
		}
		fp, _, err := s.fingerprint(ctx, r.Lo, r.Hi)
		if err != nil {
			return nil, fmt.Errorf("reconcile: sub-range fingerprint: %w", err)
		}
		r.Fingerprint = fp
		out = append(out, r)
	}
	return out, nil
}

// bigToID converts a non-negative big.Int less than 2^256 to a 32-byte
// big-endian MessageID.
func bigToID(x *big.Int) entmoot.MessageID {
	var id entmoot.MessageID
	b := x.Bytes()
	if len(b) > 32 {
		b = b[len(b)-32:]
	}
	copy(id[32-len(b):], b)
	return id
}

// bigToIDHi converts the upper bound of a sub-range to a 32-byte MessageID.
// If the value equals the parent's 2^256 sentinel, we emit zeroID so the
// downstream storage layer treats it as "max".
func bigToIDHi(x, parentHi *big.Int) entmoot.MessageID {
	if x.Cmp(parentHi) == 0 && parentHi.BitLen() > 256 {
		return zeroID
	}
	if x.BitLen() > 256 {
		return zeroID
	}
	return bigToID(x)
}

// rangeKey is the concatenated 64-byte key used to memoize per-range
// resolution state across rounds.
func rangeKey(lo, hi entmoot.MessageID) [64]byte {
	var k [64]byte
	copy(k[:32], lo[:])
	copy(k[32:], hi[:])
	return k
}

// withDefaults fills in unset Config fields with DefaultConfig values so
// callers can pass a partially-initialised Config.
func withDefaults(cfg Config) Config {
	d := DefaultConfig()
	if cfg.LeafThreshold <= 0 {
		cfg.LeafThreshold = d.LeafThreshold
	}
	if cfg.MaxRounds <= 0 {
		cfg.MaxRounds = d.MaxRounds
	}
	if cfg.FanoutPerRound <= 0 {
		cfg.FanoutPerRound = d.FanoutPerRound
	}
	return cfg
}
