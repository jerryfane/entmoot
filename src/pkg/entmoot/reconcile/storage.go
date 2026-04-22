package reconcile

import (
	"context"

	"entmoot/pkg/entmoot"
)

// Storage is the read-only view a Session requires over the local set of
// message ids. All methods use half-open ranges [lo, hi) in byte-lexicographic
// order over the 32-byte id space.
//
// The all-zero MessageID is a sentinel for "max" in the hi position:
// callers pass hi == entmoot.MessageID{} to mean "every id greater than or
// equal to lo". Implementations must treat that value specially rather
// than literally (a literal all-zero hi would otherwise match nothing,
// since lo <= id < 0 has no solutions).
type Storage interface {
	// IterIDs returns the ids currently stored in [lo, hi), in ascending
	// byte-lexicographic order. Implementations should make a defensive
	// copy so the caller can safely retain the result.
	IterIDs(ctx context.Context, lo, hi entmoot.MessageID) ([]entmoot.MessageID, error)

	// CountInRange returns the count of ids currently stored in [lo, hi).
	// It is a convenience for the Session's fingerprint-vs-leaf decision
	// and may be implemented as len(IterIDs(...)).
	CountInRange(ctx context.Context, lo, hi entmoot.MessageID) (int, error)

	// HasID reports whether id is present in local storage. It is used by
	// the KindIDList handler to compute "peer has, we don't" and
	// "we have, peer doesn't" set differences without repeatedly
	// materialising the full range.
	HasID(ctx context.Context, id entmoot.MessageID) (bool, error)
}
