package gossip

import (
	"context"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/reconcile"
	"entmoot/pkg/entmoot/store"
)

// storeAdapter satisfies the reconcile.Storage interface over an
// entmoot/pkg/entmoot/store.MessageStore scoped to a single GroupID. It is
// the glue that lets reconcile.Session query the gossiper's underlying
// message store without the session package taking a dependency on store.
//
// The adapter is stateless aside from its two fields; creating one per
// session is cheap and keeps the store/GroupID binding explicit. (v1.2.1)
type storeAdapter struct {
	store   store.MessageStore
	groupID entmoot.GroupID
}

// IterIDs returns every message id currently held for groupID whose 32-byte
// identifier falls in the half-open range [lo, hi). hi == zero MessageID is
// the "unbounded upper" sentinel (see store.IterMessageIDsInIDRange).
//
// The result is sorted ascending by byte order — matching the ordering the
// reconcile.Session's fingerprint accumulator expects.
func (s *storeAdapter) IterIDs(ctx context.Context, lo, hi entmoot.MessageID) ([]entmoot.MessageID, error) {
	return s.store.IterMessageIDsInIDRange(ctx, s.groupID, lo, hi)
}

// CountInRange returns the number of ids currently stored in [lo, hi). It
// is implemented on top of IterIDs so the adapter stays small; the store's
// SQLite backend already services the underlying range query via an index
// (see TestSQLiteIterMessageIDsInIDRangeUsesIndex), so materialising the
// slice just to take len is cheap relative to the reconcile round-trip.
func (s *storeAdapter) CountInRange(ctx context.Context, lo, hi entmoot.MessageID) (int, error) {
	ids, err := s.store.IterMessageIDsInIDRange(ctx, s.groupID, lo, hi)
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

// HasID reports whether id is present in local storage for groupID. It
// delegates to store.MessageStore.Has, which is O(1) for the Memory and
// SQLite implementations.
func (s *storeAdapter) HasID(ctx context.Context, id entmoot.MessageID) (bool, error) {
	return s.store.Has(ctx, s.groupID, id)
}

// Compile-time check: *storeAdapter satisfies reconcile.Storage.
var _ reconcile.Storage = (*storeAdapter)(nil)
