// Package storetest opens throwaway SQLite message stores for tests.
//
// Production has exactly one MessageStore implementation, store.SQLite, so
// tests exercise that one rather than a second in-memory stand-in whose
// behaviour could drift from it.
package storetest

import (
	"testing"

	"entmoot/pkg/entmoot/store"
)

// New opens a SQLite message store under the test's temporary directory and
// closes it when the test finishes.
func New(tb testing.TB) *store.SQLite {
	tb.Helper()
	s, err := store.OpenSQLite(tb.TempDir())
	if err != nil {
		tb.Fatalf("storetest: open sqlite store: %v", err)
	}
	tb.Cleanup(func() {
		if err := s.Close(); err != nil {
			tb.Errorf("storetest: close sqlite store: %v", err)
		}
	})
	return s
}
