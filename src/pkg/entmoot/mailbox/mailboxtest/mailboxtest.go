// Package mailboxtest builds mailbox services backed by the SQLite cursor
// store production uses, so tests cannot pass against an in-memory stand-in
// that the daemon never constructs.
package mailboxtest

import (
	"testing"

	"entmoot/pkg/entmoot/events"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/store"
)

// New returns a mailbox service over st whose cursors live in a throwaway
// SQLite database. sink may be nil.
func New(tb testing.TB, st store.MessageStore, sink events.Sink) *mailbox.Service {
	tb.Helper()
	cursors, err := mailbox.OpenSQLiteCursorStore(tb.TempDir())
	if err != nil {
		tb.Fatalf("mailboxtest: open cursor store: %v", err)
	}
	tb.Cleanup(func() {
		if err := cursors.Close(); err != nil {
			tb.Errorf("mailboxtest: close cursor store: %v", err)
		}
	})
	svc, err := mailbox.NewWithCursorStore(st, cursors, sink)
	if err != nil {
		tb.Fatalf("mailboxtest: new mailbox service: %v", err)
	}
	return svc
}
