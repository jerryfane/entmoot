package store

import (
	"testing"
)

// hiddenNativeStore forwards MessageStore and nothing else. It mirrors what
// the daemon really passes to the mailbox: cmd/entmootd's notifyingStore wraps
// the SQLite store to broadcast Puts and implements MessageStore plus the
// paging helpers, but not SearchMessages or MessageContext. A wrapped store
// therefore takes the scan path in SearchMessages and MessageContext, so the
// scan code below is production behaviour, not a legacy stand-in.
type hiddenNativeStore struct {
	MessageStore
}

// TestWrappedStoreTakesTheScanPath pins the dispatch these suites depend on.
// If a wrapper ever satisfies the native interfaces, the scan arms below stop
// testing the scan and this test says so instead of going quietly green.
func TestWrappedStoreTakesTheScanPath(t *testing.T) {
	var wrapped MessageStore = hiddenNativeStore{MessageStore: mustOpenSQLite(t)}
	if _, ok := wrapped.(MessageSearcher); ok {
		t.Fatal("wrapped store satisfies MessageSearcher, so the scan search arm no longer tests the scan")
	}
	if _, ok := wrapped.(MessageContexter); ok {
		t.Fatal("wrapped store satisfies MessageContexter, so the scan context arm no longer tests the scan")
	}

	// The native store must still be chosen when it is not wrapped, otherwise
	// the sqlite arms would be testing the scan twice.
	var direct MessageStore = mustOpenSQLite(t)
	if _, ok := direct.(MessageSearcher); !ok {
		t.Fatal("SQLite does not satisfy MessageSearcher; the indexed arm tests nothing")
	}
	if _, ok := direct.(MessageContexter); !ok {
		t.Fatal("SQLite does not satisfy MessageContexter; the native context arm tests nothing")
	}
}
