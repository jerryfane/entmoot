package libp2ptransport

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"sync"
	"testing"

	"entmoot/pkg/entmoot"
)

// Several processes - the daemon, the CLI, the ESP - open the same ledger,
// and the first open after an upgrade adds a column. Two of them doing that
// at once must both succeed; the loser of the race used to fail with
// "duplicate column name".
func TestConcurrentOpensMigrateTheInviteLedgerOnce(t *testing.T) {
	for _, existing := range []bool{false, true} {
		dir := t.TempDir()
		if existing {
			// A ledger written before minted_at_ms existed.
			q := url.Values{}
			q.Add("_pragma", "journal_mode(WAL)")
			q.Add("_pragma", "busy_timeout(5000)")
			db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "bootstrap-admission.db")+"?"+q.Encode())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`CREATE TABLE bootstrap_invites (
				group_id BLOB NOT NULL, nonce BLOB NOT NULL, target_member_id BLOB,
				max_uses INTEGER NOT NULL DEFAULT 1, issued_at_ms INTEGER NOT NULL DEFAULT 0,
				expires_at_ms INTEGER NOT NULL DEFAULT 0, revoked_at_ms INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (group_id, nonce))`); err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
		}
		const opens = 80
		var wg sync.WaitGroup
		errs := make(chan error, opens)
		for range opens {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ledger, err := OpenInviteLedger(dir)
				if err != nil {
					errs <- err
					return
				}
				errs <- ledger.Close()
			}()
		}
		wg.Wait()
		close(errs)
		failed := 0
		var first error
		for err := range errs {
			if err != nil {
				if first == nil {
					first = err
				}
				failed++
			}
		}
		if failed > 0 {
			t.Fatalf("existing=%t: %d of %d concurrent opens failed, first: %v", existing, failed, opens, first)
		}
		ledger, err := OpenInviteLedger(dir)
		if err != nil {
			t.Fatal(err)
		}
		capability := BootstrapCapability{GroupID: entmoot.GroupID{1}, IssuedAtMS: 1, ExpiresAtMS: 2}
		if err := ledger.RecordIssuedInvite(capability); err != nil {
			t.Fatalf("existing=%t: record after migration: %v", existing, err)
		}
		_ = ledger.Close()
	}
}

// When this node first saw a removal is set once and never moved.
func TestRemovalsSeenAtKeepsTheFirstSighting(t *testing.T) {
	ledger, err := OpenInviteLedger(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	gid := entmoot.GroupID{2}
	a, b := entmoot.RosterEntryID{1}, entmoot.RosterEntryID{2}
	first, err := ledger.RemovalsSeenAt(gid, []entmoot.RosterEntryID{a}, 100)
	if err != nil || first[a] != 100 {
		t.Fatalf("first sighting = %v, %v", first, err)
	}
	again, err := ledger.RemovalsSeenAt(gid, []entmoot.RosterEntryID{a, b}, 200)
	if err != nil || again[a] != 100 || again[b] != 200 {
		t.Fatalf("second sighting = %v, %v; want a kept at 100, b at 200", again, err)
	}
}
