package esphttp

import (
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

// TestConcurrentOpenSurvivesSchemaMigration reproduces a production crash. The
// ESP bridge and the daemon both open the same esp.sqlite, so on a
// simultaneous restart both read the schema, both find a column missing, and
// the loser of the ALTER used to die on "duplicate column name". On
// 2026-09-17 that took the live ESP down for five seconds; only
// Restart=always brought it back.
func TestConcurrentOpenSurvivesSchemaMigration(t *testing.T) {
	root := t.TempDir()

	// Seed a store, then drop the columns later releases added, so every
	// opener below has real migration work to do. Without this the openers
	// race over a no-op and the test proves nothing.
	seed, err := OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, "esp.sqlite"))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	for _, stmt := range []string{
		`ALTER TABLE esp_open_invites DROP COLUMN no_fallback_peers`,
		`ALTER TABLE esp_open_invites DROP COLUMN bootstrap_peers`,
		`ALTER TABLE esp_open_invite_redemptions DROP COLUMN result`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("age the schema (%s): %v", stmt, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	const openers = 8
	var wg sync.WaitGroup
	errs := make([]error, openers)
	start := make(chan struct{})
	for i := 0; i < openers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			store, err := OpenSQLiteStateStore(root)
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = store.Close()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("opener %d failed while another migrated the same database: %v", i, err)
		}
	}
	reopened, err := OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatalf("reopen after the race: %v", err)
	}
	defer reopened.Close()
	// And the columns really are back, so the race was survived by migrating
	// rather than by skipping the work.
	cols, err := tableColumns(reopened.db, "esp_open_invites")
	if err != nil {
		t.Fatalf("inspect schema: %v", err)
	}
	for _, want := range []string{"no_fallback_peers", "bootstrap_peers"} {
		if !cols[want] {
			t.Fatalf("column %s is missing after the race: the migration was skipped, not completed", want)
		}
	}
}

// TestDuplicateColumnIsMatchedNarrowly pins that only the one error meaning
// "another writer already added this column" is swallowed. Treating any ALTER
// failure as success would hide a genuinely broken schema, which is worse than
// the crash this fix removes.
func TestDuplicateColumnIsMatchedNarrowly(t *testing.T) {
	if !isDuplicateColumn(errors.New("SQL logic error: duplicate column name: no_fallback_peers (1)"), "no_fallback_peers") {
		t.Fatal("the production error text was not recognised")
	}
	if isDuplicateColumn(errors.New("duplicate column name: some_other_column (1)"), "no_fallback_peers") {
		t.Fatal("a duplicate report for a DIFFERENT column was accepted as ours")
	}
	// This schema really contains result, publish_result and
	// operation_result, so an unanchored match would read one as another.
	if isDuplicateColumn(errors.New("SQL logic error: duplicate column name: operation_result (1)"), "result") {
		t.Fatal("a duplicate report for operation_result was accepted as one for result")
	}
	if !isDuplicateColumn(errors.New("SQL logic error: duplicate column name: result (1)"), "result") {
		t.Fatal("the report for result itself was rejected")
	}
	if isDuplicateColumn(errors.New("database or disk is full"), "no_fallback_peers") {
		t.Fatal("an unrelated failure was swallowed")
	}
	if isDuplicateColumn(nil, "no_fallback_peers") {
		t.Fatal("no error must not read as a duplicate")
	}
	// Boundary cases that defeated a byte-classifier: a multi-byte name and
	// one containing "$", which SQLite accepts in an identifier.
	if isDuplicateColumn(errors.New("SQL logic error: duplicate column name: caf\u00e9 (1)"), "caf") {
		t.Fatal("a multi-byte sibling name was accepted as a shorter one")
	}
	if !isDuplicateColumn(errors.New("SQL logic error: duplicate column name: caf\u00e9 (1)"), "caf\u00e9") {
		t.Fatal("a multi-byte name was rejected for itself")
	}
	if isDuplicateColumn(errors.New("SQL logic error: duplicate column name: a$b (1)"), "a") {
		t.Fatal("a name continuing with $ was accepted as a shorter one")
	}
	if !isDuplicateColumn(errors.New("SQL logic error: duplicate column name: a$b (1)"), "a$b") {
		t.Fatal("a name containing $ was rejected for itself")
	}
	// A quoted identifier must fail closed: the duplicate is re-raised rather
	// than swallowed.
	if isDuplicateColumn(errors.New(`SQL logic error: duplicate column name: "two words" (1)`), `"two words"`) {
		t.Fatal("a quoted identifier matched; it must fail closed so the error surfaces")
	}
}

// TestAddStateColumnIsIdempotentAndOnlyAdds pins the helper deterministically,
// without depending on goroutine interleaving: adding twice must succeed, and
// anything that is not an ADD COLUMN must be refused rather than have its
// duplicate error swallowed. A RENAME COLUMN reports the same duplicate text,
// so a future rename routed through here would otherwise be skipped silently
// while reporting success.
func TestAddStateColumnIsIdempotentAndOnlyAdds(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "probe.sqlite"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, spare TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	const add = `ALTER TABLE t ADD COLUMN result BLOB`
	if err := addStateColumn(db, "t", "result", add); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := addStateColumn(db, "t", "result", add); err != nil {
		t.Fatalf("second add must be a no-op, got: %v", err)
	}

	// A genuine failure must still surface.
	if err := addStateColumn(db, "t", "x", `ALTER TABLE missing_table ADD COLUMN x BLOB`); err == nil {
		t.Fatal("adding to a missing table reported success")
	}

	// And a rename, whose duplicate error means something else entirely.
	rename := `ALTER TABLE t RENAME COLUMN spare TO result`
	if err := addStateColumn(db, "t", "result", rename); err == nil {
		t.Fatal("a RENAME COLUMN was accepted, so its duplicate error would be swallowed")
	}
	cols, err := tableColumns(db, "t")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !cols["spare"] {
		t.Fatal("the refused rename was executed anyway")
	}
}
