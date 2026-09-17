package esphttp

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// TestOpenRetiresFleetTables proves the removed feature's tables are dropped
// from a database written by an older binary, not merely absent from new ones.
func TestOpenRetiresFleetTables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "esp.sqlite")
	legacy, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	for _, table := range RetiredTables {
		if _, err := legacy.Exec(`CREATE TABLE ` + table + ` (id TEXT PRIMARY KEY, note TEXT)`); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
		if _, err := legacy.Exec(`INSERT INTO `+table+` VALUES (?, ?)`, "row-1", "stale fleet state"); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	store, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()

	probe, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatalf("open probe: %v", err)
	}
	defer probe.Close()
	for _, table := range RetiredTables {
		var n int
		if err := probe.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != 0 {
			t.Fatalf("%s survived the open", table)
		}
	}
}

// TestRetireFleetTablesIsBoundedUnderAContendedLock pins the cost of the
// retirement itself. Dropping the tables needs the write lock; an unrelated
// writer holding it must not stall the ESP start, so the attempt is bounded and
// gives up, leaving the tables for a later open.
//
// This calls retireRemovedFeatureTables directly rather than going through
// OpenSQLiteStateStore, because that path already waits the full busy_timeout
// for an unrelated reason: deleteExpiredIdempotency runs at open and writes
// (mobile.go, "_, _ = store.deleteExpiredIdempotency"). Measuring the whole
// open would credit this code with a stall it does not cause, or hide one it
// does. See issue #131 for the pre-existing open-path wait.
func TestRetireFleetTablesIsBoundedUnderAContendedLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "esp.sqlite")
	first, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	legacy, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	defer legacy.Close()
	if _, err := legacy.Exec(`CREATE TABLE esp_fleets (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("recreate esp_fleets: %v", err)
	}

	holder, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(250)")
	if err != nil {
		t.Fatalf("open holder: %v", err)
	}
	defer holder.Close()
	tx, err := holder.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO esp_fleets VALUES ('lock')`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("take write lock: %v", err)
	}

	target, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("open target: %v", err)
	}
	defer target.Close()
	started := time.Now()
	retireRemovedFeatureTables(target, path)
	waited := time.Since(started)
	if waited > 2*time.Second {
		_ = tx.Rollback()
		t.Fatalf("retirement waited %s under contention, want it bounded", waited)
	}
	var n int
	if err := target.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='esp_fleets'`).Scan(&n); err != nil {
		_ = tx.Rollback()
		t.Fatalf("count while locked: %v", err)
	}
	if n != 1 {
		_ = tx.Rollback()
		t.Fatalf("esp_fleets = %d rows in sqlite_master, want it still present after a refused drop", n)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// The busy_timeout must be back to the open path's value, or every later
	// query on this handle would inherit the retirement's short timeout.
	var ms int
	if err := target.QueryRow(`PRAGMA busy_timeout`).Scan(&ms); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if ms != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000 (the retirement's short timeout leaked)", ms)
	}

	// Uncontended, a later call finishes the job.
	retireRemovedFeatureTables(target, path)
	if err := target.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='esp_fleets'`).Scan(&n); err != nil {
		t.Fatalf("count after retry: %v", err)
	}
	if n != 0 {
		t.Fatalf("esp_fleets survived an uncontended retirement")
	}
}
