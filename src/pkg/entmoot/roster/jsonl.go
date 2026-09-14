package roster

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const (
	rosterFileName     = "roster.jsonl"
	rosterDBFileName   = "roster.sqlite"
	rosterLockFileName = "roster.writer.lock"
)

var ErrWriterActive = errors.New("roster: writer already active")

const rosterSchema = `
CREATE TABLE IF NOT EXISTS roster_meta (
  group_id        BLOB PRIMARY KEY,
  version         INTEGER NOT NULL,
  head_id         BLOB NOT NULL,
  import_complete INTEGER NOT NULL CHECK (import_complete = 1)
);
CREATE TABLE IF NOT EXISTS roster_entries (
  entry_id        BLOB PRIMARY KEY,
  group_id        BLOB NOT NULL,
  sequence        INTEGER NOT NULL,
  parent_id       BLOB,
  canonical_bytes BLOB NOT NULL,
  op              TEXT NOT NULL,
  timestamp_ms    INTEGER NOT NULL,
  UNIQUE (group_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_roster_entries_group_sequence
  ON roster_entries(group_id, sequence);
`

type writerLease struct {
	mu   sync.Mutex
	path string
	file *os.File
}

func (l *writerLease) claim() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return nil
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("roster: open writer lock %q: %w", l.path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("%w for %q", ErrWriterActive, l.path)
		}
		return fmt.Errorf("roster: lock writer %q: %w", l.path, err)
	}
	l.file = f
	return nil
}

func (l *writerLease) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	f := l.file
	l.file = nil
	unlockErr := unix.Flock(int(f.Fd()), unix.LOCK_UN)
	closeErr := f.Close()
	if unlockErr != nil {
		return fmt.Errorf("roster: unlock writer %q: %w", l.path, unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("roster: close writer lock %q: %w", l.path, closeErr)
	}
	return nil
}

// OpenJSONL opens the transactional roster store for groupID. The legacy
// roster.jsonl is treated as an immutable import source: a first open validates
// its complete canonical chain before committing it to roster.sqlite. Readers
// do not take the writer lease. Mutation acquires the lease non-blockingly and
// holds it until Close; daemons should call ClaimWriter during startup.
func OpenJSONL(root string, groupID entmoot.GroupID) (*RosterLog, error) {
	if root == "" {
		return nil, errors.New("roster: OpenJSONL root path is empty")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("roster: resolve root %q: %w", root, err)
	}
	groupDir := filepath.Join(absRoot, "groups", encodeGroupDirName(groupID))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		return nil, fmt.Errorf("roster: mkdir %q: %w", groupDir, err)
	}
	db, err := openRosterDB(filepath.Join(groupDir, rosterDBFileName))
	if err != nil {
		return nil, err
	}
	legacyPath := filepath.Join(groupDir, rosterFileName)
	if err := importLegacyJSONL(context.Background(), db, groupID, legacyPath); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("roster: import %q: %w", legacyPath, err)
	}

	r := New(groupID)
	if err := loadSQLite(r, db, groupID); err != nil {
		_ = db.Close()
		return nil, err
	}
	lease := &writerLease{path: filepath.Join(groupDir, rosterLockFileName)}
	r.claimWriter = lease.claim
	r.persist = func(entry entmoot.RosterEntry) error {
		return persistRosterEntry(context.Background(), db, groupID, entry)
	}
	r.replace = func(chain []entmoot.RosterEntry) error {
		return replaceRosterChain(context.Background(), db, groupID, chain)
	}
	r.storedChain = func() ([]entmoot.RosterEntry, error) {
		return readRosterChain(context.Background(), db, groupID)
	}
	r.closeFn = func() error {
		leaseErr := lease.close()
		dbErr := db.Close()
		if leaseErr != nil {
			return leaseErr
		}
		if dbErr != nil {
			return fmt.Errorf("roster: close sqlite: %w", dbErr)
		}
		return nil
	}
	return r, nil
}

// Exists reports whether groupID has a committed SQLite roster or a non-empty
// legacy JSONL source. An unreadable SQLite file counts as present so callers
// proceed to OpenJSONL and surface its diagnostic instead of hiding it.
func Exists(root string, groupID entmoot.GroupID) bool {
	groupDir := filepath.Join(root, "groups", encodeGroupDirName(groupID))
	if info, err := os.Stat(filepath.Join(groupDir, rosterFileName)); err == nil && !info.IsDir() && info.Size() > 0 {
		return true
	}
	dbPath := filepath.Join(groupDir, rosterDBFileName)
	if _, err := os.Stat(dbPath); err != nil {
		return false
	}
	q := url.Values{}
	q.Set("mode", "ro")
	db, err := sql.Open("sqlite", "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return true
	}
	defer db.Close()
	var hasMetaTable int
	if err := db.QueryRow(`
		SELECT EXISTS(
		  SELECT 1 FROM sqlite_master
		  WHERE type = 'table' AND name = 'roster_meta'
		);`,
	).Scan(&hasMetaTable); err != nil {
		return true
	}
	if hasMetaTable == 0 {
		return false
	}
	var one int
	err = db.QueryRow(`SELECT 1 FROM roster_meta WHERE group_id = ? AND import_complete = 1;`, groupID[:]).Scan(&one)
	return err == nil || !errors.Is(err, sql.ErrNoRows)
}

func encodeGroupDirName(gid entmoot.GroupID) string {
	return base64.RawURLEncoding.EncodeToString(gid[:])
}

func openRosterDB(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		f, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if createErr != nil && !errors.Is(createErr, os.ErrExist) {
			return nil, fmt.Errorf("roster: precreate %q: %w", path, createErr)
		}
		if f != nil {
			if err := f.Close(); err != nil {
				return nil, fmt.Errorf("roster: close precreate %q: %w", path, err)
			}
		}
	} else if err != nil {
		return nil, fmt.Errorf("roster: stat %q: %w", path, err)
	}
	q := url.Values{}
	q.Set("mode", "rw")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "busy_timeout(100)")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("roster: open sqlite %q: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("roster: ping sqlite %q: %w", path, err)
	}
	if err := migrateRosterDB(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("roster: migrate sqlite: %w", err)
	}
	if _, err := db.Exec(rosterSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("roster: apply schema: %w", err)
	}
	return db, nil
}

func migrateRosterDB(db *sql.DB) error {
	var legacyColumns int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM pragma_table_info('roster_meta')
		WHERE name = 'founder_node_id';`,
	).Scan(&legacyColumns); err != nil {
		return err
	}
	if legacyColumns == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`
		DROP TABLE IF EXISTS roster_meta_new;
		DROP TABLE IF EXISTS roster_entries_new;
		CREATE TABLE roster_meta_new (
		  group_id BLOB PRIMARY KEY,
		  version INTEGER NOT NULL,
		  head_id BLOB NOT NULL,
		  import_complete INTEGER NOT NULL CHECK (import_complete = 1)
		);
		INSERT INTO roster_meta_new (group_id, version, head_id, import_complete)
		  SELECT group_id, version, head_id, import_complete FROM roster_meta;
		CREATE TABLE roster_entries_new (
		  entry_id BLOB PRIMARY KEY,
		  group_id BLOB NOT NULL,
		  sequence INTEGER NOT NULL,
		  parent_id BLOB,
		  canonical_bytes BLOB NOT NULL,
		  op TEXT NOT NULL,
		  timestamp_ms INTEGER NOT NULL,
		  UNIQUE (group_id, sequence)
		);
		INSERT INTO roster_entries_new
		  (entry_id, group_id, sequence, parent_id, canonical_bytes, op, timestamp_ms)
		  SELECT entry_id, group_id, sequence, parent_id, canonical_bytes, op, timestamp_ms
		  FROM roster_entries;
		DROP INDEX IF EXISTS idx_roster_entries_group_sequence;
		DROP TABLE IF EXISTS roster_members;
		DROP TABLE roster_entries;
		DROP TABLE roster_meta;
		ALTER TABLE roster_entries_new RENAME TO roster_entries;
		ALTER TABLE roster_meta_new RENAME TO roster_meta;
		CREATE INDEX idx_roster_entries_group_sequence
		  ON roster_entries(group_id, sequence);
	`); err != nil {
		return err
	}
	return tx.Commit()
}

func importLegacyJSONL(ctx context.Context, db *sql.DB, groupID entmoot.GroupID, path string) error {
	var imported int
	err := db.QueryRowContext(ctx, `SELECT import_complete FROM roster_meta WHERE group_id = ?;`, groupID[:]).Scan(&imported)
	if err == nil {
		if imported != 1 {
			return errors.New("roster metadata has incomplete import marker")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read import marker: %w", err)
	}
	entries, err := readAndValidateLegacy(path, groupID)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin import: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, entry := range entries {
		if err := persistRosterEntryTx(ctx, tx, groupID, entry); err != nil {
			return fmt.Errorf("commit imported entry %s: %w", entry.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit import: %w", err)
	}
	return nil
}

func readAndValidateLegacy(path string, groupID entmoot.GroupID) ([]entmoot.RosterEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	if info.Size() > 0 {
		var final [1]byte
		if _, err := f.ReadAt(final[:], info.Size()-1); err != nil {
			return nil, fmt.Errorf("read final byte: %w", err)
		}
		if final[0] != '\n' {
			return nil, errors.New("truncated legacy log: final entry has no newline")
		}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind: %w", err)
	}

	candidate := New(groupID)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	var entries []entmoot.RosterEntry
	for lineNo := 1; scanner.Scan(); lineNo++ {
		raw := append([]byte(nil), scanner.Bytes()...)
		if len(raw) == 0 {
			return nil, fmt.Errorf("line %d: empty entry", lineNo)
		}
		var entry entmoot.RosterEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("line %d: malformed JSON: %w", lineNo, err)
		}
		canonicalBytes, err := canonical.Encode(entry)
		if err != nil {
			return nil, fmt.Errorf("line %d: canonical encode: %w", lineNo, err)
		}
		if !bytes.Equal(canonicalBytes, raw) {
			return nil, fmt.Errorf("line %d: entry is not exact canonical JSON", lineNo)
		}
		candidate.mu.Lock()
		if len(candidate.entries) == 0 {
			err = validateGenesis(entry, groupID)
			if err == nil {
				candidate.founder = entry.Subject
				candidate.applyLocked(entry)
			}
		} else {
			err = candidate.validateLocked(entry)
			if err == nil {
				candidate.applyLocked(entry)
			}
		}
		candidate.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return entries, nil
}

// ValidateLegacyJSONL validates an immutable legacy roster without importing
// or modifying it. Conversion uses this during preflight.
func ValidateLegacyJSONL(path string, groupID entmoot.GroupID) ([]entmoot.RosterEntry, error) {
	return readAndValidateLegacy(path, groupID)
}

// ValidateEntries verifies a decoded roster chain in order without persisting
// it. The caller remains responsible for checking its stored canonical bytes.
func ValidateEntries(groupID entmoot.GroupID, entries []entmoot.RosterEntry) error {
	candidate := New(groupID)
	for i, entry := range entries {
		candidate.mu.Lock()
		var err error
		if len(candidate.entries) == 0 {
			err = validateGenesis(entry, groupID)
			if err == nil {
				candidate.founder = entry.Subject
				candidate.applyLocked(entry)
			}
		} else {
			err = candidate.validateLocked(entry)
			if err == nil {
				candidate.applyLocked(entry)
			}
		}
		candidate.mu.Unlock()
		if err != nil {
			return fmt.Errorf("entry %d: %w", i+1, err)
		}
	}
	return nil
}

func loadSQLite(r *RosterLog, db *sql.DB, groupID entmoot.GroupID) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("roster: begin load snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		version    int
		storedHead []byte
		imported   int
	)
	err = tx.QueryRowContext(ctx, `
		SELECT version, head_id, import_complete
		FROM roster_meta WHERE group_id = ?;`, groupID[:],
	).Scan(&version, &storedHead, &imported)
	if errors.Is(err, sql.ErrNoRows) {
		var entryCount int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM roster_entries WHERE group_id = ?;`, groupID[:]).Scan(&entryCount); err != nil {
			return fmt.Errorf("roster: inspect empty sqlite state: %w", err)
		}
		if entryCount != 0 {
			return errors.New("roster: sqlite entries exist without committed metadata")
		}
		return tx.Commit()
	}
	if err != nil {
		return fmt.Errorf("roster: load sqlite metadata: %w", err)
	}
	if imported != 1 {
		return errors.New("roster: sqlite import is not complete")
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT canonical_bytes FROM roster_entries
		WHERE group_id = ? ORDER BY sequence;`, groupID[:])
	if err != nil {
		return fmt.Errorf("roster: load sqlite entries: %w", err)
	}
	r.mu.Lock()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			r.mu.Unlock()
			_ = rows.Close()
			return fmt.Errorf("roster: scan sqlite entry: %w", err)
		}
		var entry entmoot.RosterEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			r.mu.Unlock()
			_ = rows.Close()
			return fmt.Errorf("roster: decode sqlite entry: %w", err)
		}
		encoded, err := canonical.Encode(entry)
		if err != nil || !bytes.Equal(encoded, raw) {
			r.mu.Unlock()
			_ = rows.Close()
			return errors.New("roster: sqlite entry canonical bytes are corrupt")
		}
		if len(r.entries) == 0 {
			if err := validateGenesis(entry, r.groupID); err != nil {
				r.mu.Unlock()
				_ = rows.Close()
				return fmt.Errorf("roster: validate sqlite genesis: %w", err)
			}
			r.founder = entry.Subject
		} else if err := r.validateLocked(entry); err != nil {
			r.mu.Unlock()
			_ = rows.Close()
			return fmt.Errorf("roster: validate sqlite entry %s: %w", entry.ID, err)
		}
		r.applyLocked(entry)
	}
	if err := rows.Err(); err != nil {
		r.mu.Unlock()
		_ = rows.Close()
		return fmt.Errorf("roster: iterate sqlite entries: %w", err)
	}
	if err := rows.Close(); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("roster: close sqlite entries: %w", err)
	}
	if len(r.entries) != version || !bytes.Equal(storedHead, r.head[:]) {
		r.mu.Unlock()
		return errors.New("roster: sqlite metadata does not match validated entry chain")
	}
	r.mu.Unlock()
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("roster: finish load snapshot: %w", err)
	}
	return nil
}

func persistRosterEntry(ctx context.Context, db *sql.DB, groupID entmoot.GroupID, entry entmoot.RosterEntry) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := persistRosterEntryTx(ctx, tx, groupID, entry); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func persistRosterEntryTx(ctx context.Context, tx *sql.Tx, groupID entmoot.GroupID, entry entmoot.RosterEntry) error {
	encoded, err := canonical.Encode(entry)
	if err != nil {
		return fmt.Errorf("canonical encode: %w", err)
	}
	var version int64
	var currentHead []byte
	err = tx.QueryRowContext(ctx, `
		SELECT version, head_id
		FROM roster_meta WHERE group_id = ?;`, groupID[:],
	).Scan(&version, &currentHead)
	if errors.Is(err, sql.ErrNoRows) {
		if len(entry.Parents) != 0 {
			return fmt.Errorf("%w: persistent roster is empty", entmoot.ErrRosterReject)
		}
	} else if err != nil {
		return fmt.Errorf("read head: %w", err)
	} else {
		if len(entry.Parents) != 1 || !bytes.Equal(currentHead, entry.Parents[0][:]) {
			return fmt.Errorf("%w: persistent head changed", entmoot.ErrRosterReject)
		}
	}
	var parent any
	if len(entry.Parents) == 1 {
		parent = entry.Parents[0][:]
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO roster_entries
		  (entry_id, group_id, sequence, parent_id, canonical_bytes, op, timestamp_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?);`,
		entry.ID[:], groupID[:], version+1, parent, encoded, entry.Op, entry.Timestamp,
	); err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO roster_meta
		  (group_id, version, head_id, import_complete)
		VALUES (?, ?, ?, 1)
		ON CONFLICT(group_id) DO UPDATE SET
		  version = excluded.version,
		  head_id = excluded.head_id,
		  import_complete = 1;`,
		groupID[:], version+1, entry.ID[:],
	); err != nil {
		return fmt.Errorf("advance head: %w", err)
	}
	return nil
}

// replaceRosterChain rewrites one group's committed chain in a single
// transaction. The caller has already validated the chain; this is the durable
// half of RosterLog.ReplaceChain, so a crash either leaves the previous chain
// or lands the new one whole.
func replaceRosterChain(ctx context.Context, db *sql.DB, groupID entmoot.GroupID, chain []entmoot.RosterEntry) error {
	if len(chain) == 0 {
		return errors.New("roster: replacement chain is empty")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM roster_entries WHERE group_id = ?;`, groupID[:]); err != nil {
		return fmt.Errorf("clear entries: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM roster_meta WHERE group_id = ?;`, groupID[:]); err != nil {
		return fmt.Errorf("clear metadata: %w", err)
	}
	for _, entry := range chain {
		if err := persistRosterEntryTx(ctx, tx, groupID, entry); err != nil {
			return fmt.Errorf("commit replacement entry %s: %w", entry.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit replace: %w", err)
	}
	return nil
}

// readRosterChain returns the committed chain in sequence order, decoded from
// its stored canonical bytes. Repair uses it to find out what the store
// actually holds after a write whose outcome is unknown.
func readRosterChain(ctx context.Context, db *sql.DB, groupID entmoot.GroupID) ([]entmoot.RosterEntry, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT canonical_bytes FROM roster_entries
		WHERE group_id = ? ORDER BY sequence;`, groupID[:])
	if err != nil {
		return nil, fmt.Errorf("read chain: %w", err)
	}
	defer rows.Close()
	var out []entmoot.RosterEntry
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan entry: %w", err)
		}
		var entry entmoot.RosterEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("decode entry: %w", err)
		}
		encoded, err := canonical.Encode(entry)
		if err != nil || !bytes.Equal(encoded, raw) {
			return nil, errors.New("stored entry canonical bytes are corrupt")
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate entries: %w", err)
	}
	return out, nil
}
