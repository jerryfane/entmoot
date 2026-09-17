package membership

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	storeFileName   = "membership.sqlite"
	lockFileName    = "membership.writer.lock"
	legacyDBName    = "roster.sqlite"
	schemaVersion   = 1
	legacyJSONLName = "roster.jsonl"
)

// ErrWriterActive means another process holds this group's writer lease.
var ErrWriterActive = errors.New("membership: writer already active")

const storeSchema = `
CREATE TABLE IF NOT EXISTS membership_meta (
  group_id       BLOB PRIMARY KEY,
  schema_version INTEGER NOT NULL,
  canonical_id   BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS membership_checkpoints (
  checkpoint_id   BLOB PRIMARY KEY,
  group_id        BLOB NOT NULL,
  sequence        INTEGER NOT NULL,
  previous_id     BLOB,
  timestamp_ms    INTEGER NOT NULL,
  canonical       INTEGER NOT NULL,
  canonical_bytes BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_membership_checkpoints_seq
  ON membership_checkpoints(group_id, sequence);
CREATE TABLE IF NOT EXISTS membership_records (
  record_id         BLOB PRIMARY KEY,
  group_id          BLOB NOT NULL,
  kind              TEXT NOT NULL,
  subject_member_id BLOB,
  timestamp_ms      INTEGER NOT NULL,
  canonical_bytes   BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_membership_records_ts
  ON membership_records(group_id, timestamp_ms);
`

// writerLease is the single-writer guard: one process at a time may mutate a
// group's membership store, so an offline command cannot race the daemon.
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
		return fmt.Errorf("membership: open writer lock %q: %w", l.path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("%w for %q", ErrWriterActive, l.path)
		}
		return fmt.Errorf("membership: lock writer %q: %w", l.path, err)
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
		return fmt.Errorf("membership: unlock writer %q: %w", l.path, unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("membership: close writer lock %q: %w", l.path, closeErr)
	}
	return nil
}

func groupDir(root string, groupID entmoot.GroupID) (string, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("membership: resolve root %q: %w", root, err)
	}
	return filepath.Join(absRoot, "groups", groupID.DirName()), nil
}

// Exists reports whether groupID has a committed membership store.
func Exists(root string, groupID entmoot.GroupID) bool {
	dir, err := groupDir(root, groupID)
	if err != nil {
		return false
	}
	path := filepath.Join(dir, storeFileName)
	if _, err := os.Stat(path); err != nil {
		return false
	}
	q := url.Values{}
	q.Set("mode", "ro")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return true
	}
	defer db.Close()
	var one int
	err = db.QueryRow(`SELECT 1 FROM membership_meta WHERE group_id = ?;`, groupID[:]).Scan(&one)
	return err == nil
}

// LegacyExists reports whether groupID still has only the linear roster chain,
// which means it awaits its first checkpoint.
func LegacyExists(root string, groupID entmoot.GroupID) bool {
	dir, err := groupDir(root, groupID)
	if err != nil {
		return false
	}
	for _, name := range []string{legacyDBName, legacyJSONLName} {
		if info, err := os.Stat(filepath.Join(dir, name)); err == nil && !info.IsDir() && info.Size() > 0 {
			return true
		}
	}
	return false
}

func openStoreDB(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		f, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if createErr != nil && !errors.Is(createErr, os.ErrExist) {
			return nil, fmt.Errorf("membership: precreate %q: %w", path, createErr)
		}
		if f != nil {
			if err := f.Close(); err != nil {
				return nil, fmt.Errorf("membership: close precreate %q: %w", path, err)
			}
		}
	} else if err != nil {
		return nil, fmt.Errorf("membership: stat %q: %w", path, err)
	}
	q := url.Values{}
	q.Set("mode", "rw")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "busy_timeout(100)")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("membership: open sqlite %q: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("membership: ping sqlite %q: %w", path, err)
	}
	if _, err := db.Exec(storeSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("membership: apply schema: %w", err)
	}
	return db, nil
}

type storedState struct {
	checkpoints []Checkpoint
	records     []Record
	canonicalID entmoot.RosterEntryID
	present     bool
}

func loadStore(ctx context.Context, db *sql.DB, groupID entmoot.GroupID) (storedState, error) {
	var out storedState
	var canonicalID []byte
	var version int
	err := db.QueryRowContext(ctx,
		`SELECT schema_version, canonical_id FROM membership_meta WHERE group_id = ?;`, groupID[:],
	).Scan(&version, &canonicalID)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("membership: read metadata: %w", err)
	}
	if version != schemaVersion {
		return out, fmt.Errorf("membership: store schema version %d, want %d", version, schemaVersion)
	}
	if len(canonicalID) != len(out.canonicalID) {
		return out, errors.New("membership: stored canonical checkpoint id is malformed")
	}
	copy(out.canonicalID[:], canonicalID)
	out.present = true

	checkpointRows, err := db.QueryContext(ctx,
		`SELECT canonical_bytes FROM membership_checkpoints WHERE group_id = ? ORDER BY sequence;`, groupID[:])
	if err != nil {
		return out, fmt.Errorf("membership: read checkpoints: %w", err)
	}
	defer checkpointRows.Close()
	for checkpointRows.Next() {
		var raw []byte
		if err := checkpointRows.Scan(&raw); err != nil {
			return out, fmt.Errorf("membership: scan checkpoint: %w", err)
		}
		var cp Checkpoint
		if err := json.Unmarshal(raw, &cp); err != nil {
			return out, fmt.Errorf("membership: decode checkpoint: %w", err)
		}
		if err := verifyStoredBytes(cp, raw); err != nil {
			return out, fmt.Errorf("membership: checkpoint %s: %w", cp.ID, err)
		}
		out.checkpoints = append(out.checkpoints, cp)
	}
	if err := checkpointRows.Err(); err != nil {
		return out, fmt.Errorf("membership: iterate checkpoints: %w", err)
	}

	recordRows, err := db.QueryContext(ctx,
		`SELECT canonical_bytes FROM membership_records WHERE group_id = ? ORDER BY timestamp_ms;`, groupID[:])
	if err != nil {
		return out, fmt.Errorf("membership: read records: %w", err)
	}
	defer recordRows.Close()
	for recordRows.Next() {
		var raw []byte
		if err := recordRows.Scan(&raw); err != nil {
			return out, fmt.Errorf("membership: scan record: %w", err)
		}
		var rec Record
		if err := json.Unmarshal(raw, &rec); err != nil {
			return out, fmt.Errorf("membership: decode record: %w", err)
		}
		if err := verifyStoredBytes(rec, raw); err != nil {
			return out, fmt.Errorf("membership: record %s: %w", rec.ID, err)
		}
		out.records = append(out.records, rec)
	}
	if err := recordRows.Err(); err != nil {
		return out, fmt.Errorf("membership: iterate records: %w", err)
	}
	return out, nil
}

// verifyStoredBytes re-encodes a decoded row and compares it to what was
// stored, so silent corruption of the store is caught on load rather than
// being projected into membership.
func verifyStoredBytes(value any, raw []byte) error {
	encoded, err := canonical.Encode(value)
	if err != nil {
		return fmt.Errorf("re-encode: %w", err)
	}
	if !bytes.Equal(encoded, raw) {
		return errors.New("stored bytes are corrupt")
	}
	return nil
}

func insertRecordTx(ctx context.Context, tx *sql.Tx, rec Record) error {
	encoded, err := canonical.Encode(rec)
	if err != nil {
		return fmt.Errorf("membership: encode record: %w", err)
	}
	var subject []byte
	if id, err := rec.SubjectMemberID(); err == nil {
		subject = id[:]
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO membership_records
		  (record_id, group_id, kind, subject_member_id, timestamp_ms, canonical_bytes)
		VALUES (?, ?, ?, ?, ?, ?);`,
		rec.ID[:], rec.GroupID[:], string(rec.Kind), subject, rec.Timestamp, encoded,
	); err != nil {
		return fmt.Errorf("membership: insert record: %w", err)
	}
	return nil
}

func insertCheckpointTx(ctx context.Context, tx *sql.Tx, cp Checkpoint, isCanonical bool) error {
	encoded, err := canonical.Encode(cp)
	if err != nil {
		return fmt.Errorf("membership: encode checkpoint: %w", err)
	}
	var previous []byte
	if cp.Previous != (entmoot.RosterEntryID{}) {
		previous = cp.Previous[:]
	}
	canonicalFlag := 0
	if isCanonical {
		canonicalFlag = 1
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO membership_checkpoints
		  (checkpoint_id, group_id, sequence, previous_id, timestamp_ms, canonical, canonical_bytes)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(checkpoint_id) DO UPDATE SET canonical = excluded.canonical;`,
		cp.ID[:], cp.GroupID[:], int64(cp.Sequence), previous, cp.Timestamp, canonicalFlag, encoded,
	); err != nil {
		return fmt.Errorf("membership: insert checkpoint: %w", err)
	}
	return nil
}

func setCanonicalTx(ctx context.Context, tx *sql.Tx, groupID entmoot.GroupID, id entmoot.RosterEntryID) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO membership_meta (group_id, schema_version, canonical_id)
		VALUES (?, ?, ?)
		ON CONFLICT(group_id) DO UPDATE SET schema_version = excluded.schema_version, canonical_id = excluded.canonical_id;`,
		groupID[:], schemaVersion, id[:],
	); err != nil {
		return fmt.Errorf("membership: set canonical checkpoint: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE membership_checkpoints SET canonical = CASE WHEN checkpoint_id = ? THEN 1 ELSE 0 END WHERE group_id = ?;`,
		id[:], groupID[:],
	); err != nil {
		return fmt.Errorf("membership: mark canonical checkpoint: %w", err)
	}
	return nil
}

// deleteRecordsThroughTx retires records a checkpoint covers, inclusive of its
// own timestamp: a checkpoint accounts for every record up to and including the
// newest one it folded in.
func deleteRecordsThroughTx(ctx context.Context, tx *sql.Tx, groupID entmoot.GroupID, timestamp int64) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM membership_records WHERE group_id = ? AND timestamp_ms <= ?;`, groupID[:], timestamp,
	); err != nil {
		return fmt.Errorf("membership: retire records: %w", err)
	}
	return nil
}

func deleteCheckpointTx(ctx context.Context, tx *sql.Tx, id entmoot.RosterEntryID) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM membership_checkpoints WHERE checkpoint_id = ?;`, id[:],
	); err != nil {
		return fmt.Errorf("membership: delete checkpoint: %w", err)
	}
	return nil
}
