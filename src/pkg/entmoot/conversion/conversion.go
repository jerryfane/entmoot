// Package conversion performs the one-way Pilot-to-libp2p data-root upgrade.
package conversion

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/order"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"

	_ "modernc.org/sqlite"
)

// Stage names are durable and ordered. Never reorder or reuse them.
type Stage string

const (
	StagePreflight                  Stage = "preflight"
	StageVerifiedBackup             Stage = "verified_backup"
	StageLegacyImported             Stage = "legacy_imported"
	StageUpgradeCheckpointCommitted Stage = "upgrade_checkpoint_committed"
	StageOperationalSchemaCommitted Stage = "operational_schema_committed"
	StageComplete                   Stage = "complete"
)

var orderedStages = []Stage{
	StagePreflight,
	StageVerifiedBackup,
	StageLegacyImported,
	StageUpgradeCheckpointCommitted,
	StageOperationalSchemaCommitted,
	StageComplete,
}

const (
	journalName    = "conversion.sqlite"
	backupDir      = "conversion-backup"
	checkpointName = "upgrade-checkpoint.json"
)

type fileRecord struct {
	Path   string
	Size   int64
	SHA256 string
}

type groupState struct {
	ID               entmoot.GroupID
	Dir              string
	Entries          []entmoot.RosterEntry
	LegacyMessageIDs []entmoot.MessageID
}

type checkpoint struct {
	Version          uint8                           `json:"version"`
	GroupID          entmoot.GroupID                 `json:"group_id"`
	RosterHead       entmoot.RosterEntryID           `json:"roster_head"`
	Mappings         []entmoot.LegacyIdentityMapping `json:"mappings"`
	UpgradeEntry     entmoot.RosterEntry             `json:"upgrade_entry"`
	LegacyMessageIDs []entmoot.MessageID             `json:"legacy_message_ids,omitempty"`
}

// Status is the durable conversion journal state.
type Status struct {
	Stage        Stage
	SourceSHA256 string
	BackupSHA256 string
}

// Run completes or resumes conversion under a cross-process data-root lock.
// Completed roots only read their journal; normal command startup must not
// checkpoint or reconfigure databases owned by a serving daemon.
// Legacy roots require the founder identity to sign upgrade mappings. No
// operational file changes before preflight and a verified byte-for-byte backup.
func Run(root string, founder *keystore.Identity) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("conversion: resolve data root: %w", err)
	}
	if err := os.MkdirAll(absRoot, 0o700); err != nil {
		return fmt.Errorf("conversion: create data root: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(absRoot, "conversion.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("conversion: open data-root lock: %w", err)
	}
	defer lock.Close()
	// Even read-only SQLite opens can contend while initializing WAL shared
	// memory. Serialize journal access as well as mutating conversion work.
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("conversion: lock data root: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	journalPath := filepath.Join(absRoot, journalName)
	status, exists, err := readStatus(journalPath)
	if err != nil {
		return err
	}
	if exists && status.Stage == StageComplete {
		return nil
	}

	var files []fileRecord
	var groups []groupState
	if !exists {
		files, groups, err = preflight(absRoot)
		if err != nil {
			return fmt.Errorf("conversion: preflight: %w", err)
		}
		sourceHash := hashManifest(files)
		if err := createJournal(journalPath, Status{Stage: StagePreflight, SourceSHA256: sourceHash}, files); err != nil {
			return err
		}
		status = Status{Stage: StagePreflight, SourceSHA256: sourceHash}
	} else {
		files, err = readManifest(journalPath)
		if err != nil {
			return err
		}
		groups, err = preflightGroups(absRoot)
		if err != nil {
			return fmt.Errorf("conversion: resume preflight: %w", err)
		}
	}

	if stageBefore(status.Stage, StageVerifiedBackup) {
		backupHash, err := createAndVerifyBackup(absRoot, files)
		if err != nil {
			return fmt.Errorf("conversion: verified backup: %w", err)
		}
		if err := advance(journalPath, status.Stage, StageVerifiedBackup, backupHash); err != nil {
			return err
		}
		status.Stage, status.BackupSHA256 = StageVerifiedBackup, backupHash
	} else if err := verifyBackup(absRoot, files, status.BackupSHA256); err != nil {
		return fmt.Errorf("conversion: verify existing backup: %w", err)
	}

	if stageBefore(status.Stage, StageLegacyImported) {
		if err := importLegacyGroups(groups); err != nil {
			return fmt.Errorf("conversion: import legacy records: %w", err)
		}
		if err := advance(journalPath, status.Stage, StageLegacyImported, status.BackupSHA256); err != nil {
			return err
		}
		status.Stage = StageLegacyImported
	}

	if stageBefore(status.Stage, StageUpgradeCheckpointCommitted) {
		if err := writeUpgradeCheckpoints(groups, founder); err != nil {
			return fmt.Errorf("conversion: upgrade checkpoint: %w", err)
		}
		if err := advance(journalPath, status.Stage, StageUpgradeCheckpointCommitted, status.BackupSHA256); err != nil {
			return err
		}
		status.Stage = StageUpgradeCheckpointCommitted
	}

	if stageBefore(status.Stage, StageOperationalSchemaCommitted) {
		mappings, err := loadMappings(groups)
		if err != nil {
			return fmt.Errorf("conversion: load upgrade mappings: %w", err)
		}
		if err := migrateOperationalSchemas(absRoot, mappings); err != nil {
			return fmt.Errorf("conversion: operational schema: %w", err)
		}
		if err := advance(journalPath, status.Stage, StageOperationalSchemaCommitted, status.BackupSHA256); err != nil {
			return err
		}
		status.Stage = StageOperationalSchemaCommitted
	}

	if err := validateOperationalRoot(absRoot); err != nil {
		return fmt.Errorf("conversion: validate operational schema: %w", err)
	}
	return advance(journalPath, status.Stage, StageComplete, status.BackupSHA256)
}

// ReadStatus returns the journal state without changing the root.
func ReadStatus(root string) (Status, bool, error) {
	return readStatus(filepath.Join(root, journalName))
}

func stageBefore(current, target Stage) bool {
	index := func(stage Stage) int {
		for i, candidate := range orderedStages {
			if candidate == stage {
				return i
			}
		}
		return -1
	}
	return index(current) < index(target)
}

func openJournal(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func createJournal(path string, status Status, files []fileRecord) error {
	db, err := openJournal(path)
	if err != nil {
		return fmt.Errorf("conversion: open journal: %w", err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`CREATE TABLE conversion_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), stage TEXT NOT NULL, source_sha256 TEXT NOT NULL, backup_sha256 TEXT NOT NULL DEFAULT '', updated_at_ms INTEGER NOT NULL); CREATE TABLE conversion_files (path TEXT PRIMARY KEY, size INTEGER NOT NULL, source_sha256 TEXT NOT NULL); CREATE TABLE conversion_history (sequence INTEGER PRIMARY KEY AUTOINCREMENT, stage TEXT NOT NULL UNIQUE, committed_at_ms INTEGER NOT NULL);`); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO conversion_state(singleton,stage,source_sha256,updated_at_ms) VALUES(1,?,?,?)`, status.Stage, status.SourceSHA256, time.Now().UnixMilli()); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO conversion_history(stage,committed_at_ms) VALUES(?,?)`, status.Stage, time.Now().UnixMilli()); err != nil {
		return err
	}
	for _, file := range files {
		if _, err = tx.Exec(`INSERT INTO conversion_files(path,size,source_sha256) VALUES(?,?,?)`, file.Path, file.Size, file.SHA256); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func readStatus(path string) (Status, bool, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return Status{}, false, nil
	} else if err != nil {
		return Status{}, false, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return Status{}, false, err
	}
	defer db.Close()
	var s Status
	err = db.QueryRow(`SELECT stage,source_sha256,backup_sha256 FROM conversion_state WHERE singleton=1`).Scan(&s.Stage, &s.SourceSHA256, &s.BackupSHA256)
	if err != nil {
		return Status{}, false, fmt.Errorf("conversion: read journal: %w", err)
	}
	if !validStage(s.Stage) {
		return Status{}, false, fmt.Errorf("conversion: unknown journal stage %q", s.Stage)
	}
	return s, true, nil
}

func readManifest(path string) ([]fileRecord, error) {
	db, err := openJournal(path)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(`SELECT path,size,source_sha256 FROM conversion_files ORDER BY path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fileRecord
	for rows.Next() {
		var f fileRecord
		if err := rows.Scan(&f.Path, &f.Size, &f.SHA256); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func advance(path string, from, to Stage, backupHash string) error {
	if !validTransition(from, to) {
		return fmt.Errorf("conversion: invalid stage transition %q -> %q", from, to)
	}
	db, err := openJournal(path)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	res, err := tx.Exec(`UPDATE conversion_state SET stage=?,backup_sha256=?,updated_at_ms=? WHERE singleton=1 AND stage=?`, to, backupHash, now, from)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("conversion: journal stage changed concurrently")
	}
	if _, err := tx.Exec(`INSERT INTO conversion_history(stage,committed_at_ms) VALUES(?,?)`, to, now); err != nil {
		return err
	}
	return tx.Commit()
}

func validStage(stage Stage) bool {
	for _, s := range orderedStages {
		if s == stage {
			return true
		}
	}
	return false
}
func validTransition(from, to Stage) bool {
	for i := 0; i+1 < len(orderedStages); i++ {
		if orderedStages[i] == from && orderedStages[i+1] == to {
			return true
		}
	}
	return false
}

func sourceFiles(root string) ([]fileRecord, error) {
	var records []fileRecord
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if rel == backupDir {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		base := filepath.Base(path)
		if base == journalName || strings.HasSuffix(base, ".lock") || base == checkpointName {
			return nil
		}
		sum, size, err := hashFile(path)
		if err != nil {
			return err
		}
		records = append(records, fileRecord{Path: filepath.ToSlash(rel), Size: size, SHA256: sum})
		return nil
	})
	sort.Slice(records, func(i, j int) bool { return records[i].Path < records[j].Path })
	return records, err
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
func hashManifest(files []fileRecord) string {
	h := sha256.New()
	for _, f := range files {
		fmt.Fprintf(h, "%s\x00%d\x00%s\n", f.Path, f.Size, f.SHA256)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func preflight(root string) ([]fileRecord, []groupState, error) {
	if err := checkpointSQLiteFiles(root); err != nil {
		return nil, nil, err
	}
	files, err := sourceFiles(root)
	if err != nil {
		return nil, nil, err
	}
	groups, err := preflightGroups(root)
	if err != nil {
		return nil, nil, err
	}
	return files, groups, nil
}

func checkpointSQLiteFiles(root string) error {
	return filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			if filepath.Base(path) == backupDir {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".sqlite") || filepath.Base(path) == journalName {
			return nil
		}
		db, err := sql.Open("sqlite", "file:"+path+"?mode=rw&_pragma=busy_timeout(5000)")
		if err != nil {
			return err
		}
		defer db.Close()
		var result string
		if err = db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
			return fmt.Errorf("%s integrity: %w", path, err)
		}
		if result != "ok" {
			return fmt.Errorf("%s integrity: %s", path, result)
		}
		if _, err = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			return fmt.Errorf("%s checkpoint: %w", path, err)
		}
		return db.Close()
	})
}

func groupIDFromDir(path string) (entmoot.GroupID, error) {
	var gid entmoot.GroupID
	raw, err := base64.RawURLEncoding.DecodeString(filepath.Base(path))
	if err != nil || len(raw) != len(gid) {
		return gid, fmt.Errorf("invalid group directory %q", path)
	}
	copy(gid[:], raw)
	return gid, nil
}

func preflightGroups(root string) ([]groupState, error) {
	groupsDir := filepath.Join(root, "groups")
	entries, err := os.ReadDir(groupsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []groupState
	for _, de := range entries {
		if !de.IsDir() {
			continue
		}
		dir := filepath.Join(groupsDir, de.Name())
		gid, err := groupIDFromDir(dir)
		if err != nil {
			return nil, err
		}
		state := groupState{ID: gid, Dir: dir}
		jsonlPath := filepath.Join(dir, "roster.jsonl")
		jsonEntries, err := roster.ValidateLegacyJSONL(jsonlPath, gid)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", jsonlPath, err)
		}
		sqlEntries, err := readRosterSQLite(filepath.Join(dir, "roster.sqlite"), gid)
		if err != nil {
			return nil, err
		}
		if len(sqlEntries) > 0 {
			state.Entries = sqlEntries
			if len(jsonEntries) > len(sqlEntries) {
				return nil, fmt.Errorf("%s is ahead of roster.sqlite", jsonlPath)
			}
			for i := range jsonEntries {
				a, _ := canonical.Encode(jsonEntries[i])
				b, _ := canonical.Encode(sqlEntries[i])
				if !bytes.Equal(a, b) {
					return nil, fmt.Errorf("%s diverges from roster.sqlite at entry %d", jsonlPath, i+1)
				}
			}
		} else {
			state.Entries = jsonEntries
		}
		messagesPath := filepath.Join(dir, "messages.sqlite")
		if err := validateMessagesSQLite(messagesPath, gid); err != nil {
			return nil, err
		}
		state.LegacyMessageIDs, err = readLegacyMessageIDs(messagesPath)
		if err != nil {
			return nil, err
		}
		out = append(out, state)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, nil
}

func readRosterSQLite(path string, gid entmoot.GroupID) ([]entmoot.RosterEntry, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var exists int
	if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='roster_entries')`).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, nil
	}
	rows, err := db.Query(`SELECT canonical_bytes FROM roster_entries WHERE group_id=? ORDER BY sequence`, gid[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []entmoot.RosterEntry
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var entry entmoot.RosterEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, err
		}
		canon, err := canonical.Encode(entry)
		if err != nil || !bytes.Equal(canon, raw) {
			return nil, fmt.Errorf("%s contains non-canonical roster entry", path)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := roster.ValidateEntries(gid, entries); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return entries, nil
}

func validateMessagesSQLite(path string, gid entmoot.GroupID) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	var exists int
	if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages')`).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return nil
	}
	rows, err := db.Query(`SELECT canonical_bytes FROM messages`)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var msg entmoot.Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			return fmt.Errorf("%s malformed message: %w", path, err)
		}
		if msg.GroupID != gid {
			return fmt.Errorf("%s message %s has wrong group", path, msg.ID)
		}
		canon, err := canonical.Encode(msg)
		if err != nil || !bytes.Equal(canon, raw) {
			return fmt.Errorf("%s message %s is not exact canonical JSON", path, msg.ID)
		}
		if err := signing.VerifyMessage(msg, msg.Author); err != nil {
			return fmt.Errorf("%s message %s: %w", path, msg.ID, err)
		}
	}
	return rows.Err()
}
func readLegacyMessageIDs(path string) ([]entmoot.MessageID, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var exists int
	if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages')`).Scan(&exists); err != nil || exists == 0 {
		return nil, err
	}
	rows, err := db.Query(`SELECT canonical_bytes FROM messages`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []entmoot.Message
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var message entmoot.Message
		if err := json.Unmarshal(raw, &message); err != nil {
			return nil, err
		}
		if message.Version == 0 {
			messages = append(messages, message)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return order.Topological(messages)
}

func createAndVerifyBackup(root string, files []fileRecord) (string, error) {
	dir := filepath.Join(root, backupDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	for _, record := range files {
		src := filepath.Join(root, filepath.FromSlash(record.Path))
		dst := filepath.Join(dir, filepath.FromSlash(record.Path))
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return "", err
		}
		if _, err := os.Stat(dst); errors.Is(err, os.ErrNotExist) {
			if err := copyFile(src, dst); err != nil {
				return "", err
			}
		} else if err != nil {
			return "", err
		}
		sum, size, err := hashFile(dst)
		if err != nil {
			return "", err
		}
		if sum != record.SHA256 || size != record.Size {
			return "", fmt.Errorf("backup mismatch for %s", record.Path)
		}
	}
	return hashManifest(files), nil
}
func verifyBackup(root string, files []fileRecord, want string) error {
	if hashManifest(files) != want {
		return errors.New("backup manifest hash mismatch")
	}
	_, err := createAndVerifyBackup(root, files)
	return err
}
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = out.Close()
		if !ok {
			_ = os.Remove(dst)
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

func importLegacyGroups(groups []groupState) error {
	for _, group := range groups {
		if len(group.Entries) == 0 {
			continue
		}
		path := filepath.Join(group.Dir, "roster.sqlite")
		db, err := sql.Open("sqlite", "file:"+path)
		if err != nil {
			return err
		}
		if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS roster_meta(group_id BLOB PRIMARY KEY,version INTEGER NOT NULL,head_id BLOB NOT NULL,import_complete INTEGER NOT NULL CHECK(import_complete=1)); CREATE TABLE IF NOT EXISTS roster_entries(entry_id BLOB PRIMARY KEY,group_id BLOB NOT NULL,sequence INTEGER NOT NULL,parent_id BLOB,canonical_bytes BLOB NOT NULL,op TEXT NOT NULL,timestamp_ms INTEGER NOT NULL,UNIQUE(group_id,sequence)); CREATE INDEX IF NOT EXISTS idx_roster_entries_group_sequence ON roster_entries(group_id,sequence)`); err != nil {
			_ = db.Close()
			return err
		}
		var count int
		if err = db.QueryRow(`SELECT COUNT(*) FROM roster_entries WHERE group_id=?`, group.ID[:]).Scan(&count); err != nil {
			_ = db.Close()
			return err
		}
		if count == 0 {
			tx, err := db.Begin()
			if err != nil {
				_ = db.Close()
				return err
			}
			for index, entry := range group.Entries {
				raw, err := canonical.Encode(entry)
				if err != nil {
					_ = tx.Rollback()
					_ = db.Close()
					return err
				}
				var parent any
				if len(entry.Parents) == 1 {
					parent = entry.Parents[0][:]
				}
				if _, err = tx.Exec(`INSERT INTO roster_entries(entry_id,group_id,sequence,parent_id,canonical_bytes,op,timestamp_ms) VALUES(?,?,?,?,?,?,?)`, entry.ID[:], group.ID[:], index+1, parent, raw, entry.Op, entry.Timestamp); err != nil {
					_ = tx.Rollback()
					_ = db.Close()
					return err
				}
			}
			head := group.Entries[len(group.Entries)-1].ID
			if _, err = tx.Exec(`INSERT INTO roster_meta(group_id,version,head_id,import_complete) VALUES(?,?,?,1)`, group.ID[:], len(group.Entries), head[:]); err != nil {
				_ = tx.Rollback()
				_ = db.Close()
				return err
			}
			if err = tx.Commit(); err != nil {
				_ = db.Close()
				return err
			}
		}
		if err = db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func writeUpgradeCheckpoints(groups []groupState, founder *keystore.Identity) error {
	for _, group := range groups {
		legacy := legacyRosterEntries(group.Entries)
		if len(legacy) == 0 {
			continue
		}
		path := filepath.Join(group.Dir, checkpointName)
		var data []byte
		if existing, err := os.ReadFile(path); err == nil {
			data = existing
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		} else {
			founderInfo := legacy[0].Subject
			if founder == nil {
				return fmt.Errorf("group %s requires a founder-signed upgrade checkpoint", group.ID)
			}
			if !bytes.Equal(founder.PublicKey, founderInfo.EntmootPubKey) {
				return fmt.Errorf("group %s conversion must run with founder identity or an existing founder-signed checkpoint", group.ID)
			}
			type mappingKey struct {
				nodeID   entmoot.NodeID
				memberID entmoot.MemberID
			}
			infos := map[mappingKey]entmoot.NodeInfo{}
			for _, entry := range legacy {
				if entry.Subject.PilotNodeID == 0 {
					continue
				}
				memberID, err := entmoot.MemberIDFromPublicKey(entry.Subject.EntmootPubKey)
				if err != nil {
					return err
				}
				infos[mappingKey{nodeID: entry.Subject.PilotNodeID, memberID: memberID}] = entry.Subject
			}
			keys := make([]mappingKey, 0, len(infos))
			for key := range infos {
				keys = append(keys, key)
			}
			sort.Slice(keys, func(i, j int) bool {
				if keys[i].nodeID != keys[j].nodeID {
					return keys[i].nodeID < keys[j].nodeID
				}
				return bytes.Compare(keys[i].memberID[:], keys[j].memberID[:]) < 0
			})
			head := legacy[len(legacy)-1].ID
			cp := checkpoint{Version: 1, GroupID: group.ID, RosterHead: head, LegacyMessageIDs: append([]entmoot.MessageID(nil), group.LegacyMessageIDs...)}
			for _, key := range keys {
				info := infos[key]
				mapping := entmoot.LegacyIdentityMapping{GroupID: group.ID, LegacyNodeID: key.nodeID, MemberID: key.memberID, MemberPubKey: append([]byte(nil), info.EntmootPubKey...), RosterHead: head, Founder: founderInfo}
				if err := entmoot.SignLegacyIdentityMapping(founder, &mapping); err != nil {
					return err
				}
				cp.Mappings = append(cp.Mappings, mapping)
			}
			mappingsBytes, err := canonical.Encode(cp.Mappings)
			if err != nil {
				return err
			}
			mappingsHash := sha256.Sum256(mappingsBytes)
			historyTree := merkle.New(cp.LegacyMessageIDs)
			historyRoot := historyTree.Root()
			upgradePolicy := entmoot.LegacyIdentityUpgradePolicy{
				Type:               "legacy_identity_upgrade",
				MappingsSHA256:     hex.EncodeToString(mappingsHash[:]),
				LegacyHistoryRoot:  hex.EncodeToString(historyRoot[:]),
				LegacyHistoryCount: len(cp.LegacyMessageIDs),
			}
			policy, err := canonical.Encode(upgradePolicy)
			if err != nil {
				return err
			}
			founderMemberID, err := entmoot.MemberIDFromPublicKey(founder.PublicKey)
			if err != nil {
				return err
			}
			groupID := group.ID
			cp.UpgradeEntry = entmoot.RosterEntry{
				Op:            "policy_change",
				Policy:        policy,
				ActorMemberID: &founderMemberID,
				Timestamp:     legacy[len(legacy)-1].Timestamp + 1,
				Parents:       []entmoot.RosterEntryID{head},
				Version:       roster.CurrentEntryVersion,
				GroupID:       &groupID,
				Sequence:      uint64(len(legacy) + 1),
			}
			cp.UpgradeEntry.ID = canonical.RosterEntryID(cp.UpgradeEntry)
			signingBytes, err := canonical.RosterEntrySigningBytes(cp.UpgradeEntry)
			if err != nil {
				return err
			}
			cp.UpgradeEntry.Signature = founder.Sign(signingBytes)
			data, err = canonical.Encode(cp)
			if err != nil {
				return err
			}
			temporary := path + ".tmp"
			if err = os.WriteFile(temporary, data, 0o600); err != nil {
				return err
			}
			if err = os.Rename(temporary, path); err != nil {
				return err
			}
		}
		if err := validateCheckpoint(data, group); err != nil {
			return err
		}
		var cp checkpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			return err
		}
		if err := commitUpgradeEntry(filepath.Join(group.Dir, "roster.sqlite"), group.ID, cp); err != nil {
			return err
		}
	}
	return nil
}

func legacyRosterEntries(entries []entmoot.RosterEntry) []entmoot.RosterEntry {
	end := 0
	for end < len(entries) && entries[end].Version == 0 {
		end++
	}
	return entries[:end]
}

func hasLegacyRoster(entries []entmoot.RosterEntry) bool {
	return len(legacyRosterEntries(entries)) > 0
}

func validateCheckpoint(data []byte, group groupState) error {
	var cp checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}
	canon, err := canonical.Encode(cp)
	if err != nil || !bytes.Equal(canon, data) {
		return errors.New("upgrade checkpoint is not canonical")
	}
	legacy := legacyRosterEntries(group.Entries)
	if cp.Version != 1 || cp.GroupID != group.ID || len(legacy) == 0 || cp.RosterHead != legacy[len(legacy)-1].ID {
		return errors.New("upgrade checkpoint does not match legacy roster head")
	}
	if len(cp.LegacyMessageIDs) != len(group.LegacyMessageIDs) {
		return errors.New("upgrade checkpoint does not match legacy history")
	}
	for i := range cp.LegacyMessageIDs {
		if cp.LegacyMessageIDs[i] != group.LegacyMessageIDs[i] {
			return errors.New("upgrade checkpoint does not match legacy history")
		}
	}
	for _, mapping := range cp.Mappings {
		if mapping.GroupID != cp.GroupID || mapping.RosterHead != cp.RosterHead {
			return errors.New("upgrade mapping checkpoint mismatch")
		}
		if err := entmoot.VerifyLegacyIdentityMapping(mapping); err != nil {
			return err
		}
	}
	mappingBytes, err := canonical.Encode(cp.Mappings)
	if err != nil {
		return err
	}
	mappingsHash := sha256.Sum256(mappingBytes)
	historyTree := merkle.New(cp.LegacyMessageIDs)
	historyRoot := historyTree.Root()
	expectedPolicy, err := canonical.Encode(entmoot.LegacyIdentityUpgradePolicy{
		Type:               "legacy_identity_upgrade",
		MappingsSHA256:     hex.EncodeToString(mappingsHash[:]),
		LegacyHistoryRoot:  hex.EncodeToString(historyRoot[:]),
		LegacyHistoryCount: len(cp.LegacyMessageIDs),
	})
	if err != nil {
		return err
	}
	if !bytes.Equal(cp.UpgradeEntry.Policy, expectedPolicy) {
		return errors.New("upgrade entry does not bind checkpoint mappings and legacy history")
	}
	candidate := append(append([]entmoot.RosterEntry(nil), legacy...), cp.UpgradeEntry)
	if err := roster.ValidateEntries(group.ID, candidate); err != nil {
		return fmt.Errorf("invalid founder-signed upgrade entry: %w", err)
	}
	return nil
}

// LoadLegacyHistoryTree verifies the founder-signed conversion checkpoint
// against the current legacy message set and returns its immutable tree.
func LoadLegacyHistoryTree(groupDir string, groupID entmoot.GroupID, entries []entmoot.RosterEntry) (*merkle.Tree, error) {
	path := filepath.Join(groupDir, checkpointName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cp checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	group := groupState{ID: groupID, Dir: groupDir, Entries: entries, LegacyMessageIDs: cp.LegacyMessageIDs}
	if err := validateCheckpoint(data, group); err != nil {
		return nil, err
	}
	tree := merkle.New(cp.LegacyMessageIDs)
	currentIDs, err := readLegacyMessageIDs(filepath.Join(groupDir, "messages.sqlite"))
	if err != nil {
		return nil, err
	}
	for _, id := range currentIDs {
		if _, err := tree.Proof(id); err != nil {
			return nil, fmt.Errorf("legacy message %s is outside the founder-signed conversion commitment", id)
		}
	}
	return tree, nil
}

func commitUpgradeEntry(path string, groupID entmoot.GroupID, cp checkpoint) error {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=rw")
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	var head []byte
	if err := tx.QueryRow(`SELECT version,head_id FROM roster_meta WHERE group_id=?`, groupID[:]).Scan(&version, &head); err != nil {
		return err
	}
	if bytes.Equal(head, cp.UpgradeEntry.ID[:]) {
		return tx.Commit()
	}
	if !bytes.Equal(head, cp.RosterHead[:]) {
		return errors.New("roster head changed before upgrade checkpoint commit")
	}
	raw, err := canonical.Encode(cp.UpgradeEntry)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO roster_entries(entry_id,group_id,sequence,parent_id,canonical_bytes,op,timestamp_ms) VALUES(?,?,?,?,?,?,?)`, cp.UpgradeEntry.ID[:], groupID[:], version+1, cp.RosterHead[:], raw, cp.UpgradeEntry.Op, cp.UpgradeEntry.Timestamp); err != nil {
		return err
	}
	if _, err = tx.Exec(`UPDATE roster_meta SET version=?,head_id=?,import_complete=1 WHERE group_id=?`, version+1, cp.UpgradeEntry.ID[:], groupID[:]); err != nil {
		return err
	}
	return tx.Commit()
}

type legacyIdentityInterval struct {
	memberID entmoot.MemberID
	startMS  int64
	endMS    int64
}

type legacyIdentityResolver struct {
	byNode         map[entmoot.NodeID]map[entmoot.MemberID]struct{}
	intervals      map[entmoot.NodeID][]legacyIdentityInterval
	byGroup        map[string]map[entmoot.NodeID]map[entmoot.MemberID]struct{}
	groupIntervals map[string]map[entmoot.NodeID][]legacyIdentityInterval
}

func loadMappings(groups []groupState) (*legacyIdentityResolver, error) {
	out := &legacyIdentityResolver{
		byNode:         map[entmoot.NodeID]map[entmoot.MemberID]struct{}{},
		intervals:      map[entmoot.NodeID][]legacyIdentityInterval{},
		byGroup:        map[string]map[entmoot.NodeID]map[entmoot.MemberID]struct{}{},
		groupIntervals: map[string]map[entmoot.NodeID][]legacyIdentityInterval{},
	}
	for _, group := range groups {
		if !hasLegacyRoster(group.Entries) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(group.Dir, checkpointName))
		if err != nil {
			return nil, err
		}
		if err = validateCheckpoint(data, group); err != nil {
			return nil, err
		}
		var cp checkpoint
		_ = json.Unmarshal(data, &cp)
		groupKey := string(group.ID[:])
		groupMembers := map[entmoot.NodeID]map[entmoot.MemberID]struct{}{}
		groupIntervals := map[entmoot.NodeID][]legacyIdentityInterval{}
		for _, mapping := range cp.Mappings {
			members := out.byNode[mapping.LegacyNodeID]
			if members == nil {
				members = map[entmoot.MemberID]struct{}{}
				out.byNode[mapping.LegacyNodeID] = members
			}
			members[mapping.MemberID] = struct{}{}
			members = groupMembers[mapping.LegacyNodeID]
			if members == nil {
				members = map[entmoot.MemberID]struct{}{}
				groupMembers[mapping.LegacyNodeID] = members
			}
			members[mapping.MemberID] = struct{}{}
		}

		type activeKey struct {
			nodeID   entmoot.NodeID
			memberID entmoot.MemberID
		}
		type activeIndexes struct {
			global int
			group  int
		}
		active := map[activeKey]activeIndexes{}
		for _, entry := range legacyRosterEntries(group.Entries) {
			if entry.Subject.PilotNodeID == 0 {
				continue
			}
			memberID, err := entmoot.MemberIDFromPublicKey(entry.Subject.EntmootPubKey)
			if err != nil {
				return nil, err
			}
			key := activeKey{nodeID: entry.Subject.PilotNodeID, memberID: memberID}
			switch entry.Op {
			case "add":
				global := out.intervals[key.nodeID]
				local := groupIntervals[key.nodeID]
				active[key] = activeIndexes{global: len(global), group: len(local)}
				out.intervals[key.nodeID] = append(global, legacyIdentityInterval{memberID: memberID, startMS: entry.Timestamp})
				groupIntervals[key.nodeID] = append(local, legacyIdentityInterval{memberID: memberID, startMS: entry.Timestamp})
			case "remove":
				indexes, ok := active[key]
				if !ok {
					continue
				}
				global := out.intervals[key.nodeID]
				global[indexes.global].endMS = entry.Timestamp
				out.intervals[key.nodeID] = global
				local := groupIntervals[key.nodeID]
				local[indexes.group].endMS = entry.Timestamp
				groupIntervals[key.nodeID] = local
				delete(active, key)
			}
		}
		out.byGroup[groupKey] = groupMembers
		out.groupIntervals[groupKey] = groupIntervals
	}
	return out, nil
}

func (r *legacyIdentityResolver) resolve(nodeID entmoot.NodeID, encodedPublicKey string, timestampMS int64) (entmoot.MemberID, error) {
	return r.resolveMapped(nodeID, encodedPublicKey, timestampMS, r.byNode[nodeID], r.intervals[nodeID])
}

func (r *legacyIdentityResolver) resolveForGroup(groupID []byte, nodeID entmoot.NodeID, encodedPublicKey string, timestampMS int64) (entmoot.MemberID, error) {
	if len(groupID) == 0 {
		return r.resolve(nodeID, encodedPublicKey, timestampMS)
	}
	groupKey := string(groupID)
	return r.resolveMapped(nodeID, encodedPublicKey, timestampMS, r.byGroup[groupKey][nodeID], r.groupIntervals[groupKey][nodeID])
}

func (r *legacyIdentityResolver) resolveMapped(nodeID entmoot.NodeID, encodedPublicKey string, timestampMS int64, members map[entmoot.MemberID]struct{}, intervals []legacyIdentityInterval) (entmoot.MemberID, error) {
	if len(members) == 0 {
		return entmoot.MemberID{}, fmt.Errorf("legacy node %d has no founder mapping", nodeID)
	}
	if strings.TrimSpace(encodedPublicKey) != "" {
		publicKey, err := base64.StdEncoding.DecodeString(encodedPublicKey)
		if err != nil {
			return entmoot.MemberID{}, fmt.Errorf("legacy node %d has invalid public key: %w", nodeID, err)
		}
		memberID, err := entmoot.MemberIDFromPublicKey(publicKey)
		if err != nil {
			return entmoot.MemberID{}, err
		}
		if _, ok := members[memberID]; !ok {
			return entmoot.MemberID{}, fmt.Errorf("legacy node %d public key has no founder mapping", nodeID)
		}
		return memberID, nil
	}
	if len(members) == 1 {
		for memberID := range members {
			return memberID, nil
		}
	}
	active := map[entmoot.MemberID]struct{}{}
	if timestampMS > 0 {
		for _, interval := range intervals {
			if interval.startMS <= timestampMS && (interval.endMS == 0 || timestampMS < interval.endMS) {
				active[interval.memberID] = struct{}{}
			}
		}
	}
	if len(active) == 1 {
		for memberID := range active {
			return memberID, nil
		}
	}
	return entmoot.MemberID{}, fmt.Errorf("legacy node %d is ambiguous at timestamp %d without a signing key", nodeID, timestampMS)
}

func migrateOperationalSchemas(root string, mappings *legacyIdentityResolver) error {
	groupsDir := filepath.Join(root, "groups")
	dirs, err := os.ReadDir(groupsDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, de := range dirs {
		if !de.IsDir() {
			continue
		}
		dir := filepath.Join(groupsDir, de.Name())
		if err := migrateRoster(filepath.Join(dir, "roster.sqlite")); err != nil {
			return err
		}
		if err := migrateMessages(filepath.Join(dir, "messages.sqlite")); err != nil {
			return err
		}
	}
	if err := migrateESP(filepath.Join(root, "esp.sqlite"), mappings); err != nil {
		return err
	}
	return nil
}

func migrateRoster(path string) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=rw")
	if err != nil {
		return err
	}
	defer db.Close()
	var legacy int
	if err = db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('roster_meta') WHERE name='founder_node_id'`).Scan(&legacy); err != nil {
		return err
	}
	if legacy == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`DROP TABLE IF EXISTS roster_meta_new; DROP TABLE IF EXISTS roster_entries_new; CREATE TABLE roster_meta_new(group_id BLOB PRIMARY KEY,version INTEGER NOT NULL,head_id BLOB NOT NULL,import_complete INTEGER NOT NULL CHECK(import_complete=1)); INSERT INTO roster_meta_new(group_id,version,head_id,import_complete) SELECT group_id,version,head_id,import_complete FROM roster_meta; CREATE TABLE roster_entries_new(entry_id BLOB PRIMARY KEY,group_id BLOB NOT NULL,sequence INTEGER NOT NULL,parent_id BLOB,canonical_bytes BLOB NOT NULL,op TEXT NOT NULL,timestamp_ms INTEGER NOT NULL,UNIQUE(group_id,sequence)); INSERT INTO roster_entries_new(entry_id,group_id,sequence,parent_id,canonical_bytes,op,timestamp_ms) SELECT entry_id,group_id,sequence,parent_id,canonical_bytes,op,timestamp_ms FROM roster_entries; DROP INDEX IF EXISTS idx_roster_entries_group_sequence; DROP TABLE IF EXISTS roster_members; DROP TABLE roster_entries; DROP TABLE roster_meta; ALTER TABLE roster_entries_new RENAME TO roster_entries; ALTER TABLE roster_meta_new RENAME TO roster_meta; CREATE INDEX idx_roster_entries_group_sequence ON roster_entries(group_id,sequence)`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func migrateMessages(path string) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=rw")
	if err != nil {
		return err
	}
	defer db.Close()
	cols, err := columns(db, "messages")
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if cols["author_node_id"] && !cols["author_member_id"] {
		rows, err := tx.Query(`SELECT canonical_bytes FROM messages`)
		if err != nil {
			return err
		}
		type row struct {
			raw    []byte
			msg    entmoot.Message
			member entmoot.MemberID
		}
		var all []row
		for rows.Next() {
			var r row
			if err = rows.Scan(&r.raw); err != nil {
				rows.Close()
				return err
			}
			if err = json.Unmarshal(r.raw, &r.msg); err != nil {
				rows.Close()
				return err
			}
			r.member, err = entmoot.ResolvedMemberID(r.msg.Author)
			if err != nil {
				rows.Close()
				return err
			}
			all = append(all, r)
		}
		if err = rows.Close(); err != nil {
			return err
		}
		if _, err = tx.Exec(`DROP TRIGGER IF EXISTS message_search_docs_ai; DROP TRIGGER IF EXISTS message_search_docs_ad; DROP TRIGGER IF EXISTS message_search_docs_au; DROP TABLE IF EXISTS message_search_fts; DROP TABLE IF EXISTS message_search_docs; DROP INDEX IF EXISTS idx_messages_group_latest; DROP INDEX IF EXISTS idx_messages_group_author; ALTER TABLE messages RENAME TO messages_legacy; CREATE TABLE messages(message_id BLOB PRIMARY KEY,group_id BLOB NOT NULL,author_member_id BLOB NOT NULL,timestamp_ms INTEGER NOT NULL,content BLOB NOT NULL,parents BLOB NOT NULL,signature BLOB NOT NULL,canonical_bytes BLOB NOT NULL)`); err != nil {
			return err
		}
		for _, r := range all {
			parents, _ := json.Marshal(r.msg.Parents)
			if _, err = tx.Exec(`INSERT INTO messages(message_id,group_id,author_member_id,timestamp_ms,content,parents,signature,canonical_bytes) VALUES(?,?,?,?,?,?,?,?)`, r.msg.ID[:], r.msg.GroupID[:], r.member[:], r.msg.Timestamp, r.msg.Content, parents, r.msg.Signature, r.raw); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(`DROP TABLE messages_legacy`); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`DROP TABLE IF EXISTS transport_ads; DROP TABLE IF EXISTS transport_ad_seqs; DROP TABLE IF EXISTS member_profile_ads; DROP TABLE IF EXISTS member_profile_ad_seqs`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func migrateESP(path string, mappings *legacyIdentityResolver) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=rw")
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`DROP TABLE IF EXISTS esp_open_invite_challenges`); err != nil {
		return err
	}
	// The removed Fleet feature's tables go before the identity walk below.
	// They are dropped on ESP open too, but conversion runs first on a legacy
	// root (setup() converts before anything opens the ESP store), and the walk
	// would otherwise try to rewrite rows it can no longer key or scope: a
	// reassigned legacy node id in a fleet row would fail closed and abort the
	// whole conversion, leaving the daemon unable to start.
	for _, table := range esphttp.RetiredFleetTables {
		if _, err = tx.Exec(`DROP TABLE IF EXISTS ` + table); err != nil {
			return err
		}
	}
	renames := map[string]string{"node_id": "member_id", "coordinator_node_id": "coordinator_member_id", "actor_node_id": "actor_member_id", "subject_node_id": "subject_member_id", "creator_node_id": "creator_member_id", "assignee_node_id": "assignee_member_id", "author_node_id": "author_member_id", "issuer_node_id": "issuer_member_id", "agent_node_id": "agent_member_id", "last_seen_author_node_id": "last_seen_author_member_id"}
	tables, err := tableNames(tx)
	if err != nil {
		return err
	}
	for _, table := range tables {
		cols, err := columnsTx(tx, table)
		if err != nil {
			return err
		}
		for old, newName := range renames {
			if cols[old] && !cols[newName] {
				if _, err = tx.Exec(fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN %s TO %s`, quoteIdent(table), quoteIdent(old), quoteIdent(newName))); err != nil {
					return err
				}
				cols[newName] = true
				delete(cols, old)
			}
		}
		for _, name := range []string{"member_id", "coordinator_member_id", "actor_member_id", "subject_member_id", "creator_member_id", "assignee_member_id", "author_member_id", "issuer_member_id", "agent_member_id", "last_seen_author_member_id"} {
			if !cols[name] {
				continue
			}
			publicKeyExpr := `''`
			if publicKeyColumn := espIdentityPublicKeyColumn(table, name); publicKeyColumn != "" && cols[publicKeyColumn] {
				publicKeyExpr = fmt.Sprintf(`COALESCE(CAST(%s AS TEXT),'')`, quoteIdent(publicKeyColumn))
			}
			query := fmt.Sprintf(
				`SELECT rowid,%s,%s,%s,%s FROM %s`,
				quoteIdent(name),
				publicKeyExpr,
				espIdentityTimestampExpression(cols),
				espIdentityGroupExpression(cols),
				quoteIdent(table),
			)
			rows, err := tx.Query(query)
			if err != nil {
				return err
			}
			type update struct {
				rowID int64
				value []byte
			}
			var updates []update
			for rows.Next() {
				var rowID, timestampMS int64
				var value any
				var groupID []byte
				var encodedPublicKey string
				if err = rows.Scan(&rowID, &value, &encodedPublicKey, &timestampMS, &groupID); err != nil {
					rows.Close()
					return err
				}
				var legacyNodeID entmoot.NodeID
				switch v := value.(type) {
				case int64:
					if v == 0 {
						updates = append(updates, update{rowID: rowID, value: []byte{}})
						continue
					}
					legacyNodeID = entmoot.NodeID(v)
				case []byte:
					switch len(v) {
					case 0, len(entmoot.MemberID{}):
						continue
					case 4:
						legacyNodeID = entmoot.NodeID(uint32(v[0]) | uint32(v[1])<<8 | uint32(v[2])<<16 | uint32(v[3])<<24)
						if legacyNodeID == 0 {
							updates = append(updates, update{rowID: rowID, value: []byte{}})
							continue
						}
					default:
						rows.Close()
						return fmt.Errorf("%s.%s has invalid identity width %d", table, name, len(v))
					}
				default:
					rows.Close()
					return fmt.Errorf("%s.%s has unsupported legacy identity type %T", table, name, value)
				}
				memberID, err := mappings.resolveForGroup(groupID, legacyNodeID, encodedPublicKey, timestampMS)
				if err != nil {
					rows.Close()
					return fmt.Errorf("%s.%s: %w", table, name, err)
				}
				updates = append(updates, update{rowID: rowID, value: append([]byte(nil), memberID[:]...)})
			}
			if err = rows.Close(); err != nil {
				return err
			}
			for _, update := range updates {
				if _, err = tx.Exec(fmt.Sprintf(`UPDATE %s SET %s=? WHERE rowid=?`, quoteIdent(table), quoteIdent(name)), update.value, update.rowID); err != nil {
					return err
				}
			}
		}
	}
	return tx.Commit()
}

// espIdentityGroupExpression names the column a row's group is read from, so
// a rewritten identity can be scoped to the group it belonged to. Rows that
// carry no group at all yield an empty blob and are matched on identity alone.
func espIdentityGroupExpression(cols map[string]bool) string {
	if cols["control_group_id"] {
		return fmt.Sprintf(`COALESCE(CAST(%s AS BLOB),X'')`, quoteIdent("control_group_id"))
	}
	if cols["group_id"] {
		return fmt.Sprintf(`COALESCE(CAST(%s AS BLOB),X'')`, quoteIdent("group_id"))
	}
	return `X''`
}

func espIdentityPublicKeyColumn(table, memberColumn string) string {
	switch table + "." + memberColumn {
	case "esp_node_profile_sources.member_id", "esp_node_profiles.member_id":
		return "entmoot_pubkey"
	default:
		return ""
	}
}

func espIdentityTimestampExpression(cols map[string]bool) string {
	var candidates []string
	for _, name := range []string{"created_at_ms", "observed_at_ms", "started_at_ms", "completed_at_ms", "updated_at_ms", "invited_at_ms", "accepted_at_ms"} {
		if cols[name] {
			candidates = append(candidates, fmt.Sprintf("NULLIF(%s,0)", quoteIdent(name)))
		}
	}
	if len(candidates) == 0 {
		return "0"
	}
	return "COALESCE(" + strings.Join(candidates, ",") + ",0)"
}

func tableNames(tx *sql.Tx) ([]string, error) {
	rows, err := tx.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
func columns(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + quoteIdent(table) + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColumns(rows)
}
func columnsTx(tx *sql.Tx, table string) (map[string]bool, error) {
	rows, err := tx.Query(`PRAGMA table_info(` + quoteIdent(table) + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColumns(rows)
}
func scanColumns(rows *sql.Rows) (map[string]bool, error) {
	out := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func validateOperationalRoot(root string) error {
	if err := checkpointSQLiteFiles(root); err != nil {
		return err
	}
	groups, err := preflightGroups(root)
	if err != nil {
		return err
	}
	for _, group := range groups {
		for _, entry := range group.Entries {
			if entry.Version >= 2 && (entry.Op == "add" || entry.Op == "remove") &&
				(entry.Subject.MemberID == nil || entry.Subject.PilotNodeID != 0) {
				return fmt.Errorf("group %s has invalid operational roster identity", group.ID)
			}
		}
	}
	return nil
}
