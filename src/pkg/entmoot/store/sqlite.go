package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/order"

	// Register the pure-Go SQLite driver under the name "sqlite".
	_ "modernc.org/sqlite"
)

// sqliteDriver is the database/sql driver name registered by modernc.org/sqlite.
const sqliteDriver = "sqlite"

const (
	maxLiveTombstonesPerGroup = 1_000_000
	tombstoneMinimumAge       = 90 * 24 * time.Hour
	tombstoneCleanupBatch     = 4096
)

// sqliteSchema is applied idempotently on first open of every per-group
// database. Mirrors docs/CLI_DESIGN.md §4.2 exactly.
const sqliteSchema = `
CREATE TABLE IF NOT EXISTS messages (
  message_id      BLOB PRIMARY KEY,
  group_id        BLOB NOT NULL,
  author_member_id BLOB NOT NULL,
  timestamp_ms    INTEGER NOT NULL,
  content         BLOB NOT NULL,
  parents         BLOB NOT NULL,
  signature       BLOB NOT NULL,
  canonical_bytes BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_group_time
  ON messages(group_id, timestamp_ms DESC);

CREATE INDEX IF NOT EXISTS idx_messages_group_latest
  ON messages(group_id, timestamp_ms DESC, author_member_id DESC, message_id DESC);

CREATE INDEX IF NOT EXISTS idx_messages_group_id_range
  ON messages(group_id, message_id ASC);

CREATE INDEX IF NOT EXISTS idx_messages_group_author
  ON messages(group_id, author_member_id, timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS message_topics (
  message_id BLOB NOT NULL,
  topic      TEXT NOT NULL,
  PRIMARY KEY (message_id, topic)
);

CREATE INDEX IF NOT EXISTS idx_topic_lookup
  ON message_topics(topic, message_id);

CREATE TABLE IF NOT EXISTS message_search_docs (
  doc_id         INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id     BLOB NOT NULL UNIQUE,
  group_id       BLOB NOT NULL,
  author_member_id BLOB NOT NULL,
  timestamp_ms   INTEGER NOT NULL,
  content_text   TEXT NOT NULL,
  topics_text    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_message_search_docs_group_latest
  ON message_search_docs(group_id, timestamp_ms DESC, author_member_id DESC, message_id DESC);

CREATE VIRTUAL TABLE IF NOT EXISTS message_search_fts USING fts5(
  content_text,
  content='message_search_docs',
  content_rowid='doc_id'
);

CREATE TRIGGER IF NOT EXISTS message_search_docs_ai
AFTER INSERT ON message_search_docs BEGIN
  INSERT INTO message_search_fts(rowid, content_text)
  VALUES (new.doc_id, new.content_text);
END;

CREATE TRIGGER IF NOT EXISTS message_search_docs_ad
AFTER DELETE ON message_search_docs BEGIN
  INSERT INTO message_search_fts(message_search_fts, rowid, content_text)
  VALUES ('delete', old.doc_id, old.content_text);
END;

CREATE TRIGGER IF NOT EXISTS message_search_docs_au
AFTER UPDATE ON message_search_docs BEGIN
  INSERT INTO message_search_fts(message_search_fts, rowid, content_text)
  VALUES ('delete', old.doc_id, old.content_text);
  INSERT INTO message_search_fts(rowid, content_text)
  VALUES (new.doc_id, new.content_text);
END;


CREATE TABLE IF NOT EXISTS group_sync_state (
  group_id          BLOB PRIMARY KEY,
  generation        INTEGER NOT NULL DEFAULT 0,
  root_generation   INTEGER NOT NULL DEFAULT -1,
  merkle_root       BLOB,
  coverage_floor_ms INTEGER NOT NULL DEFAULT 0
);

-- Generation invalidation is enforced in SQLite so future bulk import and
-- conversion writers cannot bypass the cache contract.
CREATE TRIGGER IF NOT EXISTS messages_sync_insert
AFTER INSERT ON messages
BEGIN
  INSERT OR IGNORE INTO group_sync_state
    (group_id, generation, root_generation, coverage_floor_ms)
    VALUES (NEW.group_id, 0, -1, 0);
  UPDATE group_sync_state
    SET generation = generation + 1,
        root_generation = -1,
        merkle_root = NULL
    WHERE group_id = NEW.group_id;
END;

CREATE TRIGGER IF NOT EXISTS messages_sync_delete
AFTER DELETE ON messages
BEGIN
  INSERT OR IGNORE INTO group_sync_state
    (group_id, generation, root_generation, coverage_floor_ms)
    VALUES (OLD.group_id, 0, -1, 0);
  UPDATE group_sync_state
    SET generation = generation + 1,
        root_generation = -1,
        merkle_root = NULL
    WHERE group_id = OLD.group_id;
END;

CREATE TABLE IF NOT EXISTS message_tombstones (
  message_id    BLOB PRIMARY KEY,
  group_id      BLOB NOT NULL,
  timestamp_ms  INTEGER NOT NULL,
  pruned_at_ms  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_message_tombstones_group_time
  ON message_tombstones(group_id, timestamp_ms, pruned_at_ms);
`

// SQLite is a MessageStore backed by one SQLite database per group,
// stored under <root>/groups/<base64url(gid)>/messages.sqlite.
//
// Safe for concurrent use; WAL mode allows readers during writes and no
// cross-process coordination is required beyond what SQLite itself provides.
//
// Per-group databases are opened lazily on first access. OpenSQLite only
// prepares the directory layout; opening the physical database files is
// deferred until a Put/Get/Has/Range/MerkleRoot call names a particular group.
type SQLite struct {
	root      string
	groupsDir string

	mu  sync.RWMutex
	dbs map[entmoot.GroupID]*sql.DB
}

// OpenSQLite opens or creates a data root directory. Returns a SQLite store
// ready for Put/Get/Has/Range/MerkleRoot calls across any group. Each group's
// database is opened lazily on first access.
//
// The root and <root>/groups/ are created with 0700 permissions if missing,
// matching JSONL's convention.
func OpenSQLite(root string) (*SQLite, error) {
	if root == "" {
		return nil, errors.New("store: SQLite root path is empty")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("store: resolve root %q: %w", root, err)
	}
	if err := os.MkdirAll(absRoot, 0o700); err != nil {
		return nil, fmt.Errorf("store: mkdir root %q: %w", absRoot, err)
	}
	groupsDir := filepath.Join(absRoot, "groups")
	if err := os.MkdirAll(groupsDir, 0o700); err != nil {
		return nil, fmt.Errorf("store: mkdir groups %q: %w", groupsDir, err)
	}
	return &SQLite{
		root:      absRoot,
		groupsDir: groupsDir,
		dbs:       make(map[entmoot.GroupID]*sql.DB),
	}, nil
}

// Close flushes WAL (wal_checkpoint(TRUNCATE)) and closes every open group
// database. Safe to call multiple times; after Close the store must not be
// used. Returns the first non-nil error encountered while checkpointing or
// closing; remaining databases are still processed.
func (s *SQLite) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for gid, db := range s.dbs {
		if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE);"); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("store: wal_checkpoint: %w", err)
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("store: db close: %w", err)
		}
		delete(s.dbs, gid)
	}
	return firstErr
}

// Put implements MessageStore.Put. Inserts the row and its topic rows inside
// a single transaction. Duplicates are no-ops and return inserted=false.
func (s *SQLite) Put(ctx context.Context, expectedGroup entmoot.GroupID, m entmoot.Message) (bool, error) {
	if expectedGroup != m.GroupID {
		return false, fmt.Errorf("%w: expected group %s, got %s", ErrInvalidMessage, expectedGroup, m.GroupID)
	}
	if isZeroGroupID(m.GroupID) {
		return false, fmt.Errorf("%w: zero group id", ErrInvalidMessage)
	}
	if isZeroMessageID(m.ID) {
		return false, fmt.Errorf("%w: zero message id", ErrInvalidMessage)
	}

	encoded, err := canonical.Encode(m)
	if err != nil {
		return false, fmt.Errorf("store: canonical encode: %w", err)
	}

	db, err := s.dbFor(m.GroupID)
	if err != nil {
		return false, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("store: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureGroupSyncStateTx(ctx, tx, m.GroupID); err != nil {
		return false, err
	}
	var tombstoned int
	err = tx.QueryRowContext(ctx, `
		SELECT 1 FROM message_tombstones
		WHERE group_id = ? AND message_id = ?;`,
		m.GroupID[:], m.ID[:],
	).Scan(&tombstoned)
	if err == nil {
		return false, fmt.Errorf("%w: %s", ErrPruned, m.ID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("store: check tombstone: %w", err)
	}

	authorMemberID := messageMemberID(m)
	result, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO messages
		  (message_id, group_id, author_member_id, timestamp_ms,
		   content, parents, signature, canonical_bytes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		m.ID[:],
		m.GroupID[:],
		authorMemberID[:],
		m.Timestamp,
		notNilBytes(m.Content),
		parentsBlob(m.Parents),
		notNilBytes(m.Signature),
		encoded,
	)
	if err != nil {
		return false, fmt.Errorf("store: insert message: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: determine insert result: %w", err)
	}
	if rowsAffected == 0 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("store: commit duplicate: %w", err)
		}
		return false, nil
	}

	for _, topic := range m.Topics {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO message_topics (message_id, topic)
			VALUES (?, ?);`,
			m.ID[:], topic,
		); err != nil {
			return false, fmt.Errorf("store: insert topic: %w", err)
		}
	}
	if err := insertMessageSearchDocTx(ctx, tx, m); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit: %w", err)
	}
	return true, nil
}

func ensureGroupSyncStateTx(ctx context.Context, tx *sql.Tx, groupID entmoot.GroupID) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO group_sync_state
		  (group_id, generation, root_generation, coverage_floor_ms)
		VALUES (
		  ?,
		  CASE WHEN EXISTS (SELECT 1 FROM messages WHERE group_id = ?) THEN 1 ELSE 0 END,
		  -1,
		  0
		);`,
		groupID[:], groupID[:],
	); err != nil {
		return fmt.Errorf("store: ensure group sync state: %w", err)
	}
	return nil
}

func ensureGroupSyncStateDB(ctx context.Context, db *sql.DB, groupID entmoot.GroupID) error {
	if _, err := db.ExecContext(ctx, `
		INSERT OR IGNORE INTO group_sync_state
		  (group_id, generation, root_generation, coverage_floor_ms)
		VALUES (
		  ?,
		  CASE WHEN EXISTS (SELECT 1 FROM messages WHERE group_id = ?) THEN 1 ELSE 0 END,
		  -1,
		  0
		);`,
		groupID[:], groupID[:],
	); err != nil {
		return fmt.Errorf("store: ensure group sync state: %w", err)
	}
	return nil
}
func bumpGroupGenerationTx(ctx context.Context, tx *sql.Tx, groupID entmoot.GroupID) error {
	if err := ensureGroupSyncStateTx(ctx, tx, groupID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE group_sync_state
		SET generation = generation + 1, root_generation = -1, merkle_root = NULL
		WHERE group_id = ?;`,
		groupID[:],
	); err != nil {
		return fmt.Errorf("store: bump group generation: %w", err)
	}
	return nil
}

// PruneBefore removes messages in groupID older than beforeMillis.
func (s *SQLite) PruneBefore(ctx context.Context, groupID entmoot.GroupID, beforeMillis int64) (int64, error) {
	return s.PruneBeforeExceptTopics(ctx, groupID, beforeMillis, nil)
}

// PruneBeforeExceptTopics removes old content messages but preserves messages
// carrying exemptTopics.
func (s *SQLite) PruneBeforeExceptTopics(ctx context.Context, groupID entmoot.GroupID, beforeMillis int64, exemptTopics []string) (int64, error) {
	if beforeMillis <= 0 {
		return 0, nil
	}
	db, err := s.dbFor(groupID)
	if err != nil {
		return 0, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: begin prune tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureGroupSyncStateTx(ctx, tx, groupID); err != nil {
		return 0, err
	}

	var oldCoverageFloor int64
	if err := tx.QueryRowContext(ctx, `
		SELECT coverage_floor_ms FROM group_sync_state WHERE group_id = ?;`,
		groupID[:],
	).Scan(&oldCoverageFloor); err != nil {
		return 0, fmt.Errorf("store: read coverage floor: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE group_sync_state
		SET coverage_floor_ms = MAX(coverage_floor_ms, ?)
		WHERE group_id = ?;`,
		beforeMillis, groupID[:],
	); err != nil {
		return 0, fmt.Errorf("store: advance coverage floor: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM message_tombstones
		WHERE message_id IN (
		  SELECT message_id FROM message_tombstones
		  WHERE group_id = ? AND timestamp_ms < ? AND pruned_at_ms < ?
		  ORDER BY pruned_at_ms
		  LIMIT ?
		);`,
		groupID[:], beforeMillis, time.Now().Add(-tombstoneMinimumAge).UnixMilli(), tombstoneCleanupBatch,
	); err != nil {
		return 0, fmt.Errorf("store: clean tombstones: %w", err)
	}

	exemptClause, exemptArgs := pruneExemptTopicClause(exemptTopics)
	var liveTombstones, newTombstones int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM message_tombstones WHERE group_id = ?;`,
		groupID[:],
	).Scan(&liveTombstones); err != nil {
		return 0, fmt.Errorf("store: count tombstones: %w", err)
	}
	countArgs := append([]any{groupID[:], beforeMillis}, exemptArgs...)
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM messages
		WHERE group_id = ? AND timestamp_ms < ?`+exemptClause+`
		  AND NOT EXISTS (
		    SELECT 1 FROM message_tombstones tomb
		    WHERE tomb.message_id = messages.message_id
		  );`,
		countArgs...,
	).Scan(&newTombstones); err != nil {
		return 0, fmt.Errorf("store: count new tombstones: %w", err)
	}
	if liveTombstones+newTombstones > maxLiveTombstonesPerGroup {
		return 0, fmt.Errorf("store: prune would exceed tombstone cap %d", maxLiveTombstonesPerGroup)
	}

	insertArgs := append([]any{time.Now().UnixMilli(), groupID[:], beforeMillis}, exemptArgs...)
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO message_tombstones
		  (message_id, group_id, timestamp_ms, pruned_at_ms)
		SELECT message_id, group_id, timestamp_ms, ?
		FROM messages
		WHERE group_id = ? AND timestamp_ms < ?`+exemptClause+`;`,
		insertArgs...,
	); err != nil {
		return 0, fmt.Errorf("store: insert tombstones: %w", err)
	}

	deleteArgs := append([]any{groupID[:], beforeMillis}, exemptArgs...)
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM message_topics
		WHERE message_id IN (
		  SELECT message_id FROM messages
		  WHERE group_id = ? AND timestamp_ms < ?`+exemptClause+`
		);`,
		deleteArgs...,
	); err != nil {
		return 0, fmt.Errorf("store: prune topics: %w", err)
	}
	deleteArgs = append([]any{groupID[:], beforeMillis}, exemptArgs...)
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM message_search_docs
		WHERE message_id IN (
		  SELECT message_id FROM messages
		  WHERE group_id = ? AND timestamp_ms < ?`+exemptClause+`
		);`,
		deleteArgs...,
	); err != nil {
		return 0, fmt.Errorf("store: prune search docs: %w", err)
	}
	deleteArgs = append([]any{groupID[:], beforeMillis}, exemptArgs...)
	res, err := tx.ExecContext(ctx, `
		DELETE FROM messages
		WHERE group_id = ? AND timestamp_ms < ?`+exemptClause+`;`,
		deleteArgs...,
	)
	if err != nil {
		return 0, fmt.Errorf("store: prune messages: %w", err)
	}
	pruned, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: prune rows affected: %w", err)
	}
	// Row deletes already advance generation through messages_sync_delete.
	// Advance it explicitly only when the coverage metadata changed alone.
	if pruned == 0 && beforeMillis > oldCoverageFloor {
		if err := bumpGroupGenerationTx(ctx, tx, groupID); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: commit prune: %w", err)
	}
	return pruned, nil
}

func pruneExemptTopicClause(topics []string) (string, []any) {
	if len(topics) == 0 {
		return "", nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(topics)), ",")
	args := make([]any, 0, len(topics))
	for _, topic := range topics {
		args = append(args, topic)
	}
	return `
		  AND NOT EXISTS (
		    SELECT 1 FROM message_topics keep
		    WHERE keep.message_id = messages.message_id
		      AND keep.topic IN (` + placeholders + `)
		  )`, args
}

// Get implements MessageStore.Get.
func (s *SQLite) Get(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error) {
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return entmoot.Message{}, err
	}
	if !exists {
		return entmoot.Message{}, ErrNotFound
	}

	row := db.QueryRowContext(ctx, `
		SELECT canonical_bytes FROM messages
		WHERE message_id = ? AND group_id = ?;`,
		id[:], groupID[:],
	)
	var canonBytes []byte
	if err := row.Scan(&canonBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return entmoot.Message{}, ErrNotFound
		}
		return entmoot.Message{}, fmt.Errorf("store: scan: %w", err)
	}
	return decodeMessage(canonBytes)
}

// Has implements MessageStore.Has. Never returns ErrNotFound.
func (s *SQLite) Has(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT 1 FROM messages
		WHERE message_id = ? AND group_id = ?
		LIMIT 1;`,
		id[:], groupID[:],
	).Scan(&n); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("store: scan: %w", err)
	}
	return true, nil
}

// Range implements MessageStore.Range. Reads are served with SQLite's shared
// lock; in WAL mode they never block concurrent writers. The returned slice
// is passed through order.Topological before being returned.
func (s *SQLite) Range(ctx context.Context, groupID entmoot.GroupID, sinceMillis, untilMillis int64) ([]entmoot.Message, error) {
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []entmoot.Message{}, nil
	}

	// "No upper bound" sentinel is untilMillis == 0 per the interface docs.
	var (
		rows *sql.Rows
	)
	if untilMillis == 0 {
		rows, err = db.QueryContext(ctx, `
			SELECT canonical_bytes FROM messages
			WHERE group_id = ? AND timestamp_ms >= ?
			ORDER BY timestamp_ms, author_member_id, message_id;`,
			groupID[:], sinceMillis,
		)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT canonical_bytes FROM messages
			WHERE group_id = ? AND timestamp_ms >= ? AND timestamp_ms < ?
			ORDER BY timestamp_ms, author_member_id, message_id;`,
			groupID[:], sinceMillis, untilMillis,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("store: range query: %w", err)
	}
	defer rows.Close()

	var candidates []entmoot.Message
	for rows.Next() {
		var canonBytes []byte
		if err := rows.Scan(&canonBytes); err != nil {
			return nil, fmt.Errorf("store: range scan: %w", err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: range iterate: %w", err)
	}

	return topoOrder(candidates)
}

// MessageIDsPage returns one timestamp/author/id keyset page from a committed
// group generation. A caller continuing an older generation receives
// SnapshotChanged and must restart rather than silently skip a concurrent
// insert or prune.
func (s *SQLite) MessageIDsPage(ctx context.Context, groupID entmoot.GroupID, sinceMillis int64, after *RangeCursor, expectedGeneration uint64, limit int) (MessageIDPage, error) {
	return s.MessageIDsPageWindow(ctx, groupID, sinceMillis, 0, after, expectedGeneration, limit)
}

// MessageIDsPageWindow is MessageIDsPage constrained to [sinceMillis,
// untilMillis). Zero untilMillis has no upper bound.
func (s *SQLite) MessageIDsPageWindow(ctx context.Context, groupID entmoot.GroupID, sinceMillis, untilMillis int64, after *RangeCursor, expectedGeneration uint64, limit int) (MessageIDPage, error) {
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return MessageIDPage{}, err
	}
	if !exists {
		return MessageIDPage{IDs: []entmoot.MessageID{}}, nil
	}
	if limit <= 0 {
		limit = 256
	}
	if limit > 1024 {
		limit = 1024
	}
	if err := ensureGroupSyncStateDB(ctx, db, groupID); err != nil {
		return MessageIDPage{}, err
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return MessageIDPage{}, fmt.Errorf("store: begin message-id page: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var generation uint64
	var coverageFloor int64
	if err := tx.QueryRowContext(ctx, `
		SELECT generation, coverage_floor_ms
		FROM group_sync_state WHERE group_id = ?;`,
		groupID[:],
	).Scan(&generation, &coverageFloor); err != nil {
		return MessageIDPage{}, fmt.Errorf("store: read message-id page generation: %w", err)
	}
	if expectedGeneration != 0 && expectedGeneration != generation {
		if err := tx.Commit(); err != nil {
			return MessageIDPage{}, fmt.Errorf("store: finish changed message-id page: %w", err)
		}
		return MessageIDPage{
			IDs:             []entmoot.MessageID{},
			Generation:      generation,
			SnapshotChanged: true,
			CoverageFloorMS: coverageFloor,
		}, nil
	}

	var rows *sql.Rows
	if after == nil {
		rows, err = tx.QueryContext(ctx, `
			SELECT message_id, timestamp_ms, author_member_id
			FROM messages
			WHERE group_id = ? AND timestamp_ms >= ?
			  AND (? = 0 OR timestamp_ms < ?)
			ORDER BY timestamp_ms, author_member_id, message_id
			LIMIT ?;`,
			groupID[:], sinceMillis, untilMillis, untilMillis, limit+1,
		)
	} else {
		rows, err = tx.QueryContext(ctx, `
			SELECT message_id, timestamp_ms, author_member_id
			FROM messages
			WHERE group_id = ? AND timestamp_ms >= ?
			  AND (? = 0 OR timestamp_ms < ?)
			  AND (
			    timestamp_ms > ?
			    OR (timestamp_ms = ? AND author_member_id > ?)
			    OR (timestamp_ms = ? AND author_member_id = ? AND message_id > ?)
			  )
			ORDER BY timestamp_ms, author_member_id, message_id
			LIMIT ?;`,
			groupID[:], sinceMillis, untilMillis, untilMillis,
			after.TimestampMS,
			after.TimestampMS, after.AuthorMemberID[:],
			after.TimestampMS, after.AuthorMemberID[:], after.ID[:],
			limit+1,
		)
	}
	if err != nil {
		return MessageIDPage{}, fmt.Errorf("store: message-id page query: %w", err)
	}

	type pageRow struct {
		id        entmoot.MessageID
		timestamp int64
		author    entmoot.MemberID
	}
	pageRows := make([]pageRow, 0, limit+1)
	for rows.Next() {
		var rawID []byte
		var row pageRow
		var author []byte
		if err := rows.Scan(&rawID, &row.timestamp, &author); err != nil {
			_ = rows.Close()
			return MessageIDPage{}, fmt.Errorf("store: message-id page scan: %w", err)
		}
		if len(rawID) != len(row.id) {
			_ = rows.Close()
			return MessageIDPage{}, fmt.Errorf("store: message-id page id has %d bytes", len(rawID))
		}
		copy(row.id[:], rawID)
		if len(author) != len(row.author) {
			_ = rows.Close()
			return MessageIDPage{}, fmt.Errorf("store: message-id page author has %d bytes", len(author))
		}
		copy(row.author[:], author)
		pageRows = append(pageRows, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return MessageIDPage{}, fmt.Errorf("store: message-id page iterate: %w", err)
	}
	if err := rows.Close(); err != nil {
		return MessageIDPage{}, fmt.Errorf("store: close message-id page: %w", err)
	}
	hasMore := len(pageRows) > limit
	if hasMore {
		pageRows = pageRows[:limit]
	}
	page := MessageIDPage{
		IDs:             make([]entmoot.MessageID, len(pageRows)),
		Generation:      generation,
		HasMore:         hasMore,
		CoverageFloorMS: coverageFloor,
	}
	for i := range pageRows {
		page.IDs[i] = pageRows[i].id
	}
	if hasMore && len(pageRows) > 0 {
		last := pageRows[len(pageRows)-1]
		page.Next = &RangeCursor{TimestampMS: last.timestamp, AuthorMemberID: last.author, ID: last.id}
	}
	if err := tx.Commit(); err != nil {
		return MessageIDPage{}, fmt.Errorf("store: finish message-id page: %w", err)
	}
	return page, nil
}

func (s *SQLite) HasTombstone(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	var found int
	err = db.QueryRowContext(ctx, `
		SELECT 1 FROM message_tombstones
		WHERE group_id = ? AND message_id = ?
		LIMIT 1;`,
		groupID[:], id[:],
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: query tombstone: %w", err)
	}
	return true, nil
}

func (s *SQLite) CoverageFloor(ctx context.Context, groupID entmoot.GroupID) (int64, error) {
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if err := ensureGroupSyncStateDB(ctx, db, groupID); err != nil {
		return 0, err
	}
	var floor int64
	if err := db.QueryRowContext(ctx, `
		SELECT coverage_floor_ms FROM group_sync_state
		WHERE group_id = ?;`,
		groupID[:],
	).Scan(&floor); err != nil {
		return 0, fmt.Errorf("store: query coverage floor: %w", err)
	}
	return floor, nil
}

// Latest implements MessageStore.Latest.
func (s *SQLite) Latest(ctx context.Context, groupID entmoot.GroupID, limit int) ([]entmoot.Message, error) {
	if limit <= 0 {
		return []entmoot.Message{}, nil
	}
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []entmoot.Message{}, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT canonical_bytes FROM messages
		WHERE group_id = ?
		ORDER BY timestamp_ms DESC, author_member_id DESC, message_id DESC
		LIMIT ?;`,
		groupID[:], limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: latest query: %w", err)
	}
	defer rows.Close()

	candidates := make([]entmoot.Message, 0, limit)
	for rows.Next() {
		var canonBytes []byte
		if err := rows.Scan(&canonBytes); err != nil {
			return nil, fmt.Errorf("store: latest scan: %w", err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: latest iterate: %w", err)
	}

	return topoOrder(candidates)
}

// LatestBefore implements MessageStore.LatestBefore.
func (s *SQLite) LatestBefore(ctx context.Context, groupID entmoot.GroupID, limit int, boundary *PageBoundary) ([]entmoot.Message, error) {
	if limit <= 0 {
		return []entmoot.Message{}, nil
	}
	if boundary == nil {
		return s.Latest(ctx, groupID, limit)
	}
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []entmoot.Message{}, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT canonical_bytes FROM messages
		WHERE group_id = ?
		  AND (
		    timestamp_ms < ?
		    OR (timestamp_ms = ? AND author_member_id < ?)
		    OR (timestamp_ms = ? AND author_member_id = ? AND message_id < ?)
		  )
		ORDER BY timestamp_ms DESC, author_member_id DESC, message_id DESC
		LIMIT ?;`,
		groupID[:],
		boundary.TimestampMS,
		boundary.TimestampMS, boundary.AuthorMemberID[:],
		boundary.TimestampMS, boundary.AuthorMemberID[:], boundary.MessageID[:],
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: latest before query: %w", err)
	}
	defer rows.Close()

	candidates := make([]entmoot.Message, 0, limit)
	for rows.Next() {
		var canonBytes []byte
		if err := rows.Scan(&canonBytes); err != nil {
			return nil, fmt.Errorf("store: latest before scan: %w", err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: latest before iterate: %w", err)
	}

	return topoOrder(candidates)
}

// Topics implements MessageStore.Topics.
func (s *SQLite) Topics(ctx context.Context, groupID entmoot.GroupID, limit int) ([]TopicSummary, error) {
	if limit <= 0 {
		return []TopicSummary{}, nil
	}
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []TopicSummary{}, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT mt.topic, COUNT(*) AS count, MAX(m.timestamp_ms) AS latest_message_at_ms
		FROM message_topics mt
		JOIN messages m ON m.message_id = mt.message_id
		WHERE m.group_id = ?
		GROUP BY mt.topic
		ORDER BY count DESC, latest_message_at_ms DESC, mt.topic ASC
		LIMIT ?;`,
		groupID[:], limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: topics query: %w", err)
	}
	defer rows.Close()

	out := make([]TopicSummary, 0, limit)
	for rows.Next() {
		var summary TopicSummary
		if err := rows.Scan(&summary.Topic, &summary.Count, &summary.LatestMessageAtMS); err != nil {
			return nil, fmt.Errorf("store: topics scan: %w", err)
		}
		out = append(out, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: topics iterate: %w", err)
	}
	return out, nil
}

// LatestByTopic implements MessageStore.LatestByTopic.
func (s *SQLite) LatestByTopic(ctx context.Context, groupID entmoot.GroupID, topic string, limit int) ([]entmoot.Message, error) {
	if limit <= 0 || topic == "" {
		return []entmoot.Message{}, nil
	}
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []entmoot.Message{}, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT m.canonical_bytes FROM messages m
		JOIN message_topics mt ON mt.message_id = m.message_id
		WHERE m.group_id = ? AND mt.topic = ?
		ORDER BY m.timestamp_ms DESC, m.author_member_id DESC, m.message_id DESC
		LIMIT ?;`,
		groupID[:], topic, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: latest by topic query: %w", err)
	}
	defer rows.Close()

	candidates := make([]entmoot.Message, 0, limit)
	for rows.Next() {
		var canonBytes []byte
		if err := rows.Scan(&canonBytes); err != nil {
			return nil, fmt.Errorf("store: latest by topic scan: %w", err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: latest by topic iterate: %w", err)
	}

	return topoOrder(candidates)
}

// LatestByTopicBefore implements MessageStore.LatestByTopicBefore.
func (s *SQLite) LatestByTopicBefore(ctx context.Context, groupID entmoot.GroupID, topic string, limit int, boundary *PageBoundary) ([]entmoot.Message, error) {
	if limit <= 0 || topic == "" {
		return []entmoot.Message{}, nil
	}
	if boundary == nil {
		return s.LatestByTopic(ctx, groupID, topic, limit)
	}
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []entmoot.Message{}, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT m.canonical_bytes FROM messages m
		JOIN message_topics mt ON mt.message_id = m.message_id
		WHERE m.group_id = ? AND mt.topic = ?
		  AND (
		    m.timestamp_ms < ?
		    OR (m.timestamp_ms = ? AND m.author_member_id < ?)
		    OR (m.timestamp_ms = ? AND m.author_member_id = ? AND m.message_id < ?)
		  )
		ORDER BY m.timestamp_ms DESC, m.author_member_id DESC, m.message_id DESC
		LIMIT ?;`,
		groupID[:], topic,
		boundary.TimestampMS,
		boundary.TimestampMS, boundary.AuthorMemberID[:],
		boundary.TimestampMS, boundary.AuthorMemberID[:], boundary.MessageID[:],
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: latest by topic before query: %w", err)
	}
	defer rows.Close()

	candidates := make([]entmoot.Message, 0, limit)
	for rows.Next() {
		var canonBytes []byte
		if err := rows.Scan(&canonBytes); err != nil {
			return nil, fmt.Errorf("store: latest by topic before scan: %w", err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: latest by topic before iterate: %w", err)
	}

	return topoOrder(candidates)
}

// MessageContext implements MessageContexter using keyset lookups around the
// target row.
func (s *SQLite) MessageContext(ctx context.Context, groupID entmoot.GroupID, messageID entmoot.MessageID, opts MessageContextOptions) (MessageContextResult, error) {
	opts = NormalizeMessageContextOptions(opts)
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return MessageContextResult{}, err
	}
	if !exists {
		return MessageContextResult{}, ErrNotFound
	}

	target, err := s.messageContextTarget(ctx, db, groupID, messageID, opts.Topic)
	if err != nil {
		return MessageContextResult{}, err
	}

	olderLimit := opts.Before + 1
	older, err := s.messageContextOlder(ctx, db, groupID, target, opts.Topic, olderLimit)
	if err != nil {
		return MessageContextResult{}, err
	}
	hasMoreOlder := len(older) > opts.Before
	if hasMoreOlder {
		older = older[:opts.Before]
	}
	sortMessagesOldestFirst(older)

	newer, err := s.messageContextNewer(ctx, db, groupID, target, opts.Topic, opts.After)
	if err != nil {
		return MessageContextResult{}, err
	}
	sortMessagesOldestFirst(newer)

	messages := make([]entmoot.Message, 0, len(older)+1+len(newer))
	messages = append(messages, older...)
	messages = append(messages, target)
	messages = append(messages, newer...)
	return messageContextResult(target, messages, hasMoreOlder), nil
}

func (s *SQLite) messageContextTarget(ctx context.Context, db *sql.DB, groupID entmoot.GroupID, messageID entmoot.MessageID, topic string) (entmoot.Message, error) {
	args := []any{groupID[:], messageID[:]}
	where := strings.Builder{}
	where.WriteString(`WHERE m.group_id = ? AND m.message_id = ?`)
	if topic != "" {
		where.WriteString(`
		  AND EXISTS (
		    SELECT 1 FROM message_topics mt
		    WHERE mt.message_id = m.message_id AND mt.topic = ?
		  )`)
		args = append(args, topic)
	}
	var canonBytes []byte
	err := db.QueryRowContext(ctx, `
		SELECT m.canonical_bytes FROM messages m
		`+where.String()+`
		LIMIT 1;`,
		args...,
	).Scan(&canonBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return entmoot.Message{}, ErrNotFound
		}
		return entmoot.Message{}, fmt.Errorf("store: message context target scan: %w", err)
	}
	return decodeMessage(canonBytes)
}

func (s *SQLite) messageContextOlder(ctx context.Context, db *sql.DB, groupID entmoot.GroupID, target entmoot.Message, topic string, limit int) ([]entmoot.Message, error) {
	if limit <= 0 {
		return []entmoot.Message{}, nil
	}
	targetAuthor := messageMemberID(target)
	args := []any{
		groupID[:],
		target.Timestamp,
		target.Timestamp, targetAuthor[:],
		target.Timestamp, targetAuthor[:], target.ID[:],
	}
	where := strings.Builder{}
	where.WriteString(`
		WHERE m.group_id = ?
		  AND (
		    m.timestamp_ms < ?
		    OR (m.timestamp_ms = ? AND m.author_member_id < ?)
		    OR (m.timestamp_ms = ? AND m.author_member_id = ? AND m.message_id < ?)
		  )`)
	if topic != "" {
		where.WriteString(`
		  AND EXISTS (
		    SELECT 1 FROM message_topics mt
		    WHERE mt.message_id = m.message_id AND mt.topic = ?
		  )`)
		args = append(args, topic)
	}
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, `
		SELECT m.canonical_bytes FROM messages m
		`+where.String()+`
		ORDER BY m.timestamp_ms DESC, m.author_member_id DESC, m.message_id DESC
		LIMIT ?;`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("store: message context older query: %w", err)
	}
	defer rows.Close()
	return decodeMessageRows(rows, "message context older")
}

func (s *SQLite) messageContextNewer(ctx context.Context, db *sql.DB, groupID entmoot.GroupID, target entmoot.Message, topic string, limit int) ([]entmoot.Message, error) {
	if limit <= 0 {
		return []entmoot.Message{}, nil
	}
	targetAuthor := messageMemberID(target)
	args := []any{
		groupID[:],
		target.Timestamp,
		target.Timestamp, targetAuthor[:],
		target.Timestamp, targetAuthor[:], target.ID[:],
	}
	where := strings.Builder{}
	where.WriteString(`
		WHERE m.group_id = ?
		  AND (
		    m.timestamp_ms > ?
		    OR (m.timestamp_ms = ? AND m.author_member_id > ?)
		    OR (m.timestamp_ms = ? AND m.author_member_id = ? AND m.message_id > ?)
		  )`)
	if topic != "" {
		where.WriteString(`
		  AND EXISTS (
		    SELECT 1 FROM message_topics mt
		    WHERE mt.message_id = m.message_id AND mt.topic = ?
		  )`)
		args = append(args, topic)
	}
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, `
		SELECT m.canonical_bytes FROM messages m
		`+where.String()+`
		ORDER BY m.timestamp_ms ASC, m.author_member_id ASC, m.message_id ASC
		LIMIT ?;`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("store: message context newer query: %w", err)
	}
	defer rows.Close()
	return decodeMessageRows(rows, "message context newer")
}

func decodeMessageRows(rows *sql.Rows, label string) ([]entmoot.Message, error) {
	var out []entmoot.Message
	for rows.Next() {
		var canonBytes []byte
		if err := rows.Scan(&canonBytes); err != nil {
			return nil, fmt.Errorf("store: %s scan: %w", label, err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: %s iterate: %w", label, err)
	}
	if out == nil {
		out = []entmoot.Message{}
	}
	return out, nil
}

// SearchMessages implements MessageSearcher using SQLite FTS5.
func (s *SQLite) SearchMessages(ctx context.Context, groupID entmoot.GroupID, query SearchQuery, opts SearchOptions) (SearchResult, error) {
	if opts.Limit <= 0 {
		return SearchResult{Hits: []SearchHit{}}, nil
	}
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return SearchResult{}, err
	}
	if !exists {
		return SearchResult{Hits: []SearchHit{}}, nil
	}

	args := []any{groupID[:], query.FTS5}
	where := strings.Builder{}
	where.WriteString(`
		WHERE d.group_id = ?
		  AND message_search_fts MATCH ?`)
	if opts.Topic != "" {
		where.WriteString(`
		  AND EXISTS (
		    SELECT 1 FROM message_topics mt
		    WHERE mt.message_id = d.message_id AND mt.topic = ?
		  )`)
		args = append(args, opts.Topic)
	}
	if opts.CursorBoundary != nil {
		where.WriteString(`
		  AND (
		    d.timestamp_ms < ?
		    OR (d.timestamp_ms = ? AND d.author_member_id < ?)
		    OR (d.timestamp_ms = ? AND d.author_member_id = ? AND d.message_id < ?)
		  )`)
		boundary := opts.CursorBoundary
		args = append(args,
			boundary.TimestampMS,
			boundary.TimestampMS, boundary.AuthorMemberID[:],
			boundary.TimestampMS, boundary.AuthorMemberID[:], boundary.MessageID[:],
		)
	}
	args = append(args, opts.Limit+1)

	rows, err := db.QueryContext(ctx, `
		SELECT m.canonical_bytes,
		       snippet(message_search_fts, 0, '[', ']', '...', 12) AS snippet
		FROM message_search_fts
		JOIN message_search_docs d ON d.doc_id = message_search_fts.rowid
		JOIN messages m ON m.message_id = d.message_id AND m.group_id = d.group_id
		`+where.String()+`
		ORDER BY d.timestamp_ms DESC, d.author_member_id DESC, d.message_id DESC
		LIMIT ?;`,
		args...,
	)
	if err != nil {
		return SearchResult{}, fmt.Errorf("store: search query: %w", err)
	}
	defer rows.Close()

	hits := make([]SearchHit, 0, opts.Limit)
	for rows.Next() {
		var (
			canonBytes []byte
			snippet    string
		)
		if err := rows.Scan(&canonBytes, &snippet); err != nil {
			return SearchResult{}, fmt.Errorf("store: search scan: %w", err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return SearchResult{}, err
		}
		hits = append(hits, SearchHit{Message: msg, Snippet: snippet})
	}
	if err := rows.Err(); err != nil {
		return SearchResult{}, fmt.Errorf("store: search iterate: %w", err)
	}

	result := SearchResult{Hits: hits}
	if len(result.Hits) > opts.Limit {
		result.HasMore = true
		result.Hits = result.Hits[:opts.Limit]
	}
	if result.HasMore && len(result.Hits) > 0 {
		boundary := searchBoundaryFromMessage(result.Hits[len(result.Hits)-1].Message)
		result.NextCursorBoundary = &boundary
	}
	if result.Hits == nil {
		result.Hits = []SearchHit{}
	}
	return result, nil
}

// IterMessageIDsInIDRange implements MessageStore.IterMessageIDsInIDRange.
// Uses the PRIMARY KEY index on messages.message_id. Verified via
// EXPLAIN QUERY PLAN to issue "SEARCH messages USING INTEGER PRIMARY KEY"
// (or the BLOB-PK equivalent) during development.
func (s *SQLite) IterMessageIDsInIDRange(ctx context.Context, groupID entmoot.GroupID, loID, hiID entmoot.MessageID) ([]entmoot.MessageID, error) {
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []entmoot.MessageID{}, nil
	}

	var rows *sql.Rows
	if isZeroMessageID(hiID) {
		rows, err = db.QueryContext(ctx, `
			SELECT message_id FROM messages
			WHERE group_id = ? AND message_id >= ?
			ORDER BY message_id ASC;`,
			groupID[:], loID[:],
		)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT message_id FROM messages
			WHERE group_id = ? AND message_id >= ? AND message_id < ?
			ORDER BY message_id ASC;`,
			groupID[:], loID[:], hiID[:],
		)
	}
	if err != nil {
		return nil, fmt.Errorf("store: iter id range query: %w", err)
	}
	defer rows.Close()

	var out []entmoot.MessageID
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("store: iter id range scan: %w", err)
		}
		if len(raw) != 32 {
			return nil, fmt.Errorf("store: iter id range: message_id has %d bytes, want 32", len(raw))
		}
		var id entmoot.MessageID
		copy(id[:], raw)
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iter id range iterate: %w", err)
	}
	return out, nil
}

// MerkleRoot implements MessageStore.MerkleRoot.
func (s *SQLite) MerkleRoot(ctx context.Context, groupID entmoot.GroupID) ([32]byte, error) {
	db, exists, err := s.dbForExisting(groupID)
	if err != nil {
		return [32]byte{}, err
	}
	if !exists {
		return [32]byte{}, nil
	}
	if err := ensureGroupSyncStateDB(ctx, db, groupID); err != nil {
		return [32]byte{}, err
	}

	for attempt := 0; attempt < 8; attempt++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return [32]byte{}, fmt.Errorf("store: begin merkle snapshot: %w", err)
		}
		var generation, rootGeneration int64
		var cached []byte
		err = tx.QueryRowContext(ctx, `
			SELECT generation, root_generation, merkle_root
			FROM group_sync_state WHERE group_id = ?;`,
			groupID[:],
		).Scan(&generation, &rootGeneration, &cached)
		if errors.Is(err, sql.ErrNoRows) {
			generation, rootGeneration, cached = 0, -1, nil
		} else if err != nil {
			_ = tx.Rollback()
			return [32]byte{}, fmt.Errorf("store: read merkle cache: %w", err)
		}
		if rootGeneration == generation && len(cached) == 32 {
			var root [32]byte
			copy(root[:], cached)
			if err := tx.Commit(); err != nil {
				return [32]byte{}, fmt.Errorf("store: finish merkle snapshot: %w", err)
			}
			return root, nil
		}

		root, err := merkleRootTx(ctx, tx, groupID)
		if err != nil {
			_ = tx.Rollback()
			return [32]byte{}, err
		}
		if err := tx.Commit(); err != nil {
			return [32]byte{}, fmt.Errorf("store: finish merkle snapshot: %w", err)
		}

		if _, err := db.ExecContext(ctx, `
			INSERT OR IGNORE INTO group_sync_state
			  (group_id, generation, root_generation, coverage_floor_ms)
			VALUES (?, 0, -1, 0);`,
			groupID[:],
		); err != nil {
			return [32]byte{}, fmt.Errorf("store: initialize merkle cache: %w", err)
		}
		result, err := db.ExecContext(ctx, `
			UPDATE group_sync_state
			SET merkle_root = ?, root_generation = ?
			WHERE group_id = ? AND generation = ?;`,
			root[:], generation, groupID[:], generation,
		)
		if err != nil {
			return [32]byte{}, fmt.Errorf("store: update merkle cache: %w", err)
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return [32]byte{}, fmt.Errorf("store: inspect merkle cache update: %w", err)
		}
		if updated == 1 {
			return root, nil
		}
	}
	return [32]byte{}, errors.New("store: merkle snapshot changed repeatedly")
}

func merkleRootTx(ctx context.Context, tx *sql.Tx, groupID entmoot.GroupID) ([32]byte, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT canonical_bytes FROM messages
		WHERE group_id = ?
		ORDER BY timestamp_ms, author_member_id, message_id;`,
		groupID[:],
	)
	if err != nil {
		return [32]byte{}, fmt.Errorf("store: merkle query: %w", err)
	}
	defer rows.Close()

	var all []entmoot.Message
	for rows.Next() {
		var canonBytes []byte
		if err := rows.Scan(&canonBytes); err != nil {
			return [32]byte{}, fmt.Errorf("store: merkle scan: %w", err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return [32]byte{}, err
		}
		all = append(all, msg)
	}
	if err := rows.Err(); err != nil {
		return [32]byte{}, fmt.Errorf("store: merkle iterate: %w", err)
	}
	if len(all) == 0 {
		return [32]byte{}, nil
	}
	ids, err := order.Topological(all)
	if err != nil {
		return [32]byte{}, err
	}
	return merkle.New(ids).Root(), nil
}

// dbFor returns the database for groupID, creating it on first write.
func (s *SQLite) dbFor(groupID entmoot.GroupID) (*sql.DB, error) {
	s.mu.RLock()
	db, ok := s.dbs[groupID]
	s.mu.RUnlock()
	if ok {
		return db, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if db, ok := s.dbs[groupID]; ok {
		return db, nil
	}

	db, err := openGroupDB(s.groupsDir, groupID)
	if err != nil {
		return nil, err
	}
	s.dbs[groupID] = db
	return db, nil
}

// dbForExisting returns the database for groupID without creating a group
// directory or database on a read miss.
func (s *SQLite) dbForExisting(groupID entmoot.GroupID) (*sql.DB, bool, error) {
	s.mu.RLock()
	db, ok := s.dbs[groupID]
	s.mu.RUnlock()
	if ok {
		return db, true, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if db, ok := s.dbs[groupID]; ok {
		return db, true, nil
	}

	dbPath := filepath.Join(s.groupsDir, encodeGroupDirName(groupID), "messages.sqlite")
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, fmt.Errorf("store: stat %q: %w", dbPath, err)
	}

	db, err := openSQLiteDB(dbPath)
	if err != nil {
		return nil, false, err
	}
	s.dbs[groupID] = db
	return db, true, nil
}

// openGroupDB opens or creates the messages.sqlite for groupID.
func openGroupDB(groupsDir string, groupID entmoot.GroupID) (*sql.DB, error) {
	dir := filepath.Join(groupsDir, encodeGroupDirName(groupID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: mkdir group %q: %w", dir, err)
	}
	dbPath := filepath.Join(dir, "messages.sqlite")
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		f, createErr := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if createErr != nil && !errors.Is(createErr, os.ErrExist) {
			return nil, fmt.Errorf("store: precreate %q: %w", dbPath, createErr)
		}
		if f != nil {
			if err := f.Close(); err != nil {
				return nil, fmt.Errorf("store: close precreate %q: %w", dbPath, err)
			}
		}
	} else if err != nil {
		return nil, fmt.Errorf("store: stat %q: %w", dbPath, err)
	}
	return openSQLiteDB(dbPath)
}

func openSQLiteDB(dbPath string) (*sql.DB, error) {
	q := url.Values{}
	q.Set("mode", "rw")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	dsn := "file:" + dbPath + "?" + q.Encode()

	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite %q: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: ping sqlite %q: %w", dbPath, err)
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}
	if err := backfillMessageSearchDocs(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func insertMessageSearchDocTx(ctx context.Context, tx *sql.Tx, m entmoot.Message) error {
	authorMemberID := messageMemberID(m)
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO message_search_docs
		  (message_id, group_id, author_member_id, timestamp_ms, content_text, topics_text)
		VALUES (?, ?, ?, ?, ?, ?);`,
		m.ID[:],
		m.GroupID[:],
		authorMemberID[:],
		m.Timestamp,
		string(m.Content),
		strings.Join(m.Topics, "\n"),
	); err != nil {
		return fmt.Errorf("store: insert search doc: %w", err)
	}
	return nil
}

func backfillMessageSearchDocs(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin search backfill tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
		SELECT m.canonical_bytes
		FROM messages m
		LEFT JOIN message_search_docs d ON d.message_id = m.message_id
		WHERE d.message_id IS NULL
		ORDER BY m.timestamp_ms, m.author_member_id, m.message_id;`)
	if err != nil {
		return fmt.Errorf("store: search backfill query: %w", err)
	}
	defer rows.Close()

	var messages []entmoot.Message
	for rows.Next() {
		var canonBytes []byte
		if err := rows.Scan(&canonBytes); err != nil {
			return fmt.Errorf("store: search backfill scan: %w", err)
		}
		msg, err := decodeMessage(canonBytes)
		if err != nil {
			return err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: search backfill iterate: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("store: search backfill close rows: %w", err)
	}
	for _, msg := range messages {
		if err := insertMessageSearchDocTx(ctx, tx, msg); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit search backfill: %w", err)
	}
	return nil
}

// parentsBlob packs 0..N parent message ids into a flat byte slice of
// len*32 bytes. Empty parents produces an empty slice (never nil), matching
// the non-null storage column semantics.
func parentsBlob(parents []entmoot.MessageID) []byte {
	out := make([]byte, 0, len(parents)*32)
	for _, p := range parents {
		out = append(out, p[:]...)
	}
	return out
}

// notNilBytes returns b if non-nil, otherwise a freshly-allocated empty byte
// slice. database/sql binds a nil []byte as SQL NULL, which is incompatible
// with our NOT NULL schema columns; callers pass every non-required blob
// column through this helper.
func notNilBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// decodeMessage reconstructs a Message from its canonical_bytes. The canonical
// encoding is the ground truth on disk; re-decoding it via encoding/json and
// re-encoding through canonical.Encode produces byte-identical output for rows
// this build wrote, which the shared test suite asserts. Rows written before
// the founder acceptance certificate was removed carry an extra `acceptance`
// object; decoding drops it, so those rows do not round-trip byte-identically.
// Nothing depends on that: ids and signatures derive from the signing form,
// which never covered acceptance.
func decodeMessage(canonBytes []byte) (entmoot.Message, error) {
	var msg entmoot.Message
	if err := json.Unmarshal(canonBytes, &msg); err != nil {
		return entmoot.Message{}, fmt.Errorf("store: decode canonical: %w", err)
	}
	return msg, nil
}

// encodeGroupDirName names a group's on-disk directory. Raw-url base64 keeps
// the 32-byte id in one path-safe segment with no padding character.
func encodeGroupDirName(gid entmoot.GroupID) string {
	return base64.RawURLEncoding.EncodeToString(gid[:])
}
