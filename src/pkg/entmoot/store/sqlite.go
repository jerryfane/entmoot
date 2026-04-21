package store

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
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/order"
	"entmoot/pkg/entmoot/wire"

	// Register the pure-Go SQLite driver under the name "sqlite".
	_ "modernc.org/sqlite"
)

// sqliteDriver is the database/sql driver name registered by modernc.org/sqlite.
const sqliteDriver = "sqlite"

// sqliteSchema is applied idempotently on first open of every per-group
// database. Mirrors docs/CLI_DESIGN.md §4.2 exactly.
const sqliteSchema = `
CREATE TABLE IF NOT EXISTS messages (
  message_id      BLOB PRIMARY KEY,
  group_id        BLOB NOT NULL,
  author_node_id  INTEGER NOT NULL,
  timestamp_ms    INTEGER NOT NULL,
  content         BLOB NOT NULL,
  parents         BLOB NOT NULL,
  signature       BLOB NOT NULL,
  canonical_bytes BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_group_time
  ON messages(group_id, timestamp_ms DESC);

CREATE INDEX IF NOT EXISTS idx_messages_group_author
  ON messages(group_id, author_node_id, timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS message_topics (
  message_id BLOB NOT NULL,
  topic      TEXT NOT NULL,
  PRIMARY KEY (message_id, topic)
);

CREATE INDEX IF NOT EXISTS idx_topic_lookup
  ON message_topics(topic, message_id);

-- transport_ads holds the LWW-Register set of peer transport
-- advertisements we have received and verified. One row per
-- (group, author). The canonical column is the full JSON-encoded
-- wire.TransportAd; decoding it reproduces every field (incl.
-- Signature) byte-for-byte. (v1.2.0)
CREATE TABLE IF NOT EXISTS transport_ads (
  group_id       BLOB NOT NULL,
  author_node_id INTEGER NOT NULL,
  seq            INTEGER NOT NULL,
  canonical      BLOB NOT NULL,
  issued_at_ms   INTEGER NOT NULL,
  not_after_ms   INTEGER NOT NULL,
  signature      BLOB NOT NULL,
  PRIMARY KEY (group_id, author_node_id)
);
CREATE INDEX IF NOT EXISTS idx_transport_ads_expiry
  ON transport_ads(not_after_ms);

-- transport_ad_seqs persists this node's own most-recent Seq per
-- (group, author=self) so BumpTransportAdSeq can increment atomically
-- across daemon restarts. Kept separate from transport_ads because
-- that table holds rows about OTHER peers' ads. (v1.2.0)
CREATE TABLE IF NOT EXISTS transport_ad_seqs (
  group_id       BLOB NOT NULL,
  author_node_id INTEGER NOT NULL,
  seq            INTEGER NOT NULL,
  PRIMARY KEY (group_id, author_node_id)
);
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
// a single transaction, using INSERT OR IGNORE so duplicates are silently
// accepted (idempotent per the interface contract).
func (s *SQLite) Put(ctx context.Context, m entmoot.Message) error {
	if isZeroGroupID(m.GroupID) {
		return fmt.Errorf("%w: zero group id", ErrInvalidMessage)
	}
	if isZeroMessageID(m.ID) {
		return fmt.Errorf("%w: zero message id", ErrInvalidMessage)
	}

	encoded, err := canonical.Encode(m)
	if err != nil {
		return fmt.Errorf("store: canonical encode: %w", err)
	}

	db, err := s.dbFor(m.GroupID)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	// Ensure a rollback runs if we don't commit. Rollback after Commit is a
	// harmless no-op that returns sql.ErrTxDone, which we deliberately ignore.
	defer func() { _ = tx.Rollback() }()

	// SQLite's database/sql driver maps a nil []byte to NULL, which would
	// violate the NOT NULL constraints on content/parents/signature (and
	// silently no-op under INSERT OR IGNORE). Coerce nil slices to an empty,
	// non-nil slice so an empty blob is stored as zero-length bytes.
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO messages
		  (message_id, group_id, author_node_id, timestamp_ms,
		   content, parents, signature, canonical_bytes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		m.ID[:],
		m.GroupID[:],
		int64(m.Author.PilotNodeID),
		m.Timestamp,
		notNilBytes(m.Content),
		parentsBlob(m.Parents),
		notNilBytes(m.Signature),
		encoded,
	); err != nil {
		return fmt.Errorf("store: insert message: %w", err)
	}

	for _, topic := range m.Topics {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO message_topics (message_id, topic)
			VALUES (?, ?);`,
			m.ID[:], topic,
		); err != nil {
			return fmt.Errorf("store: insert topic: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

// Get implements MessageStore.Get.
func (s *SQLite) Get(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error) {
	db, err := s.dbFor(groupID)
	if err != nil {
		return entmoot.Message{}, err
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
	db, err := s.dbFor(groupID)
	if err != nil {
		return false, err
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
	db, err := s.dbFor(groupID)
	if err != nil {
		return nil, err
	}

	// "No upper bound" sentinel is untilMillis == 0 per the interface docs.
	var (
		rows *sql.Rows
	)
	if untilMillis == 0 {
		rows, err = db.QueryContext(ctx, `
			SELECT canonical_bytes FROM messages
			WHERE group_id = ? AND timestamp_ms >= ?
			ORDER BY timestamp_ms, author_node_id, message_id;`,
			groupID[:], sinceMillis,
		)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT canonical_bytes FROM messages
			WHERE group_id = ? AND timestamp_ms >= ? AND timestamp_ms < ?
			ORDER BY timestamp_ms, author_node_id, message_id;`,
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

// MerkleRoot implements MessageStore.MerkleRoot.
func (s *SQLite) MerkleRoot(ctx context.Context, groupID entmoot.GroupID) ([32]byte, error) {
	db, err := s.dbFor(groupID)
	if err != nil {
		return [32]byte{}, err
	}

	rows, err := db.QueryContext(ctx, `
		SELECT canonical_bytes FROM messages
		WHERE group_id = ?
		ORDER BY timestamp_ms, author_node_id, message_id;`,
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

// dbFor returns the *sql.DB for groupID, opening it on first access.
// Safe for concurrent use via a read-lock fast path and a write-locked
// double-check on cache miss.
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

// openGroupDB opens or creates the messages.sqlite for groupID, applies the
// schema, and enables WAL + NORMAL sync. The group directory is created with
// 0700 and the database file with 0600 if freshly created.
func openGroupDB(groupsDir string, groupID entmoot.GroupID) (*sql.DB, error) {
	dir := filepath.Join(groupsDir, encodeGroupDirName(groupID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: mkdir group %q: %w", dir, err)
	}
	dbPath := filepath.Join(dir, "messages.sqlite")

	// Pre-create the database file with 0600 if it doesn't exist, so modernc
	// opens an existing file rather than creating one with the process
	// default umask. This makes the permission contract explicit regardless
	// of umask.
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("store: precreate %q: %w", dbPath, err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("store: close precreate %q: %w", dbPath, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("store: stat %q: %w", dbPath, err)
	}

	// Build the DSN with pragma URL params. modernc.org/sqlite runs each
	// _pragma=... value as a PRAGMA statement after opening.
	//
	// busy_timeout gives the SQLite library up to 5 s to acquire the writer
	// lock before returning SQLITE_BUSY. Under WAL the only writer-writer
	// contention comes from simultaneous Put calls in the same process, and
	// those serialize quickly; the timeout is a generous safety margin so
	// genuine concurrency tests don't spuriously trip the error.
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	dsn := "file:" + dbPath + "?" + q.Encode()

	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite %q: %w", dbPath, err)
	}
	// Ping to force the driver to actually open the file and run the pragmas
	// so we surface errors here rather than deep in a query path.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: ping sqlite %q: %w", dbPath, err)
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}
	return db, nil
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
// re-encoding through canonical.Encode produces byte-identical output, which
// the shared test suite asserts.
func decodeMessage(canonBytes []byte) (entmoot.Message, error) {
	var msg entmoot.Message
	if err := json.Unmarshal(canonBytes, &msg); err != nil {
		return entmoot.Message{}, fmt.Errorf("store: decode canonical: %w", err)
	}
	return msg, nil
}

// PutTransportAd stores a verified transport advertisement, replacing any
// existing entry for (GroupID, Author) where new.Seq > stored.Seq OR
// (new.Seq == stored.Seq AND new.Signature lexicographically > stored.Signature).
// Returns (replaced bool, err) where replaced is true iff the incoming ad
// was newer than what was stored (or there was no prior entry). A false
// return with nil err means "we already had an equal-or-newer ad; nothing
// changed."
//
// Uses a single SELECT+DELETE+INSERT transaction for atomicity. SQLite's
// INSERT ... ON CONFLICT DO UPDATE WHERE cannot express the lex-tiebreak
// cleanly, so the comparison is done in Go under tx isolation. (v1.2.0)
func (s *SQLite) PutTransportAd(ctx context.Context, ad wire.TransportAd) (bool, error) {
	if isZeroGroupID(ad.GroupID) {
		return false, fmt.Errorf("%w: zero group id", ErrInvalidMessage)
	}

	encoded, err := json.Marshal(ad)
	if err != nil {
		return false, fmt.Errorf("store: marshal transport ad: %w", err)
	}

	db, err := s.dbFor(ad.GroupID)
	if err != nil {
		return false, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("store: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		curSeq uint64
		curSig []byte
	)
	row := tx.QueryRowContext(ctx, `
		SELECT seq, signature FROM transport_ads
		WHERE group_id = ? AND author_node_id = ?;`,
		ad.GroupID[:], int64(ad.Author.PilotNodeID),
	)
	switch err := row.Scan(&curSeq, &curSig); {
	case errors.Is(err, sql.ErrNoRows):
		// No prior row; fall through and insert.
	case err != nil:
		return false, fmt.Errorf("store: scan transport ad: %w", err)
	default:
		// LWW-Register: keep the existing row iff it's strictly newer, or
		// equal-seq with a lex-greater-or-equal signature.
		if curSeq > ad.Seq {
			return false, nil
		}
		if curSeq == ad.Seq {
			cmp := bytes.Compare(ad.Signature, curSig)
			if cmp <= 0 {
				return false, nil
			}
		}
		// Incoming is newer; remove the stale row before re-inserting.
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM transport_ads
			WHERE group_id = ? AND author_node_id = ?;`,
			ad.GroupID[:], int64(ad.Author.PilotNodeID),
		); err != nil {
			return false, fmt.Errorf("store: delete stale transport ad: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO transport_ads
		  (group_id, author_node_id, seq, canonical,
		   issued_at_ms, not_after_ms, signature)
		VALUES (?, ?, ?, ?, ?, ?, ?);`,
		ad.GroupID[:],
		int64(ad.Author.PilotNodeID),
		int64(ad.Seq),
		encoded,
		ad.IssuedAt,
		ad.NotAfter,
		notNilBytes(ad.Signature),
	); err != nil {
		return false, fmt.Errorf("store: insert transport ad: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit transport ad: %w", err)
	}
	return true, nil
}

// GetTransportAd returns the current ad for (groupID, authorNodeID), or
// (TransportAd{}, false, nil) if none exists. Does NOT filter expired —
// caller decides.
func (s *SQLite) GetTransportAd(ctx context.Context, groupID entmoot.GroupID, authorNodeID entmoot.NodeID) (wire.TransportAd, bool, error) {
	db, err := s.dbFor(groupID)
	if err != nil {
		return wire.TransportAd{}, false, err
	}
	var encoded []byte
	if err := db.QueryRowContext(ctx, `
		SELECT canonical FROM transport_ads
		WHERE group_id = ? AND author_node_id = ?;`,
		groupID[:], int64(authorNodeID),
	).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return wire.TransportAd{}, false, nil
		}
		return wire.TransportAd{}, false, fmt.Errorf("store: scan transport ad: %w", err)
	}
	ad, err := decodeTransportAd(encoded)
	if err != nil {
		return wire.TransportAd{}, false, err
	}
	return ad, true, nil
}

// GetAllTransportAds returns every ad currently stored for the group.
// Expired ads (not_after_ms < now_ms) are excluded iff includeExpired is
// false (the common case). Used by the TransportSnapshotResp handler.
// Sorted by author_node_id for determinism. (v1.2.0)
func (s *SQLite) GetAllTransportAds(ctx context.Context, groupID entmoot.GroupID, now time.Time, includeExpired bool) ([]wire.TransportAd, error) {
	db, err := s.dbFor(groupID)
	if err != nil {
		return nil, err
	}

	var rows *sql.Rows
	if includeExpired {
		rows, err = db.QueryContext(ctx, `
			SELECT canonical FROM transport_ads
			WHERE group_id = ?
			ORDER BY author_node_id;`,
			groupID[:],
		)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT canonical FROM transport_ads
			WHERE group_id = ? AND not_after_ms >= ?
			ORDER BY author_node_id;`,
			groupID[:], now.UnixMilli(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("store: query transport ads: %w", err)
	}
	defer rows.Close()

	var out []wire.TransportAd
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, fmt.Errorf("store: scan transport ads: %w", err)
		}
		ad, err := decodeTransportAd(encoded)
		if err != nil {
			return nil, err
		}
		out = append(out, ad)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate transport ads: %w", err)
	}
	return out, nil
}

// BumpTransportAdSeq atomically increments this node's own ad sequence
// counter for (groupID, authorNodeID) and returns the new value. The
// first call for a given key returns 1. Safe across restarts because
// the counter is persisted in transport_ad_seqs. (v1.2.0)
func (s *SQLite) BumpTransportAdSeq(ctx context.Context, groupID entmoot.GroupID, authorNodeID entmoot.NodeID) (uint64, error) {
	db, err := s.dbFor(groupID)
	if err != nil {
		return 0, err
	}
	var seq int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO transport_ad_seqs (group_id, author_node_id, seq)
		VALUES (?, ?, 1)
		ON CONFLICT(group_id, author_node_id)
		  DO UPDATE SET seq = seq + 1
		RETURNING seq;`,
		groupID[:], int64(authorNodeID),
	).Scan(&seq); err != nil {
		return 0, fmt.Errorf("store: bump transport ad seq: %w", err)
	}
	return uint64(seq), nil
}

// GCExpiredTransportAds deletes all ads whose NotAfter is strictly
// before now. Returns the number of rows deleted. Called
// opportunistically by the advertiser goroutine (part B), not on the
// hot receive path. The scan spans every group database currently open
// — ads in groups whose databases have not been opened in this process
// lifetime are left alone (they'll be collected the next time the
// group is accessed). (v1.2.0)
func (s *SQLite) GCExpiredTransportAds(ctx context.Context, now time.Time) (int64, error) {
	s.mu.RLock()
	dbs := make([]*sql.DB, 0, len(s.dbs))
	for _, db := range s.dbs {
		dbs = append(dbs, db)
	}
	s.mu.RUnlock()

	var total int64
	nowMs := now.UnixMilli()
	for _, db := range dbs {
		res, err := db.ExecContext(ctx, `
			DELETE FROM transport_ads WHERE not_after_ms < ?;`,
			nowMs,
		)
		if err != nil {
			return total, fmt.Errorf("store: gc transport ads: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("store: gc transport ads rows: %w", err)
		}
		total += n
	}
	return total, nil
}

// decodeTransportAd reconstructs a wire.TransportAd from its stored JSON
// bytes. Storage is the ground truth; the Signature field round-trips
// byte-for-byte because JSON's []byte base64 encoding is bijective.
func decodeTransportAd(encoded []byte) (wire.TransportAd, error) {
	var ad wire.TransportAd
	if err := json.Unmarshal(encoded, &ad); err != nil {
		return wire.TransportAd{}, fmt.Errorf("store: decode transport ad: %w", err)
	}
	return ad, nil
}
