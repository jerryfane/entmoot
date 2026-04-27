package mailbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"entmoot/pkg/entmoot"

	// Register the pure-Go SQLite driver under the name "sqlite".
	_ "modernc.org/sqlite"
)

const sqliteDriver = "sqlite"

const sqliteCursorSchema = `
CREATE TABLE IF NOT EXISTS mailbox_cursors (
  group_id      BLOB NOT NULL,
  client_id     TEXT NOT NULL,
  message_id    BLOB NOT NULL,
  timestamp_ms  INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (group_id, client_id)
);

CREATE INDEX IF NOT EXISTS idx_mailbox_cursors_client
  ON mailbox_cursors(client_id);
`

// SQLiteCursorStore persists ESP mailbox cursors in <dataDir>/mailbox.sqlite.
type SQLiteCursorStore struct {
	db   *sql.DB
	once sync.Once
}

// OpenSQLiteCursorStore opens or creates the durable mailbox cursor database.
func OpenSQLiteCursorStore(dataDir string) (*SQLiteCursorStore, error) {
	if dataDir == "" {
		return nil, errors.New("mailbox: SQLite data dir is empty")
	}
	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("mailbox: resolve data dir %q: %w", dataDir, err)
	}
	if err := os.MkdirAll(absDir, 0o700); err != nil {
		return nil, fmt.Errorf("mailbox: mkdir data dir %q: %w", absDir, err)
	}

	dbPath := filepath.Join(absDir, "mailbox.sqlite")
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("mailbox: precreate %q: %w", dbPath, err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("mailbox: close precreate %q: %w", dbPath, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("mailbox: stat %q: %w", dbPath, err)
	}

	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	dsn := "file:" + dbPath + "?" + q.Encode()

	db, err := sql.Open(sqliteDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("mailbox: open sqlite %q: %w", dbPath, err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mailbox: ping sqlite %q: %w", dbPath, err)
	}
	if _, err := db.Exec(sqliteCursorSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mailbox: apply schema: %w", err)
	}
	return &SQLiteCursorStore{db: db}, nil
}

// GetCursor implements CursorStore.
func (s *SQLiteCursorStore) GetCursor(ctx context.Context, groupID entmoot.GroupID, clientID string) (Cursor, error) {
	if clientID == "" {
		return Cursor{}, ErrInvalidClient
	}
	row := s.db.QueryRowContext(ctx, `
SELECT message_id, timestamp_ms
FROM mailbox_cursors
WHERE group_id = ? AND client_id = ?`, groupID[:], clientID)

	var messageID []byte
	var timestampMS int64
	if err := row.Scan(&messageID, &timestampMS); errors.Is(err, sql.ErrNoRows) {
		return Cursor{}, nil
	} else if err != nil {
		return Cursor{}, fmt.Errorf("mailbox: get cursor: %w", err)
	}
	id, err := messageIDFromBytes(messageID)
	if err != nil {
		return Cursor{}, err
	}
	return Cursor{MessageID: id, TimestampMS: timestampMS}, nil
}

// AckCursor implements CursorStore.
func (s *SQLiteCursorStore) AckCursor(ctx context.Context, groupID entmoot.GroupID, clientID string, cursor Cursor) (bool, error) {
	if clientID == "" {
		return false, ErrInvalidClient
	}
	if cursor == (Cursor{}) {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("mailbox: begin cursor tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	current, err := getCursorTx(ctx, tx, groupID, clientID)
	if err != nil {
		return false, err
	}
	if !cursorAfter(cursor, current) {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO mailbox_cursors (group_id, client_id, message_id, timestamp_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(group_id, client_id) DO UPDATE SET
  message_id = excluded.message_id,
  timestamp_ms = excluded.timestamp_ms,
  updated_at_ms = excluded.updated_at_ms`,
		groupID[:], clientID, cursor.MessageID[:], cursor.TimestampMS, time.Now().UnixMilli()); err != nil {
		return false, fmt.Errorf("mailbox: upsert cursor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("mailbox: commit cursor tx: %w", err)
	}
	return true, nil
}

// Close implements CursorStore.
func (s *SQLiteCursorStore) Close() error {
	var err error
	s.once.Do(func() {
		if _, checkpointErr := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE);"); checkpointErr != nil {
			err = fmt.Errorf("mailbox: wal_checkpoint: %w", checkpointErr)
			return
		}
		if closeErr := s.db.Close(); closeErr != nil {
			err = fmt.Errorf("mailbox: close sqlite: %w", closeErr)
		}
	})
	return err
}

func getCursorTx(ctx context.Context, tx *sql.Tx, groupID entmoot.GroupID, clientID string) (Cursor, error) {
	row := tx.QueryRowContext(ctx, `
SELECT message_id, timestamp_ms
FROM mailbox_cursors
WHERE group_id = ? AND client_id = ?`, groupID[:], clientID)

	var messageID []byte
	var timestampMS int64
	if err := row.Scan(&messageID, &timestampMS); errors.Is(err, sql.ErrNoRows) {
		return Cursor{}, nil
	} else if err != nil {
		return Cursor{}, fmt.Errorf("mailbox: get cursor tx: %w", err)
	}
	id, err := messageIDFromBytes(messageID)
	if err != nil {
		return Cursor{}, err
	}
	return Cursor{MessageID: id, TimestampMS: timestampMS}, nil
}

func messageIDFromBytes(raw []byte) (entmoot.MessageID, error) {
	var id entmoot.MessageID
	if len(raw) != len(id) {
		return id, fmt.Errorf("mailbox: invalid cursor message id length %d", len(raw))
	}
	copy(id[:], raw)
	return id, nil
}
