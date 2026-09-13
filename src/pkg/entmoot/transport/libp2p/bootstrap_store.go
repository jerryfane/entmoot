package libp2ptransport

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// PersistentBootstrapAdmission preserves committed use and short-lived
// enrollment reservations across daemon restarts.
type PersistentBootstrapAdmission struct {
	*BootstrapAdmission
	db *sql.DB
}

func OpenPersistentBootstrapAdmission(dataDir string) (*PersistentBootstrapAdmission, error) {
	if dataDir == "" {
		return nil, errors.New("libp2p: bootstrap admission data directory is required")
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "bootstrap-admission.db"))
	if err != nil {
		return nil, fmt.Errorf("libp2p: open bootstrap admission: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS used_bootstrap_capabilities (
			group_id BLOB NOT NULL,
			nonce BLOB NOT NULL,
			state TEXT NOT NULL DEFAULT 'used',
			reserved_at_ms INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (group_id, nonce)
		);`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("libp2p: initialize bootstrap admission: %w", err)
	}
	columns, err := bootstrapAdmissionColumns(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if !columns["state"] {
		if _, err := db.Exec(`ALTER TABLE used_bootstrap_capabilities ADD COLUMN state TEXT NOT NULL DEFAULT 'used'`); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if !columns["reserved_at_ms"] {
		if _, err := db.Exec(`ALTER TABLE used_bootstrap_capabilities ADD COLUMN reserved_at_ms INTEGER NOT NULL DEFAULT 0`); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	persistent := &PersistentBootstrapAdmission{db: db}
	admission := NewBootstrapAdmission()
	admission.unavailable = func(key capabilityKey) (bool, error) {
		var exists bool
		err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM used_bootstrap_capabilities WHERE group_id=? AND nonce=?)`, key.GroupID[:], key.Nonce[:]).Scan(&exists)
		return exists, err
	}
	admission.reserve = func(key capabilityKey) (bool, error) {
		tx, err := db.Begin()
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		staleBefore := time.Now().Add(-30 * time.Second).UnixMilli()
		if _, err := tx.Exec(`DELETE FROM used_bootstrap_capabilities WHERE state='reserved' AND reserved_at_ms < ?`, staleBefore); err != nil {
			return false, err
		}
		result, err := tx.Exec(`INSERT OR IGNORE INTO used_bootstrap_capabilities (group_id, nonce, state, reserved_at_ms) VALUES (?, ?, 'reserved', ?)`, key.GroupID[:], key.Nonce[:], time.Now().UnixMilli())
		if err != nil {
			return false, err
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return false, err
		}
		return true, tx.Commit()
	}
	admission.release = func(key capabilityKey) error {
		_, err := db.Exec(`DELETE FROM used_bootstrap_capabilities WHERE group_id=? AND nonce=? AND state='reserved'`, key.GroupID[:], key.Nonce[:])
		return err
	}
	admission.commit = func(key capabilityKey) error {
		result, err := db.Exec(`UPDATE used_bootstrap_capabilities SET state='used', reserved_at_ms=0 WHERE group_id=? AND nonce=? AND state='reserved'`, key.GroupID[:], key.Nonce[:])
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return errors.New("bootstrap capability reservation is missing")
		}
		return nil
	}
	persistent.BootstrapAdmission = admission
	return persistent, nil
}

func bootstrapAdmissionColumns(db *sql.DB) (map[string]bool, error) {
	rows, err := db.Query(`PRAGMA table_info(used_bootstrap_capabilities)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, kind string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		columns[name] = true
	}
	return columns, rows.Err()
}

func (a *PersistentBootstrapAdmission) Close() error {
	if a == nil || a.db == nil {
		return nil
	}
	return a.db.Close()
}
