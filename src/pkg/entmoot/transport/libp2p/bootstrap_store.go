package libp2ptransport

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// PersistentBootstrapAdmission preserves single-use capability consumption
// across daemon restarts.
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
			PRIMARY KEY (group_id, nonce)
		);`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("libp2p: initialize bootstrap admission: %w", err)
	}
	persistent := &PersistentBootstrapAdmission{db: db}
	persistent.BootstrapAdmission = &BootstrapAdmission{consume: func(key capabilityKey) (bool, error) {
		result, err := db.Exec(`
			INSERT OR IGNORE INTO used_bootstrap_capabilities (group_id, nonce)
			VALUES (?, ?);`, key.GroupID[:], key.Nonce[:])
		if err != nil {
			return false, err
		}
		rows, err := result.RowsAffected()
		return rows == 1, err
	}}
	return persistent, nil
}

func (a *PersistentBootstrapAdmission) Close() error {
	if a == nil || a.db == nil {
		return nil
	}
	return a.db.Close()
}
