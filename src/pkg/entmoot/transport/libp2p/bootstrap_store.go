package libp2ptransport

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"entmoot/pkg/entmoot"
)

// reservationTTL bounds how long an unfinished enrollment holds a use of an
// invite before another applicant may take it.
const reservationTTL = 30 * time.Second

// PersistentBootstrapAdmission preserves committed uses, short-lived
// enrollment reservations, issuance records and revocations across daemon
// restarts. Uses are counted per applicant peer so a multi-use invite admits
// several identities.
type PersistentBootstrapAdmission struct {
	*BootstrapAdmission
	db *sql.DB
}

// InviteRecord reports one issued invite and how much of it is spent.
type InviteRecord struct {
	GroupID        entmoot.GroupID
	Nonce          [32]byte
	TargetMemberID *entmoot.MemberID
	MaxUses        int
	UsesCommitted  int
	UsesReserved   int
	IssuedAtMS     int64
	ExpiresAtMS    int64
	RevokedAtMS    int64
}

// Open reports whether the invite is redeemable by any holder.
func (r InviteRecord) Open() bool { return r.TargetMemberID == nil }

func OpenPersistentBootstrapAdmission(dataDir string) (*PersistentBootstrapAdmission, error) {
	if dataDir == "" {
		return nil, errors.New("libp2p: bootstrap admission data directory is required")
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "bootstrap-admission.db"))
	if err != nil {
		return nil, fmt.Errorf("libp2p: open bootstrap admission: %w", err)
	}
	if err := initBootstrapAdmissionSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	persistent := &PersistentBootstrapAdmission{db: db}
	admission := NewBootstrapAdmission()
	admission.revoked = func(invite capabilityKey) (bool, error) {
		var revoked bool
		err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM bootstrap_invites WHERE group_id=? AND nonce=? AND revoked_at_ms > 0)`,
			invite.GroupID[:], invite.Nonce[:]).Scan(&revoked)
		return revoked, err
	}
	admission.unavailable = func(key redemptionKey, maxUses int) (bool, error) {
		tx, err := db.Begin()
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		if err := pruneStaleReservations(tx); err != nil {
			return false, err
		}
		var mine bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM bootstrap_redemptions WHERE group_id=? AND nonce=? AND peer_id=?)`,
			key.GroupID[:], key.Nonce[:], key.Peer).Scan(&mine); err != nil {
			return false, err
		}
		if mine {
			return true, tx.Commit()
		}
		spent, err := countRedemptions(tx, key.capabilityKey)
		if err != nil {
			return false, err
		}
		return spent >= maxUses, tx.Commit()
	}
	admission.reserve = func(key redemptionKey, maxUses int) (bool, error) {
		tx, err := db.Begin()
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		if err := pruneStaleReservations(tx); err != nil {
			return false, err
		}
		spent, err := countRedemptions(tx, key.capabilityKey)
		if err != nil {
			return false, err
		}
		if spent >= maxUses {
			return false, nil
		}
		result, err := tx.Exec(`INSERT OR IGNORE INTO bootstrap_redemptions (group_id, nonce, peer_id, state, reserved_at_ms) VALUES (?, ?, ?, 'reserved', ?)`,
			key.GroupID[:], key.Nonce[:], key.Peer, time.Now().UnixMilli())
		if err != nil {
			return false, err
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return false, err
		}
		return true, tx.Commit()
	}
	admission.release = func(key redemptionKey) error {
		_, err := db.Exec(`DELETE FROM bootstrap_redemptions WHERE group_id=? AND nonce=? AND peer_id=? AND state='reserved'`,
			key.GroupID[:], key.Nonce[:], key.Peer)
		return err
	}
	admission.commit = func(key redemptionKey) error {
		result, err := db.Exec(`UPDATE bootstrap_redemptions SET state='used', reserved_at_ms=0 WHERE group_id=? AND nonce=? AND peer_id=? AND state='reserved'`,
			key.GroupID[:], key.Nonce[:], key.Peer)
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

func initBootstrapAdmissionSchema(db *sql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS bootstrap_redemptions (
			group_id BLOB NOT NULL,
			nonce BLOB NOT NULL,
			peer_id TEXT NOT NULL,
			state TEXT NOT NULL DEFAULT 'used',
			reserved_at_ms INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (group_id, nonce, peer_id)
		);
		CREATE TABLE IF NOT EXISTS bootstrap_invites (
			group_id BLOB NOT NULL,
			nonce BLOB NOT NULL,
			target_member_id BLOB,
			max_uses INTEGER NOT NULL DEFAULT 1,
			issued_at_ms INTEGER NOT NULL DEFAULT 0,
			expires_at_ms INTEGER NOT NULL DEFAULT 0,
			revoked_at_ms INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (group_id, nonce)
		);`); err != nil {
		return fmt.Errorf("libp2p: initialize bootstrap admission: %w", err)
	}
	legacy, err := tableExists(db, "used_bootstrap_capabilities")
	if err != nil {
		return err
	}
	if !legacy {
		return nil
	}
	// Single-use rows from before per-applicant counting carry no peer id.
	// Each committed row still counts as one spent use of its invite.
	if _, err := db.Exec(`INSERT OR IGNORE INTO bootstrap_redemptions (group_id, nonce, peer_id, state, reserved_at_ms)
		SELECT group_id, nonce, '', 'used', 0 FROM used_bootstrap_capabilities WHERE state='used' OR state IS NULL`); err != nil {
		return fmt.Errorf("libp2p: migrate bootstrap admission: %w", err)
	}
	if _, err := db.Exec(`DROP TABLE used_bootstrap_capabilities`); err != nil {
		return fmt.Errorf("libp2p: drop legacy bootstrap admission table: %w", err)
	}
	return nil
}

func tableExists(db *sql.DB, name string) (bool, error) {
	var exists bool
	err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)`, name).Scan(&exists)
	return exists, err
}

func pruneStaleReservations(tx *sql.Tx) error {
	staleBefore := time.Now().Add(-reservationTTL).UnixMilli()
	_, err := tx.Exec(`DELETE FROM bootstrap_redemptions WHERE state='reserved' AND reserved_at_ms < ?`, staleBefore)
	return err
}

func countRedemptions(tx *sql.Tx, invite capabilityKey) (int, error) {
	var count int
	err := tx.QueryRow(`SELECT COUNT(*) FROM bootstrap_redemptions WHERE group_id=? AND nonce=?`,
		invite.GroupID[:], invite.Nonce[:]).Scan(&count)
	return count, err
}

// RecordIssuedInvite stores what the issuer handed out so it can be listed and
// revoked later. Recording is idempotent for the same invite.
func (a *PersistentBootstrapAdmission) RecordIssuedInvite(capability BootstrapCapability) error {
	if a == nil || a.db == nil {
		return errors.New("libp2p: bootstrap admission is not open")
	}
	var target any
	if !capability.IsOpenInvite() {
		target = capability.TargetMemberID[:]
	}
	_, err := a.db.Exec(`INSERT OR IGNORE INTO bootstrap_invites
		(group_id, nonce, target_member_id, max_uses, issued_at_ms, expires_at_ms, revoked_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, 0)`,
		capability.GroupID[:], capability.Nonce[:], target, capability.Uses(),
		capability.IssuedAtMS, capability.ExpiresAtMS)
	return err
}

// RevokeInvite withdraws an invite before it expires. It reports whether this
// call was the one that revoked it. Revoking an invite this node never
// recorded still blocks it, so a lost invite file is not a dead end.
func (a *PersistentBootstrapAdmission) RevokeInvite(groupID entmoot.GroupID, nonce [32]byte) (bool, error) {
	if a == nil || a.db == nil {
		return false, errors.New("libp2p: bootstrap admission is not open")
	}
	now := time.Now().UnixMilli()
	tx, err := a.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var revokedAt int64
	err = tx.QueryRow(`SELECT revoked_at_ms FROM bootstrap_invites WHERE group_id=? AND nonce=?`, groupID[:], nonce[:]).Scan(&revokedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(`INSERT INTO bootstrap_invites (group_id, nonce, target_member_id, max_uses, issued_at_ms, expires_at_ms, revoked_at_ms)
			VALUES (?, ?, NULL, 0, 0, 0, ?)`, groupID[:], nonce[:], now); err != nil {
			return false, err
		}
	case err != nil:
		return false, err
	case revokedAt > 0:
		return false, tx.Commit()
	default:
		if _, err := tx.Exec(`UPDATE bootstrap_invites SET revoked_at_ms=? WHERE group_id=? AND nonce=?`, now, groupID[:], nonce[:]); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}

// ListInvites returns recorded invites with their spent uses, newest first.
// A nil groupID lists every group.
func (a *PersistentBootstrapAdmission) ListInvites(groupID *entmoot.GroupID) ([]InviteRecord, error) {
	if a == nil || a.db == nil {
		return nil, errors.New("libp2p: bootstrap admission is not open")
	}
	query := `SELECT i.group_id, i.nonce, i.target_member_id, i.max_uses, i.issued_at_ms, i.expires_at_ms, i.revoked_at_ms,
			(SELECT COUNT(*) FROM bootstrap_redemptions r WHERE r.group_id=i.group_id AND r.nonce=i.nonce AND r.state='used'),
			(SELECT COUNT(*) FROM bootstrap_redemptions r WHERE r.group_id=i.group_id AND r.nonce=i.nonce AND r.state='reserved')
		FROM bootstrap_invites i`
	args := []any{}
	if groupID != nil {
		query += ` WHERE i.group_id=?`
		args = append(args, groupID[:])
	}
	query += ` ORDER BY i.issued_at_ms DESC`
	rows, err := a.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InviteRecord
	for rows.Next() {
		var group, nonce, target []byte
		var record InviteRecord
		if err := rows.Scan(&group, &nonce, &target, &record.MaxUses, &record.IssuedAtMS, &record.ExpiresAtMS,
			&record.RevokedAtMS, &record.UsesCommitted, &record.UsesReserved); err != nil {
			return nil, err
		}
		if len(group) != len(record.GroupID) || len(nonce) != len(record.Nonce) {
			return nil, fmt.Errorf("libp2p: invite record has malformed key widths %d/%d", len(group), len(nonce))
		}
		copy(record.GroupID[:], group)
		copy(record.Nonce[:], nonce)
		if len(target) == len(entmoot.MemberID{}) {
			var memberID entmoot.MemberID
			copy(memberID[:], target)
			record.TargetMemberID = &memberID
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

// DecodeInviteNonce parses the base64 nonce printed by invite create.
func DecodeInviteNonce(encoded string) ([32]byte, error) {
	var nonce [32]byte
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nonce, fmt.Errorf("libp2p: decode nonce: %w", err)
	}
	if len(raw) != len(nonce) {
		return nonce, fmt.Errorf("libp2p: nonce must be %d bytes, got %d", len(nonce), len(raw))
	}
	copy(nonce[:], raw)
	return nonce, nil
}

func (a *PersistentBootstrapAdmission) Close() error {
	if a == nil || a.db == nil {
		return nil
	}
	return a.db.Close()
}
