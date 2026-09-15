package libp2ptransport

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"entmoot/pkg/entmoot"
)

// InviteLedger is the local record of invites this node issued: what was
// handed out, to whom, and what the operator has withdrawn locally.
//
// It is deliberately not an authority. How many times an invite has been
// redeemed, and whether it still admits anybody, are properties of the
// group's signed membership state, which every node projects identically.
// A ledger row that disagrees with that state is a display artefact, not a
// second opinion: `roster status` reads the state.
type InviteLedger struct {
	db *sql.DB
}

// InviteRecord reports one issued invite.
type InviteRecord struct {
	GroupID        entmoot.GroupID
	Nonce          [32]byte
	TargetMemberID *entmoot.MemberID
	MaxUses        int
	IssuedAtMS     int64
	ExpiresAtMS    int64
	// RevokedAtMS is when the operator withdrew it here. The withdrawal that
	// other nodes honour is a signed revoke_invite record; this column is how
	// `invite list` shows the local decision.
	RevokedAtMS int64
}

// Open reports whether the invite is redeemable by any holder.
func (r InviteRecord) Open() bool { return r.TargetMemberID == nil }

func OpenInviteLedger(dataDir string) (*InviteLedger, error) {
	if dataDir == "" {
		return nil, errors.New("libp2p: invite ledger data directory is required")
	}
	// The invite CLI opens this database alongside the running daemon, so a
	// contending writer must wait rather than surface a driver error.
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "bootstrap-admission.db")+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("libp2p: open invite ledger: %w", err)
	}
	if err := initInviteLedgerSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &InviteLedger{db: db}, nil
}

func initInviteLedgerSchema(db *sql.DB) error {
	if _, err := db.Exec(`
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
		return fmt.Errorf("libp2p: initialize invite ledger: %w", err)
	}
	// Redemption counting moved into the group's signed state, where every
	// node reaches the same answer. These tables were the old local tally;
	// keeping them would invite a reader to trust the wrong one.
	for _, dead := range []string{"bootstrap_redemptions", "used_bootstrap_capabilities"} {
		if _, err := db.Exec(`DROP TABLE IF EXISTS ` + dead); err != nil {
			return fmt.Errorf("libp2p: drop legacy %s: %w", dead, err)
		}
	}
	return nil
}

// RecordIssuedInvite files an invite the local node handed out.
func (l *InviteLedger) RecordIssuedInvite(capability BootstrapCapability) error {
	if l == nil || l.db == nil {
		return errors.New("libp2p: invite ledger is not open")
	}
	var target []byte
	if !capability.IsOpenInvite() {
		target = capability.TargetMemberID[:]
	}
	_, err := l.db.Exec(`INSERT OR REPLACE INTO bootstrap_invites
		(group_id, nonce, target_member_id, max_uses, issued_at_ms, expires_at_ms, revoked_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, COALESCE((SELECT revoked_at_ms FROM bootstrap_invites WHERE group_id=? AND nonce=?), 0))`,
		capability.GroupID[:], capability.Nonce[:], target, capability.Uses(),
		capability.IssuedAtMS, capability.ExpiresAtMS,
		capability.GroupID[:], capability.Nonce[:])
	if err != nil {
		return fmt.Errorf("libp2p: record issued invite: %w", err)
	}
	return nil
}

// MarkRevoked notes locally that an invite was withdrawn. It reports whether a
// row changed, so a caller can tell "withdrawn now" from "already withdrawn or
// never issued here".
func (l *InviteLedger) MarkRevoked(groupID entmoot.GroupID, nonce [32]byte) (bool, error) {
	if l == nil || l.db == nil {
		return false, errors.New("libp2p: invite ledger is not open")
	}
	result, err := l.db.Exec(`UPDATE bootstrap_invites SET revoked_at_ms=? WHERE group_id=? AND nonce=? AND revoked_at_ms=0`,
		time.Now().UnixMilli(), groupID[:], nonce[:])
	if err != nil {
		return false, fmt.Errorf("libp2p: mark invite revoked: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

// LiveOpenInvites lists open invites this node issued that it has not revoked
// locally and that have not expired.
func (l *InviteLedger) LiveOpenInvites(groupID entmoot.GroupID) ([]InviteRecord, error) {
	all, err := l.ListInvites(&groupID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	out := make([]InviteRecord, 0, len(all))
	for _, record := range all {
		if !record.Open() || record.RevokedAtMS > 0 {
			continue
		}
		if record.ExpiresAtMS > 0 && record.ExpiresAtMS <= now {
			continue
		}
		out = append(out, record)
	}
	return out, nil
}

// ListInvites lists issued invites, for one group or all of them.
func (l *InviteLedger) ListInvites(groupID *entmoot.GroupID) ([]InviteRecord, error) {
	if l == nil || l.db == nil {
		return nil, errors.New("libp2p: invite ledger is not open")
	}
	query := `SELECT group_id, nonce, target_member_id, max_uses, issued_at_ms, expires_at_ms, revoked_at_ms
		FROM bootstrap_invites`
	args := []any{}
	if groupID != nil {
		query += ` WHERE group_id=?`
		args = append(args, groupID[:])
	}
	query += ` ORDER BY issued_at_ms DESC, nonce ASC`
	rows, err := l.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("libp2p: list invites: %w", err)
	}
	defer rows.Close()
	out := make([]InviteRecord, 0)
	for rows.Next() {
		var record InviteRecord
		var group, nonce, target []byte
		if err := rows.Scan(&group, &nonce, &target, &record.MaxUses, &record.IssuedAtMS, &record.ExpiresAtMS, &record.RevokedAtMS); err != nil {
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

func (l *InviteLedger) Close() error {
	if l == nil || l.db == nil {
		return nil
	}
	return l.db.Close()
}
