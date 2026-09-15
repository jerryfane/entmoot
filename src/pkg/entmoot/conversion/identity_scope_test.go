package conversion

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// TestRunResolvesLegacyNodeIDByIntervalAndGroup covers the two resolution
// branches that have no signing key to fall back on.
//
// esp_live_agent_cursors is the vehicle because it survives the Fleet removal
// and has the shape that forces both branches: a group_id column, a timestamp
// column, and no pubkey column. So a row can only be resolved by asking which
// key held that legacy node id in that group at that moment.
//
// Two groups reuse legacy node id 133053. In the first it was one key and then
// another; in the second it was a third key the whole time. A row must resolve
// by its own group and its own timestamp, not by whichever mapping was seen
// last.
func TestRunResolvesLegacyNodeIDByIntervalAndGroup(t *testing.T) {
	root := t.TempDir()
	founder, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	early, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	late, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	other, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}

	const reused = entmoot.NodeID(133053)
	gidA := entmoot.GroupID{9, 1, 1}
	gidB := entmoot.GroupID{9, 2, 2}

	writeChain := func(gid entmoot.GroupID, changes []struct {
		op      string
		subject *keystore.Identity
		at      int64
	}) {
		dir := filepath.Join(root, "groups", base64.RawURLEncoding.EncodeToString(gid[:]))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		entries := []entmoot.RosterEntry{legacyGenesis(t, founder, gid)}
		for _, change := range changes {
			entries = append(entries, legacyRosterChange(t, founder, change.op, entmoot.NodeInfo{
				PilotNodeID:   reused,
				EntmootPubKey: append([]byte(nil), change.subject.PublicKey...),
			}, change.at, entries[len(entries)-1].ID))
		}
		var raw []byte
		for _, entry := range entries {
			encoded, err := canonical.Encode(entry)
			if err != nil {
				t.Fatal(err)
			}
			raw = append(raw, encoded...)
			raw = append(raw, '\n')
		}
		if err := os.WriteFile(filepath.Join(dir, "roster.jsonl"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	writeChain(gidA, []struct {
		op      string
		subject *keystore.Identity
		at      int64
	}{
		{op: "add", subject: early, at: 1_700_000_002_000},
		{op: "remove", subject: early, at: 1_700_000_003_000},
		{op: "add", subject: late, at: 1_700_000_004_000},
	})
	writeChain(gidB, []struct {
		op      string
		subject *keystore.Identity
		at      int64
	}{
		{op: "add", subject: other, at: 1_700_000_002_000},
	})

	espPath := filepath.Join(root, "esp.sqlite")
	db, err := sql.Open("sqlite", "file:"+espPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
CREATE TABLE esp_live_agent_cursors(
  group_id BLOB NOT NULL,
  node_id INTEGER NOT NULL,
  last_seen_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY(group_id, node_id, updated_at_ms)
);`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	for _, row := range []struct {
		gid entmoot.GroupID
		at  int64
	}{
		{gid: gidA, at: 1_700_000_002_500},
		{gid: gidA, at: 1_700_000_004_500},
		{gid: gidB, at: 1_700_000_002_500},
	} {
		if _, err := db.Exec(`INSERT INTO esp_live_agent_cursors VALUES(?,?,?,?)`,
			row.gid[:], int64(reused), row.at, row.at); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := Run(root, founder); err != nil {
		t.Fatalf("Run: %v", err)
	}

	converted, err := sql.Open("sqlite", "file:"+espPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer converted.Close()

	earlyMember, _ := entmoot.MemberIDFromPublicKey(early.PublicKey)
	lateMember, _ := entmoot.MemberIDFromPublicKey(late.PublicKey)
	otherMember, _ := entmoot.MemberIDFromPublicKey(other.PublicKey)
	for _, want := range []struct {
		name   string
		gid    entmoot.GroupID
		at     int64
		member entmoot.MemberID
	}{
		{name: "first holder in group A", gid: gidA, at: 1_700_000_002_500, member: earlyMember},
		{name: "second holder in group A", gid: gidA, at: 1_700_000_004_500, member: lateMember},
		{name: "unrelated holder in group B", gid: gidB, at: 1_700_000_002_500, member: otherMember},
	} {
		var member []byte
		if err := converted.QueryRow(`SELECT member_id FROM esp_live_agent_cursors WHERE group_id=? AND updated_at_ms=?`,
			want.gid[:], want.at).Scan(&member); err != nil {
			t.Fatalf("%s: %v", want.name, err)
		}
		if !bytes.Equal(member, want.member[:]) {
			t.Fatalf("%s resolved to %x, want %x", want.name, member, want.member)
		}
	}
}
