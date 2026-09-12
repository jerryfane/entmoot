package conversion

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"

	_ "modernc.org/sqlite"
)

func TestRunConvertsLegacyRootWithoutChangingSignedBytes(t *testing.T) {
	root := t.TempDir()
	founder, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	gid := entmoot.GroupID{1, 2, 3}
	groupDir := filepath.Join(root, "groups", base64.RawURLEncoding.EncodeToString(gid[:]))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := legacyGenesis(t, founder, gid)
	entryBytes, err := canonical.Encode(entry)
	if err != nil {
		t.Fatal(err)
	}
	rosterBytes := append(append([]byte(nil), entryBytes...), '\n')
	rosterPath := filepath.Join(groupDir, "roster.jsonl")
	if err := os.WriteFile(rosterPath, rosterBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	msg := legacyMessage(t, founder, gid)
	messageBytes, err := canonical.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	messagesPath := filepath.Join(groupDir, "messages.sqlite")
	seedLegacyMessages(t, messagesPath, msg, messageBytes)
	espPath := filepath.Join(root, "esp.sqlite")
	seedLegacyESP(t, espPath, entry.Subject.PilotNodeID, founder.PublicKey)
	stateFiles := map[string][]byte{
		"identity.json":      []byte("identity state"),
		"runtime.env":        []byte("runtime configuration"),
		"config.json":        []byte(`{"listen_port":1004}`),
		"service.sqlite-wal": []byte("pending sqlite wal bytes"),
		"service.sqlite-shm": []byte("pending sqlite shm bytes"),
	}
	for name, content := range stateFiles {
		if err := os.WriteFile(filepath.Join(root, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := Run(root, founder); err != nil {
		t.Fatalf("Run: %v", err)
	}
	status, ok, err := ReadStatus(root)
	if err != nil || !ok || status.Stage != StageComplete {
		t.Fatalf("status = %+v, %v, %v", status, ok, err)
	}
	assertStageHistory(t, root)
	if err := Run(root, founder); err != nil {
		t.Fatalf("idempotent Run: %v", err)
	}

	backupRoster, err := os.ReadFile(filepath.Join(root, backupDir, "groups", filepath.Base(groupDir), "roster.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(backupRoster, rosterBytes) {
		t.Fatal("backup roster bytes changed")
	}
	backupDB := filepath.Join(root, backupDir, "groups", filepath.Base(groupDir), "messages.sqlite")
	backupMessage := readCanonicalMessage(t, backupDB)
	if !bytes.Equal(backupMessage, messageBytes) {
		t.Fatal("backup message bytes changed")
	}
	for name, want := range stateFiles {
		got, err := os.ReadFile(filepath.Join(root, backupDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("backup %s bytes changed", name)
		}
	}
	convertedMessage := readCanonicalMessage(t, messagesPath)
	if !bytes.Equal(convertedMessage, messageBytes) {
		t.Fatal("converted canonical message bytes changed")
	}

	db, err := sql.Open("sqlite", "file:"+messagesPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cols, err := columns(db, "messages")
	if err != nil {
		t.Fatal(err)
	}
	if !cols["author_member_id"] || cols["author_node_id"] {
		t.Fatalf("converted message columns = %+v", cols)
	}
	var memberBytes []byte
	if err := db.QueryRow(`SELECT author_member_id FROM messages`).Scan(&memberBytes); err != nil {
		t.Fatal(err)
	}
	wantMember, _ := entmoot.MemberIDFromPublicKey(founder.PublicKey)
	if !bytes.Equal(memberBytes, wantMember[:]) {
		t.Fatalf("author member id = %x, want %x", memberBytes, wantMember)
	}
	espDB, err := sql.Open("sqlite", "file:"+espPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer espDB.Close()
	espCols, err := columns(espDB, "esp_fleets")
	if err != nil {
		t.Fatal(err)
	}
	if espCols["coordinator_node_id"] || !espCols["coordinator_member_id"] || !espCols["coordinator_peer_id"] {
		t.Fatalf("converted ESP columns = %+v", espCols)
	}
	var coordinatorMember []byte
	var coordinatorPeer string
	if err := espDB.QueryRow(`SELECT coordinator_member_id,coordinator_peer_id FROM esp_fleets WHERE fleet_id='legacy-fleet'`).Scan(&coordinatorMember, &coordinatorPeer); err != nil {
		t.Fatal(err)
	}
	wantPeer, err := entmoot.PeerIDFromPublicKey(founder.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(coordinatorMember, wantMember[:]) || coordinatorPeer != wantPeer {
		t.Fatalf("converted ESP identity = (%x, %q), want (%x, %q)", coordinatorMember, coordinatorPeer, wantMember, wantPeer)
	}
	checkpointBytes, err := os.ReadFile(filepath.Join(groupDir, checkpointName))
	if err != nil {
		t.Fatal(err)
	}
	var cp checkpoint
	if err := json.Unmarshal(checkpointBytes, &cp); err != nil {
		t.Fatal(err)
	}
	if len(cp.Mappings) != 1 || cp.Mappings[0].LegacyNodeID != entry.Subject.PilotNodeID {
		t.Fatalf("checkpoint mappings = %+v", cp.Mappings)
	}
	if len(cp.LegacyMessageIDs) != 1 || cp.LegacyMessageIDs[0] != msg.ID {
		t.Fatalf("checkpoint legacy message ids = %v, want %s", cp.LegacyMessageIDs, msg.ID)
	}
	if err := entmoot.VerifyLegacyIdentityMapping(cp.Mappings[0]); err != nil {
		t.Fatalf("mapping verification: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	convertedRoster, err := roster.OpenJSONL(root, gid)
	if err != nil {
		t.Fatalf("open converted roster: %v", err)
	}
	defer convertedRoster.Close()
	if err := convertedRoster.ClaimWriter(); err != nil {
		t.Fatal(err)
	}
	next, err := convertedRoster.SignEntry(founder, "policy_change", entmoot.NodeInfo{}, json.RawMessage(`{"post_conversion":true}`), cp.UpgradeEntry.Timestamp+1)
	if err != nil {
		t.Fatalf("sign after conversion: %v", err)
	}
	if err := convertedRoster.Apply(next); err != nil {
		t.Fatalf("append after conversion: %v", err)
	}
}

func TestRunRejectsCorruptLegacyRootBeforeCreatingConversionState(t *testing.T) {
	root := t.TempDir()
	founder, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	gid := entmoot.GroupID{9, 8, 7}
	groupDir := filepath.Join(root, "groups", base64.RawURLEncoding.EncodeToString(gid[:]))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := legacyGenesis(t, founder, gid)
	raw, err := canonical.Encode(entry)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append(append(raw, '\n'), []byte(`{"truncated":`)...)
	path := filepath.Join(groupDir, "roster.jsonl")
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Run(root, founder); err == nil {
		t.Fatal("Run accepted corrupt roster")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, corrupt) {
		t.Fatal("failed conversion changed corrupt source")
	}
	if _, err := os.Stat(filepath.Join(root, journalName)); !os.IsNotExist(err) {
		t.Fatalf("journal exists after failed preflight: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, backupDir)); !os.IsNotExist(err) {
		t.Fatalf("backup exists after failed preflight: %v", err)
	}
}

func assertStageHistory(t *testing.T, root string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, journalName)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT stage FROM conversion_history ORDER BY sequence`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []Stage
	for rows.Next() {
		var stage Stage
		if err := rows.Scan(&stage); err != nil {
			t.Fatal(err)
		}
		got = append(got, stage)
	}
	if len(got) != len(orderedStages) {
		t.Fatalf("stage history = %v, want %v", got, orderedStages)
	}
	for i := range got {
		if got[i] != orderedStages[i] {
			t.Fatalf("stage history = %v, want %v", got, orderedStages)
		}
	}
}
func legacyGenesis(t *testing.T, founder *keystore.Identity, gid entmoot.GroupID) entmoot.RosterEntry {
	t.Helper()
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   entmoot.NodeInfo{PilotNodeID: 45981, EntmootPubKey: append([]byte(nil), founder.PublicKey...)},
		Actor:     45981,
		Timestamp: 1_700_000_000_000,
	}
	entry.ID = canonical.RosterEntryID(entry)
	signingBytes, err := canonical.RosterEntrySigningBytes(entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.Signature = founder.Sign(signingBytes)
	return entry
}

func legacyMessage(t *testing.T, author *keystore.Identity, gid entmoot.GroupID) entmoot.Message {
	t.Helper()
	msg := entmoot.Message{
		GroupID:   gid,
		Author:    entmoot.NodeInfo{PilotNodeID: 45981, EntmootPubKey: append([]byte(nil), author.PublicKey...)},
		Timestamp: 1_700_000_000_100,
		Topics:    []string{"conversion/fixture"},
		Content:   []byte("legacy bytes stay signed"),
	}
	msg.ID = canonical.MessageID(msg)
	signingBytes, err := canonical.MessageSigningBytes(msg)
	if err != nil {
		t.Fatal(err)
	}
	msg.Signature = author.Sign(signingBytes)
	return msg
}

func seedLegacyMessages(t *testing.T, path string, msg entmoot.Message, canonicalBytes []byte) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE messages(message_id BLOB PRIMARY KEY,group_id BLOB NOT NULL,author_node_id INTEGER NOT NULL,timestamp_ms INTEGER NOT NULL,content BLOB NOT NULL,parents BLOB NOT NULL,signature BLOB NOT NULL,canonical_bytes BLOB NOT NULL); CREATE INDEX idx_messages_group_author ON messages(group_id,author_node_id,timestamp_ms DESC);`); err != nil {
		t.Fatal(err)
	}
	parents, _ := json.Marshal(msg.Parents)
	if _, err := db.Exec(`INSERT INTO messages VALUES(?,?,?,?,?,?,?,?)`, msg.ID[:], msg.GroupID[:], int64(msg.Author.PilotNodeID), msg.Timestamp, msg.Content, parents, msg.Signature, canonicalBytes); err != nil {
		t.Fatal(err)
	}
}

func seedLegacyESP(t *testing.T, path string, nodeID entmoot.NodeID, publicKey []byte) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE esp_fleets(fleet_id TEXT PRIMARY KEY,coordinator_node_id INTEGER NOT NULL,coordinator_pubkey TEXT NOT NULL); INSERT INTO esp_fleets VALUES(?,?,?)`, "legacy-fleet", int64(nodeID), base64.StdEncoding.EncodeToString(publicKey)); err != nil {
		t.Fatal(err)
	}
}

func readCanonicalMessage(t *testing.T, path string) []byte {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var raw []byte
	if err := db.QueryRow(`SELECT canonical_bytes FROM messages`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
