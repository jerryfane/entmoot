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
	espCols, err := columns(espDB, "esp_node_profile_sources")
	if err != nil {
		t.Fatal(err)
	}
	if espCols["node_id"] || !espCols["member_id"] {
		t.Fatalf("converted ESP columns = %+v, want node_id renamed to member_id", espCols)
	}
	var sourceMember []byte
	if err := espDB.QueryRow(`SELECT member_id FROM esp_node_profile_sources WHERE source_key='legacy-source'`).Scan(&sourceMember); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sourceMember, wantMember[:]) {
		t.Fatalf("converted ESP identity = %x, want %x", sourceMember, wantMember)
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

func TestRunConvertsReassignedLegacyNodeIDBySignedIdentityAndTime(t *testing.T) {
	root := t.TempDir()
	founder, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	first, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	overlapping, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	gid := entmoot.GroupID{6, 5, 4}
	groupDir := filepath.Join(root, "groups", base64.RawURLEncoding.EncodeToString(gid[:]))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	entries := []entmoot.RosterEntry{legacyGenesis(t, founder, gid)}
	for _, change := range []struct {
		op      string
		subject *keystore.Identity
		at      int64
	}{
		{op: "add", subject: first, at: 1_700_000_002_000},
		{op: "remove", subject: first, at: 1_700_000_003_000},
		{op: "add", subject: replacement, at: 1_700_000_004_000},
		{op: "remove", subject: replacement, at: 1_700_000_005_000},
		{op: "add", subject: first, at: 1_700_000_006_000},
	} {
		entry := legacyRosterChange(t, founder, change.op, entmoot.NodeInfo{
			PilotNodeID:   133053,
			EntmootPubKey: append([]byte(nil), change.subject.PublicKey...),
		}, change.at, entries[len(entries)-1].ID)
		entries = append(entries, entry)
	}
	var rosterBytes []byte
	for _, entry := range entries {
		raw, err := canonical.Encode(entry)
		if err != nil {
			t.Fatal(err)
		}
		rosterBytes = append(rosterBytes, raw...)
		rosterBytes = append(rosterBytes, '\n')
	}
	if err := os.WriteFile(filepath.Join(groupDir, "roster.jsonl"), rosterBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	overlappingGID := entmoot.GroupID{7, 5, 4}
	overlappingDir := filepath.Join(root, "groups", base64.RawURLEncoding.EncodeToString(overlappingGID[:]))
	if err := os.MkdirAll(overlappingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	overlappingEntries := []entmoot.RosterEntry{legacyGenesis(t, founder, overlappingGID)}
	overlappingEntries = append(overlappingEntries, legacyRosterChange(t, founder, "add", entmoot.NodeInfo{
		PilotNodeID:   133053,
		EntmootPubKey: append([]byte(nil), overlapping.PublicKey...),
	}, 1_700_000_002_500, overlappingEntries[0].ID))
	var overlappingRosterBytes []byte
	for _, entry := range overlappingEntries {
		raw, err := canonical.Encode(entry)
		if err != nil {
			t.Fatal(err)
		}
		overlappingRosterBytes = append(overlappingRosterBytes, raw...)
		overlappingRosterBytes = append(overlappingRosterBytes, '\n')
	}
	if err := os.WriteFile(filepath.Join(overlappingDir, "roster.jsonl"), overlappingRosterBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	espPath := filepath.Join(root, "esp.sqlite")
	db, err := sql.Open("sqlite", "file:"+espPath)
	if err != nil {
		t.Fatal(err)
	}
	firstKey := base64.StdEncoding.EncodeToString(first.PublicKey)
	replacementKey := base64.StdEncoding.EncodeToString(replacement.PublicKey)
	// esp_node_profile_sources is the surviving ESP table keyed by a legacy
	// node id, so it is what proves the reassignment rule: two rows carrying
	// the same node id at different times must resolve to the two different
	// keys that held that id then.
	if _, err := db.Exec(`
CREATE TABLE esp_node_profile_sources(
  node_id INTEGER NOT NULL,
  entmoot_pubkey TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL,
  source_key TEXT NOT NULL,
  hostname TEXT NOT NULL,
  confidence INTEGER NOT NULL,
  observed_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0,
  source_group_id BLOB,
  PRIMARY KEY(node_id, source_key)
);`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
INSERT INTO esp_node_profile_sources
  (node_id, entmoot_pubkey, source, source_key, hostname, confidence, observed_at_ms, source_group_id)
VALUES
  (133053, ?, 'member_profile', 'replacement', 'replacement-host', 1, 1700000004500, ?),
  (133053, ?, 'member_profile', 'restored', 'restored-host', 1, 1700000006500, ?);
`, replacementKey, gid[:], firstKey, gid[:]); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := Run(root, founder); err != nil {
		t.Fatalf("Run: %v", err)
	}

	checkpointBytes, err := os.ReadFile(filepath.Join(groupDir, checkpointName))
	if err != nil {
		t.Fatal(err)
	}
	var cp checkpoint
	if err := json.Unmarshal(checkpointBytes, &cp); err != nil {
		t.Fatal(err)
	}
	var reassignedMappings int
	for _, mapping := range cp.Mappings {
		if mapping.LegacyNodeID == 133053 {
			reassignedMappings++
		}
	}
	if reassignedMappings != 2 {
		t.Fatalf("reassigned node mappings = %d, want 2", reassignedMappings)
	}

	converted, err := sql.Open("sqlite", "file:"+espPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = converted.Close() })
	firstMember, _ := entmoot.MemberIDFromPublicKey(first.PublicKey)
	replacementMember, _ := entmoot.MemberIDFromPublicKey(replacement.PublicKey)
	firstPeer, _ := entmoot.PeerIDFromPublicKey(first.PublicKey)
	replacementPeer, _ := entmoot.PeerIDFromPublicKey(replacement.PublicKey)
	cols, err := columns(converted, "esp_node_profile_sources")
	if err != nil {
		t.Fatal(err)
	}
	if cols["node_id"] || !cols["member_id"] {
		t.Fatalf("converted ESP columns = %+v, want node_id renamed to member_id", cols)
	}
	for _, want := range []struct {
		sourceKey string
		member    entmoot.MemberID
	}{
		{sourceKey: "replacement", member: replacementMember},
		{sourceKey: "restored", member: firstMember},
	} {
		var member []byte
		if err := converted.QueryRow(`SELECT member_id FROM esp_node_profile_sources WHERE source_key=?`, want.sourceKey).Scan(&member); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(member, want.member[:]) {
			t.Fatalf("%s identity = %x, want %x", want.sourceKey, member, want.member)
		}
	}
	if firstPeer == replacementPeer {
		t.Fatal("test fixture is degenerate: both keys derive the same peer id")
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

func legacyRosterChange(t *testing.T, founder *keystore.Identity, op string, subject entmoot.NodeInfo, timestamp int64, parent entmoot.RosterEntryID) entmoot.RosterEntry {
	t.Helper()
	entry := entmoot.RosterEntry{
		Op:        op,
		Subject:   subject,
		Actor:     45981,
		Timestamp: timestamp,
		Parents:   []entmoot.RosterEntryID{parent},
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
	if _, err := db.Exec(`CREATE TABLE esp_node_profile_sources(node_id INTEGER NOT NULL,entmoot_pubkey TEXT NOT NULL DEFAULT '',source TEXT NOT NULL,source_key TEXT NOT NULL,hostname TEXT NOT NULL,confidence INTEGER NOT NULL,observed_at_ms INTEGER NOT NULL,PRIMARY KEY(node_id,source_key)); INSERT INTO esp_node_profile_sources VALUES(?,?,'member_profile','legacy-source','legacy-host',1,1700000002000)`, int64(nodeID), base64.StdEncoding.EncodeToString(publicKey)); err != nil {
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
