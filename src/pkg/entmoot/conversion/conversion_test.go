package conversion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
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
	founderKey := base64.StdEncoding.EncodeToString(founder.PublicKey)
	if _, err := db.Exec(`
CREATE TABLE esp_fleets(
  fleet_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  control_group_id BLOB,
  coordinator_node_id INTEGER NOT NULL,
  coordinator_pubkey TEXT NOT NULL,
  coordinator_device_id TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  archived_at_ms INTEGER NOT NULL DEFAULT 0,
  deleted_at_ms INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE esp_fleet_activity(
  event_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  type TEXT NOT NULL,
  actor_node_id INTEGER NOT NULL,
  actor_pubkey TEXT NOT NULL,
  subject_node_id INTEGER NOT NULL,
  subject_pubkey TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE TABLE esp_fleet_command_results(
  command_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  agent_node_id INTEGER NOT NULL,
  started_at_ms INTEGER NOT NULL,
  completed_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  result BLOB NOT NULL
);
CREATE TABLE esp_agent_commands (
  command_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  control_group_id BLOB NOT NULL,
  issuer_node_id INTEGER NOT NULL,
  agent_node_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  target BLOB NOT NULL,
  instruction TEXT NOT NULL,
  context BLOB,
  args BLOB,
  command BLOB NOT NULL,
  payload BLOB NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT NOT NULL DEFAULT '',
  lease_until_ms INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0,
  received_at_ms INTEGER NOT NULL,
  started_at_ms INTEGER NOT NULL DEFAULT 0,
  completed_at_ms INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL,
  result BLOB,
  last_error TEXT NOT NULL DEFAULT ''
);`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO esp_fleets(fleet_id,name,control_group_id,coordinator_node_id,coordinator_pubkey,created_at_ms,updated_at_ms) VALUES ('fleet-a','Fleet A',?,45981,?,1700000001000,1700000001000)`, gid[:], founderKey); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
INSERT INTO esp_fleet_activity VALUES
  ('replacement','fleet-a','command.sent',45981,?,133053,?,1700000004500),
  ('restored','fleet-a','command.sent',45981,?,133053,?,1700000006500);
`, founderKey, replacementKey, founderKey, firstKey); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
INSERT INTO esp_fleet_command_results VALUES
  ('replacement','fleet-a',133053,1700000004500,1700000004501,1700000004501,?),
  ('restored','fleet-a',133053,1700000006500,1700000006501,1700000006501,?);
`,
		[]byte(`{"type":"fleet.command.result","version":1,"command_id":"replacement","fleet_id":"fleet-a","agent_node_id":133053,"completed_at_ms":1700000004501}`),
		[]byte(`{"type":"fleet.command.result","version":1,"command_id":"restored","fleet_id":"fleet-a","agent_node_id":133053,"completed_at_ms":1700000006501}`),
	); err != nil {
		db.Close()
		t.Fatal(err)
	}
	agentPayload, err := json.Marshal(map[string]any{
		"type":             esphttp.AgentInstructionPayloadType,
		"version":          1,
		"command_id":       "queued-replacement",
		"fleet_id":         "fleet-a",
		"control_group_id": base64.StdEncoding.EncodeToString(gid[:]),
		"issuer_node_id":   45981,
		"target":           map[string]any{"kind": "node", "pilot_node_id": 133053},
		"agent_node_id":    133053,
		"action":           "agent.instruction",
		"instruction":      "conversion claim regression",
		"timeout_ms":       30_000,
		"created_at_ms":    int64(1_700_000_004_500),
		"received_at_ms":   int64(1_700_000_004_500),
	})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
INSERT INTO esp_agent_commands (
  command_id,fleet_id,control_group_id,issuer_node_id,agent_node_id,action,target,
  instruction,command,payload,status,created_at_ms,received_at_ms,updated_at_ms
) VALUES ('queued-replacement','fleet-a',?,45981,133053,'agent.instruction',?,
  'conversion claim regression','null',?,'running',1700000004500,1700000004500,1700000004500)
`, gid[:], []byte(`{"kind":"node","pilot_node_id":133053}`), agentPayload); err != nil {
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
	for _, want := range []struct {
		id     string
		member entmoot.MemberID
		peer   string
	}{
		{id: "replacement", member: replacementMember, peer: replacementPeer},
		{id: "restored", member: firstMember, peer: firstPeer},
	} {
		var activityMember, resultMember, resultJSON []byte
		var activityPeer, resultPeer string
		if err := converted.QueryRow(`SELECT subject_member_id,subject_peer_id FROM esp_fleet_activity WHERE event_id=?`, want.id).Scan(&activityMember, &activityPeer); err != nil {
			t.Fatal(err)
		}
		if err := converted.QueryRow(`SELECT agent_member_id,agent_peer_id,result FROM esp_fleet_command_results WHERE command_id=?`, want.id).Scan(&resultMember, &resultPeer, &resultJSON); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(activityMember, want.member[:]) || activityPeer != want.peer {
			t.Fatalf("%s activity identity = (%x, %q), want (%x, %q)", want.id, activityMember, activityPeer, want.member, want.peer)
		}
		if !bytes.Equal(resultMember, want.member[:]) || resultPeer != want.peer {
			t.Fatalf("%s result identity = (%x, %q), want (%x, %q)", want.id, resultMember, resultPeer, want.member, want.peer)
		}
		var result map[string]any
		if err := json.Unmarshal(resultJSON, &result); err != nil {
			t.Fatal(err)
		}
		if _, ok := result["agent_node_id"]; ok {
			t.Fatalf("%s result retained legacy agent_node_id: %s", want.id, resultJSON)
		}
		if result["agent_member_id"] != want.member.String() || result["agent_peer_id"] != want.peer {
			t.Fatalf("%s result JSON identity = (%v, %v), want (%s, %s)", want.id, result["agent_member_id"], result["agent_peer_id"], want.member, want.peer)
		}
	}
	if err := converted.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := esphttp.OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	claimed, ok, err := state.ClaimNextAgentCommand(context.Background(), "conversion-test", 1_700_000_004_600, 1_700_000_005_600, 3)
	if err != nil || !ok {
		t.Fatalf("ClaimNextAgentCommand ok/err = %v/%v", ok, err)
	}
	founderMember, _ := entmoot.MemberIDFromPublicKey(founder.PublicKey)
	founderPeer, _ := entmoot.PeerIDFromPublicKey(founder.PublicKey)
	if claimed.Payload.IssuerMemberID != founderMember || claimed.Payload.IssuerPeerID != founderPeer {
		t.Fatalf("claimed issuer identity = (%s, %s), want (%s, %s)", claimed.Payload.IssuerMemberID, claimed.Payload.IssuerPeerID, founderMember, founderPeer)
	}
	if claimed.Payload.AgentMemberID != replacementMember || claimed.Payload.AgentPeerID != replacementPeer {
		t.Fatalf("claimed agent identity = (%s, %s), want (%s, %s)", claimed.Payload.AgentMemberID, claimed.Payload.AgentPeerID, replacementMember, replacementPeer)
	}
	if claimed.Payload.Target.MemberID != replacementMember || claimed.Payload.Target.PeerID != replacementPeer {
		t.Fatalf("claimed target identity = (%s, %s), want (%s, %s)", claimed.Payload.Target.MemberID, claimed.Payload.Target.PeerID, replacementMember, replacementPeer)
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
