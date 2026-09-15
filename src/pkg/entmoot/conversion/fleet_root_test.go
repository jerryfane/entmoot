package conversion

import (
	"database/sql"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// TestRunConvertsRootCarryingRetiredFleetTables pins the upgrade path for a
// pre-libp2p root that still holds the removed feature's tables: conversion
// must not abort on rows it can no longer scope or key.
func TestRunConvertsRootCarryingRetiredFleetTables(t *testing.T) {
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
	} {
		entries = append(entries, legacyRosterChange(t, founder, change.op, entmoot.NodeInfo{
			PilotNodeID:   133053,
			EntmootPubKey: append([]byte(nil), change.subject.PublicKey...),
		}, change.at, entries[len(entries)-1].ID))
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

	espPath := filepath.Join(root, "esp.sqlite")
	db, err := sql.Open("sqlite", "file:"+espPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
CREATE TABLE esp_fleets(
  fleet_id TEXT PRIMARY KEY,
  control_group_id BLOB,
  coordinator_node_id INTEGER NOT NULL,
  coordinator_pubkey TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE TABLE esp_fleet_activity(
  event_id TEXT PRIMARY KEY,
  fleet_id TEXT NOT NULL,
  actor_node_id INTEGER NOT NULL,
  actor_pubkey TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO esp_fleets VALUES('fleet-a',?,133053,?,1700000001000)`,
		gid[:], base64.StdEncoding.EncodeToString(replacement.PublicKey)); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO esp_fleet_activity VALUES('ev-1','fleet-a',133053,?,1700000004500)`,
		base64.StdEncoding.EncodeToString(replacement.PublicKey)); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := Run(root, founder); err != nil {
		t.Fatalf("Run on a root carrying retired fleet tables: %v", err)
	}
}
