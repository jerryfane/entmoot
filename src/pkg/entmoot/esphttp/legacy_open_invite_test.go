package esphttp

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
)

func TestLegacyOpenInviteCanBeRedeemedAfterUpgrade(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "esp.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	// The deployed pre-libp2p table contains numeric Pilot peers, not multiaddrs.
	_, err = db.Exec(`CREATE TABLE esp_open_invites (
		token_hash TEXT PRIMARY KEY, group_id BLOB NOT NULL,
		device_id TEXT NOT NULL DEFAULT '', max_uses INTEGER NOT NULL,
		use_count INTEGER NOT NULL DEFAULT 0, revoked INTEGER NOT NULL DEFAULT 0,
		bootstrap_peers BLOB, created_at_ms INTEGER NOT NULL,
		updated_at_ms INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL,
		no_fallback_peers INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	gid := testGroupID(3)
	_, err = db.Exec(`INSERT INTO esp_open_invites VALUES ('hash', ?, 'dev', 3, 1, 0, '[42]', 1, 1, 9999999999999, 0)`, gid[:])
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	createLegacyRedemptions(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	ctx := context.Background()
	var archivedResult string
	var archivedNode int
	if err := state.db.QueryRow(`SELECT pilot_node_id, result FROM esp_legacy_open_invite_redemptions WHERE token_hash = 'hash' AND redeemer_key = 'new-member'`).Scan(&archivedNode, &archivedResult); err != nil {
		t.Fatal(err)
	}
	if archivedNode != 42 || archivedResult != `{"invite":"retired-pilot"}` {
		t.Fatalf("legacy audit record changed: node=%d result=%s", archivedNode, archivedResult)
	}
	if _, found, err := state.GetOpenInviteRedemption(ctx, "hash", "new-member"); err != nil || found {
		t.Fatalf("legacy capability leaked into operational replay: found=%v err=%v", found, err)
	}
	rec, found, err := state.GetOpenInviteByTokenHash(ctx, "hash")
	if err != nil || !found {
		t.Fatalf("read existing invite after upgrade: found=%v err=%v", found, err)
	}
	if rec.GroupID != gid || rec.MaxUses != 3 || rec.UseCount != 1 || rec.DeviceID != "dev" || rec.Revoked || rec.NoFallbackPeers || len(rec.BootstrapMultiaddrs) != 0 {
		t.Fatalf("upgrade changed existing invite or treated numeric peers as multiaddrs: %+v", rec)
	}
	redemption := OpenInviteRedemption{RedeemerKey: "new-member"}
	rec, _, repeated, err := state.RedeemOpenInvite(ctx, "hash", redemption, 2)
	if err != nil || repeated || rec.UseCount != 2 {
		t.Fatalf("first redemption: count=%d repeated=%v err=%v", rec.UseCount, repeated, err)
	}
	result := json.RawMessage(`{"capability":"issued"}`)
	if err := state.CompleteOpenInviteRedemption(ctx, "hash", redemption.RedeemerKey, result, 3); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	rec, replay, repeated, err := state.RedeemOpenInvite(ctx, "hash", redemption, 4)
	if err != nil || !repeated || rec.UseCount != 2 || string(replay.Result) != string(result) {
		t.Fatalf("persisted retry: count=%d repeated=%v result=%s err=%v", rec.UseCount, repeated, replay.Result, err)
	}
}

func createLegacyRedemptions(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE esp_open_invite_redemptions (
		token_hash TEXT NOT NULL, redeemer_key TEXT NOT NULL,
		pilot_node_id INTEGER NOT NULL, entmoot_pubkey TEXT NOT NULL,
		result BLOB, redeemed_at_ms INTEGER NOT NULL,
		PRIMARY KEY(token_hash, redeemer_key)
	);
	INSERT INTO esp_open_invite_redemptions VALUES ('hash', 'new-member', 42, 'old-key', '{"invite":"retired-pilot"}', 1);`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentLegacyRedemptionCutover(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "esp.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	createLegacyRedemptions(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	const workers = 8
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			state, err := OpenSQLiteStateStore(dir)
			if err == nil {
				err = state.Close()
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	state, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	var result string
	if err := state.db.QueryRow(`SELECT result FROM esp_legacy_open_invite_redemptions WHERE token_hash = 'hash' AND redeemer_key = 'new-member'`).Scan(&result); err != nil || result != `{"invite":"retired-pilot"}` {
		t.Fatalf("archive was lost during concurrent startup: result=%s err=%v", result, err)
	}
	if _, found, err := state.GetOpenInviteRedemption(context.Background(), "hash", "new-member"); err != nil || found {
		t.Fatalf("operational redemption table after concurrent startup: found=%v err=%v", found, err)
	}
}
