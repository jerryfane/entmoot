package esphttp

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	ctx := context.Background()
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
