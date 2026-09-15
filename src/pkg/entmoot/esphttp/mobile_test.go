package esphttp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"slices"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestSQLiteStateStorePersistsMobileState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	req, err := store.CreateSignRequest(ctx, SignRequest{
		DeviceID:             "ios-1",
		Kind:                 "message_publish",
		Payload:              json.RawMessage(`{"content":"hello"}`),
		CanonicalType:        canonicalTypeMessageV1,
		SignatureAlgorithm:   signatureAlgorithmEd25519,
		SigningPayload:       "cGF5bG9hZA==",
		SigningPayloadSHA256: "ZGlnZXN0",
	})
	if err != nil {
		t.Fatalf("CreateSignRequest: %v", err)
	}
	if _, err := store.UpsertPushToken(ctx, "ios-1", "apns", "token-1"); err != nil {
		t.Fatalf("UpsertPushToken: %v", err)
	}
	if _, err := store.PatchNotificationPreferences(ctx, "ios-1", NotificationPreferences{
		Enabled: true,
		Topics:  []string{"ops/#"},
	}); err != nil {
		t.Fatalf("PatchNotificationPreferences: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	store, err = OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore reopen: %v", err)
	}
	defer store.Close()
	gotReq, ok, err := store.GetSignRequest(ctx, req.ID)
	if err != nil {
		t.Fatalf("GetSignRequest: %v", err)
	}
	if !ok || gotReq.Kind != "message_publish" || string(gotReq.Payload) != `{"content":"hello"}` ||
		gotReq.CanonicalType != canonicalTypeMessageV1 || gotReq.SigningPayloadSHA256 != "ZGlnZXN0" {
		t.Fatalf("sign request after reopen = %+v ok=%v", gotReq, ok)
	}
	state, err := store.GetDeviceState(ctx, "ios-1")
	if err != nil {
		t.Fatalf("GetDeviceState: %v", err)
	}
	if state.PushToken != "token-1" || len(state.NotificationPreferences.Topics) != 1 ||
		state.NotificationPreferences.Topics[0] != "ops/#" {
		t.Fatalf("device state after reopen = %+v", state)
	}
}

func TestSQLiteStateStoreMigratesSignRequests(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "esp.sqlite")+"?"+q.Encode())
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	_, err = db.Exec(`
CREATE TABLE sign_requests (
  id TEXT PRIMARY KEY,
  device_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  group_id BLOB,
  payload BLOB NOT NULL,
  signature TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE esp_devices_state (
  device_id TEXT PRIMARY KEY,
  push_platform TEXT NOT NULL DEFAULT '',
  push_token TEXT NOT NULL DEFAULT '',
  prefs BLOB NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
`)
	if err != nil {
		t.Fatalf("create old schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close old db: %v", err)
	}

	store, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()
	req, err := store.CreateSignRequest(ctx, SignRequest{
		DeviceID: "ios-1",
		Kind:     "group_create",
		Payload:  json.RawMessage(`{"name":"ops"}`),
	})
	if err != nil {
		t.Fatalf("CreateSignRequest after migration: %v", err)
	}
	if req.CanonicalType != canonicalTypeESPOperationV1 || req.SignatureAlgorithm != signatureAlgorithmEd25519 ||
		req.SigningPayload == "" || req.SigningPayloadSHA256 == "" {
		t.Fatalf("migrated sign request metadata = %+v", req)
	}
}

func TestOpenInviteRedemptionReplayRequiresActiveInvite(t *testing.T) {
	ctx := context.Background()
	gid := testMobileGroupID(8)
	bootstrap := []string{
		"/ip4/127.0.0.1/tcp/10001/p2p/" + testMobileNode(t, 45491).PeerID,
		"/ip4/127.0.0.1/tcp/10002/p2p/" + testMobileNode(t, 45460).PeerID,
	}
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			rec, err := tc.store.CreateOpenInvite(ctx, OpenInviteRecord{
				TokenHash:           "token-a",
				GroupID:             gid,
				MaxUses:             2,
				BootstrapMultiaddrs: bootstrap,
				ExpiresAtMS:         2_000,
			})
			if err != nil {
				t.Fatalf("CreateOpenInvite: %v", err)
			}
			if !slices.Equal(rec.BootstrapMultiaddrs, bootstrap) {
				t.Fatalf("created bootstrap addresses = %v", rec.BootstrapMultiaddrs)
			}
			redemption := OpenInviteRedemption{
				RedeemerKey:   "45981:key",
				MemberID:      testMemberID(45981),
				EntmootPubKey: "key",
			}
			rec, red, already, err := tc.store.RedeemOpenInvite(ctx, "token-a", redemption, 1_000)
			if err != nil || already {
				t.Fatalf("first RedeemOpenInvite err/already = %v/%v", err, already)
			}
			if !slices.Equal(rec.BootstrapMultiaddrs, bootstrap) || red.Result != nil {
				t.Fatalf("first redeem rec/red = %+v/%+v", rec, red)
			}
			result := json.RawMessage(`{"status":"redeemed","invite":{"group_id":"x"}}`)
			if err := tc.store.CompleteOpenInviteRedemption(ctx, "token-a", redemption.RedeemerKey, result, 1_001); err != nil {
				t.Fatalf("CompleteOpenInviteRedemption: %v", err)
			}
			stored, ok, err := tc.store.GetOpenInviteRedemption(ctx, "token-a", redemption.RedeemerKey)
			if err != nil || !ok {
				t.Fatalf("GetOpenInviteRedemption err/ok = %v/%v", err, ok)
			}
			if string(stored.Result) != string(result) {
				t.Fatalf("stored redemption result = %s, want %s", stored.Result, result)
			}
			rec, red, already, err = tc.store.RedeemOpenInvite(ctx, "token-a", redemption, 1_500)
			if err != nil || !already {
				t.Fatalf("repeat active RedeemOpenInvite err/already = %v/%v", err, already)
			}
			if string(red.Result) != string(result) || rec.UseCount != 1 {
				t.Fatalf("repeat active rec/red = %+v/%s", rec, red.Result)
			}
			_, _, _, err = tc.store.RedeemOpenInvite(ctx, "token-a", redemption, 2_500)
			if !errors.Is(err, ErrOpenInviteExpired) {
				t.Fatalf("repeat expired err = %v, want ErrOpenInviteExpired", err)
			}
		})
	}
}

func TestOpenInviteRepeatRedemptionHonorsRevocation(t *testing.T) {
	ctx := context.Background()
	gid := testMobileGroupID(9)
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			_, err := tc.store.CreateOpenInvite(ctx, OpenInviteRecord{
				TokenHash:   "token-b",
				GroupID:     gid,
				MaxUses:     2,
				ExpiresAtMS: 10_000,
			})
			if err != nil {
				t.Fatalf("CreateOpenInvite: %v", err)
			}
			redemption := OpenInviteRedemption{
				RedeemerKey:   "45981:key",
				MemberID:      testMemberID(45981),
				EntmootPubKey: "key",
			}
			if _, _, _, err := tc.store.RedeemOpenInvite(ctx, "token-b", redemption, 1_000); err != nil {
				t.Fatalf("first RedeemOpenInvite: %v", err)
			}
			if err := tc.store.CompleteOpenInviteRedemption(ctx, "token-b", redemption.RedeemerKey, json.RawMessage(`{"status":"redeemed"}`), 1_001); err != nil {
				t.Fatalf("CompleteOpenInviteRedemption: %v", err)
			}
			tc.revoke(t, "token-b")
			_, _, _, err = tc.store.RedeemOpenInvite(ctx, "token-b", redemption, 1_500)
			if !errors.Is(err, ErrOpenInviteRevoked) {
				t.Fatalf("repeat revoked err = %v, want ErrOpenInviteRevoked", err)
			}
		})
	}
}

func TestOpenInviteUnlimitedMaxUsesRedeemsMultipleIdentities(t *testing.T) {
	ctx := context.Background()
	gid := testMobileGroupID(10)
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			if _, err := tc.store.CreateOpenInvite(ctx, OpenInviteRecord{
				TokenHash:   "token-unlimited",
				GroupID:     gid,
				MaxUses:     OpenInviteUnlimitedMaxUses,
				ExpiresAtMS: 10_000,
			}); err != nil {
				t.Fatalf("CreateOpenInvite: %v", err)
			}
			redemptions := []OpenInviteRedemption{{
				RedeemerKey:   "45981:key-a",
				MemberID:      testMemberID(45981),
				EntmootPubKey: "key-a",
			}, {
				RedeemerKey:   "45982:key-b",
				MemberID:      testMemberID(45982),
				EntmootPubKey: "key-b",
			}, {
				RedeemerKey:   "45983:key-c",
				MemberID:      testMemberID(45983),
				EntmootPubKey: "key-c",
			}}
			for i, redemption := range redemptions {
				rec, _, already, err := tc.store.RedeemOpenInvite(ctx, "token-unlimited", redemption, 1_000+int64(i))
				if err != nil || already {
					t.Fatalf("RedeemOpenInvite %d err/already = %v/%v", i, err, already)
				}
				if rec.MaxUses != OpenInviteUnlimitedMaxUses || rec.UseCount != i+1 {
					t.Fatalf("unlimited redeem %d rec = %+v", i, rec)
				}
			}
			rec, _, already, err := tc.store.RedeemOpenInvite(ctx, "token-unlimited", redemptions[0], 2_000)
			if err != nil || !already {
				t.Fatalf("repeat RedeemOpenInvite err/already = %v/%v", err, already)
			}
			summary := OpenInviteSummaryFromRecord(rec, 2_000)
			if rec.UseCount != len(redemptions) || summary.Status != "active" {
				t.Fatalf("repeat unlimited rec = %+v summary=%+v", rec, summary)
			}
		})
	}
}

func TestOpenInviteUnlimitedStillRejectsRevokedAndExpiredInvites(t *testing.T) {
	ctx := context.Background()
	gid := testMobileGroupID(11)
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			if _, err := tc.store.CreateOpenInvite(ctx, OpenInviteRecord{
				TokenHash:   "token-unlimited-revoked",
				GroupID:     gid,
				MaxUses:     OpenInviteUnlimitedMaxUses,
				ExpiresAtMS: 10_000,
			}); err != nil {
				t.Fatalf("CreateOpenInvite revoked case: %v", err)
			}
			tc.revoke(t, "token-unlimited-revoked")
			redemption := OpenInviteRedemption{
				RedeemerKey:   "45981:key-a",
				MemberID:      testMemberID(45981),
				EntmootPubKey: "key-a",
			}
			_, _, _, err := tc.store.RedeemOpenInvite(ctx, "token-unlimited-revoked", redemption, 1_000)
			if !errors.Is(err, ErrOpenInviteRevoked) {
				t.Fatalf("revoked unlimited err = %v, want ErrOpenInviteRevoked", err)
			}
			if _, err := tc.store.CreateOpenInvite(ctx, OpenInviteRecord{
				TokenHash:   "token-unlimited-expired",
				GroupID:     gid,
				MaxUses:     OpenInviteUnlimitedMaxUses,
				ExpiresAtMS: 1_000,
			}); err != nil {
				t.Fatalf("CreateOpenInvite expired case: %v", err)
			}
			_, _, _, err = tc.store.RedeemOpenInvite(ctx, "token-unlimited-expired", redemption, 1_001)
			if !errors.Is(err, ErrOpenInviteExpired) {
				t.Fatalf("expired unlimited err = %v, want ErrOpenInviteExpired", err)
			}
		})
	}
}

func TestOpenInviteRejectsNegativeMaxUses(t *testing.T) {
	ctx := context.Background()
	gid := testMobileGroupID(12)
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			_, err := tc.store.CreateOpenInvite(ctx, OpenInviteRecord{
				TokenHash: "token-negative",
				GroupID:   gid,
				MaxUses:   -1,
			})
			if err == nil {
				t.Fatal("CreateOpenInvite err = nil, want negative max_uses rejection")
			}
		})
	}
}

func TestGroupMetadataStoresRejectNonObjectJSON(t *testing.T) {
	ctx := context.Background()
	gid := testMobileGroupID(7)
	stores := []struct {
		name  string
		store GroupMetadataStore
		close func()
	}{
		{name: "memory", store: NewMemoryStateStore()},
	}
	sqlite, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	stores = append(stores, struct {
		name  string
		store GroupMetadataStore
		close func()
	}{name: "sqlite", store: sqlite, close: func() { _ = sqlite.Close() }})

	for _, tc := range stores {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			if err := tc.store.SetGroupMetadata(ctx, gid, json.RawMessage(`{"name":"ops"}`)); err != nil {
				t.Fatalf("SetGroupMetadata object: %v", err)
			}
			for _, raw := range []json.RawMessage{
				json.RawMessage(`[]`),
				json.RawMessage(`"name"`),
				json.RawMessage(`null`),
				json.RawMessage(`true`),
				json.RawMessage(`123`),
				json.RawMessage(`{`),
			} {
				if err := tc.store.SetGroupMetadata(ctx, gid, raw); err == nil {
					t.Fatalf("SetGroupMetadata(%s) succeeded, want error", raw)
				}
			}
		})
	}
}

type openInviteStoreCase struct {
	name   string
	store  StateStore
	close  func()
	revoke func(*testing.T, string)
}

func openInviteStateStores(t *testing.T) []openInviteStoreCase {
	t.Helper()
	sqlite, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stores := []openInviteStoreCase{
		{name: "memory", store: NewMemoryStateStore()},
		{name: "sqlite", store: sqlite},
	}
	for i := range stores {
		state := stores[i].store
		stores[i].close = func() {
			if err := state.Close(); err != nil {
				t.Error(err)
			}
		}
		stores[i].revoke = func(t *testing.T, tokenHash string) {
			t.Helper()
			if _, found, err := state.RevokeOpenInvite(context.Background(), tokenHash, 1_500); err != nil || !found {
				t.Fatalf("revoke invite: found=%v err=%v", found, err)
			}
		}
	}
	return stores
}

func testMobileNode(t *testing.T, seed uint32) entmoot.NodeInfo {
	t.Helper()
	publicKey := testMemberID(seed)
	return testOperationalNodeInfo(t, publicKey[:])
}

func testMobileGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}
