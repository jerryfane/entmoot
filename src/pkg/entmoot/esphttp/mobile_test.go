package esphttp

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
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

func TestSQLiteStateStoreResetsFleetMemberTimestampsOnReinvite(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer store.Close()

	pubkey := base64.StdEncoding.EncodeToString([]byte("agent-pubkey"))
	if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
		FleetID:       "fleet-a",
		NodeID:        45460,
		EntmootPubKey: pubkey,
		Role:          FleetRoleAgent,
		Status:        FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("Upsert active member: %v", err)
	}
	if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
		FleetID:       "fleet-a",
		NodeID:        45460,
		EntmootPubKey: pubkey,
		Role:          FleetRoleAgent,
		Status:        FleetMemberRemoved,
		RemovedAtMS:   1_700_000_001_000,
	}); err != nil {
		t.Fatalf("Upsert removed member: %v", err)
	}
	if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
		FleetID:       "fleet-a",
		NodeID:        45460,
		EntmootPubKey: pubkey,
		Role:          FleetRoleAgent,
		Status:        FleetMemberInvited,
		InvitedAtMS:   1_700_000_002_000,
	}); err != nil {
		t.Fatalf("Upsert invited member: %v", err)
	}
	members, err := store.ListFleetMembers(ctx, "fleet-a")
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("members = %+v, want one member", members)
	}
	got := members[0]
	if got.Status != FleetMemberInvited || got.InvitedAtMS != 1_700_000_002_000 || got.AcceptedAtMS != 0 || got.RemovedAtMS != 0 {
		t.Fatalf("reinvited member = %+v, want invited with stale timestamps reset", got)
	}
}

func TestStateStoresArchiveFleetAndClearInvites(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		open func(*testing.T) StateStore
	}{
		{name: "memory", open: func(t *testing.T) StateStore { return NewMemoryStateStore() }},
		{name: "sqlite", open: func(t *testing.T) StateStore {
			store, err := OpenSQLiteStateStore(t.TempDir())
			if err != nil {
				t.Fatalf("OpenSQLiteStateStore: %v", err)
			}
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			defer store.Close()
			if _, err := store.CreateFleet(ctx, FleetRecord{
				FleetID:             "fleet-a",
				Name:                "Fleet A",
				Coordinator:         entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("coordinator")},
				CoordinatorDeviceID: "ios-1",
				CreatedAtMS:         1,
			}); err != nil {
				t.Fatalf("CreateFleet: %v", err)
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-a",
				FleetID:       "fleet-a",
				NodeID:        45460,
				EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("agent")),
				Status:        FleetMemberInvited,
				CreatedAtMS:   2,
			}); err != nil {
				t.Fatalf("CreateFleetInvite: %v", err)
			}
			fleet, ok, err := store.ArchiveFleet(ctx, "fleet-a", 1_700_000_000_000)
			if err != nil || !ok {
				t.Fatalf("ArchiveFleet ok/err = %v/%v", ok, err)
			}
			if fleet.Status != FleetStatusArchived || fleet.ArchivedAtMS != 1_700_000_000_000 {
				t.Fatalf("archived fleet = %+v", fleet)
			}
			invites, err := store.ListFleetInvites(ctx, "fleet-a")
			if err != nil {
				t.Fatalf("ListFleetInvites: %v", err)
			}
			if len(invites) != 0 {
				t.Fatalf("invites after archive = %+v, want none", invites)
			}
			if _, err := store.UpsertFleetMemberForActiveFleet(ctx, FleetMemberRecord{
				FleetID:       "fleet-a",
				NodeID:        45461,
				EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("agent-2")),
				Role:          FleetRoleAgent,
				Status:        FleetMemberInvited,
			}); !errors.Is(err, ErrFleetNotActive) {
				t.Fatalf("UpsertFleetMemberForActiveFleet err = %v, want ErrFleetNotActive", err)
			}
			if _, err := store.CreateFleetInviteForActiveFleet(ctx, FleetInviteRecord{
				InviteID:      "invite-b",
				FleetID:       "fleet-a",
				NodeID:        45461,
				EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("agent-2")),
				Status:        FleetMemberInvited,
			}); !errors.Is(err, ErrFleetNotActive) {
				t.Fatalf("CreateFleetInviteForActiveFleet err = %v, want ErrFleetNotActive", err)
			}
			restored, ok, err := store.RestoreFleet(ctx, "fleet-a", 1_700_000_001_000)
			if err != nil || !ok {
				t.Fatalf("RestoreFleet ok/err = %v/%v", ok, err)
			}
			if restored.Status != FleetStatusActive || restored.ArchivedAtMS != 0 || restored.UpdatedAtMS != 1_700_000_001_000 {
				t.Fatalf("restored fleet = %+v", restored)
			}
			if _, err := store.CreateFleetInviteForActiveFleet(ctx, FleetInviteRecord{
				InviteID:      "invite-c",
				FleetID:       "fleet-a",
				NodeID:        45462,
				EntmootPubKey: base64.StdEncoding.EncodeToString([]byte("agent-3")),
				Status:        FleetMemberInvited,
			}); err != nil {
				t.Fatalf("CreateFleetInviteForActiveFleet after restore: %v", err)
			}
		})
	}
}

func TestStateStoresReconcileFleetInviteAcceptanceIsConditional(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		open func(*testing.T) StateStore
	}{
		{name: "memory", open: func(t *testing.T) StateStore { return NewMemoryStateStore() }},
		{name: "sqlite", open: func(t *testing.T) StateStore {
			store, err := OpenSQLiteStateStore(t.TempDir())
			if err != nil {
				t.Fatalf("OpenSQLiteStateStore: %v", err)
			}
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			defer store.Close()
			pubkey := base64.StdEncoding.EncodeToString([]byte("agent"))
			if _, err := store.CreateFleet(ctx, FleetRecord{
				FleetID:             "fleet-a",
				Name:                "Fleet A",
				Coordinator:         entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("coordinator")},
				CoordinatorDeviceID: "ios-1",
				CreatedAtMS:         1,
			}); err != nil {
				t.Fatalf("CreateFleet: %v", err)
			}
			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-a",
				NodeID:        45460,
				EntmootPubKey: pubkey,
				Role:          FleetRoleAgent,
				Status:        FleetMemberInvited,
				InvitedAtMS:   2,
			}); err != nil {
				t.Fatalf("Upsert invited member: %v", err)
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-a",
				FleetID:       "fleet-a",
				NodeID:        45460,
				EntmootPubKey: pubkey,
				Status:        FleetMemberInvited,
				CreatedAtMS:   3,
				ExpiresAtMS:   20_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite: %v", err)
			}
			member, activity, applied, err := store.ReconcileFleetInviteAcceptance(ctx, "fleet-a", 45460, pubkey, 10_000, "deimos")
			if err != nil || !applied {
				t.Fatalf("ReconcileFleetInviteAcceptance applied/err = %v/%v", applied, err)
			}
			if member.Status != FleetMemberActive || member.AcceptedAtMS != 10_000 || member.Hostname != "deimos" {
				t.Fatalf("reconciled member = %+v, want active deimos", member)
			}
			if activity.Type != "member.accepted" || activity.EventID == "" {
				t.Fatalf("reconciled activity = %+v, want member.accepted", activity)
			}
			invites, err := store.ListFleetInvites(ctx, "fleet-a")
			if err != nil {
				t.Fatalf("ListFleetInvites: %v", err)
			}
			if len(invites) != 0 {
				t.Fatalf("invites after reconcile = %+v, want none", invites)
			}
			activityList, err := store.ListFleetActivity(ctx, "fleet-a", 10, 0)
			if err != nil {
				t.Fatalf("ListFleetActivity: %v", err)
			}
			if len(activityList) != 1 || activityList[0].Type != "member.accepted" {
				t.Fatalf("activity after reconcile = %+v, want one member.accepted", activityList)
			}

			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-a",
				NodeID:        45460,
				EntmootPubKey: pubkey,
				Role:          FleetRoleAgent,
				Status:        FleetMemberRemoved,
				RemovedAtMS:   11_000,
			}); err != nil {
				t.Fatalf("Upsert removed member: %v", err)
			}
			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-b",
				FleetID:       "fleet-a",
				NodeID:        45460,
				EntmootPubKey: pubkey,
				Status:        FleetMemberInvited,
				CreatedAtMS:   12_000,
				ExpiresAtMS:   20_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite invite-b: %v", err)
			}
			if _, _, applied, err := store.ReconcileFleetInviteAcceptance(ctx, "fleet-a", 45460, pubkey, 13_000, "deimos"); err != nil || applied {
				t.Fatalf("removed reconcile applied/err = %v/%v, want false/nil", applied, err)
			}
			members, err := store.ListFleetMembers(ctx, "fleet-a")
			if err != nil {
				t.Fatalf("ListFleetMembers: %v", err)
			}
			if len(members) != 1 || members[0].Status != FleetMemberRemoved {
				t.Fatalf("member after removed reconcile = %+v, want removed", members)
			}

			if _, err := store.UpsertFleetMember(ctx, FleetMemberRecord{
				FleetID:       "fleet-a",
				NodeID:        45460,
				EntmootPubKey: pubkey,
				Role:          FleetRoleAgent,
				Status:        FleetMemberInvited,
				InvitedAtMS:   14_000,
			}); err != nil {
				t.Fatalf("Upsert reinvited member: %v", err)
			}
			if err := store.DeleteFleetInvite(ctx, "invite-b"); err != nil {
				t.Fatalf("DeleteFleetInvite invite-b: %v", err)
			}
			if _, _, applied, err := store.ReconcileFleetInviteAcceptance(ctx, "fleet-a", 45460, pubkey, 15_000, "deimos"); err != nil || applied {
				t.Fatalf("missing invite reconcile applied/err = %v/%v, want false/nil", applied, err)
			}
			members, err = store.ListFleetMembers(ctx, "fleet-a")
			if err != nil {
				t.Fatalf("ListFleetMembers after missing invite: %v", err)
			}
			if len(members) != 1 || members[0].Status != FleetMemberInvited {
				t.Fatalf("member after missing invite reconcile = %+v, want invited", members)
			}

			if _, err := store.CreateFleetInvite(ctx, FleetInviteRecord{
				InviteID:      "invite-expired",
				FleetID:       "fleet-a",
				NodeID:        45460,
				EntmootPubKey: pubkey,
				Status:        FleetMemberInvited,
				CreatedAtMS:   16_000,
				ExpiresAtMS:   17_000,
			}); err != nil {
				t.Fatalf("CreateFleetInvite expired: %v", err)
			}
			if _, _, applied, err := store.ReconcileFleetInviteAcceptance(ctx, "fleet-a", 45460, pubkey, 18_000, "deimos"); err != nil || !applied {
				t.Fatalf("expired invite reconcile applied/err = %v/%v, want true/nil", applied, err)
			}
			invites, err = store.ListFleetInvites(ctx, "fleet-a")
			if err != nil {
				t.Fatalf("ListFleetInvites after expired reconcile: %v", err)
			}
			if len(invites) != 0 {
				t.Fatalf("invites after expired reconcile = %+v, want none", invites)
			}
		})
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
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			rec, err := tc.store.CreateOpenInvite(ctx, OpenInviteRecord{
				TokenHash:      "token-a",
				GroupID:        gid,
				MaxUses:        2,
				BootstrapPeers: []entmoot.NodeID{45491, 45460},
				ExpiresAtMS:    2_000,
			})
			if err != nil {
				t.Fatalf("CreateOpenInvite: %v", err)
			}
			if !sameNodeIDs(rec.BootstrapPeers, []entmoot.NodeID{45491, 45460}) {
				t.Fatalf("created bootstrap peers = %v", rec.BootstrapPeers)
			}
			redemption := OpenInviteRedemption{
				RedeemerKey:   "45981:key",
				PilotNodeID:   45981,
				EntmootPubKey: "key",
			}
			rec, red, already, err := tc.store.RedeemOpenInvite(ctx, "token-a", redemption, 1_000)
			if err != nil || already {
				t.Fatalf("first RedeemOpenInvite err/already = %v/%v", err, already)
			}
			if !sameNodeIDs(rec.BootstrapPeers, []entmoot.NodeID{45491, 45460}) || red.Result != nil {
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
				PilotNodeID:   45981,
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
				PilotNodeID:   45981,
				EntmootPubKey: "key-a",
			}, {
				RedeemerKey:   "45982:key-b",
				PilotNodeID:   45982,
				EntmootPubKey: "key-b",
			}, {
				RedeemerKey:   "45983:key-c",
				PilotNodeID:   45983,
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
				PilotNodeID:   45981,
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

func TestOpenInviteChallengesAreSingleUseAndExpire(t *testing.T) {
	ctx := context.Background()
	gid := testMobileGroupID(10)
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			ch := OpenInviteChallenge{
				ChallengeID:    "challenge-a",
				TokenHash:      "token-c",
				GroupID:        gid,
				PilotNodeID:    45981,
				PilotPubKey:    "pilot",
				EntmootPubKey:  "entmoot",
				Nonce:          "nonce",
				SigningPayload: "payload",
				CreatedAtMS:    1_000,
				ExpiresAtMS:    2_000,
			}
			if _, err := tc.store.CreateOpenInviteChallenge(ctx, ch); err != nil {
				t.Fatalf("CreateOpenInviteChallenge: %v", err)
			}
			stored, ok, err := tc.store.GetOpenInviteChallenge(ctx, "challenge-a")
			if err != nil || !ok || stored.GroupID != gid || stored.PilotNodeID != 45981 {
				t.Fatalf("GetOpenInviteChallenge = %+v/%v/%v", stored, ok, err)
			}
			used, err := tc.store.ConsumeOpenInviteChallenge(ctx, "challenge-a", 1_500)
			if err != nil || used.UsedAtMS != 1_500 {
				t.Fatalf("ConsumeOpenInviteChallenge used = %+v err=%v", used, err)
			}
			_, err = tc.store.ConsumeOpenInviteChallenge(ctx, "challenge-a", 1_600)
			if !errors.Is(err, ErrOpenInviteChallengeUsed) {
				t.Fatalf("second consume err = %v, want ErrOpenInviteChallengeUsed", err)
			}
			ch.ChallengeID = "challenge-b"
			if _, err := tc.store.CreateOpenInviteChallenge(ctx, ch); err != nil {
				t.Fatalf("CreateOpenInviteChallenge expired case: %v", err)
			}
			_, err = tc.store.ConsumeOpenInviteChallenge(ctx, "challenge-b", 2_500)
			if !errors.Is(err, ErrOpenInviteChallengeExpired) {
				t.Fatalf("expired consume err = %v, want ErrOpenInviteChallengeExpired", err)
			}
		})
	}
}

func TestOpenInviteChallengesReusePruneAndCapActiveRows(t *testing.T) {
	ctx := context.Background()
	gid := testMobileGroupID(11)
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			base := OpenInviteChallenge{
				ChallengeID:    "challenge-reuse",
				TokenHash:      "token-d",
				GroupID:        gid,
				PilotNodeID:    45981,
				PilotPubKey:    "pilot-a",
				EntmootPubKey:  "entmoot-a",
				Nonce:          "nonce-a",
				SigningPayload: "payload-a",
				CreatedAtMS:    1_000,
				ExpiresAtMS:    10_000,
			}
			first, err := tc.store.CreateOrReuseOpenInviteChallenge(ctx, base, 2, 2_000)
			if err != nil {
				t.Fatalf("first CreateOrReuseOpenInviteChallenge: %v", err)
			}
			reused, err := tc.store.CreateOrReuseOpenInviteChallenge(ctx, OpenInviteChallenge{
				ChallengeID:    "challenge-new",
				TokenHash:      base.TokenHash,
				GroupID:        gid,
				PilotNodeID:    base.PilotNodeID,
				PilotPubKey:    base.PilotPubKey,
				EntmootPubKey:  base.EntmootPubKey,
				Nonce:          "nonce-b",
				SigningPayload: "payload-b",
				CreatedAtMS:    3_000,
				ExpiresAtMS:    10_000,
			}, 2, 3_000)
			if err != nil {
				t.Fatalf("reuse CreateOrReuseOpenInviteChallenge: %v", err)
			}
			if reused.ChallengeID != first.ChallengeID || reused.SigningPayload != first.SigningPayload {
				t.Fatalf("reused challenge = %+v, want first %+v", reused, first)
			}
			expired := OpenInviteChallenge{
				ChallengeID:    "challenge-expired",
				TokenHash:      base.TokenHash,
				GroupID:        gid,
				PilotNodeID:    45982,
				PilotPubKey:    "pilot-expired",
				EntmootPubKey:  "entmoot-expired",
				Nonce:          "nonce-expired",
				SigningPayload: "payload-expired",
				CreatedAtMS:    1_000,
				ExpiresAtMS:    1_500,
			}
			if _, err := tc.store.CreateOpenInviteChallenge(ctx, expired); err != nil {
				t.Fatalf("seed expired challenge: %v", err)
			}
			second, err := tc.store.CreateOrReuseOpenInviteChallenge(ctx, OpenInviteChallenge{
				ChallengeID:    "challenge-second",
				TokenHash:      base.TokenHash,
				GroupID:        gid,
				PilotNodeID:    45983,
				PilotPubKey:    "pilot-second",
				EntmootPubKey:  "entmoot-second",
				Nonce:          "nonce-second",
				SigningPayload: "payload-second",
				CreatedAtMS:    4_000,
				ExpiresAtMS:    10_000,
			}, 2, 4_000)
			if err != nil {
				t.Fatalf("second active CreateOrReuseOpenInviteChallenge: %v", err)
			}
			if second.ChallengeID != "challenge-second" {
				t.Fatalf("second challenge id = %q, want challenge-second", second.ChallengeID)
			}
			_, err = tc.store.CreateOrReuseOpenInviteChallenge(ctx, OpenInviteChallenge{
				ChallengeID:    "challenge-third",
				TokenHash:      base.TokenHash,
				GroupID:        gid,
				PilotNodeID:    45984,
				PilotPubKey:    "pilot-third",
				EntmootPubKey:  "entmoot-third",
				Nonce:          "nonce-third",
				SigningPayload: "payload-third",
				CreatedAtMS:    5_000,
				ExpiresAtMS:    10_000,
			}, 2, 5_000)
			if !errors.Is(err, ErrOpenInviteChallengeLimit) {
				t.Fatalf("third active err = %v, want ErrOpenInviteChallengeLimit", err)
			}
			if _, ok, err := tc.store.GetOpenInviteChallenge(ctx, "challenge-expired"); err != nil || ok {
				t.Fatalf("expired challenge present/err = %v/%v, want pruned", ok, err)
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

func TestStateStoresClaimFleetTaskConditionally(t *testing.T) {
	ctx := context.Background()
	assigneeA := entmoot.NodeInfo{PilotNodeID: 45492, EntmootPubKey: []byte("assignee-a")}
	assigneeB := entmoot.NodeInfo{PilotNodeID: 45493, EntmootPubKey: []byte("assignee-b")}
	stores := []struct {
		name  string
		store StateStore
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
		store StateStore
		close func()
	}{name: "sqlite", store: sqlite, close: func() { _ = sqlite.Close() }})

	for _, tc := range stores {
		t.Run(tc.name, func(t *testing.T) {
			if tc.close != nil {
				defer tc.close()
			}
			base := FleetTaskRecord{
				TaskID:      "task-claim",
				FleetID:     "fleet-claim",
				Title:       "Claim once",
				Mode:        FleetTaskModeFirstClaim,
				Status:      FleetTaskStatusOpen,
				Creator:     entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("creator")},
				CreatedAtMS: 1,
				UpdatedAtMS: 1,
			}
			if _, err := tc.store.UpsertFleetTask(ctx, base); err != nil {
				t.Fatalf("UpsertFleetTask: %v", err)
			}
			first := base
			first.Status = FleetTaskStatusAssigned
			first.Assignee = &assigneeA
			claimed, ok, err := tc.store.ClaimFleetTask(ctx, first)
			if err != nil || !ok {
				t.Fatalf("first ClaimFleetTask ok/err = %v/%v", ok, err)
			}
			if claimed.Assignee == nil || claimed.Assignee.PilotNodeID != assigneeA.PilotNodeID {
				t.Fatalf("claimed assignee = %+v", claimed.Assignee)
			}
			second := base
			second.Status = FleetTaskStatusAssigned
			second.Assignee = &assigneeB
			_, ok, err = tc.store.ClaimFleetTask(ctx, second)
			if err != nil {
				t.Fatalf("second ClaimFleetTask error: %v", err)
			}
			if ok {
				t.Fatalf("second ClaimFleetTask succeeded, want conditional miss")
			}
			got, found, err := tc.store.GetFleetTask(ctx, base.FleetID, base.TaskID)
			if err != nil || !found {
				t.Fatalf("GetFleetTask found/err = %v/%v", found, err)
			}
			if got.Assignee == nil || got.Assignee.PilotNodeID != assigneeA.PilotNodeID {
				t.Fatalf("stored assignee = %+v, want first assignee", got.Assignee)
			}
			stale := got
			stale.Status = FleetTaskStatusCanceled
			if _, ok, err := tc.store.UpdateFleetTaskIfCurrent(ctx, stale, got.UpdatedAtMS-1); err != nil || ok {
				t.Fatalf("stale UpdateFleetTaskIfCurrent ok/err = %v/%v, want false/nil", ok, err)
			}
			updated, ok, err := tc.store.UpdateFleetTaskIfCurrent(ctx, stale, got.UpdatedAtMS)
			if err != nil || !ok {
				t.Fatalf("current UpdateFleetTaskIfCurrent ok/err = %v/%v", ok, err)
			}
			if updated.Status != FleetTaskStatusCanceled {
				t.Fatalf("updated status = %q, want canceled", updated.Status)
			}

			submitBase := FleetTaskRecord{
				TaskID:      "task-submit",
				FleetID:     "fleet-claim",
				Title:       "Submit atomically",
				Mode:        FleetTaskModeDirectAssignment,
				Status:      FleetTaskStatusAssigned,
				Creator:     entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("creator")},
				Assignee:    &assigneeA,
				CreatedAtMS: 1,
				UpdatedAtMS: 1,
			}
			if _, err := tc.store.UpsertFleetTask(ctx, submitBase); err != nil {
				t.Fatalf("UpsertFleetTask submit: %v", err)
			}
			current, found, err := tc.store.GetFleetTask(ctx, submitBase.FleetID, submitBase.TaskID)
			if err != nil || !found {
				t.Fatalf("GetFleetTask submit found/err = %v/%v", found, err)
			}
			submittedTask := current
			submittedTask.Status = FleetTaskStatusSubmitted
			submittedTask.UpdatedAtMS = current.UpdatedAtMS + 1
			submission := FleetTaskSubmissionRecord{
				SubmissionID: "submission-a",
				FleetID:      current.FleetID,
				TaskID:       current.TaskID,
				Author:       assigneeA,
				Content:      "done",
				Status:       FleetTaskSubmissionPending,
				CreatedAtMS:  current.UpdatedAtMS + 1,
				UpdatedAtMS:  current.UpdatedAtMS + 1,
			}
			submitted, storedSubmission, ok, err := tc.store.SubmitFleetTask(ctx, submittedTask, current.UpdatedAtMS, submission)
			if err != nil || !ok {
				t.Fatalf("SubmitFleetTask ok/err = %v/%v", ok, err)
			}
			if submitted.Status != FleetTaskStatusSubmitted || storedSubmission.SubmissionID != submission.SubmissionID {
				t.Fatalf("SubmitFleetTask result = %+v / %+v", submitted, storedSubmission)
			}
			if _, _, ok, err := tc.store.SubmitFleetTask(ctx, submittedTask, current.UpdatedAtMS, submission); err != nil || ok {
				t.Fatalf("stale SubmitFleetTask ok/err = %v/%v, want false/nil", ok, err)
			}
			submissions, err := tc.store.ListFleetTaskSubmissions(ctx, current.FleetID, current.TaskID)
			if err != nil {
				t.Fatalf("ListFleetTaskSubmissions: %v", err)
			}
			if len(submissions) != 1 || submissions[0].SubmissionID != submission.SubmissionID {
				t.Fatalf("submissions = %+v, want one stored submission", submissions)
			}

			openBase := FleetTaskRecord{
				TaskID:      "task-open-submit",
				FleetID:     "fleet-claim",
				Title:       "Open submit",
				Mode:        FleetTaskModeOpenSubmission,
				Status:      FleetTaskStatusOpen,
				Creator:     entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("creator")},
				CreatedAtMS: 1,
				UpdatedAtMS: 1,
			}
			if _, err := tc.store.UpsertFleetTask(ctx, openBase); err != nil {
				t.Fatalf("UpsertFleetTask open submit: %v", err)
			}
			currentOpen, found, err := tc.store.GetFleetTask(ctx, openBase.FleetID, openBase.TaskID)
			if err != nil || !found {
				t.Fatalf("GetFleetTask open submit found/err = %v/%v", found, err)
			}
			openMutation := currentOpen
			openMutation.UpdatedAtMS = currentOpen.UpdatedAtMS + 1
			openSubmissionA := FleetTaskSubmissionRecord{
				SubmissionID: "submission-open-a",
				FleetID:      currentOpen.FleetID,
				TaskID:       currentOpen.TaskID,
				Author:       assigneeA,
				Content:      "first",
				Status:       FleetTaskSubmissionPending,
				CreatedAtMS:  currentOpen.UpdatedAtMS + 1,
				UpdatedAtMS:  currentOpen.UpdatedAtMS + 1,
			}
			openSubmissionB := openSubmissionA
			openSubmissionB.SubmissionID = "submission-open-b"
			openSubmissionB.Author = assigneeB
			openSubmissionB.Content = "second"
			if _, _, ok, err := tc.store.SubmitFleetTask(ctx, openMutation, currentOpen.UpdatedAtMS, openSubmissionA); err != nil || !ok {
				t.Fatalf("first open SubmitFleetTask ok/err = %v/%v", ok, err)
			}
			if _, _, ok, err := tc.store.SubmitFleetTask(ctx, openMutation, currentOpen.UpdatedAtMS, openSubmissionB); err != nil || !ok {
				t.Fatalf("second open SubmitFleetTask ok/err = %v/%v", ok, err)
			}
			openSubmissions, err := tc.store.ListFleetTaskSubmissions(ctx, currentOpen.FleetID, currentOpen.TaskID)
			if err != nil {
				t.Fatalf("ListFleetTaskSubmissions open: %v", err)
			}
			if len(openSubmissions) != 2 {
				t.Fatalf("open submissions = %+v, want two", openSubmissions)
			}
		})
	}
}

func TestFleetTaskCanMutateRequiresActiveMember(t *testing.T) {
	if !FleetTaskCanMutate(FleetMemberRecord{Role: FleetRoleAgent, Status: FleetMemberActive}) {
		t.Fatalf("active agent should mutate tasks")
	}
	if FleetTaskCanMutate(FleetMemberRecord{Role: FleetRoleCoordinator, Status: FleetMemberRemoved}) {
		t.Fatalf("removed coordinator-role member should not mutate tasks")
	}
	if FleetTaskCanMutate(FleetMemberRecord{Role: FleetRoleCoordinator, Status: FleetMemberInvited}) {
		t.Fatalf("invited coordinator-role member should not mutate tasks")
	}
}

func TestApplyFleetTaskMutationAdvancesUpdatedAt(t *testing.T) {
	task := FleetTaskRecord{
		TaskID:      "task-version",
		FleetID:     "fleet-version",
		Title:       "Versioned task",
		Mode:        FleetTaskModeOpenSubmission,
		Status:      FleetTaskStatusOpen,
		Creator:     entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("creator")},
		CreatedAtMS: 100,
		UpdatedAtMS: 200,
	}
	actor := FleetMemberRecord{
		FleetID:       task.FleetID,
		NodeID:        task.Creator.PilotNodeID,
		EntmootPubKey: base64.StdEncoding.EncodeToString(task.Creator.EntmootPubKey),
		Role:          FleetRoleCoordinator,
		Status:        FleetMemberActive,
	}
	mutation, err := ApplyFleetTaskMutation(task, FleetTaskActionCancel, actor, task.UpdatedAtMS, nil, nil)
	if err != nil {
		t.Fatalf("ApplyFleetTaskMutation: %v", err)
	}
	if mutation.ExpectedUpdatedAtMS != task.UpdatedAtMS {
		t.Fatalf("expected version = %d, want %d", mutation.ExpectedUpdatedAtMS, task.UpdatedAtMS)
	}
	if mutation.Task.UpdatedAtMS <= task.UpdatedAtMS {
		t.Fatalf("updated_at_ms = %d, want > %d", mutation.Task.UpdatedAtMS, task.UpdatedAtMS)
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
	mem := NewMemoryStateStore()
	stores := []openInviteStoreCase{{
		name:  "memory",
		store: mem,
		revoke: func(t *testing.T, tokenHash string) {
			t.Helper()
			mem.mu.Lock()
			defer mem.mu.Unlock()
			rec := mem.invites[tokenHash]
			rec.Revoked = true
			mem.invites[tokenHash] = rec
		},
	}}
	sqlite, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	stores = append(stores, openInviteStoreCase{
		name:  "sqlite",
		store: sqlite,
		close: func() { _ = sqlite.Close() },
		revoke: func(t *testing.T, tokenHash string) {
			t.Helper()
			if _, err := sqlite.db.Exec(`UPDATE esp_open_invites SET revoked = 1 WHERE token_hash = ?`, tokenHash); err != nil {
				t.Fatalf("revoke sqlite invite: %v", err)
			}
		},
	})
	return stores
}

func sameNodeIDs(a, b []entmoot.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testMobileGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}
