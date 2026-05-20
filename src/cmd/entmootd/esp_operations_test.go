package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/wire"
)

func TestESPOperationUpdateGroupRejectsNonObjectMetadata(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(1)
	exec := espOperationExecutor{metadataStore: esphttp.NewMemoryStateStore()}
	for _, raw := range []json.RawMessage{
		json.RawMessage(`[]`),
		json.RawMessage(`"name"`),
		json.RawMessage(`null`),
		json.RawMessage(`true`),
		json.RawMessage(`123`),
		json.RawMessage(`{`),
	} {
		_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
			Kind:    "group_update",
			GroupID: gid,
			Payload: raw,
		}, nil)
		var opErr *esphttp.OperationError
		if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
			t.Fatalf("ExecuteSignRequest(%s) err = %v, want 400 bad_request", raw, err)
		}
	}
}

func TestESPOperationCreateGroupRejectsNonObjectMetadataBeforeDaemonLookup(t *testing.T) {
	exec := espOperationExecutor{metadataStore: esphttp.NewMemoryStateStore()}
	_, err := exec.ExecuteSignRequest(context.Background(), esphttp.SignRequest{
		Kind:    "group_create",
		Payload: json.RawMessage(`{"metadata":[]}`),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("ExecuteSignRequest err = %v, want metadata 400 bad_request", err)
	}
}

func TestLocalGroupCatalogIgnoresBadStoredMetadata(t *testing.T) {
	dataDir := t.TempDir()
	gid := testESPGroupID(2)
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	rlog, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	info := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), id.PublicKey...)}
	if err := rlog.Genesis(id, info, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	catalog := localGroupCatalog{
		dataDir:  dataDir,
		metadata: rawGroupMetadataStore{raw: json.RawMessage(`[]`)},
	}
	group, ok, err := catalog.GetGroup(context.Background(), gid)
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	if !ok {
		t.Fatal("GetGroup ok = false, want true")
	}
	if group.Metadata != nil || group.Name != "" {
		t.Fatalf("group metadata = %+v name=%q, want ignored", group.Metadata, group.Name)
	}
}

func TestLocalGroupCatalogProjectsDisplayMetadataAndHostname(t *testing.T) {
	dataDir := t.TempDir()
	gid := testESPGroupID(3)
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	rlog, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	info := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), id.PublicKey...)}
	if err := rlog.Genesis(id, info, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	catalog := localGroupCatalog{
		dataDir:  dataDir,
		metadata: rawGroupMetadataStore{raw: json.RawMessage(`{"name":"Agents","description":"Ops room","tags":["infra","ios"]}`)},
		profiles: fakeMemberProfileReader{ads: map[entmoot.NodeID]wire.MemberProfileAd{
			45491: {
				GroupID:  gid,
				Author:   info,
				Seq:      1,
				Hostname: "mars.local",
				IssuedAt: 1_000,
				NotAfter: time.Now().Add(time.Hour).UnixMilli(),
			},
		}},
	}
	group, ok, err := catalog.GetGroup(context.Background(), gid)
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	if !ok {
		t.Fatal("GetGroup ok = false, want true")
	}
	if group.Name != "Agents" || group.Description != "Ops room" {
		t.Fatalf("group display fields = name=%q description=%q", group.Name, group.Description)
	}
	if len(group.Tags) != 2 || group.Tags[0] != "infra" || group.Tags[1] != "ios" {
		t.Fatalf("group.Tags = %#v, want infra/ios", group.Tags)
	}
	members, err := catalog.ListMembers(context.Background(), gid)
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	if len(members) != 1 || members[0].Hostname != "mars.local" {
		t.Fatalf("members = %+v, want hostname mars.local", members)
	}
}

func TestLocalGroupCatalogIgnoresStaleMemberProfileIdentity(t *testing.T) {
	dataDir := t.TempDir()
	gid := testESPGroupID(4)
	currentID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate current: %v", err)
	}
	oldID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate old: %v", err)
	}
	rlog, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	info := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), currentID.PublicKey...)}
	if err := rlog.Genesis(currentID, info, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	staleInfo := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), oldID.PublicKey...)}
	catalog := localGroupCatalog{
		dataDir: dataDir,
		profiles: fakeMemberProfileReader{ads: map[entmoot.NodeID]wire.MemberProfileAd{
			45491: {
				GroupID:  gid,
				Author:   staleInfo,
				Seq:      1,
				Hostname: "old-host.local",
				IssuedAt: 1_000,
				NotAfter: time.Now().Add(time.Hour).UnixMilli(),
			},
		}},
	}

	members, err := catalog.ListMembers(context.Background(), gid)
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("len(members) = %d, want 1", len(members))
	}
	if members[0].Hostname != "" {
		t.Fatalf("members[0].Hostname = %q, want empty for stale profile identity", members[0].Hostname)
	}
}

func TestLocalGroupCatalogListMembersIncludesLiveAgentState(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testESPGroupID(41)
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	rlog, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	info := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), id.PublicKey...)}
	if err := rlog.Genesis(id, info, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	state := esphttp.NewMemoryStateStore()
	now := time.Now().UnixMilli()
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:        gid,
		NodeID:         info.PilotNodeID,
		Enabled:        true,
		Mode:           esphttp.LiveModeOperator,
		TopicFilters:   []string{"chat"},
		AllowedActions: []string{"reply", "command.send"},
		UpdatedAtMS:    now,
	}); err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	if _, err := state.UpsertLiveAgentPresence(ctx, esphttp.LiveAgentPresence{
		GroupID:      gid,
		NodeID:       info.PilotNodeID,
		Status:       esphttp.LiveStatusOnline,
		Mode:         esphttp.LiveModeOperator,
		TopicFilters: []string{"chat"},
		LastSeenAtMS: now,
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
		UpdatedAtMS:  now,
	}); err != nil {
		t.Fatalf("UpsertLiveAgentPresence: %v", err)
	}
	catalog := localGroupCatalog{dataDir: dataDir, state: state}
	members, err := catalog.ListMembers(ctx, gid)
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	if len(members) != 1 || members[0].Live == nil {
		t.Fatalf("members = %+v, want live state", members)
	}
	if members[0].Live.Status != esphttp.LiveStatusOnline || members[0].Live.Mode != esphttp.LiveModeOperator {
		t.Fatalf("live state = %+v, want online operator", members[0].Live)
	}
}

func TestESPCreateGroupUsesDeterministicID(t *testing.T) {
	req := esphttp.SignRequest{ID: "req-1", SigningPayloadSHA256: "payload-digest"}
	a, err := groupIDForCreateRequest(req)
	if err != nil {
		t.Fatalf("groupIDForCreateRequest: %v", err)
	}
	b, err := groupIDForCreateRequest(req)
	if err != nil {
		t.Fatalf("groupIDForCreateRequest repeat: %v", err)
	}
	if a != b {
		t.Fatalf("deterministic group IDs differ: %s vs %s", a, b)
	}
	c, err := groupIDForCreateRequest(esphttp.SignRequest{ID: "req-2", SigningPayloadSHA256: "payload-digest"})
	if err != nil {
		t.Fatalf("groupIDForCreateRequest different: %v", err)
	}
	if c == a {
		t.Fatalf("different request IDs produced same group ID %s", a)
	}
}

func TestNormalizeGroupMetadataMergesDisplayFields(t *testing.T) {
	raw, err := normalizeGroupMetadata(groupCreatePayload{
		Name:        "Agents",
		Description: "Ops room",
		Tags:        []string{" infra ", "ios", "infra", ""},
		Metadata:    json.RawMessage(`{"color":"green","name":"old"}`),
	})
	if err != nil {
		t.Fatalf("normalizeGroupMetadata: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got["name"] != "Agents" || got["description"] != "Ops room" || got["color"] != "green" {
		t.Fatalf("metadata = %#v, want merged display fields", got)
	}
	tags, ok := got["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "infra" || tags[1] != "ios" {
		t.Fatalf("tags = %#v, want [infra ios]", got["tags"])
	}
}

func TestNormalizeGroupMetadataPreservesJSONNumbers(t *testing.T) {
	raw, err := normalizeGroupMetadata(groupCreatePayload{
		Name: "Agents",
		Metadata: json.RawMessage(`{
			"large": 9007199254740993,
			"precise": 1.234567890123456789,
			"nested": {"seq": 12345678901234567890}
		}`),
	})
	if err != nil {
		t.Fatalf("normalizeGroupMetadata: %v", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var got map[string]any
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got["name"] != "Agents" {
		t.Fatalf("name = %v, want Agents", got["name"])
	}
	if got["large"].(json.Number).String() != "9007199254740993" {
		t.Fatalf("large = %v, want exact integer literal", got["large"])
	}
	if got["precise"].(json.Number).String() != "1.234567890123456789" {
		t.Fatalf("precise = %v, want exact decimal literal", got["precise"])
	}
	nested := got["nested"].(map[string]any)
	if nested["seq"].(json.Number).String() != "12345678901234567890" {
		t.Fatalf("nested.seq = %v, want exact integer literal", nested["seq"])
	}
}

func TestESPCreateGroupRollsBackLocalStateOnJoinFailure(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, false)
	defer stop()
	metadata := esphttp.NewMemoryStateStore()
	reg, regPath := testDeviceRegistry(t)
	req := testGroupCreateRequest("req-rollback")
	req.DeviceID = "ios-1"
	exec := espOperationExecutor{
		dataDir:       dataDir,
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		metadataStore: metadata,
		deviceGroups:  &fileBackedDeviceGroupAuthorizer{path: regPath, registry: reg},
	}
	gid, err := groupIDForCreateRequest(req)
	if err != nil {
		t.Fatalf("groupIDForCreateRequest: %v", err)
	}
	_, err = exec.ExecuteSignRequest(ctx, req, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want join failure")
	}
	if ids, err := listGroupIDs(dataDir, nil); err != nil {
		t.Fatalf("listGroupIDs: %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("listGroupIDs = %v, want empty after rollback", ids)
	}
	if _, ok, err := metadata.GetGroupMetadata(ctx, gid); err != nil {
		t.Fatalf("GetGroupMetadata: %v", err)
	} else if ok {
		t.Fatal("metadata still present after rollback")
	}
	loaded, err := esphttp.LoadDeviceRegistry(regPath)
	if err != nil {
		t.Fatalf("LoadDeviceRegistry: %v", err)
	}
	devices := loaded.Snapshot()
	if len(devices) != 1 || len(devices[0].Groups) != 0 || len(devices[0].AdminGroups) != 0 {
		t.Fatalf("devices after rollback = %+v, want no regular or admin grants", devices)
	}
}

func TestESPCreateGroupGrantsCreatingDevice(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sock := testUnixSocketPath(t)
	joinReqCh := make(chan *ipc.JoinGroupReq, 1)
	stop := serveESPAcceptInviteIPC(t, sock, id.PublicKey, true, joinReqCh)
	defer stop()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := esphttp.NewDeviceRegistry([]esphttp.Device{{
		ID:        "ios-1",
		PublicKey: pub,
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	regPath := filepath.Join(t.TempDir(), "esp-devices.json")
	if err := esphttp.SaveDeviceRegistry(regPath, reg); err != nil {
		t.Fatalf("SaveDeviceRegistry: %v", err)
	}
	req := testGroupCreateRequest("req-device-grant")
	req.DeviceID = "ios-1"
	exec := espOperationExecutor{
		dataDir:       dataDir,
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		metadataStore: esphttp.NewMemoryStateStore(),
		deviceGroups:  &fileBackedDeviceGroupAuthorizer{path: regPath, registry: reg},
	}
	raw, err := exec.ExecuteSignRequest(ctx, req, nil)
	if err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	var result struct {
		GroupID entmoot.GroupID `json:"group_id"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	loaded, err := esphttp.LoadDeviceRegistry(regPath)
	if err != nil {
		t.Fatalf("LoadDeviceRegistry: %v", err)
	}
	devices := loaded.Snapshot()
	if len(devices) != 1 || devices[0].ID != "ios-1" {
		t.Fatalf("devices = %+v, want ios-1", devices)
	}
	device := devices[0]
	if len(device.Groups) != 1 || device.Groups[0] != result.GroupID {
		t.Fatalf("device groups = %v, want [%s]", device.Groups, result.GroupID)
	}
	if len(device.AdminGroups) != 1 || device.AdminGroups[0] != result.GroupID {
		t.Fatalf("device admin groups = %v, want [%s]", device.AdminGroups, result.GroupID)
	}
}

func TestESPAcceptInviteGrantsDeviceAccess(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sock := testUnixSocketPath(t)
	joinReqCh := make(chan *ipc.JoinGroupReq, 1)
	stop := serveESPAcceptInviteIPC(t, sock, id.PublicKey, true, joinReqCh)
	defer stop()
	reg, regPath := testDeviceRegistry(t)
	gid := testESPGroupID(9)
	req := esphttp.SignRequest{
		Kind:     "invite_accept",
		DeviceID: "ios-1",
		Payload:  mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}
	exec := espOperationExecutor{
		dataDir:      dataDir,
		identity:     id,
		socketPath:   sock,
		timeout:      time.Second,
		deviceGroups: &fileBackedDeviceGroupAuthorizer{path: regPath, registry: reg},
	}
	raw, err := exec.ExecuteSignRequest(ctx, req, nil)
	if err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	var result struct {
		GroupID entmoot.GroupID `json:"group_id"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result.GroupID != gid {
		t.Fatalf("result group_id = %s, want %s", result.GroupID, gid)
	}
	assertDeviceGroups(t, regPath, gid)
}

func TestESPAcceptInviteWithoutDeviceSkipsDeviceGrant(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sock := testUnixSocketPath(t)
	joinReqCh := make(chan *ipc.JoinGroupReq, 1)
	stop := serveESPAcceptInviteIPC(t, sock, id.PublicKey, true, joinReqCh)
	defer stop()
	gid := testESPGroupID(10)
	req := esphttp.SignRequest{
		Kind:    "invite_accept",
		Payload: mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}
	exec := espOperationExecutor{
		dataDir:    dataDir,
		identity:   id,
		socketPath: sock,
		timeout:    time.Second,
	}
	raw, err := exec.ExecuteSignRequest(ctx, req, nil)
	if err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	var result struct {
		GroupID entmoot.GroupID `json:"group_id"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result.GroupID != gid {
		t.Fatalf("result group_id = %s, want %s", result.GroupID, gid)
	}
}

func TestESPAcceptFleetInviteRequiresPendingFleetInvite(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	gid := testESPGroupID(91)
	state := esphttp.NewMemoryStateStore()
	metadataStore := esphttp.NewMemoryStateStore()
	createFleetForAcceptTest(t, state, gid)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	sock := testUnixSocketPath(t)
	joinReqCh := make(chan *ipc.JoinGroupReq, 1)
	stop := serveESPAcceptInviteIPC(t, sock, id.PublicKey, true, joinReqCh)
	defer stop()

	_, err = espOperationExecutor{
		dataDir:       t.TempDir(),
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		stateStore:    state,
		metadataStore: metadataStore,
	}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "invite_accept",
		Payload: mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusConflict || opErr.Code != "fleet_invite_required" {
		t.Fatalf("ExecuteSignRequest err = %v, want 409 fleet_invite_required", err)
	}
	members, err := state.ListFleetMembers(ctx, "fleet-accept")
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	member, ok := fleetMemberForNode(members, 45491)
	if !ok || member.Status != esphttp.FleetMemberInvited {
		t.Fatalf("member after rejected accept = %+v, ok=%v; want invited", member, ok)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-accept", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after rejected accept = %+v, want none", activity)
	}
	select {
	case req := <-joinReqCh:
		t.Fatalf("unexpected join request after rejected fleet accept: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestESPAcceptFleetInviteMarksPendingInviteActive(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	gid := testESPGroupID(92)
	state := esphttp.NewMemoryStateStore()
	metadataStore := esphttp.NewMemoryStateStore()
	createFleetForAcceptTest(t, state, gid)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "fleet-invite-laptop",
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_000,
		ExpiresAtMS:   time.Now().Add(time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, true)
	defer stop()
	reg, regPath := testDeviceRegistry(t)

	raw, err := espOperationExecutor{
		dataDir:       t.TempDir(),
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		stateStore:    state,
		metadataStore: metadataStore,
		deviceGroups:  &fileBackedDeviceGroupAuthorizer{path: regPath, registry: reg},
	}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "invite_accept",
		DeviceID: "ios-1",
		Payload:  mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}, nil)
	if err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	var result struct {
		FleetMember esphttp.FleetMemberRecord `json:"fleet_member"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result.FleetMember.Status != esphttp.FleetMemberActive || result.FleetMember.AcceptedAtMS == 0 {
		t.Fatalf("fleet member result = %+v, want active with accepted_at_ms", result.FleetMember)
	}
	invites, err := state.ListFleetInvites(ctx, "fleet-accept")
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after accept = %+v, want none", invites)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-accept", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "member.accepted" {
		t.Fatalf("activity after accept = %+v, want one member.accepted", activity)
	}
	loaded, err := esphttp.LoadDeviceRegistry(regPath)
	if err != nil {
		t.Fatalf("LoadDeviceRegistry: %v", err)
	}
	devices := loaded.Snapshot()
	if len(devices) != 1 || devices[0].PilotNodeID != 45491 || !bytes.Equal(devices[0].EntmootPubKey, id.PublicKey) {
		t.Fatalf("device identity after accept = %+v, want accepted fleet member identity", devices)
	}
	if len(devices[0].Groups) != 1 || devices[0].Groups[0] != gid {
		t.Fatalf("device groups after accept = %+v, want control group", devices[0].Groups)
	}
}

func TestESPAcceptFleetInviteUsesFleetRecordWhenMetadataIsStale(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	gid := testESPGroupID(96)
	state := esphttp.NewMemoryStateStore()
	metadataStore := esphttp.NewMemoryStateStore()
	if err := metadataStore.SetGroupMetadata(ctx, gid, json.RawMessage(`{"fleet_control":false}`)); err != nil {
		t.Fatalf("SetGroupMetadata: %v", err)
	}
	createFleetForAcceptTest(t, state, gid)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "fleet-invite-laptop",
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_000,
		ExpiresAtMS:   time.Now().Add(time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, true)
	defer stop()

	raw, err := espOperationExecutor{
		dataDir:       t.TempDir(),
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		stateStore:    state,
		metadataStore: metadataStore,
	}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "invite_accept",
		Payload: mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}, nil)
	if err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	var result struct {
		FleetMember esphttp.FleetMemberRecord `json:"fleet_member"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result.FleetMember.Status != esphttp.FleetMemberActive || result.FleetMember.AcceptedAtMS == 0 {
		t.Fatalf("fleet member result = %+v, want active with accepted_at_ms", result.FleetMember)
	}
	invites, err := state.ListFleetInvites(ctx, "fleet-accept")
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after accept = %+v, want none", invites)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-accept", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "member.accepted" {
		t.Fatalf("activity after accept = %+v, want one member.accepted", activity)
	}
}

func TestESPAcceptFleetInviteActivityFailureDoesNotJoin(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	gid := testESPGroupID(94)
	state := &failingFleetStateStore{
		MemoryStateStore:   esphttp.NewMemoryStateStore(),
		failAppendActivity: true,
	}
	createFleetForAcceptTest(t, state, gid)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "fleet-invite-laptop",
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_000,
		ExpiresAtMS:   time.Now().Add(time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	joinReqCh := make(chan *ipc.JoinGroupReq, 1)
	stop := serveESPAcceptInviteIPC(t, sock, id.PublicKey, true, joinReqCh)
	defer stop()

	_, err = espOperationExecutor{
		dataDir:    t.TempDir(),
		identity:   id,
		socketPath: sock,
		timeout:    time.Second,
		stateStore: state,
	}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "invite_accept",
		Payload: mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want activity failure")
	}
	select {
	case req := <-joinReqCh:
		t.Fatalf("unexpected join request after fleet activity failure: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	members, err := state.ListFleetMembers(ctx, "fleet-accept")
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	member, ok := fleetMemberForNode(members, 45491)
	if !ok || member.Status != esphttp.FleetMemberInvited {
		t.Fatalf("member after activity failure = %+v, ok=%v; want invited", member, ok)
	}
	invites, err := state.ListFleetInvites(ctx, "fleet-accept")
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].InviteID != "fleet-invite-laptop" {
		t.Fatalf("invites after activity failure = %+v, want restored pending invite", invites)
	}
}

func TestESPAcceptFleetInviteJoinFailureRollsBackPreparedStateAndDeviceGrant(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	gid := testESPGroupID(95)
	state := esphttp.NewMemoryStateStore()
	createFleetForAcceptTest(t, state, gid)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "fleet-invite-laptop",
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_000,
		ExpiresAtMS:   time.Now().Add(time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	reg, regPath := testDeviceRegistry(t)
	sock := testUnixSocketPath(t)
	joinReqCh := make(chan *ipc.JoinGroupReq, 1)
	stop := serveESPAcceptInviteIPC(t, sock, id.PublicKey, false, joinReqCh)
	defer stop()

	_, err = espOperationExecutor{
		dataDir:      t.TempDir(),
		identity:     id,
		socketPath:   sock,
		timeout:      time.Second,
		stateStore:   state,
		deviceGroups: &fileBackedDeviceGroupAuthorizer{path: regPath, registry: reg},
	}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "invite_accept",
		DeviceID: "ios-1",
		Payload:  mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want join failure")
	}
	select {
	case req := <-joinReqCh:
		if req.Invite.GroupID != gid {
			t.Fatalf("join group = %s, want %s", req.Invite.GroupID, gid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for join request")
	}
	members, err := state.ListFleetMembers(ctx, "fleet-accept")
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	member, ok := fleetMemberForNode(members, 45491)
	if !ok || member.Status != esphttp.FleetMemberInvited {
		t.Fatalf("member after join rollback = %+v, ok=%v; want invited", member, ok)
	}
	invites, err := state.ListFleetInvites(ctx, "fleet-accept")
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].InviteID != "fleet-invite-laptop" {
		t.Fatalf("invites after join rollback = %+v, want restored pending invite", invites)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-accept", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after join rollback = %+v, want none", activity)
	}
	loaded, err := esphttp.LoadDeviceRegistry(regPath)
	if err != nil {
		t.Fatalf("LoadDeviceRegistry: %v", err)
	}
	devices := loaded.Snapshot()
	if len(devices) != 1 || len(devices[0].Groups) != 0 {
		t.Fatalf("device groups after join rollback = %+v, want none", devices)
	}
}

func TestESPAcceptFleetInviteRejectsCoordinatorIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	storedID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate stored identity: %v", err)
	}
	gid := testESPGroupID(97)
	state := esphttp.NewMemoryStateStore()
	fleet, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:             "fleet-coordinator-mismatch",
		Name:                "Coordinator Mismatch",
		ControlGroupID:      gid,
		Coordinator:         entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), storedID.PublicKey...)},
		CoordinatorDeviceID: "ios-1",
		CreatedAtMS:         1_700_000_000_000,
	})
	if err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45491,
		EntmootPubKey: encodeBase64(storedID.PublicKey),
		Hostname:      "coordinator",
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember coordinator: %v", err)
	}
	sock := testUnixSocketPath(t)
	joinReqCh := make(chan *ipc.JoinGroupReq, 1)
	stop := serveESPAcceptInviteIPC(t, sock, id.PublicKey, true, joinReqCh)
	defer stop()

	_, err = espOperationExecutor{
		dataDir:    t.TempDir(),
		identity:   id,
		socketPath: sock,
		timeout:    time.Second,
		stateStore: state,
	}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "invite_accept",
		Payload: mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusConflict || opErr.Code != "member_identity_conflict" {
		t.Fatalf("ExecuteSignRequest err = %v, want 409 member_identity_conflict", err)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	member, ok := fleetMemberForNode(members, 45491)
	if !ok || member.Role != esphttp.FleetRoleCoordinator || member.Status != esphttp.FleetMemberActive || member.EntmootPubKey != encodeBase64(storedID.PublicKey) {
		t.Fatalf("member after rejected coordinator accept = %+v, ok=%v; want unchanged coordinator", member, ok)
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after rejected coordinator accept = %+v, want none", activity)
	}
	select {
	case req := <-joinReqCh:
		t.Fatalf("unexpected join request after coordinator identity conflict: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestESPAcceptFleetInviteRetryKeepsActiveMemberIdempotent(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	gid := testESPGroupID(93)
	state := esphttp.NewMemoryStateStore()
	createFleetForAcceptTest(t, state, gid)
	const acceptedAt = int64(1_700_000_000_000)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       "fleet-accept",
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  acceptedAt,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, true)
	defer stop()

	raw, err := espOperationExecutor{
		dataDir:    t.TempDir(),
		identity:   id,
		socketPath: sock,
		timeout:    time.Second,
		stateStore: state,
	}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "invite_accept",
		Payload: mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
	}, nil)
	if err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	var result struct {
		FleetMember esphttp.FleetMemberRecord `json:"fleet_member"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result.FleetMember.AcceptedAtMS != acceptedAt {
		t.Fatalf("accepted_at_ms = %d, want preserved %d", result.FleetMember.AcceptedAtMS, acceptedAt)
	}
	activity, err := state.ListFleetActivity(ctx, "fleet-accept", 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after retry = %+v, want none", activity)
	}
}

func TestESPAcceptInviteWithDeviceRequiresAuthorizer(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, true)
	defer stop()
	_, err = espOperationExecutor{
		dataDir:    t.TempDir(),
		identity:   id,
		socketPath: sock,
		timeout:    time.Second,
	}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "invite_accept",
		DeviceID: "ios-1",
		Payload:  mustMarshalJSON(t, entmoot.Invite{GroupID: testESPGroupID(11)}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusServiceUnavailable || opErr.Code != "device_registry_unavailable" {
		t.Fatalf("ExecuteSignRequest err = %v, want 503 device_registry_unavailable", err)
	}
}

func TestESPBuildInviteCreateRequestNormalizesPayload(t *testing.T) {
	gid := testESPGroupID(12)
	targetID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target: %v", err)
	}
	req, err := buildInviteCreateIPCRequest(gid, inviteCreatePayload{
		ValidFor: "2d",
		Peers:    "45491, 45460",
		Target: &inviteTargetPayload{
			PilotNodeID:   45981,
			PilotPubKey:   bytes.Repeat([]byte{0x42}, 32),
			EntmootPubKey: append([]byte(nil), targetID.PublicKey...),
		},
	})
	if err != nil {
		t.Fatalf("buildInviteCreateIPCRequest: %v", err)
	}
	if req.GroupID != gid || req.Target.PilotNodeID != 45981 || !bytes.Equal(req.Target.EntmootPubKey, targetID.PublicKey) {
		t.Fatalf("request target/group = %+v", req)
	}
	if req.ValidForMS != int64((48*time.Hour)/time.Millisecond) {
		t.Fatalf("valid_for_ms = %d, want 48h", req.ValidForMS)
	}
	if len(req.BootstrapPeers) != 2 || req.BootstrapPeers[0] != 45491 || req.BootstrapPeers[1] != 45460 {
		t.Fatalf("bootstrap peers = %v, want [45491 45460]", req.BootstrapPeers)
	}
}

func TestESPInviteResolvesKnownMootMemberTarget(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	sourceGID := testESPGroupID(31)
	targetGID := testESPGroupID(32)
	memberID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate member: %v", err)
	}
	member := entmoot.NodeInfo{PilotNodeID: 45460, EntmootPubKey: append([]byte(nil), memberID.PublicKey...)}
	rlog, err := roster.OpenJSONL(dataDir, sourceGID)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	if err := rlog.Genesis(memberID, member, 1_700_000_000_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	if err := rlog.Close(); err != nil {
		t.Fatalf("Close roster: %v", err)
	}
	_, devicePriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey device: %v", err)
	}
	reg, err := esphttp.NewDeviceRegistry([]esphttp.Device{{
		ID:        "ios-1",
		PublicKey: devicePriv.Public().(ed25519.PublicKey),
		Groups:    []entmoot.GroupID{sourceGID},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	targetPilotPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	sock := testUnixSocketPath(t)
	reqCh := make(chan *ipc.InviteCreateReq, 1)
	stop := serveESPInviteCreateIPC(t, sock, targetGID, reqCh)
	defer stop()
	exec := espOperationExecutor{
		dataDir:      dataDir,
		socketPath:   sock,
		timeout:      time.Second,
		deviceGroups: &fileBackedDeviceGroupAuthorizer{registry: reg},
		pilotLookup: func(_ context.Context, nodeID entmoot.NodeID) (string, error) {
			if nodeID != member.PilotNodeID {
				t.Fatalf("pilot lookup node = %d, want %d", nodeID, member.PilotNodeID)
			}
			return base64.StdEncoding.EncodeToString(targetPilotPub), nil
		},
	}
	if _, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "invite_create",
		GroupID:  targetGID,
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, inviteCreatePayload{
			SourceGroupID: sourceGID,
			PilotNodeID:   member.PilotNodeID,
			ValidFor:      "24h",
		}),
	}, nil); err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	select {
	case req := <-reqCh:
		if req.GroupID != targetGID || req.Target.PilotNodeID != member.PilotNodeID || !bytes.Equal(req.Target.EntmootPubKey, member.EntmootPubKey) || !bytes.Equal(req.TargetPilotPubKey, targetPilotPub) {
			t.Fatalf("invite request = %+v pilot_pub=%x, want target group and resolved member identity", req, req.TargetPilotPubKey)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
}

func TestESPBuildInviteCreateRequestRejectsSubMillisecondValidFor(t *testing.T) {
	gid := testESPGroupID(18)
	targetID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target: %v", err)
	}
	for _, validFor := range []string{"1ns", "500us"} {
		_, err := buildInviteCreateIPCRequest(gid, inviteCreatePayload{
			ValidFor: validFor,
			Target: &inviteTargetPayload{
				PilotNodeID:   45981,
				PilotPubKey:   bytes.Repeat([]byte{0x42}, 32),
				EntmootPubKey: append([]byte(nil), targetID.PublicKey...),
			},
		})
		var opErr *esphttp.OperationError
		if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
			t.Fatalf("valid_for %q err = %v, want 400 bad_request", validFor, err)
		}
	}

	req, err := buildInviteCreateIPCRequest(gid, inviteCreatePayload{
		ValidFor: "1ms",
		Target: &inviteTargetPayload{
			PilotNodeID:   45981,
			PilotPubKey:   bytes.Repeat([]byte{0x42}, 32),
			EntmootPubKey: append([]byte(nil), targetID.PublicKey...),
		},
	})
	if err != nil {
		t.Fatalf("valid_for 1ms err = %v, want nil", err)
	}
	if req.ValidForMS != 1 {
		t.Fatalf("valid_for_ms = %d, want 1", req.ValidForMS)
	}
}

func TestESPBuildInviteRequiresTarget(t *testing.T) {
	_, err := buildInviteCreateIPCRequest(testESPGroupID(13), inviteCreatePayload{})
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "target_required" {
		t.Fatalf("buildInviteCreateIPCRequest err = %v, want 400 target_required", err)
	}
}

func TestESPBuildInviteInvalidValidForDoesNotMutateRoster(t *testing.T) {
	dataDir := t.TempDir()
	gid := testESPGroupID(16)
	founderID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate founder: %v", err)
	}
	targetID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target: %v", err)
	}
	founder := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), founderID.PublicKey...)}
	createTestRoster(t, dataDir, gid, founderID, founder)

	_, err = buildInviteCreateIPCRequest(gid, inviteCreatePayload{
		ValidFor: "not-a-duration",
		Target: &inviteTargetPayload{
			PilotNodeID:   45981,
			PilotPubKey:   bytes.Repeat([]byte{0x42}, 32),
			EntmootPubKey: append([]byte(nil), targetID.PublicKey...),
		},
	})
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("buildInviteCreateIPCRequest err = %v, want 400 bad_request", err)
	}
	assertRosterMemberAbsent(t, dataDir, gid, 45981)
}

func TestESPBuildInviteMalformedPeersDoesNotMutateRoster(t *testing.T) {
	dataDir := t.TempDir()
	gid := testESPGroupID(17)
	founderID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate founder: %v", err)
	}
	targetID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target: %v", err)
	}
	founder := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), founderID.PublicKey...)}
	createTestRoster(t, dataDir, gid, founderID, founder)

	_, err = buildInviteCreateIPCRequest(gid, inviteCreatePayload{
		Peers: "not-a-node-id",
		Target: &inviteTargetPayload{
			PilotNodeID:   45981,
			PilotPubKey:   bytes.Repeat([]byte{0x42}, 32),
			EntmootPubKey: append([]byte(nil), targetID.PublicKey...),
		},
	})
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("buildInviteCreateIPCRequest err = %v, want 400 bad_request", err)
	}
	assertRosterMemberAbsent(t, dataDir, gid, 45981)
}

func TestESPOpenInviteCreateRequiresLiveInviteAuthority(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(22)
	exec := espOperationExecutor{
		stateStore: esphttp.NewMemoryStateStore(),
		socketPath: filepath.Join(t.TempDir(), "missing.sock"),
		timeout:    time.Millisecond,
	}
	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_create",
		GroupID: gid,
		Payload: json.RawMessage(`{"max_uses":1,"valid_for":"24h"}`),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusServiceUnavailable || opErr.Code != "join_unavailable" {
		t.Fatalf("open_invite_create err = %v, want 503 join_unavailable", err)
	}
}

func TestESPFleetControlGroupRollsBackOnJoinFailure(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, false)
	defer stop()
	metadata := &recordingGroupMetadataStore{}
	exec := espOperationExecutor{
		dataDir:       dataDir,
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		metadataStore: metadata,
	}
	founder := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), id.PublicKey...)}

	if _, err := exec.createFleetControlGroup(ctx, testESPGroupID(24), "fleet-a", "Ops Fleet", founder, 1_700_000_000_000); err == nil {
		t.Fatal("createFleetControlGroup succeeded, want join failure")
	}
	if ids, err := listGroupIDs(dataDir, nil); err != nil {
		t.Fatalf("listGroupIDs: %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("listGroupIDs = %v, want empty after rollback", ids)
	}
	if metadata.setGroup == (entmoot.GroupID{}) {
		t.Fatal("metadata was not written before failed join")
	}
	if metadata.deletedGroup != metadata.setGroup {
		t.Fatalf("deleted metadata group = %s, want %s", metadata.deletedGroup, metadata.setGroup)
	}
}

func TestESPFleetCreateRollsBackLocalStateOnControlGroupFailure(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, false)
	defer stop()
	metadata := &recordingGroupMetadataStore{}
	state := esphttp.NewMemoryStateStore()
	exec := espOperationExecutor{
		dataDir:       dataDir,
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		metadataStore: metadata,
		stateStore:    state,
	}

	_, err = exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_create",
		DeviceID: "ios-1",
		Payload:  mustMarshalJSON(t, fleetCreatePayload{Name: "Ops Fleet"}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want join failure")
	}
	fleets, err := state.ListFleets(ctx)
	if err != nil {
		t.Fatalf("ListFleets: %v", err)
	}
	if len(fleets) != 0 {
		t.Fatalf("fleets after rollback = %+v, want empty", fleets)
	}
	if ids, err := listGroupIDs(dataDir, nil); err != nil {
		t.Fatalf("listGroupIDs: %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("listGroupIDs = %v, want empty after rollback", ids)
	}
	if metadata.setGroup == (entmoot.GroupID{}) {
		t.Fatal("metadata was not written before failed join")
	}
	if metadata.deletedGroup != metadata.setGroup {
		t.Fatalf("deleted metadata group = %s, want %s", metadata.deletedGroup, metadata.setGroup)
	}
}

func TestESPFleetInviteRejectsCoordinatorTarget(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	coordID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate coordinator: %v", err)
	}
	coordinator := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: append([]byte(nil), coordID.PublicKey...)}
	fleet, err := state.CreateFleet(ctx, esphttp.FleetRecord{
		FleetID:             "fleet-a",
		Name:                "Ops Fleet",
		ControlGroupID:      testESPGroupID(23),
		Coordinator:         coordinator,
		CoordinatorDeviceID: "ios-1",
		CreatedAtMS:         1_700_000_000_000,
	})
	if err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        coordinator.PilotNodeID,
		EntmootPubKey: encodeBase64(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	exec := espOperationExecutor{stateStore: state}

	_, err = exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   coordinator.PilotNodeID,
				PilotPubKey:   bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize),
				EntmootPubKey: append([]byte(nil), coordinator.EntmootPubKey...),
			},
		}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("ExecuteSignRequest err = %v, want 400 bad_request", err)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after rejected self-invite = %+v, want active coordinator", members)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after rejected self-invite = %+v, want none", invites)
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after rejected self-invite = %+v, want none", activity)
	}
}

func TestESPFleetArchiveMarksFleetAndClearsInvites(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-a",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64([]byte("agent")),
		Status:        esphttp.FleetMemberInvited,
		CreatedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("CreateFleetInvite: %v", err)
	}
	exec := espOperationExecutor{stateStore: state}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_archive",
		DeviceID: "ios-1",
		Payload:  mustMarshalJSON(t, fleetScopedPayload{FleetID: fleet.FleetID}),
	}, nil)
	if err != nil {
		t.Fatalf("ExecuteSignRequest archive: %v", err)
	}
	var out struct {
		Status string              `json:"status"`
		Fleet  esphttp.FleetRecord `json:"fleet"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal archive result: %v", err)
	}
	if out.Status != "archived" || out.Fleet.Status != esphttp.FleetStatusArchived {
		t.Fatalf("archive result = %s/%+v", out.Status, out.Fleet)
	}
	if out.Fleet.Coordinator.PilotNodeID != coordinator.PilotNodeID {
		t.Fatalf("archive coordinator = %+v, want %+v", out.Fleet.Coordinator, coordinator)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after archive = %+v, want none", invites)
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "fleet.archived" {
		t.Fatalf("activity after archive = %+v", activity)
	}
}

func TestESPFleetInviteRejectsArchivedFleet(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, _ := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	if _, ok, err := state.ArchiveFleet(ctx, fleet.FleetID, 1_700_000_000_000); err != nil || !ok {
		t.Fatalf("ArchiveFleet ok/err = %v/%v", ok, err)
	}
	exec := espOperationExecutor{stateStore: state}
	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				EntmootPubKey: []byte("agent"),
			},
		}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.Code != "fleet_archived" {
		t.Fatalf("archive invite err = %v, want fleet_archived", err)
	}
}

func TestESPFleetInviteAcceptanceRejectsArchivedFleetAtMutation(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, _ := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	target := entmoot.NodeInfo{PilotNodeID: 45460, EntmootPubKey: targetPub}
	targetPubKey := encodeBase64(targetPub)
	previous := esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        target.PilotNodeID,
		EntmootPubKey: targetPubKey,
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}
	if _, err := state.UpsertFleetMember(ctx, previous); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	invite := esphttp.FleetInviteRecord{
		InviteID:      "invite-a",
		FleetID:       fleet.FleetID,
		NodeID:        target.PilotNodeID,
		EntmootPubKey: targetPubKey,
		Status:        esphttp.FleetMemberInvited,
		CreatedAtMS:   previous.InvitedAtMS,
	}
	if _, err := state.CreateFleetInvite(ctx, invite); err != nil {
		t.Fatalf("CreateFleetInvite: %v", err)
	}
	member := previous
	member.Status = esphttp.FleetMemberActive
	member.AcceptedAtMS = 1_700_000_010_000
	plan := &fleetInviteAcceptancePlan{
		fleet:         fleet,
		target:        target,
		member:        member,
		previous:      previous,
		previousFound: true,
		invites:       []esphttp.FleetInviteRecord{invite},
	}
	if _, ok, err := state.ArchiveFleet(ctx, fleet.FleetID, 1_700_000_005_000); err != nil || !ok {
		t.Fatalf("ArchiveFleet ok/err = %v/%v", ok, err)
	}
	exec := espOperationExecutor{stateStore: state}
	_, _, rollback, err := exec.prepareFleetInviteAcceptance(ctx, plan)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.Code != "fleet_archived" {
		t.Fatalf("prepare acceptance err = %v, want fleet_archived", err)
	}
	if rollback != nil {
		t.Fatalf("prepare acceptance rollback = %p, want nil on rejected archived fleet", rollback)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	got, ok := fleetMemberForNode(members, target.PilotNodeID)
	if !ok || got.Status != esphttp.FleetMemberInvited {
		t.Fatalf("member after archived acceptance = %+v/%v, want invited previous member", got, ok)
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after archived acceptance = %+v, want none", activity)
	}
}

func TestESPFleetArchiveWaitsForInviteAcceptance(t *testing.T) {
	ctx := context.Background()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	gid := testESPGroupID(96)
	state := esphttp.NewMemoryStateStore()
	fleet := createFleetForAcceptTest(t, state, gid)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "fleet-invite-laptop",
		FleetID:       fleet.FleetID,
		NodeID:        45491,
		EntmootPubKey: encodeBase64(id.PublicKey),
		Hostname:      "laptop",
		Status:        esphttp.FleetMemberInvited,
		CreatedAtMS:   1_700_000_000_000,
		ExpiresAtMS:   time.Now().Add(time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	joinReqCh := make(chan *ipc.JoinGroupReq, 1)
	releaseJoin := make(chan struct{})
	var releaseJoinOnce sync.Once
	releaseJoinNow := func() {
		releaseJoinOnce.Do(func() { close(releaseJoin) })
	}
	defer releaseJoinNow()
	stop := serveESPAcceptInviteIPCWithJoinGate(t, sock, id.PublicKey, joinReqCh, releaseJoin)
	defer stop()
	exec := espOperationExecutor{
		dataDir:    t.TempDir(),
		identity:   id,
		socketPath: sock,
		timeout:    time.Second,
		stateStore: state,
	}
	acceptDone := make(chan error, 1)
	go func() {
		_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
			Kind:    "invite_accept",
			Payload: mustMarshalJSON(t, entmoot.Invite{GroupID: gid}),
		}, nil)
		acceptDone <- err
	}()
	select {
	case req := <-joinReqCh:
		if req.Invite.GroupID != gid {
			t.Fatalf("join group = %s, want %s", req.Invite.GroupID, gid)
		}
	case err := <-acceptDone:
		t.Fatalf("accept finished before join gate: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for join request")
	}
	archiveDone := make(chan error, 1)
	go func() {
		_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
			Kind:     "fleet_archive",
			DeviceID: "ios-1",
			Payload:  mustMarshalJSON(t, fleetScopedPayload{FleetID: fleet.FleetID}),
		}, nil)
		archiveDone <- err
	}()
	select {
	case err := <-archiveDone:
		t.Fatalf("archive finished while accept held fleet lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseJoinNow()
	if err := <-acceptDone; err != nil {
		t.Fatalf("accept after join release: %v", err)
	}
	if err := <-archiveDone; err != nil {
		t.Fatalf("archive after accept: %v", err)
	}
	gotFleet, ok, err := state.GetFleet(ctx, fleet.FleetID)
	if err != nil || !ok {
		t.Fatalf("GetFleet ok/err = %v/%v", ok, err)
	}
	if gotFleet.Status != esphttp.FleetStatusArchived {
		t.Fatalf("fleet status = %q, want archived", gotFleet.Status)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	member, ok := fleetMemberForNode(members, 45491)
	if !ok || member.Status != esphttp.FleetMemberActive {
		t.Fatalf("member after serialized accept/archive = %+v/%v, want active accepted member", member, ok)
	}
}

func TestESPFleetInviteRejectsExistingCurrentMember(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	targetPilotPub := bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteAndRemoveIPC(t, sock, fleet.ControlGroupID, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				PilotPubKey:   append([]byte(nil), targetPilotPub...),
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
			Hostname: "phobos",
		}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusConflict || opErr.Code != "fleet_member_exists" {
		t.Fatalf("ExecuteSignRequest err = %v, want 409 fleet_member_exists", err)
	}
	select {
	case req := <-inviteReqCh:
		t.Fatalf("unexpected invite create IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case req := <-removeReqCh:
		t.Fatalf("unexpected member remove IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("members after rejected re-invite = %+v, want coordinator plus target", members)
	}
	for _, member := range members {
		if member.NodeID == 45460 && member.Status != esphttp.FleetMemberActive {
			t.Fatalf("target member after rejected re-invite = %+v, want active", member)
		}
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after rejected re-invite = %+v, want none", invites)
	}
	_ = coordinator
}

func TestESPFleetInviteReissuesPendingMemberInvite(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	targetPilotPub := bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "old-invite",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"old"}`),
		CreatedAtMS:   1_700_000_000_500,
		ExpiresAtMS:   1_700_000_001_000,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteAndRemoveIPC(t, sock, fleet.ControlGroupID, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	if _, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				PilotPubKey:   append([]byte(nil), targetPilotPub...),
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
			Hostname: "phobos",
		}),
	}, nil); err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	select {
	case req := <-inviteReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("invite group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	select {
	case req := <-removeReqCh:
		t.Fatalf("unexpected rollback member remove IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].InviteID == "old-invite" {
		t.Fatalf("invites after reissue = %+v, want one fresh invite", invites)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	for _, member := range members {
		if member.NodeID == 45460 && member.Status != esphttp.FleetMemberInvited {
			t.Fatalf("target member after reissue = %+v, want invited", member)
		}
	}
	_ = coordinator
}

func TestESPFleetInviteResolvesKnownMootMemberTarget(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	sourceGID := testESPGroupID(31)
	memberID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate member: %v", err)
	}
	member := entmoot.NodeInfo{PilotNodeID: 45460, EntmootPubKey: append([]byte(nil), memberID.PublicKey...)}
	rlog, err := roster.OpenJSONL(dataDir, sourceGID)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	if err := rlog.Genesis(memberID, member, 1_700_000_000_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	if err := rlog.Close(); err != nil {
		t.Fatalf("Close roster: %v", err)
	}
	state := esphttp.NewMemoryStateStore()
	fleet, _ := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	_, devicePriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey device: %v", err)
	}
	reg, err := esphttp.NewDeviceRegistry([]esphttp.Device{{
		ID:        "ios-1",
		PublicKey: devicePriv.Public().(ed25519.PublicKey),
		Groups:    []entmoot.GroupID{sourceGID},
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	targetPilotPub := bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteAndRemoveIPC(t, sock, fleet.ControlGroupID, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{
		dataDir:      dataDir,
		stateStore:   state,
		socketPath:   sock,
		timeout:      time.Second,
		deviceGroups: &fileBackedDeviceGroupAuthorizer{registry: reg},
		pilotLookup: func(_ context.Context, nodeID entmoot.NodeID) (string, error) {
			if nodeID != member.PilotNodeID {
				t.Fatalf("pilot lookup node = %d, want %d", nodeID, member.PilotNodeID)
			}
			return base64.StdEncoding.EncodeToString(targetPilotPub), nil
		},
	}
	if _, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID:       fleet.FleetID,
			SourceGroupID: sourceGID,
			PilotNodeID:   member.PilotNodeID,
			Hostname:      "phobos",
		}),
	}, nil); err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	select {
	case req := <-inviteReqCh:
		if req.Target.PilotNodeID != member.PilotNodeID || !bytes.Equal(req.Target.EntmootPubKey, member.EntmootPubKey) || !bytes.Equal(req.TargetPilotPubKey, targetPilotPub) {
			t.Fatalf("invite target = %+v pilot_pub=%x, want member and looked-up Pilot pubkey", req.Target, req.TargetPilotPubKey)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].NodeID != member.PilotNodeID || invites[0].EntmootPubKey != encodeBase64(member.EntmootPubKey) {
		t.Fatalf("fleet invites = %+v, want resolved member identity", invites)
	}
}

func TestESPFleetInviteRollsBackLocalStateOnActivityFailure(t *testing.T) {
	ctx := context.Background()
	state := &failingFleetStateStore{
		MemoryStateStore:   esphttp.NewMemoryStateStore(),
		failAppendActivity: true,
	}
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	targetPilotPub := bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteAndRemoveIPC(t, sock, fleet.ControlGroupID, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				PilotPubKey:   append([]byte(nil), targetPilotPub...),
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
			Hostname: "phobos",
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want activity failure")
	}
	select {
	case req := <-inviteReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("invite create group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	select {
	case req := <-removeReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("rollback remove group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rollback member remove IPC request")
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after invite rollback = %+v, want active coordinator only", members)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after invite rollback = %+v, want none", invites)
	}
	_ = coordinator
}

func TestESPFleetInviteRollbackTreatsControlNotMemberAsRevoked(t *testing.T) {
	ctx := context.Background()
	state := &failingFleetStateStore{
		MemoryStateStore:   esphttp.NewMemoryStateStore(),
		failAppendActivity: true,
	}
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	targetPilotPub := bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteRemoveNotMemberIPC(t, sock, fleet.ControlGroupID, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				PilotPubKey:   append([]byte(nil), targetPilotPub...),
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
			Hostname: "phobos",
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want activity failure")
	}
	select {
	case req := <-inviteReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("invite create group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	select {
	case req := <-removeReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("rollback remove group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rollback member remove IPC request")
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after invite rollback = %+v, want active coordinator only", members)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after invite rollback = %+v, want none", invites)
	}
	_ = coordinator
}

func TestESPFleetInviteRevokesControlGroupAfterUncertainIPC(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	targetPilotPub := bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteUncertainIPC(t, sock, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				PilotPubKey:   append([]byte(nil), targetPilotPub...),
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
			Hostname: "phobos",
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want uncertain IPC failure")
	}
	select {
	case req := <-inviteReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("invite create group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	select {
	case req := <-removeReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("rollback remove group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rollback member remove IPC request")
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after uncertain invite rollback = %+v, want active coordinator only", members)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after uncertain invite rollback = %+v, want none", invites)
	}
	_ = coordinator
}

func TestESPFleetInviteReissueRestoresStaleInviteOnFailure(t *testing.T) {
	ctx := context.Background()
	state := &failingFleetStateStore{
		MemoryStateStore:   esphttp.NewMemoryStateStore(),
		failAppendActivity: true,
	}
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	targetPilotPub := bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "old-invite",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"old"}`),
		CreatedAtMS:   1_700_000_000_500,
		ExpiresAtMS:   1_700_000_001_000,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	inviteReqCh := make(chan *ipc.InviteCreateReq, 1)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetInviteAndRemoveIPC(t, sock, fleet.ControlGroupID, inviteReqCh, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_invite_create",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetInviteCreatePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				PilotPubKey:   append([]byte(nil), targetPilotPub...),
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
			Hostname: "phobos",
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want activity failure")
	}
	select {
	case <-inviteReqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	select {
	case <-removeReqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rollback member remove IPC request")
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].InviteID != "old-invite" {
		t.Fatalf("invites after reissue rollback = %+v, want restored old invite", invites)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	for _, member := range members {
		if member.NodeID == 45460 && (member.Status != esphttp.FleetMemberInvited || member.InvitedAtMS != 1_700_000_000_000) {
			t.Fatalf("target member after reissue rollback = %+v, want previous invited member", member)
		}
	}
	_ = coordinator
}

func TestESPFleetRemoveRollsBackLocalStateOnActivityFailure(t *testing.T) {
	ctx := context.Background()
	state := &failingFleetStateStore{
		MemoryStateStore:   esphttp.NewMemoryStateStore(),
		failAppendActivity: true,
	}
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-phobos",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_500,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveIPC(t, sock, testESPGroupID(26), removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want activity failure")
	}
	select {
	case req := <-removeReqCh:
		t.Fatalf("unexpected member remove IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("members after remove rollback = %+v, want coordinator plus target", members)
	}
	for _, member := range members {
		if member.NodeID == 45460 && member.Status != esphttp.FleetMemberActive {
			t.Fatalf("target member after rollback = %+v, want active", member)
		}
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after remove rollback = %+v, want none", activity)
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].InviteID != "invite-phobos" {
		t.Fatalf("invites after remove rollback = %+v, want restored invite", invites)
	}
	_ = coordinator
}

func TestESPFleetRemoveKeepsLocalRemovalAfterUncertainIPC(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-phobos",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_500,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveUncertainIPC(t, sock, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
		}),
	}, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want uncertain IPC failure")
	}
	select {
	case req := <-removeReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("remove group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for member remove IPC request")
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	for _, member := range members {
		if member.NodeID == 45460 && member.Status != esphttp.FleetMemberRemoved {
			t.Fatalf("target member after uncertain remove = %+v, want removed", member)
		}
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after uncertain remove = %+v, want none", invites)
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 1 || activity[0].Type != "member.removed" {
		t.Fatalf("activity after uncertain remove = %+v, want one removal event", activity)
	}
	_ = coordinator
}

func TestESPFleetRemoveRejectsWrongStoredMemberKey(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	wrongPub := bytes.Repeat([]byte{0x25}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-phobos",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_500,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveIPC(t, sock, fleet.ControlGroupID, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				EntmootPubKey: append([]byte(nil), wrongPub...),
			},
		}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusConflict || opErr.Code != "member_identity_conflict" {
		t.Fatalf("ExecuteSignRequest err = %v, want 409 member_identity_conflict", err)
	}
	select {
	case req := <-removeReqCh:
		t.Fatalf("unexpected member remove IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	for _, member := range members {
		if member.NodeID == 45460 && (member.Status != esphttp.FleetMemberActive || member.EntmootPubKey != encodeBase64(targetPub)) {
			t.Fatalf("target member after rejected key mismatch = %+v, want active stored key", member)
		}
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 1 || invites[0].InviteID != "invite-phobos" {
		t.Fatalf("invites after rejected key mismatch = %+v, want preserved invite", invites)
	}
	_ = coordinator
}

func TestESPFleetRemoveRejectsCoordinator(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveIPC(t, sock, fleet.ControlGroupID, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   coordinator.PilotNodeID,
				EntmootPubKey: append([]byte(nil), coordinator.EntmootPubKey...),
			},
		}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("ExecuteSignRequest err = %v, want 400 bad_request", err)
	}
	select {
	case req := <-removeReqCh:
		t.Fatalf("unexpected member remove IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after rejected coordinator remove = %+v, want active coordinator", members)
	}
	_ = coordinator
}

func TestESPFleetRemoveDeletesPendingInvite(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-phobos",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_500,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveIPC(t, sock, fleet.ControlGroupID, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	if _, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
		}),
	}, nil); err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	select {
	case req := <-removeReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("remove group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for member remove IPC request")
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after remove = %+v, want none", invites)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	for _, member := range members {
		if member.NodeID == 45460 && member.Status != esphttp.FleetMemberRemoved {
			t.Fatalf("target member after remove = %+v, want removed", member)
		}
	}
	_ = coordinator
}

func TestESPFleetRemoveCancelsPendingInviteWhenControlNotMember(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberInvited,
		InvitedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-phobos",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_500,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveNotMemberIPC(t, sock, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	if _, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
		}),
	}, nil); err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	select {
	case req := <-removeReqCh:
		if req.GroupID != fleet.ControlGroupID {
			t.Fatalf("remove group = %s, want %s", req.GroupID, fleet.ControlGroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for member remove IPC request")
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after pending cancellation = %+v, want none", invites)
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	for _, member := range members {
		if member.NodeID == 45460 && member.Status != esphttp.FleetMemberRemoved {
			t.Fatalf("target member after pending cancellation = %+v, want removed", member)
		}
	}
	_ = coordinator
}

func TestESPFleetRemoveAlreadyRemovedDeletesStaleInvite(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Role:          esphttp.FleetRoleAgent,
		Status:        esphttp.FleetMemberRemoved,
		RemovedAtMS:   1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember target: %v", err)
	}
	if _, err := state.CreateFleetInvite(ctx, esphttp.FleetInviteRecord{
		InviteID:      "invite-phobos",
		FleetID:       fleet.FleetID,
		NodeID:        45460,
		EntmootPubKey: encodeBase64(targetPub),
		Hostname:      "phobos",
		Status:        esphttp.FleetMemberInvited,
		Invite:        json.RawMessage(`{"group_id":"control"}`),
		CreatedAtMS:   1_700_000_000_500,
	}); err != nil {
		t.Fatalf("CreateFleetInvite target: %v", err)
	}
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveIPC(t, sock, fleet.ControlGroupID, removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	if _, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
		}),
	}, nil); err != nil {
		t.Fatalf("ExecuteSignRequest: %v", err)
	}
	select {
	case req := <-removeReqCh:
		t.Fatalf("unexpected member remove IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	invites, err := state.ListFleetInvites(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetInvites: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("invites after already-removed cleanup = %+v, want none", invites)
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after already-removed cleanup = %+v, want none", activity)
	}
	_ = coordinator
}

func TestESPFleetRemoveRejectsUnknownMember(t *testing.T) {
	ctx := context.Background()
	state := esphttp.NewMemoryStateStore()
	fleet, coordinator := createFleetForTest(t, state, "fleet-a", "Ops Fleet")
	targetPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	sock := testUnixSocketPath(t)
	removeReqCh := make(chan *ipc.MemberRemoveReq, 1)
	stop := serveESPFleetMemberRemoveIPC(t, sock, testESPGroupID(26), removeReqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}

	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "fleet_member_remove",
		DeviceID: "ios-1",
		Payload: mustMarshalJSON(t, fleetMemberRemovePayload{
			FleetID: fleet.FleetID,
			Target: &inviteTargetPayload{
				PilotNodeID:   45460,
				EntmootPubKey: append([]byte(nil), targetPub...),
			},
		}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusNotFound || opErr.Code != "not_member" {
		t.Fatalf("ExecuteSignRequest err = %v, want 404 not_member", err)
	}
	select {
	case req := <-removeReqCh:
		t.Fatalf("unexpected member remove IPC request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
	members, err := state.ListFleetMembers(ctx, fleet.FleetID)
	if err != nil {
		t.Fatalf("ListFleetMembers: %v", err)
	}
	if len(members) != 1 || members[0].Role != esphttp.FleetRoleCoordinator || members[0].Status != esphttp.FleetMemberActive {
		t.Fatalf("members after rejected remove = %+v, want active coordinator only", members)
	}
	activity, err := state.ListFleetActivity(ctx, fleet.FleetID, 10, 0)
	if err != nil {
		t.Fatalf("ListFleetActivity: %v", err)
	}
	if len(activity) != 0 {
		t.Fatalf("activity after rejected remove = %+v, want none", activity)
	}
	_ = coordinator
}

func TestESPOpenInviteStoresAndRedeemsBootstrapPeers(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(19)
	state := esphttp.NewMemoryStateStore()
	sock := testUnixSocketPath(t)
	reqCh := make(chan *ipc.InviteCreateReq, 1)
	stop := serveESPInviteCreateIPC(t, sock, gid, reqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "open_invite_create",
		GroupID:  gid,
		DeviceID: "ios-1",
		Payload:  json.RawMessage(`{"max_uses":2,"valid_for":"24h","bootstrap_peers":[45491,45460]}`),
	}, nil)
	if err != nil {
		t.Fatalf("open_invite_create: %v", err)
	}
	var created struct {
		Token          string           `json:"token"`
		BootstrapPeers []entmoot.NodeID `json:"bootstrap_peers"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	if created.Token == "" || len(created.BootstrapPeers) != 2 || created.BootstrapPeers[0] != 45491 || created.BootstrapPeers[1] != 45460 {
		t.Fatalf("created open invite = %+v", created)
	}
	rec, ok, err := state.GetOpenInviteByTokenHash(ctx, esphttp.HashOpenInviteToken(created.Token))
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteByTokenHash err/ok = %v/%v", err, ok)
	}
	if len(rec.BootstrapPeers) != 2 || rec.BootstrapPeers[0] != 45491 || rec.BootstrapPeers[1] != 45460 {
		t.Fatalf("stored bootstrap peers = %v", rec.BootstrapPeers)
	}

	targetPilotPub, targetPilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey pilot: %v", err)
	}
	targetEntmootPub := bytes.Repeat([]byte{0x43}, 32)
	redeemPayload := openInviteRedeemPayloadForTest(t, exec, ctx, created.Token, 45981, targetPilotPub, targetPilotPriv, targetEntmootPub)
	redeemed, err := exec.RedeemOpenInvite(ctx, created.Token, redeemPayload)
	if err != nil {
		t.Fatalf("RedeemOpenInvite: %v", err)
	}
	select {
	case req := <-reqCh:
		if len(req.BootstrapPeers) != 2 || req.BootstrapPeers[0] != 45491 || req.BootstrapPeers[1] != 45460 {
			t.Fatalf("redeem IPC bootstrap peers = %v", req.BootstrapPeers)
		}
		if !req.RequirePilotProof || !ed25519.Verify(targetPilotPub, pilotChallengeSigningBytes(req.TargetPilotProof), req.TargetPilotSignature) {
			t.Fatalf("redeem IPC pilot proof did not verify")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for invite create IPC request")
	}
	redeemedAgain, err := exec.RedeemOpenInvite(ctx, created.Token, redeemPayload)
	if err != nil {
		t.Fatalf("same-body repeat RedeemOpenInvite: %v", err)
	}
	if !bytes.Equal(redeemed, redeemedAgain) {
		t.Fatalf("same-body repeat redeem result changed:\nfirst=%s\nsecond=%s", redeemed, redeemedAgain)
	}
	select {
	case req := <-reqCh:
		t.Fatalf("same-body repeat redeem minted another invite: %+v", req)
	default:
	}
	replayWithoutProof := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":  45981,
		"pilot_pubkey":   base64.StdEncoding.EncodeToString(targetPilotPub),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(targetEntmootPub),
	})
	_, err = exec.RedeemOpenInvite(ctx, created.Token, replayWithoutProof)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("replay without proof err = %v, want 400 bad_request", err)
	}
	select {
	case req := <-reqCh:
		t.Fatalf("replay without proof minted another invite: %+v", req)
	default:
	}
}

func TestESPOpenInviteAllowsUnlimitedMaxUses(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(20)
	state := esphttp.NewMemoryStateStore()
	sock := testUnixSocketPath(t)
	reqCh := make(chan *ipc.InviteCreateReq, 2)
	stop := serveESPInviteCreateIPC(t, sock, gid, reqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "open_invite_create",
		GroupID:  gid,
		DeviceID: "ios-1",
		Payload:  json.RawMessage(`{"max_uses":0,"valid_for":"24h"}`),
	}, nil)
	if err != nil {
		t.Fatalf("open_invite_create unlimited: %v", err)
	}
	var created struct {
		Token   string `json:"token"`
		MaxUses int    `json:"max_uses"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	if created.Token == "" || created.MaxUses != esphttp.OpenInviteUnlimitedMaxUses {
		t.Fatalf("created unlimited open invite = %+v", created)
	}

	firstPilotPub, firstPilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey first pilot: %v", err)
	}
	firstPayload := openInviteRedeemPayloadForTest(t, exec, ctx, created.Token, 45981, firstPilotPub, firstPilotPriv, bytes.Repeat([]byte{0x61}, 32))
	if _, err := exec.RedeemOpenInvite(ctx, created.Token, firstPayload); err != nil {
		t.Fatalf("first RedeemOpenInvite: %v", err)
	}
	select {
	case <-reqCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first invite create IPC request")
	}

	secondPilotPub, secondPilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey second pilot: %v", err)
	}
	secondPayload := openInviteRedeemPayloadForTest(t, exec, ctx, created.Token, 45982, secondPilotPub, secondPilotPriv, bytes.Repeat([]byte{0x62}, 32))
	if _, err := exec.RedeemOpenInvite(ctx, created.Token, secondPayload); err != nil {
		t.Fatalf("second RedeemOpenInvite: %v", err)
	}
	select {
	case <-reqCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second invite create IPC request")
	}
	rec, ok, err := state.GetOpenInviteByTokenHash(ctx, esphttp.HashOpenInviteToken(created.Token))
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteByTokenHash err/ok = %v/%v", err, ok)
	}
	if rec.MaxUses != esphttp.OpenInviteUnlimitedMaxUses || rec.UseCount != 2 {
		t.Fatalf("stored unlimited open invite = %+v", rec)
	}
}

func TestESPOpenInviteCreateRequiresExplicitMaxUses(t *testing.T) {
	ctx := context.Background()
	exec := espOperationExecutor{stateStore: esphttp.NewMemoryStateStore(), timeout: time.Second}
	_, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_create",
		GroupID: testESPGroupID(21),
		Payload: json.RawMessage(`{"valid_for":"24h"}`),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" || opErr.Message != "max_uses is required" {
		t.Fatalf("open_invite_create missing max_uses err = %v, want 400 max_uses is required", err)
	}
}

func TestESPOpenInviteAcceptRedeemsViaIssuerAndJoinsGroup(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(27)
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	reg, regPath := testDeviceRegistry(t)
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, identity.PublicKey, true)
	defer stop()

	pilotPub, pilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey pilot: %v", err)
	}
	challengeID := "challenge-1"
	challengePayload, err := canonical.Encode(openInvitePilotProofEnvelope{
		Type:          "entmoot.open_invite.redeem.v1",
		TokenHash:     esphttp.HashOpenInviteToken("open-token"),
		GroupID:       gid,
		ChallengeID:   challengeID,
		Nonce:         "nonce-1",
		IssuedAtMS:    time.Now().UnixMilli(),
		ExpiresAtMS:   time.Now().Add(time.Minute).UnixMilli(),
		PilotNodeID:   45981,
		PilotPubKey:   pilotPub,
		EntmootPubKey: identity.PublicKey,
	})
	if err != nil {
		t.Fatalf("Encode challenge payload: %v", err)
	}
	var challengeCalled, redeemCalled bool
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/open-invites/open-token/challenge":
			challengeCalled = true
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode challenge request: %v", err)
			}
			if body["pilot_pubkey"] != base64.StdEncoding.EncodeToString(pilotPub) {
				t.Errorf("pilot_pubkey = %v", body["pilot_pubkey"])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"challenge_id":           challengeID,
				"signing_payload":        base64.StdEncoding.EncodeToString(challengePayload),
				"signing_payload_sha256": sha256Base64(challengePayload),
				"expires_at_ms":          time.Now().Add(time.Minute).UnixMilli(),
			})
		case "/v1/open-invites/open-token/redeem":
			redeemCalled = true
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode redeem request: %v", err)
			}
			sigText, _ := body["pilot_signature"].(string)
			sig, _ := base64.StdEncoding.DecodeString(sigText)
			if !ed25519.Verify(pilotPub, pilotChallengeSigningBytes(challengePayload), sig) {
				t.Errorf("pilot signature did not verify")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":        "redeemed",
				"group_id":      gid,
				"invite":        entmoot.Invite{GroupID: gid},
				"max_uses":      5,
				"use_count":     1,
				"expires_at_ms": time.Now().Add(time.Hour).UnixMilli(),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer issuer.Close()

	exec := espOperationExecutor{
		identity:     identity,
		socketPath:   sock,
		timeout:      time.Second,
		deviceGroups: &fileBackedDeviceGroupAuthorizer{path: regPath, registry: reg},
		pilotIdentity: func(context.Context) (entmoot.NodeID, string, error) {
			return 45981, base64.StdEncoding.EncodeToString(pilotPub), nil
		},
		pilotSignChallenge: func(_ context.Context, payload []byte) (string, error) {
			if !bytes.Equal(payload, challengePayload) {
				t.Fatalf("challenge payload = %q, want %q", payload, challengePayload)
			}
			return base64.StdEncoding.EncodeToString(ed25519.Sign(pilotPriv, pilotChallengeSigningBytes(payload))), nil
		},
	}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:     "open_invite_accept",
		DeviceID: "ios-1",
		Payload:  mustMarshalJSON(t, openInviteAcceptPayload{IssuerURL: issuer.URL, Token: "open-token"}),
	}, nil)
	if err != nil {
		t.Fatalf("open_invite_accept: %v", err)
	}
	var out struct {
		Status  string          `json:"status"`
		GroupID entmoot.GroupID `json:"group_id"`
		Members int             `json:"members"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal open_invite_accept result: %v", err)
	}
	if !challengeCalled || !redeemCalled {
		t.Fatalf("issuer calls challenge=%v redeem=%v, want both", challengeCalled, redeemCalled)
	}
	if out.Status != "joined" || out.GroupID != gid || out.Members != 1 {
		t.Fatalf("open invite accept result = %+v", out)
	}
	assertDeviceGroups(t, regPath, gid)
}

func TestESPOpenInviteIssuerRedirectsAreRejected(t *testing.T) {
	ctx := context.Background()
	var redirectTargetCalled bool
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectTargetCalled = true
	}))
	defer redirectTarget.Close()
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL+"/leaked", http.StatusTemporaryRedirect)
	}))
	defer issuer.Close()

	var out map[string]any
	err := (espOperationExecutor{timeout: time.Second}).postIssuerJSON(ctx, issuer.URL+"/v1/open-invites/token/challenge", map[string]any{
		"pilot_node_id":  45981,
		"pilot_pubkey":   "pilot",
		"entmoot_pubkey": "entmoot",
	}, &out)

	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadGateway || opErr.Code != "issuer_redirect_disallowed" {
		t.Fatalf("postIssuerJSON err = %v, want issuer_redirect_disallowed 502", err)
	}
	if redirectTargetCalled {
		t.Fatal("redirect target was called")
	}
}

func TestValidateOpenInviteIssuerChallengeRejectsMismatches(t *testing.T) {
	gid := testESPGroupID(28)
	pilotPub := bytes.Repeat([]byte{0x21}, ed25519.PublicKeySize)
	entmootPub := bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)
	token := "open-token"
	challengeID := "challenge-1"
	now := time.Now().UnixMilli()
	base := openInvitePilotProofEnvelope{
		Type:          "entmoot.open_invite.redeem.v1",
		TokenHash:     esphttp.HashOpenInviteToken(token),
		GroupID:       gid,
		ChallengeID:   challengeID,
		Nonce:         "nonce-1",
		IssuedAtMS:    now,
		ExpiresAtMS:   now + int64(time.Minute/time.Millisecond),
		PilotNodeID:   45981,
		PilotPubKey:   append([]byte(nil), pilotPub...),
		EntmootPubKey: append([]byte(nil), entmootPub...),
	}
	validProof, err := canonical.Encode(base)
	if err != nil {
		t.Fatalf("Encode valid proof: %v", err)
	}
	validGroupID, err := validateOpenInviteIssuerChallenge(validProof, challengeID, token, base.PilotNodeID, pilotPub, entmootPub, now)
	if err != nil {
		t.Fatalf("valid challenge rejected: %v", err)
	}
	if validGroupID != gid {
		t.Fatalf("valid challenge group = %s, want %s", validGroupID, gid)
	}

	tests := []struct {
		name  string
		proof func() []byte
	}{
		{
			name: "wrong type",
			proof: func() []byte {
				env := base
				env.Type = "other"
				return mustCanonicalProof(t, env)
			},
		},
		{
			name: "wrong token",
			proof: func() []byte {
				env := base
				env.TokenHash = esphttp.HashOpenInviteToken("other-token")
				return mustCanonicalProof(t, env)
			},
		},
		{
			name: "wrong challenge id",
			proof: func() []byte {
				env := base
				env.ChallengeID = "challenge-2"
				return mustCanonicalProof(t, env)
			},
		},
		{
			name: "wrong pilot node",
			proof: func() []byte {
				env := base
				env.PilotNodeID = 45982
				return mustCanonicalProof(t, env)
			},
		},
		{
			name: "wrong pilot pubkey",
			proof: func() []byte {
				env := base
				env.PilotPubKey = bytes.Repeat([]byte{0x22}, ed25519.PublicKeySize)
				return mustCanonicalProof(t, env)
			},
		},
		{
			name: "wrong entmoot pubkey",
			proof: func() []byte {
				env := base
				env.EntmootPubKey = bytes.Repeat([]byte{0x43}, ed25519.PublicKeySize)
				return mustCanonicalProof(t, env)
			},
		},
		{
			name: "expired",
			proof: func() []byte {
				env := base
				env.ExpiresAtMS = now
				return mustCanonicalProof(t, env)
			},
		},
		{
			name: "non-canonical",
			proof: func() []byte {
				out, err := json.Marshal(base)
				if err != nil {
					t.Fatalf("Marshal non-canonical proof: %v", err)
				}
				return out
			},
		},
		{
			name: "malformed",
			proof: func() []byte {
				return []byte("not-json")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateOpenInviteIssuerChallenge(tt.proof(), challengeID, token, base.PilotNodeID, pilotPub, entmootPub, now)
			var opErr *esphttp.OperationError
			if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadGateway || opErr.Code != "issuer_bad_response" {
				t.Fatalf("validateOpenInviteIssuerChallenge err = %v, want 502 issuer_bad_response", err)
			}
		})
	}
}

func TestESPOpenInviteAcceptRejectsMismatchedIssuerChallengeBeforeSigning(t *testing.T) {
	ctx := context.Background()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	pilotPub := bytes.Repeat([]byte{0x24}, ed25519.PublicKeySize)
	proof, err := canonical.Encode(openInvitePilotProofEnvelope{
		Type:          "entmoot.open_invite.redeem.v1",
		TokenHash:     esphttp.HashOpenInviteToken("other-token"),
		GroupID:       testESPGroupID(29),
		ChallengeID:   "challenge-1",
		Nonce:         "nonce-1",
		IssuedAtMS:    time.Now().UnixMilli(),
		ExpiresAtMS:   time.Now().Add(time.Minute).UnixMilli(),
		PilotNodeID:   45981,
		PilotPubKey:   pilotPub,
		EntmootPubKey: identity.PublicKey,
	})
	if err != nil {
		t.Fatalf("Encode proof: %v", err)
	}
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/open-invites/open-token/challenge" {
			t.Errorf("unexpected issuer path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"challenge_id":           "challenge-1",
			"signing_payload":        base64.StdEncoding.EncodeToString(proof),
			"signing_payload_sha256": sha256Base64(proof),
		})
	}))
	defer issuer.Close()

	var signed bool
	exec := espOperationExecutor{
		identity: identity,
		timeout:  time.Second,
		pilotIdentity: func(context.Context) (entmoot.NodeID, string, error) {
			return 45981, base64.StdEncoding.EncodeToString(pilotPub), nil
		},
		pilotSignChallenge: func(context.Context, []byte) (string, error) {
			signed = true
			return "", errors.New("should not sign")
		},
	}
	_, err = exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_accept",
		Payload: mustMarshalJSON(t, openInviteAcceptPayload{IssuerURL: issuer.URL, Token: "open-token"}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadGateway || opErr.Code != "issuer_bad_response" {
		t.Fatalf("open_invite_accept err = %v, want 502 issuer_bad_response", err)
	}
	if signed {
		t.Fatal("pilotSignChallenge was called")
	}
}

func TestESPOpenInviteAcceptRejectsRedeemedInviteGroupMismatch(t *testing.T) {
	ctx := context.Background()
	challengeGroup := testESPGroupID(30)
	redeemedGroup := testESPGroupID(31)
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	pilotPub, pilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey pilot: %v", err)
	}
	challengeID := "challenge-1"
	challengePayload, err := canonical.Encode(openInvitePilotProofEnvelope{
		Type:          "entmoot.open_invite.redeem.v1",
		TokenHash:     esphttp.HashOpenInviteToken("open-token"),
		GroupID:       challengeGroup,
		ChallengeID:   challengeID,
		Nonce:         "nonce-1",
		IssuedAtMS:    time.Now().UnixMilli(),
		ExpiresAtMS:   time.Now().Add(time.Minute).UnixMilli(),
		PilotNodeID:   45981,
		PilotPubKey:   pilotPub,
		EntmootPubKey: identity.PublicKey,
	})
	if err != nil {
		t.Fatalf("Encode challenge payload: %v", err)
	}
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/open-invites/open-token/challenge":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"challenge_id":           challengeID,
				"signing_payload":        base64.StdEncoding.EncodeToString(challengePayload),
				"signing_payload_sha256": sha256Base64(challengePayload),
				"expires_at_ms":          time.Now().Add(time.Minute).UnixMilli(),
			})
		case "/v1/open-invites/open-token/redeem":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":        "redeemed",
				"group_id":      redeemedGroup,
				"invite":        entmoot.Invite{GroupID: redeemedGroup},
				"max_uses":      1,
				"use_count":     1,
				"expires_at_ms": time.Now().Add(time.Hour).UnixMilli(),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer issuer.Close()

	exec := espOperationExecutor{
		identity: identity,
		timeout:  time.Second,
		pilotIdentity: func(context.Context) (entmoot.NodeID, string, error) {
			return 45981, base64.StdEncoding.EncodeToString(pilotPub), nil
		},
		pilotSignChallenge: func(_ context.Context, payload []byte) (string, error) {
			return base64.StdEncoding.EncodeToString(ed25519.Sign(pilotPriv, pilotChallengeSigningBytes(payload))), nil
		},
	}
	_, err = exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_accept",
		Payload: mustMarshalJSON(t, openInviteAcceptPayload{IssuerURL: issuer.URL, Token: "open-token"}),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadGateway || opErr.Code != "issuer_bad_response" {
		t.Fatalf("open_invite_accept err = %v, want 502 issuer_bad_response", err)
	}
}

func TestESPOpenInvitePendingRedemptionDoesNotRefundUse(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(21)
	state := esphttp.NewMemoryStateStore()
	sock := testUnixSocketPath(t)
	reqCh := make(chan *ipc.InviteCreateReq, 2)
	stop := serveESPInviteCreateIPCFailFirst(t, sock, gid, reqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_create",
		GroupID: gid,
		Payload: json.RawMessage(`{"max_uses":1,"valid_for":"24h"}`),
	}, nil)
	if err != nil {
		t.Fatalf("open_invite_create: %v", err)
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}

	targetPilotPub, targetPilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey target pilot: %v", err)
	}
	targetEntmootPub := bytes.Repeat([]byte{0x45}, 32)
	redeemPayload := openInviteRedeemPayloadForTest(t, exec, ctx, created.Token, 45981, targetPilotPub, targetPilotPriv, targetEntmootPub)
	if _, err := exec.RedeemOpenInvite(ctx, created.Token, redeemPayload); err == nil {
		t.Fatal("first RedeemOpenInvite err = nil, want ambiguous IPC error")
	}
	select {
	case <-reqCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first invite create IPC request")
	}
	rec, ok, err := state.GetOpenInviteByTokenHash(ctx, esphttp.HashOpenInviteToken(created.Token))
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteByTokenHash err/ok = %v/%v", err, ok)
	}
	if rec.UseCount != 1 {
		t.Fatalf("use_count after ambiguous IPC failure = %d, want 1", rec.UseCount)
	}

	otherPilotPub, otherPilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey other pilot: %v", err)
	}
	otherPayload := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":   45982,
		"pilot_pubkey":    base64.StdEncoding.EncodeToString(otherPilotPub),
		"entmoot_pubkey":  base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x46}, 32)),
		"challenge_id":    "unused",
		"pilot_signature": base64.StdEncoding.EncodeToString(ed25519.Sign(otherPilotPriv, []byte("unused"))),
	})
	_, err = exec.RedeemOpenInvite(ctx, created.Token, otherPayload)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.Code != "open_invite_exhausted" {
		t.Fatalf("other redeemer err = %v, want open_invite_exhausted", err)
	}
	select {
	case req := <-reqCh:
		t.Fatalf("exhausted other redeemer reached IPC: %+v", req)
	default:
	}

	redeemed, err := exec.RedeemOpenInvite(ctx, created.Token, redeemPayload)
	if err != nil {
		t.Fatalf("same redeemer retry RedeemOpenInvite: %v", err)
	}
	if len(redeemed) == 0 {
		t.Fatal("same redeemer retry returned empty result")
	}
	select {
	case <-reqCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retry invite create IPC request")
	}
	rec, ok, err = state.GetOpenInviteByTokenHash(ctx, esphttp.HashOpenInviteToken(created.Token))
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteByTokenHash after retry err/ok = %v/%v", err, ok)
	}
	if rec.UseCount != 1 {
		t.Fatalf("use_count after same redeemer retry = %d, want 1", rec.UseCount)
	}
}

func TestESPOpenInvitePreSendFailureReleasesRedemption(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(23)
	state := esphttp.NewMemoryStateStore()
	sock := testUnixSocketPath(t)
	stop := serveESPInviteCreateIPC(t, sock, gid, nil)
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_create",
		GroupID: gid,
		Payload: json.RawMessage(`{"max_uses":1,"valid_for":"24h"}`),
	}, nil)
	if err != nil {
		stop()
		t.Fatalf("open_invite_create: %v", err)
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	targetPilotPub, targetPilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey target pilot: %v", err)
	}
	redeemPayload := openInviteRedeemPayloadForTest(t, exec, ctx, created.Token, 45981, targetPilotPub, targetPilotPriv, bytes.Repeat([]byte{0x47}, 32))
	stop()
	if _, err := exec.RedeemOpenInvite(ctx, created.Token, redeemPayload); err == nil {
		t.Fatal("RedeemOpenInvite err = nil, want dial failure")
	}
	rec, ok, err := state.GetOpenInviteByTokenHash(ctx, esphttp.HashOpenInviteToken(created.Token))
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteByTokenHash err/ok = %v/%v", err, ok)
	}
	if rec.UseCount != 0 {
		t.Fatalf("use_count after pre-send failure = %d, want 0", rec.UseCount)
	}
	redeemerKey := fmt.Sprintf("%d:%s", entmoot.NodeID(45981), base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x47}, 32)))
	if redemption, ok, err := state.GetOpenInviteRedemption(ctx, esphttp.HashOpenInviteToken(created.Token), redeemerKey); err != nil || ok {
		t.Fatalf("redemption after pre-send failure = %+v/%v/%v, want none", redemption, ok, err)
	}
}

func TestESPOpenInviteDaemonRejectionReleasesRedemption(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(25)
	state := esphttp.NewMemoryStateStore()
	sock := testUnixSocketPath(t)
	reqCh := make(chan *ipc.InviteCreateReq, 2)
	stop := serveESPInviteCreateIPCErrorFirst(t, sock, gid, reqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_create",
		GroupID: gid,
		Payload: json.RawMessage(`{"max_uses":1,"valid_for":"24h"}`),
	}, nil)
	if err != nil {
		t.Fatalf("open_invite_create: %v", err)
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	targetPilotPub, targetPilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey target pilot: %v", err)
	}
	targetEntmootPub := bytes.Repeat([]byte{0x4a}, 32)
	redeemPayload := openInviteRedeemPayloadForTest(t, exec, ctx, created.Token, 45981, targetPilotPub, targetPilotPriv, targetEntmootPub)
	_, err = exec.RedeemOpenInvite(ctx, created.Token, redeemPayload)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusConflict || opErr.Code != "member_identity_conflict" {
		t.Fatalf("first RedeemOpenInvite err = %v, want 409 member_identity_conflict", err)
	}
	select {
	case <-reqCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rejected invite create IPC request")
	}
	tokenHash := esphttp.HashOpenInviteToken(created.Token)
	rec, ok, err := state.GetOpenInviteByTokenHash(ctx, tokenHash)
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteByTokenHash err/ok = %v/%v", err, ok)
	}
	if rec.UseCount != 0 {
		t.Fatalf("use_count after explicit daemon rejection = %d, want 0", rec.UseCount)
	}
	redeemerKey := fmt.Sprintf("%d:%s", entmoot.NodeID(45981), base64.StdEncoding.EncodeToString(targetEntmootPub))
	if redemption, ok, err := state.GetOpenInviteRedemption(ctx, tokenHash, redeemerKey); err != nil || ok {
		t.Fatalf("redemption after explicit daemon rejection = %+v/%v/%v, want none", redemption, ok, err)
	}

	otherPilotPub, otherPilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey other pilot: %v", err)
	}
	otherPayload := openInviteRedeemPayloadForTest(t, exec, ctx, created.Token, 45982, otherPilotPub, otherPilotPriv, bytes.Repeat([]byte{0x4b}, 32))
	redeemed, err := exec.RedeemOpenInvite(ctx, created.Token, otherPayload)
	if err != nil {
		t.Fatalf("second RedeemOpenInvite after refund: %v", err)
	}
	if len(redeemed) == 0 {
		t.Fatal("second RedeemOpenInvite returned empty result")
	}
	select {
	case <-reqCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for successful invite create IPC request")
	}
	rec, ok, err = state.GetOpenInviteByTokenHash(ctx, tokenHash)
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteByTokenHash after success err/ok = %v/%v", err, ok)
	}
	if rec.UseCount != 1 {
		t.Fatalf("use_count after successful retry = %d, want 1", rec.UseCount)
	}
}

func TestESPOpenInviteChallengeRejectsExhaustedInvite(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(24)
	state := esphttp.NewMemoryStateStore()
	sock := testUnixSocketPath(t)
	stop := serveESPInviteCreateIPC(t, sock, gid, nil)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_create",
		GroupID: gid,
		Payload: json.RawMessage(`{"max_uses":1,"valid_for":"24h"}`),
	}, nil)
	if err != nil {
		t.Fatalf("open_invite_create: %v", err)
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	tokenHash := esphttp.HashOpenInviteToken(created.Token)
	usedEntmootPub := bytes.Repeat([]byte{0x48}, 32)
	_, _, _, err = state.RedeemOpenInvite(ctx, tokenHash, esphttp.OpenInviteRedemption{
		RedeemerKey:   openInviteRedeemerKey(45981, usedEntmootPub),
		PilotNodeID:   45981,
		EntmootPubKey: base64.StdEncoding.EncodeToString(usedEntmootPub),
	}, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("seed exhausted redemption: %v", err)
	}
	pilotPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey pilot: %v", err)
	}
	challengeReq := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":  45982,
		"pilot_pubkey":   base64.StdEncoding.EncodeToString(pilotPub),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x49}, 32)),
	})
	_, err = exec.CreateOpenInviteChallenge(ctx, created.Token, challengeReq)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusConflict || opErr.Code != "open_invite_exhausted" {
		t.Fatalf("CreateOpenInviteChallenge err = %v, want 409 open_invite_exhausted", err)
	}

	usedPilotPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey used pilot: %v", err)
	}
	retryReq := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":  45981,
		"pilot_pubkey":   base64.StdEncoding.EncodeToString(usedPilotPub),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(usedEntmootPub),
	})
	retryChallenge, err := exec.CreateOpenInviteChallenge(ctx, created.Token, retryReq)
	if err != nil {
		t.Fatalf("same redeemer CreateOpenInviteChallenge after exhaustion: %v", err)
	}
	var retryOut struct {
		ChallengeID string `json:"challenge_id"`
	}
	if err := json.Unmarshal(retryChallenge, &retryOut); err != nil {
		t.Fatalf("unmarshal retry challenge: %v", err)
	}
	if retryOut.ChallengeID == "" {
		t.Fatal("same redeemer retry challenge id is empty")
	}
}

func TestESPOpenInviteChallengeRequiresInviteAuthority(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(25)
	state := esphttp.NewMemoryStateStore()
	token, tokenHash, err := esphttp.NewOpenInviteToken()
	if err != nil {
		t.Fatalf("NewOpenInviteToken: %v", err)
	}
	if _, err := state.CreateOpenInvite(ctx, esphttp.OpenInviteRecord{
		TokenHash:   tokenHash,
		GroupID:     gid,
		MaxUses:     1,
		CreatedAtMS: time.Now().UnixMilli(),
		ExpiresAtMS: time.Now().Add(time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("CreateOpenInvite: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPAuthorityIPCError(t, sock, ipc.CodeUnavailable, "pilot lookup unavailable")
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	pilotPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey pilot: %v", err)
	}
	challengeReq := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":  45982,
		"pilot_pubkey":   base64.StdEncoding.EncodeToString(pilotPub),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x49}, 32)),
	})
	_, err = exec.CreateOpenInviteChallenge(ctx, token, challengeReq)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusServiceUnavailable || opErr.Code != "join_unavailable" {
		t.Fatalf("CreateOpenInviteChallenge err = %v, want 503 join_unavailable", err)
	}
}

func TestESPOpenInviteChallengeCapsActiveRedeemers(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(26)
	state := esphttp.NewMemoryStateStore()
	sock := testUnixSocketPath(t)
	stop := serveESPInviteCreateIPC(t, sock, gid, nil)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_create",
		GroupID: gid,
		Payload: json.RawMessage(`{"max_uses":1,"valid_for":"24h"}`),
	}, nil)
	if err != nil {
		t.Fatalf("open_invite_create: %v", err)
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	firstPilotPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey first pilot: %v", err)
	}
	firstReq := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":  45981,
		"pilot_pubkey":   base64.StdEncoding.EncodeToString(firstPilotPub),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x50}, 32)),
	})
	firstRaw, err := exec.CreateOpenInviteChallenge(ctx, created.Token, firstReq)
	if err != nil {
		t.Fatalf("first CreateOpenInviteChallenge: %v", err)
	}
	reusedRaw, err := exec.CreateOpenInviteChallenge(ctx, created.Token, firstReq)
	if err != nil {
		t.Fatalf("reuse CreateOpenInviteChallenge: %v", err)
	}
	if !bytes.Equal(firstRaw, reusedRaw) {
		t.Fatalf("same redeemer challenge changed:\nfirst=%s\nsecond=%s", firstRaw, reusedRaw)
	}
	for i := 1; i < minOpenInviteActiveChallenges; i++ {
		pilotPub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("GenerateKey pilot %d: %v", i, err)
		}
		req := mustMarshalJSON(t, map[string]any{
			"pilot_node_id":  45981 + i,
			"pilot_pubkey":   base64.StdEncoding.EncodeToString(pilotPub),
			"entmoot_pubkey": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{byte(0x50 + i)}, 32)),
		})
		if _, err := exec.CreateOpenInviteChallenge(ctx, created.Token, req); err != nil {
			t.Fatalf("CreateOpenInviteChallenge %d: %v", i, err)
		}
	}
	overflowPilotPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey overflow pilot: %v", err)
	}
	overflowReq := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":  46999,
		"pilot_pubkey":   base64.StdEncoding.EncodeToString(overflowPilotPub),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x5f}, 32)),
	})
	_, err = exec.CreateOpenInviteChallenge(ctx, created.Token, overflowReq)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusTooManyRequests || opErr.Code != "open_invite_challenge_limit" {
		t.Fatalf("overflow CreateOpenInviteChallenge err = %v, want 429 open_invite_challenge_limit", err)
	}
}

func TestOpenInviteChallengeCapTreatsUnlimitedUsesAsMaxCap(t *testing.T) {
	rec := esphttp.OpenInviteRecord{
		MaxUses:  esphttp.OpenInviteUnlimitedMaxUses,
		UseCount: 512,
	}
	if got := openInviteChallengeCap(rec); got != maxOpenInviteActiveChallenges {
		t.Fatalf("openInviteChallengeCap(unlimited) = %d, want %d", got, maxOpenInviteActiveChallenges)
	}
}

func TestESPOpenInviteRedemptionLockIsReclaimedAfterFailure(t *testing.T) {
	m := keyedMutexMap{}
	unlock := m.Lock("token\x00redeemer")
	if got := m.Len(); got != 1 {
		t.Fatalf("Len while locked = %d, want 1", got)
	}
	acquired := make(chan func(), 1)
	go func() {
		acquired <- m.Lock("token\x00redeemer")
	}()
	time.Sleep(20 * time.Millisecond)
	if got := m.Len(); got != 1 {
		t.Fatalf("Len while waiter exists = %d, want 1", got)
	}
	unlock()
	waiterUnlock := <-acquired
	if got := m.Len(); got != 1 {
		t.Fatalf("Len while waiter owns lock = %d, want 1", got)
	}
	waiterUnlock()
	if got := m.Len(); got != 0 {
		t.Fatalf("Len after unlock = %d, want 0", got)
	}
}

func TestOperationIPCErrorMapsUnavailable(t *testing.T) {
	err := operationIPCError(&ipc.ErrorFrame{Code: ipc.CodeUnavailable, Message: "missing lookup_node"})
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("operationIPCError err = %T, want OperationError", err)
	}
	if opErr.HTTPStatus != http.StatusServiceUnavailable || opErr.Code != "join_unavailable" {
		t.Fatalf("operation error = %d/%s, want 503/join_unavailable", opErr.HTTPStatus, opErr.Code)
	}
}

func TestESPOpenInviteRedeemRequiresPilotProof(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(20)
	state := esphttp.NewMemoryStateStore()
	sock := testUnixSocketPath(t)
	reqCh := make(chan *ipc.InviteCreateReq, 1)
	stop := serveESPInviteCreateIPC(t, sock, gid, reqCh)
	defer stop()
	exec := espOperationExecutor{stateStore: state, socketPath: sock, timeout: time.Second}
	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "open_invite_create",
		GroupID: gid,
		Payload: json.RawMessage(`{"max_uses":1,"valid_for":"24h"}`),
	}, nil)
	if err != nil {
		t.Fatalf("open_invite_create: %v", err)
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	pilotPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey pilot: %v", err)
	}
	body := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":  45981,
		"pilot_pubkey":   base64.StdEncoding.EncodeToString(pilotPub),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x44}, 32)),
	})
	_, err = exec.RedeemOpenInvite(ctx, created.Token, body)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("RedeemOpenInvite err = %v, want 400 bad_request", err)
	}
}

func openInviteRedeemPayloadForTest(t *testing.T, exec espOperationExecutor, ctx context.Context, token string, nodeID entmoot.NodeID, pilotPub ed25519.PublicKey, pilotPriv ed25519.PrivateKey, entmootPub []byte) json.RawMessage {
	t.Helper()
	challengeReq := mustMarshalJSON(t, map[string]any{
		"pilot_node_id":  nodeID,
		"pilot_pubkey":   base64.StdEncoding.EncodeToString(pilotPub),
		"entmoot_pubkey": base64.StdEncoding.EncodeToString(entmootPub),
	})
	raw, err := exec.CreateOpenInviteChallenge(ctx, token, challengeReq)
	if err != nil {
		t.Fatalf("CreateOpenInviteChallenge: %v", err)
	}
	var challenge struct {
		ChallengeID    string `json:"challenge_id"`
		SigningPayload string `json:"signing_payload"`
	}
	if err := json.Unmarshal(raw, &challenge); err != nil {
		t.Fatalf("unmarshal challenge: %v", err)
	}
	payload, err := base64.StdEncoding.DecodeString(challenge.SigningPayload)
	if err != nil {
		t.Fatalf("decode challenge signing payload: %v", err)
	}
	return mustMarshalJSON(t, map[string]any{
		"pilot_node_id":   nodeID,
		"pilot_pubkey":    base64.StdEncoding.EncodeToString(pilotPub),
		"entmoot_pubkey":  base64.StdEncoding.EncodeToString(entmootPub),
		"challenge_id":    challenge.ChallengeID,
		"pilot_signature": base64.StdEncoding.EncodeToString(ed25519.Sign(pilotPriv, pilotChallengeSigningBytes(payload))),
	})
}

func mustCanonicalProof(t *testing.T, env openInvitePilotProofEnvelope) []byte {
	t.Helper()
	proof, err := canonical.Encode(env)
	if err != nil {
		t.Fatalf("Encode proof: %v", err)
	}
	return proof
}

func assertRosterMemberAbsent(t *testing.T, dataDir string, gid entmoot.GroupID, nodeID entmoot.NodeID) {
	t.Helper()
	rlog, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	if member, ok := rlog.MemberInfo(nodeID); ok {
		t.Fatalf("member %d exists after failed invite: %+v", nodeID, member)
	}
	if got := len(rlog.Entries()); got != 1 {
		t.Fatalf("len(entries) = %d, want genesis only", got)
	}
}

type rawGroupMetadataStore struct {
	raw json.RawMessage
}

func (s rawGroupMetadataStore) GetGroupMetadata(context.Context, entmoot.GroupID) (json.RawMessage, bool, error) {
	return append(json.RawMessage(nil), s.raw...), true, nil
}

func (s rawGroupMetadataStore) SetGroupMetadata(context.Context, entmoot.GroupID, json.RawMessage) error {
	return nil
}

func (s rawGroupMetadataStore) DeleteGroupMetadata(context.Context, entmoot.GroupID) error {
	return nil
}

type recordingGroupMetadataStore struct {
	setGroup     entmoot.GroupID
	deletedGroup entmoot.GroupID
}

func (s *recordingGroupMetadataStore) GetGroupMetadata(context.Context, entmoot.GroupID) (json.RawMessage, bool, error) {
	return nil, false, nil
}

func (s *recordingGroupMetadataStore) SetGroupMetadata(_ context.Context, gid entmoot.GroupID, _ json.RawMessage) error {
	s.setGroup = gid
	return nil
}

func (s *recordingGroupMetadataStore) DeleteGroupMetadata(_ context.Context, gid entmoot.GroupID) error {
	s.deletedGroup = gid
	return nil
}

type fakeMemberProfileReader struct {
	ads map[entmoot.NodeID]wire.MemberProfileAd
}

func (r fakeMemberProfileReader) GetMemberProfileAd(_ context.Context, _ entmoot.GroupID, nodeID entmoot.NodeID, _ time.Time) (wire.MemberProfileAd, bool, error) {
	ad, ok := r.ads[nodeID]
	return ad, ok, nil
}

func testESPGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}

func testGroupCreateRequest(id string) esphttp.SignRequest {
	return esphttp.SignRequest{
		ID:                   id,
		Kind:                 "group_create",
		Payload:              json.RawMessage(`{"name":"ops"}`),
		SigningPayloadSHA256: "test-signing-payload-digest",
		CreatedAtMS:          1_700_000_000_000,
	}
}

func testUnixSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "entmoot-esp-ipc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "sock")
}

func createTestRoster(t *testing.T, dataDir string, gid entmoot.GroupID, id *keystore.Identity, founder entmoot.NodeInfo) {
	t.Helper()
	rlog, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	if err := rlog.Genesis(id, founder, 1_700_000_000_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
}

func verifyInviteForTest(t *testing.T, invite entmoot.Invite, pub []byte) bool {
	t.Helper()
	signing := invite
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		t.Fatalf("canonical encode invite: %v", err)
	}
	return keystore.Verify(pub, sigInput, invite.Signature)
}

func testDeviceRegistry(t *testing.T) (*esphttp.DeviceRegistry, string) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	reg, err := esphttp.NewDeviceRegistry([]esphttp.Device{{
		ID:        "ios-1",
		PublicKey: pub,
	}})
	if err != nil {
		t.Fatalf("NewDeviceRegistry: %v", err)
	}
	regPath := filepath.Join(t.TempDir(), "esp-devices.json")
	if err := esphttp.SaveDeviceRegistry(regPath, reg); err != nil {
		t.Fatalf("SaveDeviceRegistry: %v", err)
	}
	return reg, regPath
}

func assertDeviceGroups(t *testing.T, regPath string, want entmoot.GroupID) {
	t.Helper()
	loaded, err := esphttp.LoadDeviceRegistry(regPath)
	if err != nil {
		t.Fatalf("LoadDeviceRegistry: %v", err)
	}
	devices := loaded.Snapshot()
	if len(devices) != 1 || devices[0].ID != "ios-1" {
		t.Fatalf("devices = %+v, want ios-1", devices)
	}
	device := devices[0]
	if len(device.Groups) != 1 || device.Groups[0] != want {
		t.Fatalf("device groups = %v, want [%s]", device.Groups, want)
	}
	if len(device.AdminGroups) != 0 {
		t.Fatalf("device admin groups = %v, want none", device.AdminGroups)
	}
}

func mustMarshalJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return raw
}

func serveESPGroupCreateIPC(t *testing.T, sock string, pub []byte, joinOK bool) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InfoReq:
				_ = v
				_ = ipc.EncodeAndWrite(conn, &ipc.InfoResp{
					PilotNodeID:   45491,
					EntmootPubKey: append([]byte(nil), pub...),
					Running:       true,
				})
			case *ipc.JoinGroupReq:
				if joinOK {
					_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: v.Invite.GroupID, Members: 1})
				}
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPAcceptInviteIPC(t *testing.T, sock string, pub []byte, joinOK bool, joinReqCh chan<- *ipc.JoinGroupReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InfoReq:
				_ = v
				_ = ipc.EncodeAndWrite(conn, &ipc.InfoResp{
					PilotNodeID:   45491,
					EntmootPubKey: append([]byte(nil), pub...),
					Running:       true,
				})
			case *ipc.JoinGroupReq:
				if joinReqCh != nil {
					joinReqCh <- v
				}
				if joinOK {
					_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: v.Invite.GroupID, Members: 1})
				}
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPAcceptInviteIPCWithJoinGate(t *testing.T, sock string, pub []byte, joinReqCh chan<- *ipc.JoinGroupReq, releaseJoin <-chan struct{}) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InfoReq:
				_ = v
				_ = ipc.EncodeAndWrite(conn, &ipc.InfoResp{
					PilotNodeID:   45491,
					EntmootPubKey: append([]byte(nil), pub...),
					Running:       true,
				})
			case *ipc.JoinGroupReq:
				if joinReqCh != nil {
					joinReqCh <- v
				}
				<-releaseJoin
				_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: v.Invite.GroupID, Members: 1})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPInviteCreateIPC(t *testing.T, sock string, gid entmoot.GroupID, reqCh chan<- *ipc.InviteCreateReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InviteAuthorityCheckReq:
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
					Status:     "ok",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			case *ipc.InviteCreateReq:
				if reqCh != nil {
					reqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{
					Status:     "created",
					GroupID:    gid,
					Invite:     entmoot.Invite{GroupID: gid},
					RosterHead: entmoot.RosterEntryID{},
					Members:    2,
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPFleetInviteAndRemoveIPC(t *testing.T, sock string, gid entmoot.GroupID, inviteReqCh chan<- *ipc.InviteCreateReq, removeReqCh chan<- *ipc.MemberRemoveReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InviteAuthorityCheckReq:
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
					Status:     "ok",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			case *ipc.InviteCreateReq:
				if inviteReqCh != nil {
					inviteReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{
					Status:     "created",
					GroupID:    gid,
					Invite:     entmoot.Invite{GroupID: gid},
					RosterHead: entmoot.RosterEntryID{},
					Members:    2,
				})
			case *ipc.MemberRemoveReq:
				if removeReqCh != nil {
					removeReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.MemberRemoveResp{
					Status:     "removed",
					GroupID:    gid,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPFleetInviteRemoveNotMemberIPC(t *testing.T, sock string, gid entmoot.GroupID, inviteReqCh chan<- *ipc.InviteCreateReq, removeReqCh chan<- *ipc.MemberRemoveReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InviteAuthorityCheckReq:
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
					Status:     "ok",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			case *ipc.InviteCreateReq:
				if inviteReqCh != nil {
					inviteReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{
					Status:     "created",
					GroupID:    gid,
					Invite:     entmoot.Invite{GroupID: gid},
					RosterHead: entmoot.RosterEntryID{},
					Members:    2,
				})
			case *ipc.MemberRemoveReq:
				if removeReqCh != nil {
					removeReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{
					Code:    ipc.CodeNotMember,
					GroupID: &v.GroupID,
					Message: "target is not a member",
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPFleetInviteUncertainIPC(t *testing.T, sock string, inviteReqCh chan<- *ipc.InviteCreateReq, removeReqCh chan<- *ipc.MemberRemoveReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InviteCreateReq:
				if inviteReqCh != nil {
					inviteReqCh <- v
				}
				_ = conn.Close()
				continue
			case *ipc.MemberRemoveReq:
				if removeReqCh != nil {
					removeReqCh <- v
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.MemberRemoveResp{
					Status:     "removed",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPInviteCreateIPCFailFirst(t *testing.T, sock string, gid entmoot.GroupID, reqCh chan<- *ipc.InviteCreateReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		seen := 0
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InviteAuthorityCheckReq:
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
					Status:     "ok",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			case *ipc.InviteCreateReq:
				seen++
				if reqCh != nil {
					reqCh <- v
				}
				if seen == 1 {
					_ = conn.Close()
					continue
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{
					Status:     "created",
					GroupID:    gid,
					Invite:     entmoot.Invite{GroupID: gid},
					RosterHead: entmoot.RosterEntryID{},
					Members:    2,
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPInviteCreateIPCErrorFirst(t *testing.T, sock string, gid entmoot.GroupID, reqCh chan<- *ipc.InviteCreateReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		seen := 0
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			switch v := payload.(type) {
			case *ipc.InviteAuthorityCheckReq:
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
					Status:     "ok",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			case *ipc.InviteCreateReq:
				seen++
				if reqCh != nil {
					reqCh <- v
				}
				if seen == 1 {
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{
						Code:    ipc.CodeConflict,
						Message: "target identity conflict",
					})
					_ = conn.Close()
					continue
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{
					Status:     "created",
					GroupID:    gid,
					Invite:     entmoot.Invite{GroupID: gid},
					RosterHead: entmoot.RosterEntryID{},
					Members:    2,
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPAuthorityIPCError(t *testing.T, sock string, code ipc.ErrorCode, message string) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err == nil {
				if req, ok := payload.(*ipc.InviteAuthorityCheckReq); ok {
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{
						Code:    code,
						GroupID: &req.GroupID,
						Message: message,
					})
				}
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

type failingFleetStateStore struct {
	*esphttp.MemoryStateStore
	failAppendActivity    bool
	failCreateFleetInvite bool
}

func (s *failingFleetStateStore) AppendFleetActivity(ctx context.Context, rec esphttp.FleetActivityRecord) (esphttp.FleetActivityRecord, error) {
	if s.failAppendActivity {
		return esphttp.FleetActivityRecord{}, fmt.Errorf("fleet activity store unavailable")
	}
	return s.MemoryStateStore.AppendFleetActivity(ctx, rec)
}

func (s *failingFleetStateStore) CreateFleetInvite(ctx context.Context, rec esphttp.FleetInviteRecord) (esphttp.FleetInviteRecord, error) {
	if s.failCreateFleetInvite {
		return esphttp.FleetInviteRecord{}, fmt.Errorf("fleet invite store unavailable")
	}
	return s.MemoryStateStore.CreateFleetInvite(ctx, rec)
}

func createFleetForTest(t *testing.T, state esphttp.StateStore, fleetID, name string) (esphttp.FleetRecord, entmoot.NodeInfo) {
	t.Helper()
	coordinatorID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate coordinator: %v", err)
	}
	coordinator := entmoot.NodeInfo{
		PilotNodeID:   45491,
		EntmootPubKey: append([]byte(nil), coordinatorID.PublicKey...),
	}
	fleet, err := state.CreateFleet(context.Background(), esphttp.FleetRecord{
		FleetID:             fleetID,
		Name:                name,
		ControlGroupID:      testESPGroupID(90),
		Coordinator:         coordinator,
		CoordinatorDeviceID: "ios-1",
		CreatedAtMS:         1_700_000_000_000,
	})
	if err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(context.Background(), esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        coordinator.PilotNodeID,
		EntmootPubKey: encodeBase64(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	return fleet, coordinator
}

func createFleetForAcceptTest(t *testing.T, state esphttp.StateStore, controlGroupID entmoot.GroupID) esphttp.FleetRecord {
	t.Helper()
	coordinatorID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate coordinator: %v", err)
	}
	coordinator := entmoot.NodeInfo{
		PilotNodeID:   45460,
		EntmootPubKey: append([]byte(nil), coordinatorID.PublicKey...),
	}
	fleet, err := state.CreateFleet(context.Background(), esphttp.FleetRecord{
		FleetID:             "fleet-accept",
		Name:                "Accept Fleet",
		ControlGroupID:      controlGroupID,
		Coordinator:         coordinator,
		CoordinatorDeviceID: "ios-1",
		CreatedAtMS:         1_700_000_000_000,
	})
	if err != nil {
		t.Fatalf("CreateFleet: %v", err)
	}
	if _, err := state.UpsertFleetMember(context.Background(), esphttp.FleetMemberRecord{
		FleetID:       fleet.FleetID,
		NodeID:        coordinator.PilotNodeID,
		EntmootPubKey: encodeBase64(coordinator.EntmootPubKey),
		Role:          esphttp.FleetRoleCoordinator,
		Status:        esphttp.FleetMemberActive,
		AcceptedAtMS:  1_700_000_000_000,
	}); err != nil {
		t.Fatalf("UpsertFleetMember: %v", err)
	}
	return fleet
}

func serveESPFleetMemberRemoveIPC(t *testing.T, sock string, gid entmoot.GroupID, reqCh chan<- *ipc.MemberRemoveReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			if req, ok := payload.(*ipc.MemberRemoveReq); ok {
				if reqCh != nil {
					reqCh <- req
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.MemberRemoveResp{
					Status:     "removed",
					GroupID:    gid,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
				_ = req
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPFleetMemberRemoveNotMemberIPC(t *testing.T, sock string, reqCh chan<- *ipc.MemberRemoveReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			if req, ok := payload.(*ipc.MemberRemoveReq); ok {
				if reqCh != nil {
					reqCh <- req
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{
					Code:    ipc.CodeNotMember,
					GroupID: &req.GroupID,
					Message: "target is not a member",
				})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func serveESPFleetMemberRemoveUncertainIPC(t *testing.T, sock string, reqCh chan<- *ipc.MemberRemoveReq) func() {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, payload, err := ipc.ReadAndDecode(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			if req, ok := payload.(*ipc.MemberRemoveReq); ok {
				if reqCh != nil {
					reqCh <- req
				}
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}
