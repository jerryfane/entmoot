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
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, true)
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
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, true)
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
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, true)
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
