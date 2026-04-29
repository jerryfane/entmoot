package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
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
	req := testGroupCreateRequest("req-rollback")
	exec := espOperationExecutor{
		dataDir:       dataDir,
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		metadataStore: metadata,
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
