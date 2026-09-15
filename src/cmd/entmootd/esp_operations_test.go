package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
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
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox/mailboxtest"
	"entmoot/pkg/entmoot/membership"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/publicmoot"
	"entmoot/pkg/entmoot/store/storetest"
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

func TestESPPolicyPayloadRejectsFileSourceAndConflictingSelectors(t *testing.T) {
	if _, err := resolveESPGroupPolicyPayload(groupPolicyPayload{PolicySource: "file:/tmp/policy.json"}); err == nil {
		t.Fatal("resolveESPGroupPolicyPayload file source succeeded, want error")
	}
	if _, err := resolveESPGroupPolicyPayload(groupPolicyPayload{PolicySource: "preset:standard", Preset: "relaxed"}); err == nil {
		t.Fatal("resolveESPGroupPolicyPayload conflicting selectors succeeded, want error")
	}
	resolved, err := resolveESPGroupPolicyPayload(groupPolicyPayload{Preset: "relaxed"})
	if err != nil {
		t.Fatalf("resolveESPGroupPolicyPayload relaxed: %v", err)
	}
	if resolved.Policy == nil || *resolved.Policy != entpolicy.Relaxed() || resolved.Source != "preset:relaxed" {
		t.Fatalf("resolved policy = %+v, want relaxed preset", resolved)
	}
}

func TestESPOperationGroupPolicyReportUpdateAndClear(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(3)
	dataDir := t.TempDir()
	exec := espOperationExecutor{dataDir: dataDir}

	raw, err := exec.GroupPolicyReport(ctx, gid)
	if err != nil {
		t.Fatalf("GroupPolicyReport initial: %v", err)
	}
	var initial groupPolicyReport
	if err := json.Unmarshal(raw, &initial); err != nil {
		t.Fatalf("initial report JSON: %v", err)
	}
	if initial.PolicyConfigured || initial.EffectiveMode != groupPolicyModeLegacy {
		t.Fatalf("initial report = %+v, want legacy unconfigured", initial)
	}

	updatedRaw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "group_policy_update",
		GroupID: gid,
		Payload: json.RawMessage(`{"preset":"standard"}`),
	}, nil)
	if err != nil {
		t.Fatalf("group_policy_update: %v", err)
	}
	var updated groupPolicyReport
	if err := json.Unmarshal(updatedRaw, &updated); err != nil {
		t.Fatalf("updated report JSON: %v", err)
	}
	if !updated.PolicyConfigured || updated.Policy == nil || *updated.Policy != entpolicy.Standard() || updated.Source != "preset:standard" {
		t.Fatalf("updated report = %+v, want standard configured", updated)
	}

	clearedRaw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "group_policy_clear",
		GroupID: gid,
		Payload: json.RawMessage(`{}`),
	}, nil)
	if err != nil {
		t.Fatalf("group_policy_clear: %v", err)
	}
	var cleared groupPolicyReport
	if err := json.Unmarshal(clearedRaw, &cleared); err != nil {
		t.Fatalf("cleared report JSON: %v", err)
	}
	if cleared.PolicyConfigured || cleared.Source != "clear" || cleared.EffectiveMode != groupPolicyModeLegacy {
		t.Fatalf("cleared report = %+v, want clear legacy", cleared)
	}
}

func TestESPOperationGroupPolicyUpdatePublishesToConfiguredSocket(t *testing.T) {
	ctx := context.Background()
	gid := testESPGroupID(4)
	dataDir := t.TempDir()
	sock := testUnixSocketPath(t)
	publishReqCh := make(chan *ipc.PublishReq, 1)
	stop := serveESPPublishIPC(t, sock, publishReqCh)
	defer stop()
	exec := espOperationExecutor{dataDir: dataDir, socketPath: sock}

	raw, err := exec.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "group_policy_update",
		GroupID: gid,
		Payload: json.RawMessage(`{"preset":"standard"}`),
	}, nil)
	if err != nil {
		t.Fatalf("group_policy_update: %v", err)
	}
	var report groupPolicyReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("report JSON: %v", err)
	}
	if !report.Published || !report.RuntimeAppliedKnown {
		t.Fatalf("report = %+v, want published runtime-applied report", report)
	}
	select {
	case req := <-publishReqCh:
		if req.GroupID == nil || *req.GroupID != gid {
			t.Fatalf("publish group = %v, want %s", req.GroupID, gid)
		}
		if len(req.Topics) != 1 || req.Topics[0] != entpolicy.UpdateTopic {
			t.Fatalf("publish topics = %v, want [%s]", req.Topics, entpolicy.UpdateTopic)
		}
		var update entpolicy.Update
		if err := json.Unmarshal(req.Content, &update); err != nil {
			t.Fatalf("policy update JSON: %v", err)
		}
		if update.GroupID != gid || update.Policy == nil || *update.Policy != entpolicy.Standard() {
			t.Fatalf("policy update = %+v, want standard update for %s", update, gid)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for publish on configured socket")
	}
}

func TestESPOperationPublicPublishRequiresESPURL(t *testing.T) {
	_, err := espOperationExecutor{}.ExecuteSignRequest(context.Background(), esphttp.SignRequest{
		Kind:    "group_public_publish",
		GroupID: testESPGroupID(5),
		Payload: json.RawMessage(`{}`),
	}, nil)
	var opErr *esphttp.OperationError
	if !errors.As(err, &opErr) || opErr.HTTPStatus != http.StatusBadRequest || opErr.Code != "bad_request" {
		t.Fatalf("group_public_publish err = %v, want 400 bad_request", err)
	}
}

func TestESPOperationPublicPublishUsesExecutorIdentity(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	gid := testESPGroupID(5)
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	founder := testESPNodeInfo(t, id.PublicKey)
	createTestGroup(t, dataDir, gid, id, founder)

	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	if err := state.SetGroupMetadata(ctx, gid, json.RawMessage(`{"name":"Public Agents","visibility":"public","join_mode":"invite_only","tags":["ios"]}`)); err != nil {
		t.Fatalf("SetGroupMetadata: %v", err)
	}
	policyStore, err := entpolicy.OpenFileStore(dataDir)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	policy := entpolicy.Standard()
	if err := policyStore.Put(ctx, gid, policy); err != nil {
		t.Fatalf("policy Put: %v", err)
	}

	var posted publicmoot.Descriptor
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/public-moots" {
			t.Fatalf("request = %s %s, want POST /v1/public-moots", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
			t.Fatalf("decode descriptor: %v", err)
		}
		if err := publicmoot.Verify(posted); err != nil {
			t.Fatalf("Verify descriptor: %v", err)
		}
		if posted.GroupID != gid || !bytes.Equal(posted.Founder.EntmootPubKey, id.PublicKey) {
			t.Fatalf("posted descriptor group/founder = %s/%x, want %s/%x", posted.GroupID, posted.Founder.EntmootPubKey, gid, id.PublicKey)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"indexed"}`))
	}))
	defer server.Close()

	raw, err := espOperationExecutor{dataDir: dataDir, identity: id}.ExecuteSignRequest(ctx, esphttp.SignRequest{
		Kind:    "group_public_publish",
		GroupID: gid,
		Payload: json.RawMessage(fmt.Sprintf(`{"esp_url":%q}`, server.URL)),
	}, nil)
	if err != nil {
		t.Fatalf("group_public_publish: %v", err)
	}
	var result groupPublicPublishResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("publish result JSON: %v", err)
	}
	if result.Status != "published" || result.GroupID != gid || result.ESPURL != server.URL || !json.Valid(result.Response) {
		t.Fatalf("publish result = %+v, want published response", result)
	}
	if len(posted.Signature) == 0 {
		t.Fatal("posted descriptor signature is empty")
	}
}

func TestLocalGroupCatalogIgnoresBadStoredMetadata(t *testing.T) {
	dataDir := t.TempDir()
	gid := testESPGroupID(2)
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	info := testESPNodeInfo(t, id.PublicKey)
	createTestGroup(t, dataDir, gid, id, info)
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

func TestNormalizeGroupMetadataValidatesVisibilityAndJoinMode(t *testing.T) {
	raw, err := normalizeGroupMetadata(groupCreatePayload{
		Visibility: "PUBLIC",
		JoinMode:   groupJoinModeOpenInvite,
	})
	if err != nil {
		t.Fatalf("normalizeGroupMetadata valid enums: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got["visibility"] != groupVisibilityPublic || got["join_mode"] != groupJoinModeOpenInvite {
		t.Fatalf("metadata = %#v, want normalized visibility/join_mode", got)
	}

	for _, tc := range []struct {
		name    string
		payload groupCreatePayload
	}{
		{name: "visibility", payload: groupCreatePayload{Visibility: "friends"}},
		{name: "join_mode", payload: groupCreatePayload{JoinMode: "openInvite"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeGroupMetadata(tc.payload); err == nil {
				t.Fatal("normalizeGroupMetadata succeeded, want error")
			}
		})
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

func TestESPCreateGroupJoinFailureLeavesNoUsableOpenInvite(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sock := testUnixSocketPath(t)
	stop := serveESPGroupCreateIPC(t, sock, id.PublicKey, false)
	defer stop()
	state := esphttp.NewMemoryStateStore()
	req := testGroupCreateRequest("req-open-invite-join-rollback")
	req.Payload = json.RawMessage(`{"name":"ops","visibility":"public","join_mode":"open_invite","issuer_url":"http://127.0.0.1:8911"}`)
	exec := espOperationExecutor{
		dataDir:       dataDir,
		identity:      id,
		socketPath:    sock,
		timeout:       time.Second,
		metadataStore: state,
		stateStore:    state,
	}
	gid, err := groupIDForCreateRequest(req)
	if err != nil {
		t.Fatalf("groupIDForCreateRequest: %v", err)
	}
	_, err = exec.ExecuteSignRequest(ctx, req, nil)
	if err == nil {
		t.Fatal("ExecuteSignRequest succeeded, want join failure")
	}
	invites, err := state.ListOpenInvitesByGroup(ctx, gid)
	if err != nil {
		t.Fatalf("ListOpenInvitesByGroup: %v", err)
	}
	for _, invite := range invites {
		if !invite.Revoked {
			t.Fatalf("usable open invite remains after failed group creation: %+v", invite)
		}
	}
	if _, ok, err := state.GetGroupMetadata(ctx, gid); err != nil {
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
	if len(device.AdminGroups) != 1 || device.AdminGroups[0] != result.GroupID {
		t.Fatalf("device admin groups = %v, want [%s]", device.AdminGroups, result.GroupID)
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

func createTestGroup(t *testing.T, dataDir string, gid entmoot.GroupID, id *keystore.Identity, founder entmoot.NodeInfo) {
	t.Helper()
	group, err := membership.Create(dataDir, id, founder, gid, membership.DefaultPolicy(), 1_700_000_000_000)
	if err != nil {
		t.Fatalf("membership.Create: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("membership close: %v", err)
	}
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

func serveESPGroupCreateIPC(t *testing.T, sock string, pub []byte, joinOK bool) func() {
	t.Helper()
	node := testESPNodeInfo(t, pub)
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
					MemberID:      *node.MemberID,
					PeerID:        node.PeerID,
					EntmootPubKey: append([]byte(nil), pub...),
					Running:       true,
				})
			case *ipc.InviteAuthorityCheckReq:
				_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
					Status:     "ok",
					GroupID:    v.GroupID,
					RosterHead: entmoot.RosterEntryID{},
					Members:    1,
				})
			case *ipc.JoinGroupReq:
				if joinOK {
					if v.LocalGroupID == nil {
						t.Error("create join request must activate the enrolled local group")
					} else {
						_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: *v.LocalGroupID, Members: 1})
					}
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

func serveESPPublishIPC(t *testing.T, sock string, publishReqCh chan<- *ipc.PublishReq) func() {
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
			case *ipc.PublishReq:
				if publishReqCh != nil {
					req := *v
					publishReqCh <- &req
				}
				groupID := entmoot.GroupID{}
				if v.GroupID != nil {
					groupID = *v.GroupID
				}
				_ = ipc.EncodeAndWrite(conn, &ipc.PublishResp{GroupID: groupID, TimestampMS: time.Now().UnixMilli()})
			default:
				_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "unexpected request"})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func testESPNodeInfo(t *testing.T, pub []byte) entmoot.NodeInfo {
	t.Helper()
	member, err := entmoot.MemberIDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := entmoot.PeerIDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return entmoot.NodeInfo{MemberID: &member, PeerID: peer, EntmootPubKey: append([]byte(nil), pub...)}
}

func TestGroupCreateOpenInviteIssuerURLForUsesEnvFallback(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_URL", "http://127.0.0.1:8911")
	issuer, err := groupCreateOpenInviteIssuerURLFor("")
	if err != nil {
		t.Fatalf("groupCreateOpenInviteIssuerURLFor env fallback: %v", err)
	}
	if issuer != "http://127.0.0.1:8911" {
		t.Fatalf("issuer = %q, want env fallback", issuer)
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
	info := testESPNodeInfo(t, id.PublicKey)
	createTestGroup(t, dataDir, gid, id, info)
	state := esphttp.NewMemoryStateStore()
	now := time.Now().UnixMilli()
	if _, err := state.UpsertLiveAgentConfig(ctx, esphttp.LiveAgentConfig{
		GroupID:        gid,
		MemberID:       *info.MemberID,
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
		MemberID:     *info.MemberID,
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
	if len(members) != 1 || members[0].MemberID != *info.MemberID || members[0].PeerID != info.PeerID || members[0].Live == nil {
		t.Fatalf("members = %+v, want live state", members)
	}
	if members[0].Live.Status != esphttp.LiveStatusOnline || members[0].Live.Mode != esphttp.LiveModeOperator {
		t.Fatalf("live state = %+v, want online operator", members[0].Live)
	}
	otherGroup := testESPGroupID(42)
	for _, profile := range []esphttp.NodeProfileRecord{
		{MemberID: *info.MemberID, EntmootPubKey: encodeBase64(info.EntmootPubKey), Hostname: "current", Source: esphttp.NodeProfileSourceMemberProfile, SourceGroupID: &gid, ObservedAtMS: now},
		{MemberID: *info.MemberID, EntmootPubKey: encodeBase64(testAgentLiveAuthor(99).EntmootPubKey), Hostname: "stale-key", Source: esphttp.NodeProfileSourceMemberProfile, SourceGroupID: &gid, ObservedAtMS: now + 1},
		{MemberID: *info.MemberID, EntmootPubKey: encodeBase64(info.EntmootPubKey), Hostname: "other-room", Source: esphttp.NodeProfileSourceMemberProfile, SourceGroupID: &otherGroup, ObservedAtMS: now + 2},
	} {
		if _, _, err := state.UpsertNodeProfile(ctx, profile); err != nil {
			t.Fatal(err)
		}
	}
	service := mailboxtest.New(t, storetest.New(t), nil)
	handler, err := esphttp.NewHandler(esphttp.Config{Token: "catalog-test", Service: service, State: state, Groups: catalog})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/groups/"+gid.String()+"/members", nil)
	request.Header.Set("Authorization", "Bearer catalog-test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("member HTTP response: %d %s", response.Code, response.Body.String())
	}
	var result struct {
		Members []esphttp.MemberSummary `json:"members"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Members) != 1 || result.Members[0].MemberID != *info.MemberID || result.Members[0].PeerID != info.PeerID || result.Members[0].GlobalHostname != "current" || result.Members[0].Live == nil || result.Members[0].Live.Status != esphttp.LiveStatusOnline {
		t.Fatalf("HTTP member projection lost identity or trusted stale profiles: %+v", result.Members)
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
