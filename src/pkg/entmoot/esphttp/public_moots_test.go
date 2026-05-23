package esphttp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/publicmoot"
	"entmoot/pkg/entmoot/store"
)

func TestHandlerPublicMootsPostListAndGetWithoutMembership(t *testing.T) {
	gid := testGroupID(61)
	state := NewMemoryStateStore()
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(2_000) }, nil, state, nil)
	desc := mustPublicMootDescriptor(t, gid, nil, 1_000)

	posted := doPublicJSONRequest[struct {
		Status     string                   `json:"status"`
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusAccepted)
	if posted.Status != "indexed" || posted.PublicMoot.Descriptor.GroupID != gid || posted.PublicMoot.Status != PublicMootStatusListed {
		t.Fatalf("posted = %+v, want indexed listed descriptor", posted)
	}
	if posted.PublicMoot.MirrorState != PublicMootMirrorNone || posted.PublicMoot.MessageHistoryAvailable {
		t.Fatalf("mirror fields = %q/%v, want none/false", posted.PublicMoot.MirrorState, posted.PublicMoot.MessageHistoryAvailable)
	}
	if posted.PublicMoot.PolicySummary == "" {
		t.Fatalf("policy summary empty: %+v", posted.PublicMoot)
	}

	list := doPublicJSONRequest[struct {
		PublicMoots []PublicMootDirectoryEntry `json:"public_moots"`
	}](t, handler, http.MethodGet, "/v1/public-moots", nil, http.StatusOK)
	if len(list.PublicMoots) != 1 || list.PublicMoots[0].Descriptor.GroupID != gid {
		t.Fatalf("public list = %+v, want directory-only public moot", list.PublicMoots)
	}
	got := doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodGet, "/v1/public-moots/"+url.PathEscape(gid.String()), nil, http.StatusOK)
	if got.PublicMoot.Descriptor.GroupID != gid {
		t.Fatalf("public get = %+v, want %s", got.PublicMoot, gid.String())
	}

	groups := doJSONRequest[struct {
		Groups []GroupSummary `json:"groups"`
	}](t, handler, http.MethodGet, "/v1/groups", nil, http.StatusOK)
	if len(groups.Groups) != 0 {
		t.Fatalf("/v1/groups = %+v, want public directory not mixed into member groups", groups.Groups)
	}
}

func TestHandlerPublicMootsReportsMemberMirrorState(t *testing.T) {
	gid := testGroupID(161)
	handler := testMobileHandlerFull(t, gid, nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(2_000) }, nil, NewMemoryStateStore(), nil)
	desc := mustPublicMootDescriptor(t, gid, nil, 1_000)

	posted := doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusAccepted)
	if posted.PublicMoot.MirrorState != PublicMootMirrorMember || !posted.PublicMoot.MessageHistoryAvailable {
		t.Fatalf("posted mirror fields = %q/%v, want member/true", posted.PublicMoot.MirrorState, posted.PublicMoot.MessageHistoryAvailable)
	}
	list := doPublicJSONRequest[struct {
		PublicMoots []PublicMootDirectoryEntry `json:"public_moots"`
	}](t, handler, http.MethodGet, "/v1/public-moots", nil, http.StatusOK)
	if len(list.PublicMoots) != 1 || list.PublicMoots[0].MirrorState != PublicMootMirrorMember || !list.PublicMoots[0].MessageHistoryAvailable {
		t.Fatalf("list mirror fields = %+v, want member/true", list.PublicMoots)
	}
}

func TestHandlerPublicMootsDefaultGroupExistsDoesNotAdvertiseMirrorState(t *testing.T) {
	gid := testGroupID(162)
	msgStore := store.NewMemory()
	svc, err := mailbox.New(msgStore, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	handler, err := NewHandler(Config{
		Token:    "secret",
		AuthMode: AuthModeBearer,
		Service:  svc,
		State:    NewMemoryStateStore(),
		Clock:    func() time.Time { return time.UnixMilli(2_000) },
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	desc := mustPublicMootDescriptor(t, gid, nil, 1_000)

	posted := doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusAccepted)
	if posted.PublicMoot.MirrorState != PublicMootMirrorNone || posted.PublicMoot.MessageHistoryAvailable {
		t.Fatalf("mirror fields = %q/%v, want none/false with default groupExists", posted.PublicMoot.MirrorState, posted.PublicMoot.MessageHistoryAvailable)
	}
}

func TestHandlerPublicMootsIgnoresStaleDescriptor(t *testing.T) {
	gid := testGroupID(62)
	id := mustPublicMootIdentity(t)
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(3_000) }, nil, NewMemoryStateStore(), nil)
	newer := mustPublicMootDescriptor(t, gid, id, 2_000)
	stale := mustPublicMootDescriptor(t, gid, id, 1_500)

	_ = doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", newer, http.StatusAccepted)
	staleResp := doPublicJSONRequest[struct {
		Status     string                   `json:"status"`
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", stale, http.StatusOK)
	if staleResp.Status != "stale_ignored" || staleResp.PublicMoot.Descriptor.UpdatedAtMS != newer.UpdatedAtMS {
		t.Fatalf("stale response = %+v, want ignored current descriptor", staleResp)
	}
}

func TestHandlerPublicMootsRefreshPreservesIndexedAt(t *testing.T) {
	gid := testGroupID(63)
	id := mustPublicMootIdentity(t)
	nowMS := int64(3_000)
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(nowMS) }, nil, NewMemoryStateStore(), nil)
	first := mustPublicMootDescriptor(t, gid, id, 1_000)
	refreshed := mustPublicMootDescriptor(t, gid, id, 2_000)

	posted := doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", first, http.StatusAccepted)
	nowMS = 5_000
	updated := doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", refreshed, http.StatusAccepted)
	if updated.PublicMoot.IndexedAtMS != posted.PublicMoot.IndexedAtMS {
		t.Fatalf("indexed_at_ms after refresh = %d, want original %d", updated.PublicMoot.IndexedAtMS, posted.PublicMoot.IndexedAtMS)
	}
}

func TestHandlerPublicMootsRejectsFounderHijack(t *testing.T) {
	gid := testGroupID(64)
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(3_000) }, nil, NewMemoryStateStore(), nil)
	original := mustPublicMootDescriptor(t, gid, mustPublicMootIdentity(t), 1_000)
	hijack := mustPublicMootDescriptor(t, gid, mustPublicMootIdentity(t), 2_000)

	_ = doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", original, http.StatusAccepted)
	errResp := doPublicJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", hijack, http.StatusForbidden)
	if errResp.Error.Code != "public_moot_founder_mismatch" {
		t.Fatalf("error code = %q, want public_moot_founder_mismatch", errResp.Error.Code)
	}
	got := doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodGet, "/v1/public-moots/"+url.PathEscape(gid.String()), nil, http.StatusOK)
	if got.PublicMoot.Descriptor.UpdatedAtMS != original.UpdatedAtMS {
		t.Fatalf("stored descriptor updated_at_ms = %d, want original %d", got.PublicMoot.Descriptor.UpdatedAtMS, original.UpdatedAtMS)
	}
}

func TestHandlerPublicMootsHidesExpiredDescriptors(t *testing.T) {
	gid := testGroupID(65)
	id := mustPublicMootIdentity(t)
	nowMS := int64(1_000)
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(nowMS) }, nil, NewMemoryStateStore(), nil)
	desc := mustPublicMootDescriptorWithExpires(t, gid, id, 1_000, 1_500)

	_ = doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusAccepted)
	nowMS = 2_000
	list := doPublicJSONRequest[struct {
		PublicMoots []PublicMootDirectoryEntry `json:"public_moots"`
	}](t, handler, http.MethodGet, "/v1/public-moots", nil, http.StatusOK)
	if len(list.PublicMoots) != 0 {
		t.Fatalf("public list = %+v, want expired descriptor hidden", list.PublicMoots)
	}
	errResp := doPublicJSONRequest[errorEnvelope](t, handler, http.MethodGet, "/v1/public-moots/"+url.PathEscape(gid.String()), nil, http.StatusNotFound)
	if errResp.Error.Code != "public_moot_not_found" {
		t.Fatalf("get expired error = %+v, want public_moot_not_found", errResp)
	}
}

func TestHandlerPublicMootsRejectsExpiredDescriptor(t *testing.T) {
	gid := testGroupID(165)
	id := mustPublicMootIdentity(t)
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(2_000) }, nil, NewMemoryStateStore(), nil)
	desc := mustPublicMootDescriptorWithExpires(t, gid, id, 1_000, 1_500)

	errResp := doPublicJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusBadRequest)
	if errResp.Error.Code != "invalid_public_moot" {
		t.Fatalf("error code = %q, want invalid_public_moot", errResp.Error.Code)
	}
	list := doPublicJSONRequest[struct {
		PublicMoots []PublicMootDirectoryEntry `json:"public_moots"`
	}](t, handler, http.MethodGet, "/v1/public-moots", nil, http.StatusOK)
	if len(list.PublicMoots) != 0 {
		t.Fatalf("public list = %+v, want rejected expired descriptor absent", list.PublicMoots)
	}
}

func TestHandlerPublicMootsRejectsInvalidSignature(t *testing.T) {
	gid := testGroupID(66)
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, nil, nil, NewMemoryStateStore(), nil)
	desc := mustPublicMootDescriptor(t, gid, nil, 1_000)
	desc.Name = "tampered"

	errResp := doPublicJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusBadRequest)
	if errResp.Error.Code != "invalid_public_moot" {
		t.Fatalf("error code = %q, want invalid_public_moot", errResp.Error.Code)
	}
}

func TestHandlerPublicMootsRejectsBlockedGroupAndFounder(t *testing.T) {
	gid := testGroupID(67)
	secondGID := testGroupID(68)
	id := mustPublicMootIdentity(t)
	state := NewMemoryStateStore()
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(4_000) }, nil, state, nil)
	desc := mustPublicMootDescriptor(t, gid, id, 1_000)
	second := mustPublicMootDescriptor(t, secondGID, id, 1_100)

	_ = doPublicJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusAccepted)
	blocked := doJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPatch, "/v1/public-moots/"+url.PathEscape(gid.String())+"/index-status", map[string]any{"status": PublicMootStatusBlocked}, http.StatusOK)
	if blocked.PublicMoot.Status != PublicMootStatusBlocked {
		t.Fatalf("blocked status = %+v", blocked.PublicMoot)
	}

	newerSameGroup := mustPublicMootDescriptor(t, gid, id, 1_200)
	groupErr := doPublicJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", newerSameGroup, http.StatusForbidden)
	if groupErr.Error.Code != "public_moot_blocked" {
		t.Fatalf("group error = %+v, want blocked", groupErr)
	}
	founderErr := doPublicJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", second, http.StatusForbidden)
	if founderErr.Error.Code != "public_moot_blocked" {
		t.Fatalf("founder error = %+v, want blocked founder", founderErr)
	}
	list := doPublicJSONRequest[struct {
		PublicMoots []PublicMootDirectoryEntry `json:"public_moots"`
	}](t, handler, http.MethodGet, "/v1/public-moots", nil, http.StatusOK)
	if len(list.PublicMoots) != 0 {
		t.Fatalf("public list = %+v, want blocked entry hidden", list.PublicMoots)
	}
}

func TestHandlerPublicMootsSupportsPreemptiveBlockedGroup(t *testing.T) {
	gid := testGroupID(167)
	handler := testMobileHandlerFull(t, testGroupID(1), nil, &fakeCatalog{}, func() time.Time { return time.UnixMilli(4_000) }, nil, NewMemoryStateStore(), nil)
	blocked := doJSONRequest[struct {
		PublicMoot PublicMootDirectoryEntry `json:"public_moot"`
	}](t, handler, http.MethodPatch, "/v1/public-moots/"+url.PathEscape(gid.String())+"/index-status", map[string]any{"status": PublicMootStatusBlocked}, http.StatusOK)
	if blocked.PublicMoot.Status != PublicMootStatusBlocked || blocked.PublicMoot.Descriptor.GroupID != gid {
		t.Fatalf("blocked public moot = %+v, want descriptorless group block", blocked.PublicMoot)
	}

	desc := mustPublicMootDescriptor(t, gid, nil, 4_100)
	errResp := doPublicJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusForbidden)
	if errResp.Error.Code != "public_moot_blocked" {
		t.Fatalf("error code = %q, want public_moot_blocked", errResp.Error.Code)
	}
}

func TestStateStoresPublicMootsRejectHijackAndPreemptiveBlock(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		open func(*testing.T) StateStore
	}{
		{name: "memory", open: func(t *testing.T) StateStore { return NewMemoryStateStore() }},
		{name: "sqlite", open: func(t *testing.T) StateStore {
			store, err := OpenSQLiteStateStore(t.TempDir())
			if err != nil {
				t.Fatalf("OpenSQLiteStateStore: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			gid := testGroupID(168)
			original := mustPublicMootDescriptor(t, gid, mustPublicMootIdentity(t), 1_000)
			hijack := mustPublicMootDescriptor(t, gid, mustPublicMootIdentity(t), 2_000)
			if _, changed, err := store.UpsertPublicMoot(ctx, PublicMootRecord{Descriptor: original}, 2_000); err != nil || !changed {
				t.Fatalf("UpsertPublicMoot original changed=%v err=%v", changed, err)
			}
			if _, changed, err := store.UpsertPublicMoot(ctx, PublicMootRecord{Descriptor: hijack}, 3_000); !errors.Is(err, ErrPublicMootFounderMismatch) || changed {
				t.Fatalf("UpsertPublicMoot hijack changed=%v err=%v, want founder mismatch", changed, err)
			}
			got, ok, err := store.GetPublicMoot(ctx, gid)
			if err != nil || !ok || got.Descriptor.UpdatedAtMS != original.UpdatedAtMS {
				t.Fatalf("GetPublicMoot after hijack = %+v ok=%v err=%v, want original", got, ok, err)
			}

			blockedGID := testGroupID(169)
			blocked, ok, err := store.UpdatePublicMootIndexStatus(ctx, blockedGID, PublicMootStatusBlocked, 4_000)
			if err != nil || !ok || blocked.Descriptor.GroupID != blockedGID || blocked.Status != PublicMootStatusBlocked {
				t.Fatalf("UpdatePublicMootIndexStatus block = %+v ok=%v err=%v", blocked, ok, err)
			}
			blockedDesc := mustPublicMootDescriptor(t, blockedGID, mustPublicMootIdentity(t), 4_100)
			if _, changed, err := store.UpsertPublicMoot(ctx, PublicMootRecord{Descriptor: blockedDesc}, 4_200); !errors.Is(err, ErrPublicMootBlocked) || changed {
				t.Fatalf("UpsertPublicMoot blocked changed=%v err=%v, want blocked", changed, err)
			}
		})
	}
}

func TestSQLiteStateStorePersistsPublicMootDirectory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	gid := testGroupID(69)
	desc := mustPublicMootDescriptor(t, gid, nil, 1_000)
	store, err := OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	if _, changed, err := store.UpsertPublicMoot(ctx, PublicMootRecord{Descriptor: desc}, 2_000); err != nil || !changed {
		t.Fatalf("UpsertPublicMoot changed=%v err=%v", changed, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	store, err = OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore reopen: %v", err)
	}
	defer store.Close()
	list, err := store.ListPublicMoots(ctx, PublicMootListFilter{Statuses: []string{PublicMootStatusListed}})
	if err != nil {
		t.Fatalf("ListPublicMoots: %v", err)
	}
	if len(list) != 1 || list[0].Descriptor.GroupID != gid || list[0].IndexedAtMS != 2_000 {
		t.Fatalf("list after reopen = %+v, want persisted descriptor", list)
	}
}

func doPublicJSONRequest[T any](t *testing.T, handler http.Handler, method, path string, body any, wantStatus int) T {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("Marshal request body: %v", err)
		}
		reader = bytes.NewReader(data)
	}
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, reader)
	handler.ServeHTTP(resp, req)
	if resp.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d\nbody=%s", method, path, resp.Code, wantStatus, resp.Body.String())
	}
	var out T
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("Unmarshal response: %v\n%s", err, resp.Body.String())
	}
	return out
}

func mustPublicMootIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	return id
}

func mustPublicMootDescriptor(t *testing.T, gid entmoot.GroupID, id *keystore.Identity, updatedAtMS int64) publicmoot.Descriptor {
	t.Helper()
	return mustPublicMootDescriptorWithExpires(t, gid, id, updatedAtMS, 0)
}

func mustPublicMootDescriptorWithExpires(t *testing.T, gid entmoot.GroupID, id *keystore.Identity, updatedAtMS, expiresAtMS int64) publicmoot.Descriptor {
	t.Helper()
	if id == nil {
		id = mustPublicMootIdentity(t)
	}
	desc := publicmoot.Descriptor{
		Type:        publicmoot.DescriptorType,
		GroupID:     gid,
		Name:        "Mars Hub",
		Description: "Public Mars coordination moot",
		Tags:        []string{"mars", "ops"},
		Visibility:  publicmoot.VisibilityPublic,
		JoinMode:    publicmoot.JoinModeInviteOnly,
		Policy:      entpolicy.Standard(),
		Founder: entmoot.NodeInfo{
			PilotNodeID:   45491,
			EntmootPubKey: ed25519.PublicKey(id.PublicKey),
		},
		Indexing: publicmoot.Indexing{
			Directory: true,
			Messages:  false,
		},
		UpdatedAtMS: updatedAtMS,
		ExpiresAtMS: expiresAtMS,
	}
	signed, err := publicmoot.Sign(desc, id)
	if err != nil {
		t.Fatalf("Sign descriptor: %v", err)
	}
	return signed
}
