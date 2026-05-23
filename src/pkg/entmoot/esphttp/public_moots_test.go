package esphttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/publicmoot"
	"entmoot/pkg/entmoot/store"
)

func TestStateStoresPublicMootsStoreNewestDescriptor(t *testing.T) {
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
			ctx := context.Background()
			state := tc.open(t)
			t.Cleanup(func() { _ = state.Close() })
			id := mustESPHTTPPublicMootIdentity(t)
			desc := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x41, 2_000))
			stale := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x41, 1_000))
			if _, stored, err := state.UpsertPublicMootDescriptor(ctx, PublicMootRecord{Descriptor: desc}, 3_000); err != nil || !stored {
				t.Fatalf("upsert fresh stored/err = %t/%v, want true/nil", stored, err)
			}
			got, stored, err := state.UpsertPublicMootDescriptor(ctx, PublicMootRecord{Descriptor: stale}, 3_100)
			if err != nil || stored {
				t.Fatalf("upsert stale stored/err = %t/%v, want false/nil", stored, err)
			}
			if got.Descriptor.UpdatedAtMS != desc.UpdatedAtMS {
				t.Fatalf("stored updated_at_ms = %d, want %d", got.Descriptor.UpdatedAtMS, desc.UpdatedAtMS)
			}
			listed, err := state.ListPublicMoots(ctx, PublicMootStatusListed)
			if err != nil {
				t.Fatalf("ListPublicMoots: %v", err)
			}
			if len(listed) != 1 || listed[0].Descriptor.GroupID != desc.GroupID || listed[0].Status != PublicMootStatusListed {
				t.Fatalf("listed = %+v, want one listed descriptor", listed)
			}
			var blockedGID entmoot.GroupID
			blockedGID[0] = 0x99
			blocked, ok, err := state.SetPublicMootIndexStatus(ctx, blockedGID, PublicMootStatusBlocked, 3_200)
			if err != nil || !ok {
				t.Fatalf("preemptive block ok/err = %t/%v, want true/nil", ok, err)
			}
			if blocked.Status != PublicMootStatusBlocked || blocked.Descriptor.GroupID != blockedGID {
				t.Fatalf("blocked record = %+v, want descriptor-less blocked group", blocked)
			}
			blockedList, err := state.ListPublicMoots(ctx, PublicMootStatusBlocked)
			if err != nil {
				t.Fatalf("ListPublicMoots blocked: %v", err)
			}
			if len(blockedList) != 1 || blockedList[0].Descriptor.GroupID != blockedGID {
				t.Fatalf("blocked list = %+v, want descriptor-less blocked group", blockedList)
			}
			otherID := mustESPHTTPPublicMootIdentity(t)
			hijack := mustESPHTTPPublicMootSign(t, otherID, testPublicMootDescriptor(0x41, 4_000))
			got, stored, err = state.UpsertPublicMootDescriptor(ctx, PublicMootRecord{Descriptor: hijack}, 4_100)
			if !errors.Is(err, ErrPublicMootFounderMismatch) || stored {
				t.Fatalf("upsert hijack stored/err = %t/%v, want false/founder mismatch", stored, err)
			}
			if !bytes.Equal(got.Descriptor.Founder.EntmootPubKey, desc.Founder.EntmootPubKey) {
				t.Fatalf("stored founder changed after hijack attempt")
			}
		})
	}
}

func TestHandlerPublicMootDirectoryListsWithoutGroupMembership(t *testing.T) {
	id := mustESPHTTPPublicMootIdentity(t)
	desc := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x42, 2_000))
	state := NewMemoryStateStore()
	handler := testPublicMootHandler(t, state, nil)

	post := doUnauthedJSONRequest[publicMootPostResponse](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusCreated)
	if post.Status != "indexed" || post.Moot.GroupID != desc.GroupID || post.Moot.MirrorState != PublicMootMirrorNone || post.Moot.MessageHistoryAvailable {
		t.Fatalf("post response = %+v, want directory-only indexed moot", post)
	}
	list := doUnauthedJSONRequest[struct {
		Moots []PublicMootDirectoryEntry `json:"moots"`
	}](t, handler, http.MethodGet, "/v1/public-moots", nil, http.StatusOK)
	if len(list.Moots) != 1 || list.Moots[0].GroupID != desc.GroupID || list.Moots[0].PolicySummary == "" {
		t.Fatalf("list = %+v, want one public moot with policy summary", list)
	}
	detail := doUnauthedJSONRequest[PublicMootDirectoryEntry](t, handler, http.MethodGet, "/v1/public-moots/"+desc.GroupID.String(), nil, http.StatusOK)
	if detail.GroupID != desc.GroupID || detail.IndexStatus != PublicMootStatusListed {
		t.Fatalf("detail = %+v, want listed public moot", detail)
	}
	groupErr := doUnauthedJSONRequest[errorEnvelope](t, handler, http.MethodGet, "/v1/groups", nil, http.StatusUnauthorized)
	if groupErr.Error.Code != "unauthorized" {
		t.Fatalf("unauth groups error = %+v, want unauthorized", groupErr)
	}
	groups := doJSONRequest[struct {
		Groups []GroupSummary `json:"groups"`
	}](t, handler, http.MethodGet, "/v1/groups", nil, http.StatusOK)
	if len(groups.Groups) != 0 {
		t.Fatalf("groups = %+v, want no local member groups", groups)
	}
}

func TestHandlerPublicMootDirectoryReportsMemberMirrorState(t *testing.T) {
	id := mustESPHTTPPublicMootIdentity(t)
	desc := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x43, 2_000))
	state := NewMemoryStateStore()
	handler := testPublicMootHandler(t, state, &fakeCatalog{groups: []GroupSummary{{GroupID: desc.GroupID}}})

	entry := doUnauthedJSONRequest[publicMootPostResponse](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusCreated).Moot
	if entry.MirrorState != PublicMootMirrorMember || !entry.MessageHistoryAvailable {
		t.Fatalf("entry mirror/history = %q/%v, want member/true", entry.MirrorState, entry.MessageHistoryAvailable)
	}
}

func TestHandlerPublicMootDirectoryMirrorStateUsesGroupExists(t *testing.T) {
	id := mustESPHTTPPublicMootIdentity(t)
	desc := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x48, 2_000))
	state := NewMemoryStateStore()
	catalog := &panicGetGroupCatalog{}
	handler := testPublicMootHandlerWithGroupExists(t, state, catalog, nil, func(_ context.Context, got entmoot.GroupID) (bool, error) {
		if got != desc.GroupID {
			t.Fatalf("GroupExists got %s, want %s", got.String(), desc.GroupID.String())
		}
		return false, nil
	})

	entry := doUnauthedJSONRequest[publicMootPostResponse](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusCreated).Moot
	if entry.MirrorState != PublicMootMirrorNone || entry.MessageHistoryAvailable {
		t.Fatalf("entry = %+v, want no mirror state from GroupExists=false", entry)
	}
	if catalog.getGroupCalled {
		t.Fatal("GetGroup was called while rendering public moot mirror state")
	}
}

func TestHandlerPublicMootDirectoryRejectsInvalidSignature(t *testing.T) {
	id := mustESPHTTPPublicMootIdentity(t)
	desc := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x44, 2_000))
	desc.Name = "tampered"
	handler := testPublicMootHandler(t, NewMemoryStateStore(), nil)
	errResp := doUnauthedJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", desc, http.StatusBadRequest)
	if errResp.Error.Code != "invalid_descriptor" {
		t.Fatalf("error = %+v, want invalid_descriptor", errResp)
	}
}

func TestHandlerPublicMootDirectoryIgnoresStaleDescriptor(t *testing.T) {
	id := mustESPHTTPPublicMootIdentity(t)
	fresh := testPublicMootDescriptor(0x45, 2_000)
	fresh.Name = "Fresh"
	stale := testPublicMootDescriptor(0x45, 1_000)
	stale.Name = "Stale"
	handler := testPublicMootHandler(t, NewMemoryStateStore(), nil)

	doUnauthedJSONRequest[publicMootPostResponse](t, handler, http.MethodPost, "/v1/public-moots", mustESPHTTPPublicMootSign(t, id, fresh), http.StatusCreated)
	staleResp := doUnauthedJSONRequest[publicMootPostResponse](t, handler, http.MethodPost, "/v1/public-moots", mustESPHTTPPublicMootSign(t, id, stale), http.StatusOK)
	if staleResp.Status != "stale_ignored" || staleResp.Moot.Name != "Fresh" {
		t.Fatalf("stale response = %+v, want stale_ignored fresh descriptor", staleResp)
	}
}

func TestHandlerPublicMootDirectoryRejectsFounderHijack(t *testing.T) {
	id := mustESPHTTPPublicMootIdentity(t)
	otherID := mustESPHTTPPublicMootIdentity(t)
	first := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x49, 2_000))
	hijack := mustESPHTTPPublicMootSign(t, otherID, testPublicMootDescriptor(0x49, 3_000))
	handler := testPublicMootHandler(t, NewMemoryStateStore(), nil)

	doUnauthedJSONRequest[publicMootPostResponse](t, handler, http.MethodPost, "/v1/public-moots", first, http.StatusCreated)
	errResp := doUnauthedJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", hijack, http.StatusConflict)
	if errResp.Error.Code != "public_moot_founder_mismatch" {
		t.Fatalf("hijack error = %+v, want public_moot_founder_mismatch", errResp)
	}
	detail := doUnauthedJSONRequest[PublicMootDirectoryEntry](t, handler, http.MethodGet, "/v1/public-moots/"+first.GroupID.String(), nil, http.StatusOK)
	if !bytes.Equal(detail.Founder.EntmootPubKey, first.Founder.EntmootPubKey) {
		t.Fatalf("detail founder changed after hijack attempt")
	}
}

func TestHandlerPublicMootDirectoryHidesExpiredDescriptor(t *testing.T) {
	id := mustESPHTTPPublicMootIdentity(t)
	desc := testPublicMootDescriptor(0x48, 4_000)
	desc.ExpiresAtMS = 8_000
	nowMS := int64(5_000)
	handler := testPublicMootHandlerWithClock(t, NewMemoryStateStore(), nil, func() time.Time {
		return time.UnixMilli(nowMS)
	})

	doUnauthedJSONRequest[publicMootPostResponse](t, handler, http.MethodPost, "/v1/public-moots", mustESPHTTPPublicMootSign(t, id, desc), http.StatusCreated)
	list := doUnauthedJSONRequest[struct {
		Moots []PublicMootDirectoryEntry `json:"moots"`
	}](t, handler, http.MethodGet, "/v1/public-moots", nil, http.StatusOK)
	if len(list.Moots) != 1 {
		t.Fatalf("list before expiry = %+v, want one moot", list)
	}
	nowMS = 9_000
	expiredList := doUnauthedJSONRequest[struct {
		Moots []PublicMootDirectoryEntry `json:"moots"`
	}](t, handler, http.MethodGet, "/v1/public-moots", nil, http.StatusOK)
	if len(expiredList.Moots) != 0 {
		t.Fatalf("list after expiry = %+v, want empty", expiredList)
	}
	errResp := doUnauthedJSONRequest[errorEnvelope](t, handler, http.MethodGet, "/v1/public-moots/"+desc.GroupID.String(), nil, http.StatusNotFound)
	if errResp.Error.Code != "public_moot_not_found" {
		t.Fatalf("detail after expiry error = %+v, want public_moot_not_found", errResp)
	}
}

func TestHandlerPublicMootDirectoryRejectsBlockedGroupAndFounder(t *testing.T) {
	id := mustESPHTTPPublicMootIdentity(t)
	first := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x46, 2_000))
	handler := testPublicMootHandler(t, NewMemoryStateStore(), nil)

	doUnauthedJSONRequest[publicMootPostResponse](t, handler, http.MethodPost, "/v1/public-moots", first, http.StatusCreated)
	for name, body := range map[string]any{
		"empty":  nil,
		"object": map[string]any{},
	} {
		errResp := doJSONRequest[errorEnvelope](t, handler, http.MethodPatch, "/v1/public-moots/"+first.GroupID.String()+"/index-status", body, http.StatusBadRequest)
		if errResp.Error.Code != "bad_request" {
			t.Fatalf("%s patch error = %+v, want bad_request", name, errResp)
		}
	}
	patch := doJSONRequest[PublicMootDirectoryEntry](t, handler, http.MethodPatch, "/v1/public-moots/"+first.GroupID.String()+"/index-status", map[string]any{"status": "blocked"}, http.StatusOK)
	if patch.IndexStatus != PublicMootStatusBlocked {
		t.Fatalf("patch = %+v, want blocked", patch)
	}
	newerBlockedGroup := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x46, 3_000))
	groupErr := doUnauthedJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", newerBlockedGroup, http.StatusForbidden)
	if groupErr.Error.Code != "public_moot_blocked" {
		t.Fatalf("blocked group error = %+v, want public_moot_blocked", groupErr)
	}
	blockedFounderOtherGroup := mustESPHTTPPublicMootSign(t, id, testPublicMootDescriptor(0x47, 3_000))
	founderErr := doUnauthedJSONRequest[errorEnvelope](t, handler, http.MethodPost, "/v1/public-moots", blockedFounderOtherGroup, http.StatusForbidden)
	if founderErr.Error.Code != "public_moot_blocked" {
		t.Fatalf("blocked founder error = %+v, want public_moot_blocked", founderErr)
	}
}

func testPublicMootHandler(t *testing.T, state StateStore, catalog GroupCatalog) http.Handler {
	t.Helper()
	return testPublicMootHandlerWithClock(t, state, catalog, func() time.Time {
		return time.UnixMilli(10_000)
	})
}

func testPublicMootHandlerWithClock(t *testing.T, state StateStore, catalog GroupCatalog, clock func() time.Time) http.Handler {
	t.Helper()
	return testPublicMootHandlerWithGroupExists(t, state, catalog, clock, nil)
}

func testPublicMootHandlerWithGroupExists(t *testing.T, state StateStore, catalog GroupCatalog, clock func() time.Time, groupExists GroupExistsFunc) http.Handler {
	t.Helper()
	if clock == nil {
		clock = func() time.Time { return time.UnixMilli(10_000) }
	}
	st := store.NewMemory()
	svc, err := mailbox.New(st, nil)
	if err != nil {
		t.Fatalf("mailbox.New: %v", err)
	}
	handler, err := NewHandler(Config{
		Token:       "secret",
		Service:     svc,
		State:       state,
		Groups:      catalog,
		GroupExists: groupExists,
		Clock:       clock,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

type panicGetGroupCatalog struct {
	getGroupCalled bool
}

func (c *panicGetGroupCatalog) ListGroups(context.Context) ([]GroupSummary, error) {
	return nil, nil
}

func (c *panicGetGroupCatalog) GetGroup(context.Context, entmoot.GroupID) (GroupSummary, bool, error) {
	c.getGroupCalled = true
	return GroupSummary{}, false, errors.New("GetGroup should not be called")
}

func (c *panicGetGroupCatalog) ListMembers(context.Context, entmoot.GroupID) ([]MemberSummary, error) {
	return nil, nil
}

func testPublicMootDescriptor(seed byte, updatedAtMS int64) publicmoot.Descriptor {
	var gid entmoot.GroupID
	gid[0] = seed
	return publicmoot.Descriptor{
		Type:        publicmoot.DescriptorType,
		GroupID:     gid,
		Name:        "Mars Hub",
		Description: "Public Mars coordination moot",
		Tags:        []string{"mars", "ops"},
		Visibility:  publicmoot.VisibilityPublic,
		JoinMode:    publicmoot.JoinModeInviteOnly,
		Policy:      policy.Standard(),
		Founder: entmoot.NodeInfo{
			PilotNodeID: 42,
		},
		Indexing: publicmoot.Indexing{
			Directory: true,
			Messages:  false,
		},
		UpdatedAtMS: updatedAtMS,
	}
}

func mustESPHTTPPublicMootIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return id
}

func mustESPHTTPPublicMootSign(t *testing.T, id *keystore.Identity, desc publicmoot.Descriptor) publicmoot.Descriptor {
	t.Helper()
	signed, err := publicmoot.Sign(desc, id)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return signed
}

func doUnauthedJSONRequest[T any](t *testing.T, handler http.Handler, method, path string, body any, wantStatus int) T {
	t.Helper()
	var reqBody *bytes.Reader
	if body == nil {
		reqBody = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reqBody = bytes.NewReader(data)
	}
	req := httptest.NewRequest(method, path, reqBody)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d body=%s", method, path, resp.Code, wantStatus, resp.Body.String())
	}
	var out T
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal response: %v\n%s", err, resp.Body.String())
	}
	return out
}
