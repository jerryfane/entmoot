package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/publicmoot"
)

func TestBuildPublicMootDescriptorRequiresFounderAndPublicVisibility(t *testing.T) {
	for _, visibility := range []string{groupVisibilityPrivate, groupVisibilityUnlisted} {
		t.Run(visibility, func(t *testing.T) {
			gf, gid := seedPublicMootLocalState(t, visibility, groupJoinModeInviteOnly, nil)
			_, err := buildPublicMootDescriptor(context.Background(), gf, gid, 1_000)
			if err == nil || !strings.Contains(err.Error(), "visibility") {
				t.Fatalf("buildPublicMootDescriptor err = %v, want visibility rejection", err)
			}
		})
	}

	t.Run("wrong founder key", func(t *testing.T) {
		gf, gid := seedPublicMootLocalState(t, groupVisibilityPublic, groupJoinModeInviteOnly, nil)
		other := mustGenerateGroupCreateIdentity(t)
		if err := other.Save(gf.identity); err != nil {
			t.Fatalf("save other identity: %v", err)
		}
		_, err := buildPublicMootDescriptor(context.Background(), gf, gid, 1_000)
		if err == nil || !strings.Contains(err.Error(), "not the group founder") {
			t.Fatalf("buildPublicMootDescriptor err = %v, want founder rejection", err)
		}
	})
}

func TestCmdGroupPublicDescriptorOutputsSignedDescriptor(t *testing.T) {
	gf, gid := seedPublicMootLocalState(t, groupVisibilityPublic, groupJoinModeInviteOnly, nil)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdGroupPublic(gf, []string{"descriptor", "-group", gid.String(), "--json"})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("descriptor code=%d stderr=%q", code, stderr)
	}
	desc, err := publicmoot.Parse([]byte(stdout))
	if err != nil {
		t.Fatalf("Parse descriptor: %v", err)
	}
	if err := publicmoot.Verify(desc); err != nil {
		t.Fatalf("Verify descriptor: %v", err)
	}
	if desc.GroupID != gid || desc.Visibility != groupVisibilityPublic || desc.Policy != entpolicy.Standard() {
		t.Fatalf("descriptor = %+v, want seeded public group with standard policy", desc)
	}
}

func TestCmdGroupPublicDescriptorHelpStopsBeforeSetup(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdGroupPublic(&globalFlags{}, []string{"descriptor", "-h"})
	})
	if code != exitOK || stdout != "" {
		t.Fatalf("descriptor help code/stdout = %d/%q", code, stdout)
	}
	if !strings.Contains(stderr, "Usage of group public descriptor") {
		t.Fatalf("descriptor help stderr = %q, want usage", stderr)
	}
	if strings.Contains(stderr, "invalid group id") || strings.Contains(stderr, "identity") {
		t.Fatalf("descriptor help stderr = %q, want no descriptor execution error", stderr)
	}
}

func TestCmdGroupPublicDescriptorPreservesEmbeddedOpenInvite(t *testing.T) {
	openInvite := map[string]any{
		"issuer_url": "https://esp.example.com",
		"token":      "open-token",
		"link":       "entmoot://open-invite?issuer=https%3A%2F%2Fesp.example.com&token=open-token",
	}
	gf, gid := seedPublicMootLocalState(t, groupVisibilityPublic, groupJoinModeOpenInvite, openInvite)
	desc, err := buildPublicMootDescriptor(context.Background(), gf, gid, 1_000)
	if err != nil {
		t.Fatalf("buildPublicMootDescriptor: %v", err)
	}
	if desc.OpenInvite == nil || desc.OpenInvite.Token != "open-token" || desc.OpenInvite.Link == "" {
		t.Fatalf("open_invite = %+v, want preserved descriptor", desc.OpenInvite)
	}
	if err := publicmoot.Verify(desc); err != nil {
		t.Fatalf("Verify descriptor: %v", err)
	}
}

func TestCmdGroupPublicDescriptorRejectsInviteOnlyOpenInviteMetadata(t *testing.T) {
	openInvite := map[string]any{
		"issuer_url": "https://esp.example.com",
		"token":      "open-token",
		"link":       "entmoot://open-invite?issuer=https%3A%2F%2Fesp.example.com&token=open-token",
	}
	gf, gid := seedPublicMootLocalState(t, groupVisibilityPublic, groupJoinModeInviteOnly, openInvite)
	_, err := buildPublicMootDescriptor(context.Background(), gf, gid, 1_000)
	if err == nil || !strings.Contains(err.Error(), "open_invite is not allowed") {
		t.Fatalf("buildPublicMootDescriptor err = %v, want invite_only open_invite rejection", err)
	}
}

func TestPublicOpenInviteCreatePersistsDescriptorMetadata(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "entmoot-public-open-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	id := mustGenerateGroupCreateIdentity(t)
	identityPath := filepath.Join(dir, "identity.json")
	if err := id.Save(identityPath); err != nil {
		t.Fatalf("save identity: %v", err)
	}
	gid := testGroupCreateID(0x7a)
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:     dir,
		Identity:    id,
		FounderNode: 77,
		GroupID:     gid,
		Name:        "Public Open",
		Visibility:  groupVisibilityPublic,
		JoinMode:    groupJoinModeOpenInvite,
		Policy:      ptrPolicy(entpolicy.Standard()),
		NowMS:       1_010,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	stop := serveGroupCreateOpenInviteIPC(t, controlSocketPath(dir), gid)
	defer stop()
	t.Setenv("ENTMOOT_ESP_URL", "https://esp.example.com")
	out, err := maybeCreateGroupOpenInvite(context.Background(), &globalFlags{data: dir}, state)
	if err != nil {
		t.Fatalf("maybeCreateGroupOpenInvite: %v", err)
	}
	meta := readGroupCreateMetadata(t, dir, gid)
	if _, ok := meta["open_invite"].(map[string]any); !ok {
		t.Fatalf("metadata open_invite = %#v, want object", meta["open_invite"])
	}
	desc, err := buildPublicMootDescriptor(context.Background(), &globalFlags{data: dir, identity: identityPath}, gid, 1_011)
	if err != nil {
		t.Fatalf("buildPublicMootDescriptor: %v", err)
	}
	if desc.OpenInvite == nil || desc.OpenInvite.Token != out.Token || desc.OpenInvite.Link != out.Link {
		t.Fatalf("descriptor open_invite = %+v, want generated invite %+v", desc.OpenInvite, out)
	}
}

func TestCmdGroupPublicPublishPostsDescriptor(t *testing.T) {
	gf, gid := seedPublicMootLocalState(t, groupVisibilityPublic, groupJoinModeInviteOnly, nil)
	var posted publicmoot.Descriptor
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/public-moots" {
			t.Fatalf("request = %s %s, want POST /v1/public-moots", r.Method, r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("Content-Type = %q, want application/json", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if err := publicmoot.Verify(posted); err != nil {
			t.Fatalf("Verify posted descriptor: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"indexed"}`))
	}))
	t.Cleanup(server.Close)

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdGroupPublic(gf, []string{"publish", "-group", gid.String(), "-esp-url", server.URL, "--json"})
	})
	if code != exitOK || stderr != "" {
		t.Fatalf("publish code=%d stderr=%q", code, stderr)
	}
	var out groupPublicPublishResult
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("unmarshal publish output: %v", err)
	}
	if out.Status != "published" || out.GroupID != gid || posted.GroupID != gid {
		t.Fatalf("publish output=%+v posted=%+v", out, posted)
	}
}

func TestCmdGroupPublicPublishHelpStopsBeforeSetup(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdGroupPublic(&globalFlags{}, []string{"publish", "-h"})
	})
	if code != exitOK || stdout != "" {
		t.Fatalf("publish help code/stdout = %d/%q", code, stdout)
	}
	if !strings.Contains(stderr, "Usage of group public publish") {
		t.Fatalf("publish help stderr = %q, want usage", stderr)
	}
	if strings.Contains(stderr, "invalid group id") || strings.Contains(stderr, "identity") {
		t.Fatalf("publish help stderr = %q, want no publish execution error", stderr)
	}
}

func seedPublicMootLocalState(t *testing.T, visibility, joinMode string, openInvite map[string]any) (*globalFlags, entmoot.GroupID) {
	t.Helper()
	dir := t.TempDir()
	id := mustGenerateGroupCreateIdentity(t)
	identityPath := filepath.Join(dir, "identity.json")
	if err := id.Save(identityPath); err != nil {
		t.Fatalf("save identity: %v", err)
	}
	gid := testGroupCreateID(byte(len(visibility) + len(joinMode) + 1))
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:      dir,
		Identity:     id,
		FounderNode:  42,
		GroupID:      gid,
		Name:         "Mars Hub",
		Description:  "Public Mars coordination moot",
		Tags:         []string{"mars", "ops", "mars"},
		Visibility:   visibility,
		JoinMode:     joinMode,
		Policy:       ptrPolicy(entpolicy.Standard()),
		PolicySource: "preset:standard",
		NowMS:        1_000,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	if openInvite != nil {
		meta := readGroupCreateMetadata(t, dir, gid)
		meta["open_invite"] = openInvite
		raw, err := json.Marshal(meta)
		if err != nil {
			t.Fatalf("marshal metadata: %v", err)
		}
		store, err := esphttp.OpenSQLiteStateStore(dir)
		if err != nil {
			t.Fatalf("OpenSQLiteStateStore: %v", err)
		}
		if err := store.SetGroupMetadata(context.Background(), gid, raw); err != nil {
			_ = store.Close()
			t.Fatalf("SetGroupMetadata: %v", err)
		}
		_ = store.Close()
	}
	if state.GroupID != gid {
		t.Fatalf("state group id = %s, want %s", state.GroupID.String(), gid.String())
	}
	return &globalFlags{data: dir, identity: identityPath}, gid
}
