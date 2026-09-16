package esphttp

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestListLiveAgentsReturnsTheConfigs pins the response body, not just the
// status. The route answered 200 with an EMPTY body: its payload is keyed by
// MemberID, encoding/json needs a TextMarshaler for map keys, and writeJSON
// discarded the encoder error after the status line had gone out. Every
// existing test asserted the authorization boolean or the status, so nothing
// noticed that the phone received nothing.
func TestListLiveAgentsReturnsTheConfigs(t *testing.T) {
	gid := testGroupID(1)
	state := NewMemoryStateStore()
	memberID := testMemberID(7)
	if _, err := state.UpsertLiveAgentConfig(context.Background(), LiveAgentConfig{
		GroupID: gid, MemberID: memberID, Enabled: true, Mode: "reply_on_mention",
	}); err != nil {
		t.Fatalf("UpsertLiveAgentConfig: %v", err)
	}
	handler, err := NewHandler(Config{
		Token:   "secret",
		Service: mustMailboxService(t, gid),
		State:   state,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/groups/"+gid.String()+"/live-agents", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if resp.Body.Len() == 0 {
		t.Fatal("body is empty: the response encoded to nothing")
	}
	var body struct {
		Configs []LiveAgentConfig         `json:"configs"`
		Members map[string]LiveAgentState `json:"members"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal %s: %v", resp.Body.String(), err)
	}
	if len(body.Configs) != 1 || body.Configs[0].MemberID != memberID {
		t.Fatalf("configs = %+v, want the one stored config", body.Configs)
	}
	if _, ok := body.Members[memberID.String()]; !ok {
		t.Fatalf("members = %+v, want a key for %s", body.Members, memberID.String())
	}
}

// TestEncodeFailureReachesTheConfiguredLogger pins where the diagnosis goes. An
// encoding failure cannot become an error response — the status line is gone —
// so the log is the only signal, and a daemon that configures Config.Logger
// must receive it. Sending it to slog.Default() instead would leave the
// operator with the same silence that hid the empty-body defect: a map keyed
// by a type without a TextMarshaler encodes to nothing, exactly like the
// unencodable value used here.
func TestEncodeFailureReachesTheConfiguredLogger(t *testing.T) {
	gid := testGroupID(1)
	var logged strings.Builder
	handler, err := NewHandler(Config{
		Token:   "secret",
		Service: mustMailboxService(t, gid),
		State:   NewMemoryStateStore(),
		Logger:  slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	const path = "/v1/groups/probe/live-agents"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	resp := httptest.NewRecorder()
	// A map keyed by a type with no TextMarshaler is the real shape of the
	// defect; json.Marshal refuses it exactly as it refused MemberID keys.
	handler.writeJSON(resp, req, http.StatusOK, map[[2]byte]string{{1, 2}: "x"})

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: the status line goes out before encoding", resp.Code, http.StatusOK)
	}
	if resp.Body.Len() != 0 {
		t.Fatalf("body = %q, want empty: this value cannot encode", resp.Body.String())
	}
	out := logged.String()
	if !strings.Contains(out, "response encoding failed") {
		t.Fatalf("configured logger received %q, want the encoding failure", out)
	}
	if !strings.Contains(out, path) || !strings.Contains(out, http.MethodGet) {
		t.Fatalf("log line %q names no route; an operator cannot find the request", out)
	}
}
