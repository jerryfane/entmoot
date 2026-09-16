package esphttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
