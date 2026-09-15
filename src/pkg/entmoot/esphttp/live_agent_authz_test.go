package esphttp

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	entmoot "entmoot/pkg/entmoot"
)

// liveAgentAuthzCatalog lists two members of one group, which is all the
// authorization check consults.
type liveAgentAuthzCatalog struct {
	groupID entmoot.GroupID
	members []MemberSummary
}

func (c liveAgentAuthzCatalog) ListGroups(context.Context) ([]GroupSummary, error) {
	return []GroupSummary{{GroupID: c.groupID}}, nil
}

func (c liveAgentAuthzCatalog) GetGroup(_ context.Context, gid entmoot.GroupID) (GroupSummary, bool, error) {
	if gid != c.groupID {
		return GroupSummary{}, false, nil
	}
	return GroupSummary{GroupID: c.groupID}, true, nil
}

func (c liveAgentAuthzCatalog) ListMembers(_ context.Context, gid entmoot.GroupID) ([]MemberSummary, error) {
	if gid != c.groupID {
		return nil, nil
	}
	return c.members, nil
}

// TestMemberCannotWriteAnotherMembersLiveAgentConfig pins the rule the handler
// states in its own error message: "member can only manage its own live agent
// config". A live-agent config decides whether a node answers automatically,
// with which actions and under which topic filters, so a member able to write
// a peer's config can make that peer's node act — the peer's key signs the
// resulting messages, not the writer's.
func TestMemberCannotWriteAnotherMembersLiveAgentConfig(t *testing.T) {
	gid := testGroupID(1)

	attackerPub, attackerPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	victimPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	attackerID, err := entmoot.MemberIDFromPublicKey(attackerPub)
	if err != nil {
		t.Fatalf("MemberIDFromPublicKey: %v", err)
	}
	victimID, err := entmoot.MemberIDFromPublicKey(victimPub)
	if err != nil {
		t.Fatalf("MemberIDFromPublicKey: %v", err)
	}
	attackerPeer, err := entmoot.PeerIDFromPublicKey(attackerPub)
	if err != nil {
		t.Fatalf("PeerIDFromPublicKey: %v", err)
	}

	catalog := liveAgentAuthzCatalog{
		groupID: gid,
		members: []MemberSummary{
			{MemberID: attackerID, EntmootPubKey: base64.StdEncoding.EncodeToString(attackerPub)},
			{MemberID: victimID, EntmootPubKey: base64.StdEncoding.EncodeToString(victimPub)},
		},
	}
	handler, err := NewHandler(Config{
		Token:    "secret",
		AuthMode: AuthModeBearer,
		Service:  mustMailboxService(t, gid),
		State:    NewMemoryStateStore(),
		Groups:   catalog,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	probe := 0
	send := func(method, path string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(string(body)))
		timestamp := time.Now().UnixMilli()
		probe++
		nonce := "authz-probe-" + strconv.Itoa(probe)
		input := MemberSigningInput(method, path, attackerID, attackerPeer, attackerPub, timestamp, nonce, body)
		req.Header.Set(memberIDHeader, attackerID.String())
		req.Header.Set(memberPeerHeader, attackerPeer)
		req.Header.Set(memberPubKeyHeader, base64.StdEncoding.EncodeToString(attackerPub))
		req.Header.Set(timestampHeader, strconv.FormatInt(timestamp, 10))
		req.Header.Set(nonceHeader, nonce)
		req.Header.Set(memberSignatureHeader, base64.StdEncoding.EncodeToString(ed25519.Sign(attackerPriv, []byte(input))))
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		return resp
	}

	base := "/v1/groups/" + gid.String() + "/live-agents/"

	// The attacker may manage its own config: the narrow rule must not be
	// enforced by refusing everything.
	own, err := json.Marshal(map[string]any{"enabled": true, "mode": "reply_on_mention"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if resp := send(http.MethodPut, base+attackerID.String(), own); resp.Code != http.StatusOK {
		t.Fatalf("writing own config = %d, want %d: %s", resp.Code, http.StatusOK, resp.Body.String())
	}

	// The same signature against another member's path must be refused.
	hostile, err := json.Marshal(map[string]any{"enabled": true, "mode": "reply_on_mention", "topics": []string{"#"}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if resp := send(http.MethodPut, base+victimID.String(), hostile); resp.Code != http.StatusForbidden {
		t.Fatalf("writing another member's config = %d, want %d: %s", resp.Code, http.StatusForbidden, resp.Body.String())
	}
	if resp := send(http.MethodDelete, base+victimID.String(), nil); resp.Code != http.StatusForbidden {
		t.Fatalf("deleting another member's config = %d, want %d: %s", resp.Code, http.StatusForbidden, resp.Body.String())
	}
}
