package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/mailbox/mailboxtest"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store/storetest"
)

// Construct the observer wire request independently of the CLI flag so old
// servers fail behaviorally (unknown field), not because a new Go type is absent.
func postFollowAgent(t *testing.T, f *memberConnectFixture, key ed25519.PrivateKey, tamper bool) *httptest.ResponseRecorder {
	t.Helper()
	req := f.connectRequest(f.member, key, f.gid)
	if !tamper {
		memberInput := strings.Replace(esphttp.MemberConnectSigningInput(req), "\n", "/follow-agent-read-only/v1\n", 1)
		deviceInput := strings.Replace(esphttp.MemberConnectDeviceSigningInput(req), "\n", "/follow-agent-read-only/v1\n", 1)
		req.Signature = base64.StdEncoding.EncodeToString(f.member.Sign([]byte(memberInput)))
		req.DeviceSignature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, []byte(deviceInput)))
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	body["follow_agent"] = true
	raw, err = json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, esphttp.MemberConnectPath, bytes.NewReader(raw)))
	return rec
}

func TestESPFollowAgentTracksFutureMembershipAndRevocation(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	key := newDeviceKey(t)
	rec := postFollowAgent(t, f, key, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("observer enrollment: %d %s", rec.Code, rec.Body.String())
	}
	id := esphttp.SelfEnrolledDeviceID(key.Public().(ed25519.PublicKey))
	explicitKey := newDeviceKey(t)
	explicit := f.mustConnect(f.member, explicitKey, f.gid)
	assertGroups := func(want ...entmoot.GroupID) {
		t.Helper()
		rec := f.signed(id, key, http.MethodGet, "/v1/groups", nil)
		var body struct {
			Groups []esphttp.GroupSummary `json:"groups"`
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &body) != nil {
			t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
		}
		got := make(map[entmoot.GroupID]bool)
		for _, g := range body.Groups {
			got[g.GroupID] = true
		}
		if len(got) != len(want) {
			t.Fatalf("groups=%v want=%v", got, want)
		}
		for _, gid := range want {
			if !got[gid] {
				t.Fatalf("missing group %s", gid)
			}
		}
	}
	assertGroups(f.gid)
	requireESPError(t, f.history(id, key, f.otherGID), http.StatusForbidden, "not_member")
	// Agent A creates a private room AFTER its phone grant. B (the founder of
	// the original shared room and otherGID) never joins A's new room.
	future := testESPGroupID(9)
	mustCreateGroup(t, f.dataDir, future, f.member, membership.DefaultPolicy())
	assertGroups(f.gid, future)
	if rec := f.history(id, key, future); rec.Code != http.StatusOK {
		t.Fatalf("future history: %d %s", rec.Code, rec.Body.String())
	}
	if rec := f.history(explicit.DeviceID, explicitKey, future); rec.Code != http.StatusForbidden {
		t.Fatalf("explicit grant widened: %d", rec.Code)
	}
	// Reload from the durable registry, as after an ESP restart.
	reg, err := esphttp.LoadMemberDeviceRegistryOrEmpty(f.memberPath)
	if err != nil {
		t.Fatal(err)
	}
	f.handler, err = esphttp.NewHandler(esphttp.Config{
		AuthMode: esphttp.AuthModeDevice, Devices: f.registry, MemberDevices: reg,
		MemberRoster: &esphttp.MembershipRoster{Root: f.dataDir},
		Service:      mailboxtest.New(t, storetest.New(t), nil),
		Groups:       localGroupCatalog{dataDir: f.dataDir}, GroupExists: espGroupExists(f.dataDir),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertGroups(f.gid, future)
	// Membership removal applies to both direct history and the catalog.
	f.withGroup(f.gid, func(g *membership.Group) {
		if _, err := g.SignRecord(f.founder, membership.Record{Kind: membership.KindRemove, Subject: mustDaemonNodeInfo(t, f.member)}); err != nil {
			t.Fatal(err)
		}
	})
	assertGroups(future)
	requireESPError(t, f.history(id, key, f.gid), http.StatusForbidden, "not_member")
	devices := reg.Snapshot()
	for i := range devices {
		if devices[i].ID == id {
			devices[i].Disabled = true
		}
	}
	next, err := esphttp.NewDeviceRegistry(devices)
	if err != nil {
		t.Fatal(err)
	}
	reg.Replace(next)
	requireESPError(t, f.history(id, key, future), http.StatusForbidden, "device_disabled")
}

func TestESPFollowAgentRejectsWritesAndScopeTampering(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	key := newDeviceKey(t)
	requireESPError(t, postFollowAgent(t, f, key, true), http.StatusUnauthorized, "bad_signature")
	rec := postFollowAgent(t, f, key, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("enroll: %d %s", rec.Code, rec.Body.String())
	}
	id := esphttp.SelfEnrolledDeviceID(key.Public().(ed25519.PublicKey))
	for _, path := range []string{"/v1/groups", "/v1/groups/" + f.gid.String() + "/messages", "/v1/mailbox/ack", "/v1/sign-requests", "/v1/devices/current/push"} {
		requireESPError(t, f.signed(id, key, http.MethodPost, path, []byte(`{}`)), http.StatusForbidden, "observer_read_only")
	}
	// A fresh, valid member signature cannot silently turn this same phone
	// credential into an explicit grant with publication privileges.
	requireESPError(t, f.postConnect(f.connectRequest(f.member, key, f.gid)), http.StatusConflict, "device_scope_conflict")
	if rec := f.history(id, key, f.gid); rec.Code != http.StatusOK {
		t.Fatalf("read after refusals: %d %s", rec.Code, rec.Body.String())
	}
}

func TestESPFollowAgentCLIConnectAndFutureHistory(t *testing.T) {
	f := newMemberConnectFixture(t, memberConnectOptions{})
	server := httptest.NewServer(f.handler)
	defer server.Close()
	flags := daemonFlags(t, t.TempDir(), f.member)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdESPConnect(flags, []string{"-esp", server.URL, "-group", f.gid.String(), "-follow-agent"})
	})
	if code != exitOK || !strings.Contains(stdout, `"follow_agent":true`) {
		t.Fatalf("connect: %d %s %s", code, stdout, stderr)
	}
	future := testESPGroupID(10)
	mustCreateGroup(t, f.dataDir, future, f.member, membership.DefaultPolicy())
	code, stdout, stderr = captureCommandOutput(t, func() int { return cmdESPHistory(flags, []string{"-group", future.String()}) })
	if code != exitOK {
		t.Fatalf("future history: %d %s %s", code, stdout, stderr)
	}
	code, _, stderr = captureCommandOutput(t, func() int { return cmdESPConnect(flags, []string{"-esp", server.URL, "-group", f.gid.String()}) })
	if code == exitOK || !strings.Contains(stderr, "device_scope_conflict") {
		t.Fatalf("scope downgrade: %d %s", code, stderr)
	}
}
