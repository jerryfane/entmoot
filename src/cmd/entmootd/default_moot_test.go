package main

import (
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/defaultmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/roster"
)

func TestDefaultMootStatusVerifiesDescriptor(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	desc := testDefaultMootDescriptorServer(t, testAgentLiveGroupID(0x91))

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootStatus(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootStatus code = %d stderr=%s", code, stderr)
	}
	var report defaultMootStatusReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal status: %v\n%s", err, stdout)
	}
	if !report.DescriptorVerified || report.GroupID != desc.GroupID.String() || report.Consent != defaultMootConsentUnconfigured {
		t.Fatalf("status report = %+v", report)
	}
	if report.Policy == nil || report.Policy.RetentionDays != entpolicy.DefaultRetentionDays {
		t.Fatalf("policy report = %+v", report.Policy)
	}
}

func TestDefaultMootStatusUsesLocalGroupWhenDescriptorUnavailable(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x95)
	writeDefaultMootLocalRoster(t, gf, gid, 42)
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if _, err := enableAgentLiveConfig(context.Background(), state, enableAgentLiveConfigOptions{
		groupID:           gid,
		nodeID:            42,
		mode:              "converse",
		topics:            []string{"chat/general"},
		actions:           []string{"reply"},
		maxActionsPerScan: entpolicy.DefaultLiveMaxActionsPerScan,
		maxActionBytes:    entpolicy.DefaultLiveMaxActionBytes,
	}); err != nil {
		t.Fatalf("enable live config: %v", err)
	}
	_ = state.Close()
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentJoined,
		GroupID: gid.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}
	t.Setenv(defaultmoot.EnvDescriptorURL, "http://127.0.0.1:1/unreachable-default-moot.json")

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootStatus(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootStatus code = %d stderr=%s", code, stderr)
	}
	var report defaultMootStatusReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal status: %v\n%s", err, stdout)
	}
	if report.DescriptorVerified || report.GroupID != gid.String() || !report.Joined || !report.LiveEnabled {
		t.Fatalf("status report = %+v", report)
	}
	if len(report.AllowedLiveActions) != 1 || report.AllowedLiveActions[0] != "reply" {
		t.Fatalf("allowed actions = %#v", report.AllowedLiveActions)
	}
	if report.RecommendedLiveConfig != nil || strings.Contains(stdout, "recommended_live_config") {
		t.Fatalf("status reported unverified recommended live config: %+v\n%s", report.RecommendedLiveConfig, stdout)
	}
}

func TestDefaultMootStatusDoesNotTreatStaleGroupDirAsJoined(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0xa1)
	if err := os.MkdirAll(groupDirPath(gf.data, gid), 0o700); err != nil {
		t.Fatalf("mkdir group dir: %v", err)
	}
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentJoined,
		GroupID: gid.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}
	t.Setenv(defaultmoot.EnvDescriptorURL, "http://127.0.0.1:1/unreachable-default-moot.json")

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootStatus(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootStatus code = %d stderr=%s", code, stderr)
	}
	var report defaultMootStatusReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal status: %v\n%s", err, stdout)
	}
	if report.Joined {
		t.Fatalf("status report = %+v, want not joined for stale group dir", report)
	}
}

func TestDefaultMootStatusDoesNotTreatOtherIdentityRosterAsJoined(t *testing.T) {
	joinedGF := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0xa6)
	writeDefaultMootLocalRoster(t, joinedGF, gid, 42)
	if err := saveDefaultMootLocalState(joinedGF.data, defaultMootLocalState{
		Consent: defaultMootConsentJoined,
		GroupID: gid.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}
	otherGF := testBootstrapGlobalFlags(t)
	otherGF.data = joinedGF.data
	if _, err := setup(otherGF); err != nil {
		t.Fatalf("setup other identity: %v", err)
	}
	t.Setenv(defaultmoot.EnvDescriptorURL, "http://127.0.0.1:1/unreachable-default-moot.json")

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootStatus(otherGF, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootStatus code = %d stderr=%s", code, stderr)
	}
	var report defaultMootStatusReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal status: %v\n%s", err, stdout)
	}
	if report.Joined {
		t.Fatalf("status report = %+v, want not joined for other identity roster", report)
	}
}

func TestDefaultMootDeclinePersistsConsent(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x98)
	testDefaultMootDescriptorServer(t, gid)

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootDecline(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootDecline code = %d stderr=%s", code, stderr)
	}
	state, ok := loadDefaultMootLocalState(gf.data)
	if !ok || state.Consent != defaultMootConsentDeclined || state.GroupID != gid.String() {
		t.Fatalf("local state = %+v ok=%t", state, ok)
	}
}

func TestDefaultMootDeclinePreservesJoinedGroup(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x99)
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentJoined,
		GroupID: gid.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootDecline(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootDecline code = %d stderr=%s", code, stderr)
	}
	state, ok := loadDefaultMootLocalState(gf.data)
	if !ok || state.Consent != defaultMootConsentDeclined || state.GroupID != gid.String() {
		t.Fatalf("local state = %+v ok=%t", state, ok)
	}
}

func TestDefaultMootDeclineRecordsLegacyJoinedGroup(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x9b)
	testDefaultMootDescriptorServer(t, gid)
	if err := os.MkdirAll(groupDirPath(gf.data, gid), 0o700); err != nil {
		t.Fatalf("mkdir group dir: %v", err)
	}
	if err := os.WriteFile(groupRosterPath(gf.data, gid), []byte(`{"node_id":1}`+"\n"), 0o600); err != nil {
		t.Fatalf("write roster: %v", err)
	}

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootDecline(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootDecline code = %d stderr=%s", code, stderr)
	}
	state, ok := loadDefaultMootLocalState(gf.data)
	if !ok || state.Consent != defaultMootConsentDeclined || state.GroupID != gid.String() {
		t.Fatalf("local state = %+v ok=%t", state, ok)
	}
	gids, err := selectServeGroupIDs(gf.data, nil, nil)
	if !errors.Is(err, errServeNoGroups) {
		t.Fatalf("selectServeGroupIDs gids=%v err=%v, want errServeNoGroups", gids, err)
	}
	gids, err = selectServeGroupIDs(gf.data, []string{gid.String()}, nil)
	if !errors.Is(err, errServeNoGroups) {
		t.Fatalf("explicit selectServeGroupIDs gids=%v err=%v, want errServeNoGroups", gids, err)
	}
}

func TestDefaultMootDeclineFailsWhenLocalGroupCannotBeResolved(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x9c)
	if err := os.MkdirAll(groupDirPath(gf.data, gid), 0o700); err != nil {
		t.Fatalf("mkdir group dir: %v", err)
	}
	t.Setenv(defaultmoot.EnvDescriptorURL, "http://127.0.0.1:1/unreachable-default-moot.json")

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootDecline(gf, []string{"--json"})
	})
	if code != exitTransport {
		t.Fatalf("cmdDefaultMootDecline code = %d stderr=%s", code, stderr)
	}
	if _, ok := loadDefaultMootLocalState(gf.data); ok {
		t.Fatal("decline persisted incomplete state")
	}
}

func TestDefaultMootDeclinePersistsOfflineWithoutResolvedGroup(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	t.Setenv(defaultmoot.EnvDescriptorURL, "http://127.0.0.1:1/unreachable-default-moot.json")

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootDecline(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootDecline code = %d stderr=%s", code, stderr)
	}
	state, ok := loadDefaultMootLocalState(gf.data)
	if !ok || state.Consent != defaultMootConsentDeclined || state.GroupID != "" {
		t.Fatalf("local state = %+v ok=%t", state, ok)
	}
}

func TestDefaultMootJoinDryRunUsesVerifiedDescriptor(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	desc := testDefaultMootDescriptorServer(t, testAgentLiveGroupID(0x92))
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootJoin(gf, []string{"--dry-run", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootJoin dry-run code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, desc.GroupID.String()) || !strings.Contains(stdout, "verified") {
		t.Fatalf("stdout = %q", stdout)
	}
	if _, ok := loadDefaultMootLocalState(gf.data); ok {
		t.Fatal("dry-run persisted default moot state")
	}
}

func TestDefaultMootJoinUsesDaemonJoinContractAndPersistsConsent(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	useShortDefaultMootDataDir(t, gf)
	desc := testDefaultMootDescriptorServer(t, testAgentLiveGroupID(0xa7))
	requests := startDefaultMootIPCServer(t, gf, func(payload any) any {
		if _, ok := payload.(*ipc.JoinGroupReq); ok {
			issuer := desc.Issuer
			return &ipc.JoinGroupResp{Status: "joined", GroupID: desc.GroupID, Issuer: &issuer, Members: 1}
		}
		return &ipc.ErrorFrame{Code: ipc.CodeInvalidArgument, Message: fmt.Sprintf("unexpected request %T", payload)}
	})

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootJoin(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootJoin code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	req := waitDefaultMootIPCRequest[*ipc.JoinGroupReq](t, requests)
	if req.OpenInvite == nil {
		t.Fatalf("JoinGroupReq.OpenInvite = nil")
	}
	if req.OpenInvite.IssuerURL != desc.OpenInvite.IssuerURL || req.OpenInvite.Token != desc.OpenInvite.Token {
		t.Fatalf("open invite = %+v, want descriptor invite", req.OpenInvite)
	}
	if req.OpenInvite.ExpectedGroupID == nil || *req.OpenInvite.ExpectedGroupID != desc.GroupID {
		t.Fatalf("expected group = %v, want %s", req.OpenInvite.ExpectedGroupID, desc.GroupID)
	}
	if req.GroupPolicy == nil || req.GroupPolicy.RetentionDays != entpolicy.DefaultRetentionDays {
		t.Fatalf("group policy = %+v", req.GroupPolicy)
	}
	if !strings.Contains(string(req.GroupMetadata), `"name":"The Ent Moot"`) || !strings.Contains(string(req.GroupMetadata), `"default_moot":true`) {
		t.Fatalf("group metadata = %s", req.GroupMetadata)
	}
	state, ok := loadDefaultMootLocalState(gf.data)
	if !ok || state.Consent != defaultMootConsentJoined || state.GroupID != desc.GroupID.String() || state.DescriptorIssuedAtMS != desc.IssuedAtMS {
		t.Fatalf("local state = %+v ok=%t", state, ok)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
	}
	if out["status"] != "joined" || out["name"] != defaultmoot.Name {
		t.Fatalf("stdout = %s", stdout)
	}
}

func TestDefaultMootJoinPublishesOptionalIntro(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	useShortDefaultMootDataDir(t, gf)
	desc := testDefaultMootDescriptorServer(t, testAgentLiveGroupID(0xa8))
	requests := startDefaultMootIPCServer(t, gf, func(payload any) any {
		switch payload.(type) {
		case *ipc.JoinGroupReq:
			issuer := desc.Issuer
			return &ipc.JoinGroupResp{Status: "joined", GroupID: desc.GroupID, Issuer: &issuer, Members: 1}
		case *ipc.PublishReq:
			return &ipc.PublishResp{GroupID: desc.GroupID, TimestampMS: time.Now().UnixMilli()}
		default:
			return &ipc.ErrorFrame{Code: ipc.CodeInvalidArgument, Message: fmt.Sprintf("unexpected request %T", payload)}
		}
	})

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootJoin(gf, []string{"--json", "--intro", "Hello from test agent"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootJoin code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	_ = waitDefaultMootIPCRequest[*ipc.JoinGroupReq](t, requests)
	pub := waitDefaultMootIPCRequest[*ipc.PublishReq](t, requests)
	if pub.GroupID == nil || *pub.GroupID != desc.GroupID {
		t.Fatalf("publish group = %v, want %s", pub.GroupID, desc.GroupID)
	}
	if len(pub.Topics) != 1 || pub.Topics[0] != "introductions" {
		t.Fatalf("publish topics = %#v", pub.Topics)
	}
	if string(pub.Content) != "Hello from test agent" {
		t.Fatalf("publish content = %q", string(pub.Content))
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
	}
	if out["intro_published"] != true {
		t.Fatalf("stdout = %s, want intro_published", stdout)
	}
}

func TestDefaultMootIntroSkipsWhenDaemonUnavailable(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0xa9)
	status, err := publishDefaultMootIntro(context.Background(), gf, gid, "Hello later", false)
	if err != nil {
		t.Fatalf("publishDefaultMootIntro: %v", err)
	}
	if status != "skipped_no_daemon" {
		t.Fatalf("intro status = %q, want skipped_no_daemon", status)
	}
}

func TestDefaultMootLiveOnRejectsDeclinedState(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x9a)
	testDefaultMootDescriptorServer(t, gid)
	if err := os.MkdirAll(groupDirPath(gf.data, gid), 0o700); err != nil {
		t.Fatalf("mkdir group dir: %v", err)
	}
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentDeclined,
		GroupID: gid.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootLiveOn(gf, []string{"--node", "42", "--json"})
	})
	if code != exitNotMember {
		t.Fatalf("cmdDefaultMootLiveOn code = %d stderr=%s", code, stderr)
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	defer state.Close()
	if cfg, ok, err := state.GetLiveAgentConfig(context.Background(), gid, 42); err != nil || ok {
		t.Fatalf("GetLiveAgentConfig after rejected live on = %+v ok=%t err=%v", cfg, ok, err)
	}
}

func TestDefaultMootLiveOnOffUsesRecommendedConfig(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x93)
	desc := testDefaultMootDescriptorServer(t, gid)
	writeDefaultMootLocalRoster(t, gf, gid, 42)
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootLiveOn(gf, []string{"--node", "42", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootLiveOn code = %d stderr=%s", code, stderr)
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	cfg, ok, err := state.GetLiveAgentConfig(context.Background(), desc.GroupID, 42)
	if err != nil || !ok {
		t.Fatalf("GetLiveAgentConfig err/ok = %v/%v", err, ok)
	}
	if !cfg.Enabled || cfg.Mode != "converse" || len(cfg.TopicFilters) != 1 || cfg.TopicFilters[0] != "chat/general" ||
		len(cfg.AllowedActions) != 1 || cfg.AllowedActions[0] != "reply" || cfg.MaxActionsPerScan != entpolicy.DefaultLiveMaxActionsPerScan {
		t.Fatalf("live config = %+v", cfg)
	}
	_ = state.Close()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootLiveOff(gf, []string{"--node", "42", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootLiveOff code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, `"disabled":1`) {
		t.Fatalf("live off stdout = %q", stdout)
	}
}

func TestDefaultMootLiveOnRejectsStaleGroupDir(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x9d)
	testDefaultMootDescriptorServer(t, gid)
	if err := os.MkdirAll(groupDirPath(gf.data, gid), 0o700); err != nil {
		t.Fatalf("mkdir group dir: %v", err)
	}

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootLiveOn(gf, []string{"--node", "42", "--json"})
	})
	if code != exitNotMember {
		t.Fatalf("cmdDefaultMootLiveOn code = %d stderr=%s", code, stderr)
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	defer state.Close()
	if cfg, ok, err := state.GetLiveAgentConfig(context.Background(), gid, 42); err != nil || ok {
		t.Fatalf("GetLiveAgentConfig after stale dir = %+v ok=%t err=%v", cfg, ok, err)
	}
}

func TestDefaultMootLiveOffFallsBackToLocalState(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x94)
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if _, err := enableAgentLiveConfig(context.Background(), state, enableAgentLiveConfigOptions{
		groupID:           gid,
		nodeID:            42,
		mode:              "converse",
		topics:            []string{"chat/general"},
		actions:           []string{"reply"},
		maxActionsPerScan: entpolicy.DefaultLiveMaxActionsPerScan,
		maxActionBytes:    entpolicy.DefaultLiveMaxActionBytes,
	}); err != nil {
		t.Fatalf("enable live config: %v", err)
	}
	_ = state.Close()
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentJoined,
		GroupID: gid.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}
	t.Setenv(defaultmoot.EnvDescriptorURL, "http://127.0.0.1:1/unreachable-default-moot.json")

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootLiveOff(gf, []string{"--node", "42", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootLiveOff code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, `"disabled":1`) || !strings.Contains(stdout, gid.String()) {
		t.Fatalf("live off stdout = %q", stdout)
	}
}

func TestDefaultMootLiveOffPrefersPersistedGroup(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	joinedGID := testAgentLiveGroupID(0x96)
	_ = testDefaultMootDescriptorServer(t, testAgentLiveGroupID(0x97))
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if _, err := enableAgentLiveConfig(context.Background(), state, enableAgentLiveConfigOptions{
		groupID:           joinedGID,
		nodeID:            42,
		mode:              "converse",
		topics:            []string{"chat/general"},
		actions:           []string{"reply"},
		maxActionsPerScan: entpolicy.DefaultLiveMaxActionsPerScan,
		maxActionBytes:    entpolicy.DefaultLiveMaxActionBytes,
	}); err != nil {
		t.Fatalf("enable live config: %v", err)
	}
	_ = state.Close()
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentJoined,
		GroupID: joinedGID.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootLiveOff(gf, []string{"--node", "42", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootLiveOff code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, joinedGID.String()) || !strings.Contains(stdout, `"disabled":1`) {
		t.Fatalf("live off stdout = %q", stdout)
	}
	state, err = esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	defer state.Close()
	if cfg, ok, err := state.GetLiveAgentConfig(context.Background(), joinedGID, 42); err != nil || ok {
		t.Fatalf("GetLiveAgentConfig after off = %+v ok=%t err=%v", cfg, ok, err)
	}
}

func TestDefaultMootLeaveBlocksServeAutoLoad(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x98)
	if err := os.MkdirAll(groupDirPath(gf.data, gid), 0o700); err != nil {
		t.Fatalf("mkdir group dir: %v", err)
	}
	if err := os.WriteFile(groupRosterPath(gf.data, gid), []byte(`{"node_id":1}`+"\n"), 0o600); err != nil {
		t.Fatalf("write roster: %v", err)
	}
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentJoined,
		GroupID: gid.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootLeave(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdDefaultMootLeave code = %d stderr=%s", code, stderr)
	}
	if _, err := os.Stat(groupRosterPath(gf.data, gid)); err != nil {
		t.Fatalf("roster should remain for audit/history: %v", err)
	}
	gids, err := selectServeGroupIDs(gf.data, nil, nil)
	if !errors.Is(err, errServeNoGroups) {
		t.Fatalf("selectServeGroupIDs gids=%v err=%v, want errServeNoGroups", gids, err)
	}
}

func TestServeAutoLoadDoesNotFetchDescriptorForGrouplessDecline(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0xa5)
	writeDefaultMootLocalRoster(t, gf, gid, 42)
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentDeclined,
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	t.Cleanup(srv.Close)
	t.Setenv(defaultmoot.EnvDescriptorURL, srv.URL)

	gids, err := selectServeGroupIDs(gf.data, nil, nil)
	if err != nil {
		t.Fatalf("selectServeGroupIDs: %v", err)
	}
	if len(gids) != 1 || gids[0] != gid {
		t.Fatalf("selected gids = %v, want only %s", gids, gid)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("descriptor fetches = %d, want none", got)
	}
}

func TestDefaultMootLeaveWarnsWhenDaemonRunning(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	shortData, err := os.MkdirTemp("/tmp", "entmoot-default-moot-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortData) })
	gf.data = shortData
	gid := testAgentLiveGroupID(0x9f)
	if err := os.MkdirAll(gf.data, 0o700); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	if err := saveDefaultMootLocalState(gf.data, defaultMootLocalState{
		Consent: defaultMootConsentJoined,
		GroupID: gid.String(),
	}); err != nil {
		t.Fatalf("save local default moot state: %v", err)
	}
	ln, err := net.Listen("unix", controlSocketPath(gf.data))
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	defer ln.Close()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdDefaultMootLeave(gf, []string{"--json"})
	})
	if code != exitControlUnavail {
		t.Fatalf("cmdDefaultMootLeave code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "restart_required") {
		t.Fatalf("stdout = %q, want restart_required", stdout)
	}
	state, ok := loadDefaultMootLocalState(gf.data)
	if !ok || state.Consent != defaultMootConsentDeclined || state.GroupID != gid.String() {
		t.Fatalf("local state = %+v ok=%t", state, ok)
	}
}

func TestBootstrapAgentDefaultMootChoiceIsExplicit(t *testing.T) {
	gf := testBootstrapGlobalFlags(t)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(gf, []string{"--yes", "--dry-run", "--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdBootstrapAgent code = %d stderr=%s", code, stderr)
	}
	var report bootstrapAgentReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal report: %v\n%s", err, stdout)
	}
	if report.DefaultMoot.Choice != "skip" || len(report.DefaultMoot.Commands) != 0 {
		t.Fatalf("default moot report = %+v", report.DefaultMoot)
	}

	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(gf, []string{"--dry-run", "--json", "--default-moot", "join"})
	})
	if code != exitOK {
		t.Fatalf("cmdBootstrapAgent join choice code = %d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "default-moot join") {
		t.Fatalf("stdout = %q, want default-moot join guidance", stdout)
	}
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal join-choice report: %v\n%s", err, stdout)
	}
	if len(report.Commands) < 2 {
		t.Fatalf("commands = %#v, want default-moot setup before serve", report.Commands)
	}
	joinIdx := strings.Index(report.Commands[0], "default-moot join")
	serveIdx := strings.Index(report.Commands[1], "serve")
	if joinIdx < 0 || serveIdx < 0 {
		t.Fatalf("commands = %#v, want default-moot join before serve", report.Commands)
	}

	appliedGF := testBootstrapGlobalFlags(t)
	gid := testAgentLiveGroupID(0x9e)
	testDefaultMootDescriptorServer(t, gid)
	code, _, stderr = captureCommandOutput(t, func() int {
		return cmdBootstrapAgent(appliedGF, []string{"--json", "--default-moot", " decline "})
	})
	if code != exitOK {
		t.Fatalf("cmdBootstrapAgent decline choice code = %d stderr=%s", code, stderr)
	}
	state, ok := loadDefaultMootLocalState(appliedGF.data)
	if !ok || state.Consent != defaultMootConsentDeclined || state.GroupID != gid.String() {
		t.Fatalf("local state = %+v ok=%t", state, ok)
	}
}

func TestDefaultMootJoinInputUsesVerifiedDescriptor(t *testing.T) {
	desc := testDefaultMootDescriptorServer(t, testAgentLiveGroupID(0xa4))
	path, cleanup, err := writeDefaultMootJoinInput(desc)
	if err != nil {
		t.Fatalf("writeDefaultMootJoinInput: %v", err)
	}
	defer cleanup()

	input, err := loadJoinInput(path)
	if err != nil {
		t.Fatalf("loadJoinInput: %v", err)
	}
	if input.expectedGroup == nil || *input.expectedGroup != desc.GroupID {
		t.Fatalf("expected group = %v, want %s", input.expectedGroup, desc.GroupID)
	}
	if input.openInvite == nil || input.openInvite.IssuerURL != desc.OpenInvite.IssuerURL || input.openInvite.Token != desc.OpenInvite.Token {
		t.Fatalf("open invite = %+v, want descriptor payload", input.openInvite)
	}
}

func TestDefaultMootJoinSuppressesInnerStdout(t *testing.T) {
	code, stdout, stderr := captureCommandOutput(t, func() int {
		code, err := runWithStdoutDiscarded(func() int {
			_, _ = os.Stdout.Write([]byte(`{"event":"joined"}` + "\n"))
			return exitOK
		})
		if err != nil {
			t.Fatalf("runWithStdoutDiscarded: %v", err)
		}
		if code != exitOK {
			return code
		}
		return printJSON(map[string]string{"status": "joined"})
	})
	if code != exitOK {
		t.Fatalf("code = %d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, "event") {
		t.Fatalf("stdout leaked inner join output: %q", stdout)
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("stdout is not single JSON object: %v\n%s", err, stdout)
	}
	if out["status"] != "joined" {
		t.Fatalf("stdout = %q", stdout)
	}
}

func startDefaultMootIPCServer(t *testing.T, gf *globalFlags, responder func(any) any) <-chan any {
	t.Helper()
	if err := os.MkdirAll(gf.data, 0o700); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	_ = os.Remove(controlSocketPath(gf.data))
	ln, err := net.Listen("unix", controlSocketPath(gf.data))
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	requests := make(chan any, 8)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				_, payload, err := ipc.ReadAndDecode(conn)
				if err != nil {
					return
				}
				requests <- payload
				if responder == nil {
					return
				}
				if resp := responder(payload); resp != nil {
					_ = ipc.EncodeAndWrite(conn, resp)
				}
			}()
		}
	}()
	return requests
}

func useShortDefaultMootDataDir(t *testing.T, gf *globalFlags) {
	t.Helper()
	shortData, err := os.MkdirTemp("/tmp", "entmoot-default-moot-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortData) })
	gf.data = shortData
}

func waitDefaultMootIPCRequest[T any](t *testing.T, requests <-chan any) T {
	t.Helper()
	select {
	case payload := <-requests:
		req, ok := payload.(T)
		if !ok {
			var zero T
			t.Fatalf("request = %T, want requested type", payload)
			return zero
		}
		return req
	case <-time.After(2 * time.Second):
		var zero T
		t.Fatalf("timed out waiting for IPC request")
		return zero
	}
}

func testDefaultMootDescriptorServer(t *testing.T, gid entmoot.GroupID) defaultmoot.Descriptor {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey descriptor: %v", err)
	}
	var raw []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(raw)
	}))
	t.Cleanup(srv.Close)
	desc := defaultmoot.Descriptor{
		Type:    defaultmoot.DescriptorType,
		Name:    defaultmoot.Name,
		GroupID: gid,
		OpenInvite: defaultmoot.OpenInviteDescriptor{
			IssuerURL: srv.URL,
			Token:     "open-token",
		},
		Issuer: entmoot.NodeInfo{
			PilotNodeID:   45491,
			EntmootPubKey: bytesOf(0x44, ed25519.PublicKeySize),
		},
		DefaultTopics: []string{"chat/general", "introductions"},
		RecommendedLiveConfig: defaultmoot.RecommendedLiveConfig{
			Mode:           "converse",
			AllowedActions: []string{"reply"},
			MaxActions:     entpolicy.DefaultLiveMaxActionsPerScan,
			MaxActionBytes: entpolicy.DefaultLiveMaxActionBytes,
		},
		Policy:     entpolicy.TheEntMootDefault(),
		IssuedAtMS: time.Now().UnixMilli(),
	}
	signed, err := defaultmoot.Sign(desc, priv)
	if err != nil {
		t.Fatalf("Sign descriptor: %v", err)
	}
	raw, err = json.Marshal(signed)
	if err != nil {
		t.Fatalf("Marshal descriptor: %v", err)
	}
	t.Setenv(defaultmoot.EnvDescriptorURL, srv.URL)
	t.Setenv(defaultmoot.EnvDescriptorPubKey, base64.StdEncoding.EncodeToString(pub))
	return signed
}

func writeDefaultMootLocalRoster(t *testing.T, gf *globalFlags, gid entmoot.GroupID, nodeID entmoot.NodeID) {
	t.Helper()
	s, err := setup(gf)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	rlog, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	defer rlog.Close()
	info := entmoot.NodeInfo{PilotNodeID: nodeID, EntmootPubKey: append([]byte(nil), s.identity.PublicKey...)}
	if err := rlog.Genesis(s.identity, info, time.Now().UnixMilli()); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
}

func bytesOf(v byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = v
	}
	return out
}
