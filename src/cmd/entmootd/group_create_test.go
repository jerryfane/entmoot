package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	entpolicy "entmoot/pkg/entmoot/policy"
)

func TestCreateGroupLocalStateDefaultsStandardPolicyAndMetadata(t *testing.T) {
	dir := t.TempDir()
	id := mustGenerateGroupCreateIdentity(t)
	gid := testGroupCreateID(0x61)
	opts, code := parseGroupCreateOptions([]string{
		"-name", "Ops Room",
		"-tag", " ops ",
		"-tag", "ios",
		"-tag", "ops",
	})
	if code != exitOK {
		t.Fatalf("parseGroupCreateOptions code = %d", code)
	}

	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:      dir,
		Identity:     id,
		FounderNode:  42,
		GroupID:      gid,
		Name:         opts.Name,
		Tags:         opts.Tags,
		Visibility:   opts.Visibility,
		JoinMode:     opts.JoinMode,
		Policy:       opts.Policy,
		PolicySource: opts.PolicySource,
		NowMS:        1_000,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	if state.PolicySummary == "" {
		t.Fatal("PolicySummary is empty")
	}
	assertStoredPolicy(t, dir, gid, entpolicy.Standard(), true)
	meta := readGroupCreateMetadata(t, dir, gid)
	if meta["name"] != "Ops Room" || meta["visibility"] != groupVisibilityPrivate || meta["join_mode"] != groupJoinModeInviteOnly {
		t.Fatalf("metadata = %+v", meta)
	}
	tags, ok := meta["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "ops" || tags[1] != "ios" {
		t.Fatalf("metadata tags = %#v, want normalized unique tags", meta["tags"])
	}
}

func TestParseGroupCreateOptionsHelpStopsBeforeCreate(t *testing.T) {
	opts, code := parseGroupCreateOptions([]string{"-h"})
	if code != exitOK {
		t.Fatalf("parseGroupCreateOptions help code = %d, want %d", code, exitOK)
	}
	if !opts.Help {
		t.Fatal("parseGroupCreateOptions help did not set Help")
	}
	if opts.Name != "" || opts.Policy != nil {
		t.Fatalf("help opts = %+v, want no creation options", opts)
	}
}

func TestCreateGroupLocalStatePolicyNonePreservesNoPolicy(t *testing.T) {
	dir := t.TempDir()
	id := mustGenerateGroupCreateIdentity(t)
	gid := testGroupCreateID(0x62)
	resolved, err := resolveGroupCreatePolicy("none")
	if err != nil {
		t.Fatalf("resolveGroupCreatePolicy none: %v", err)
	}
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:      dir,
		Identity:     id,
		FounderNode:  43,
		GroupID:      gid,
		Name:         "Legacy",
		Visibility:   groupVisibilityPrivate,
		JoinMode:     groupJoinModeInviteOnly,
		Policy:       resolved.Policy,
		PolicySource: resolved.Source,
		NowMS:        1_001,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	if state.Policy != nil || state.PolicySummary != "" {
		t.Fatalf("state policy = %+v summary=%q, want none", state.Policy, state.PolicySummary)
	}
	assertStoredPolicy(t, dir, gid, entpolicy.Policy{}, false)
}

func TestCreateGroupLocalStateCustomPolicyFile(t *testing.T) {
	dir := t.TempDir()
	custom := entpolicy.Relaxed()
	custom.MaxMessageBytes = 12345
	path := filepath.Join(dir, "policy.json")
	raw, err := json.Marshal(custom)
	if err != nil {
		t.Fatalf("Marshal policy: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile policy: %v", err)
	}
	resolved, err := resolveGroupCreatePolicy("file:" + path)
	if err != nil {
		t.Fatalf("resolveGroupCreatePolicy file: %v", err)
	}
	gid := testGroupCreateID(0x63)
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:      dir,
		Identity:     mustGenerateGroupCreateIdentity(t),
		FounderNode:  44,
		GroupID:      gid,
		Name:         "Custom",
		Visibility:   groupVisibilityPrivate,
		JoinMode:     groupJoinModeInviteOnly,
		Policy:       resolved.Policy,
		PolicySource: resolved.Source,
		NowMS:        1_002,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	if state.Policy == nil || state.Policy.MaxMessageBytes != 12345 {
		t.Fatalf("state policy = %+v, want custom max", state.Policy)
	}
	assertStoredPolicy(t, dir, gid, custom, true)
}

func TestCreateGroupLocalStatePublicMetadata(t *testing.T) {
	dir := t.TempDir()
	gid := testGroupCreateID(0x64)
	_, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:     dir,
		Identity:    mustGenerateGroupCreateIdentity(t),
		FounderNode: 45,
		GroupID:     gid,
		Name:        "Public Moot",
		Description: "Agents talking in the open",
		Tags:        []string{"public", "agents"},
		Visibility:  groupVisibilityPublic,
		JoinMode:    groupJoinModeInviteOnly,
		Policy:      ptrPolicy(entpolicy.Standard()),
		NowMS:       1_003,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	meta := readGroupCreateMetadata(t, dir, gid)
	if meta["visibility"] != groupVisibilityPublic || meta["description"] != "Agents talking in the open" {
		t.Fatalf("metadata = %+v", meta)
	}
}

func TestGroupPublicPublishRequiresESPURL(t *testing.T) {
	gid := testGroupCreateID(0x6b)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdGroupPublic(&globalFlags{}, []string{"publish", "-group", gid.String(), "--json"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("cmdGroupPublic code=%d, want %d; stderr=%q", code, exitInvalidArgument, stderr)
	}
	if stdout != "" || !strings.Contains(stderr, "-esp-url is required") {
		t.Fatalf("stdout=%q stderr=%q, want required esp-url error", stdout, stderr)
	}
}

func TestGroupCreatePilotNodeIDUsesInfoOnly(t *testing.T) {
	sock := testUnixSocketPath(t)
	bindSeen, stop := serveGroupCreatePilotInfoOnly(t, sock, 45491)
	defer stop()
	got, err := groupCreatePilotNodeID(context.Background(), sock)
	if err != nil {
		t.Fatalf("groupCreatePilotNodeID: %v", err)
	}
	if got != 45491 {
		t.Fatalf("node id = %d, want 45491", got)
	}
	select {
	case <-bindSeen:
		t.Fatal("groupCreatePilotNodeID sent a Pilot Bind request")
	default:
	}
}

func TestCreateGroupOpenInviteFailureRollsBackLocalState(t *testing.T) {
	dir := t.TempDir()
	gid := testGroupCreateID(0x65)
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:     dir,
		Identity:    mustGenerateGroupCreateIdentity(t),
		FounderNode: 46,
		GroupID:     gid,
		Name:        "Open",
		Visibility:  groupVisibilityPrivate,
		JoinMode:    groupJoinModeOpenInvite,
		Policy:      ptrPolicy(entpolicy.Standard()),
		NowMS:       1_004,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	if _, err := maybeCreateGroupOpenInvite(context.Background(), &globalFlags{data: dir}, state); err == nil {
		t.Fatal("maybeCreateGroupOpenInvite succeeded without daemon")
	}
	rollback()
	assertStoredPolicy(t, dir, gid, entpolicy.Policy{}, false)
	if _, ok := readGroupCreateMetadataOptional(t, dir, gid); ok {
		t.Fatal("metadata remained after rollback")
	}
	if pathExists(groupDirPath(dir, gid)) {
		t.Fatalf("group dir remained after rollback: %s", groupDirPath(dir, gid))
	}
}

func TestCreateGroupOpenInviteSuccessStoresToken(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "entmoot-group-create-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	gid := testGroupCreateID(0x66)
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:     dir,
		Identity:    mustGenerateGroupCreateIdentity(t),
		FounderNode: 47,
		GroupID:     gid,
		Name:        "Open",
		Visibility:  groupVisibilityPrivate,
		JoinMode:    groupJoinModeOpenInvite,
		Policy:      ptrPolicy(entpolicy.Standard()),
		NowMS:       1_005,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	stop := serveGroupCreateOpenInviteIPC(t, controlSocketPath(dir), gid)
	defer stop()
	t.Setenv("ENTMOOT_ESP_URL", "https://esp.example.com/base/")
	out, err := maybeCreateGroupOpenInvite(context.Background(), &globalFlags{data: dir}, state)
	if err != nil {
		t.Fatalf("maybeCreateGroupOpenInvite: %v", err)
	}
	if out == nil || out.Token == "" || out.TokenHash == "" || out.MaxUses != esphttp.OpenInviteUnlimitedMaxUses {
		t.Fatalf("open invite output = %+v", out)
	}
	if out.IssuerURL != "https://esp.example.com/base" || out.Link == "" {
		t.Fatalf("open invite link fields = %+v", out)
	}
	espState, err := esphttp.OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer espState.Close()
	if rec, ok, err := espState.GetOpenInviteByTokenHash(context.Background(), out.TokenHash); err != nil || !ok || rec.GroupID != gid {
		t.Fatalf("GetOpenInviteByTokenHash rec=%+v ok=%t err=%v", rec, ok, err)
	}
}

func TestCreateGroupOpenInviteAuthorityFailureDoesNotJoin(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "entmoot-group-create-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	gid := testGroupCreateID(0x67)
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:     dir,
		Identity:    mustGenerateGroupCreateIdentity(t),
		FounderNode: 48,
		GroupID:     gid,
		Name:        "Open",
		Visibility:  groupVisibilityPrivate,
		JoinMode:    groupJoinModeOpenInvite,
		Policy:      ptrPolicy(entpolicy.Standard()),
		NowMS:       1_006,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	t.Setenv("ENTMOOT_ESP_URL", "https://esp.example.com")
	joinSeen, stop := serveGroupCreateAuthorityFailureIPC(t, controlSocketPath(dir))
	defer stop()
	_, err = maybeCreateGroupOpenInvite(context.Background(), &globalFlags{data: dir}, state)
	if err == nil || !strings.Contains(err.Error(), "open invite authority unavailable") {
		t.Fatalf("maybeCreateGroupOpenInvite err = %v, want authority failure", err)
	}
	select {
	case <-joinSeen:
		t.Fatal("daemon join request was sent after authority failure")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestCreateGroupOpenInviteRequiresIssuerURL(t *testing.T) {
	dir := t.TempDir()
	gid := testGroupCreateID(0x69)
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:     dir,
		Identity:    mustGenerateGroupCreateIdentity(t),
		FounderNode: 50,
		GroupID:     gid,
		Name:        "Open",
		Visibility:  groupVisibilityPrivate,
		JoinMode:    groupJoinModeOpenInvite,
		Policy:      ptrPolicy(entpolicy.Standard()),
		NowMS:       1_008,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	_, err = maybeCreateGroupOpenInvite(context.Background(), &globalFlags{data: dir}, state)
	if err == nil || !strings.Contains(err.Error(), "ENTMOOT_ESP_URL") {
		t.Fatalf("maybeCreateGroupOpenInvite err = %v, want ENTMOOT_ESP_URL requirement", err)
	}
}

func TestGroupCreateOpenInviteIssuerURLUsesRedeemableRules(t *testing.T) {
	t.Setenv("ENTMOOT_ESP_URL", "http://esp.example.com")
	if _, err := groupCreateOpenInviteIssuerURL(); err == nil {
		t.Fatal("groupCreateOpenInviteIssuerURL accepted non-local cleartext issuer")
	}

	t.Setenv("ENTMOOT_ESP_URL", "https://user:pass@esp.example.com")
	if _, err := groupCreateOpenInviteIssuerURL(); err == nil {
		t.Fatal("groupCreateOpenInviteIssuerURL accepted issuer credentials")
	}

	t.Setenv("ENTMOOT_ESP_URL", "http://localhost:8080/base/?ignored=1#frag")
	got, err := groupCreateOpenInviteIssuerURL()
	if err != nil {
		t.Fatalf("groupCreateOpenInviteIssuerURL localhost: %v", err)
	}
	if got != "http://localhost:8080/base" {
		t.Fatalf("issuer URL = %q, want normalized localhost base", got)
	}
}

func TestCreateGroupOpenInviteStorageFailureDeactivatesDaemon(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "entmoot-group-create-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	gid := testGroupCreateID(0x68)
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:     dir,
		Identity:    mustGenerateGroupCreateIdentity(t),
		FounderNode: 49,
		GroupID:     gid,
		Name:        "Open",
		Visibility:  groupVisibilityPrivate,
		JoinMode:    groupJoinModeOpenInvite,
		Policy:      ptrPolicy(entpolicy.Standard()),
		NowMS:       1_007,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	t.Setenv("ENTMOOT_ESP_URL", "https://esp.example.com")
	ctx, cancel := context.WithCancel(context.Background())
	deactivateSeen, stop := serveGroupCreateOpenInviteCancelAfterJoinIPC(t, controlSocketPath(dir), gid, cancel)
	defer stop()
	_, err = maybeCreateGroupOpenInvite(ctx, &globalFlags{data: dir}, state)
	if err == nil {
		t.Fatal("maybeCreateGroupOpenInvite succeeded with canceled storage context")
	}
	select {
	case <-deactivateSeen:
	case <-time.After(time.Second):
		t.Fatal("daemon deactivation request was not sent after storage failure")
	}
}

func TestCreateGroupOpenInviteJoinErrorFrameDeactivatesDaemon(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "entmoot-group-create-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	gid := testGroupCreateID(0x6a)
	state, rollback, err := createGroupLocalState(context.Background(), groupCreateLocalStateInput{
		DataDir:     dir,
		Identity:    mustGenerateGroupCreateIdentity(t),
		FounderNode: 51,
		GroupID:     gid,
		Name:        "Open",
		Visibility:  groupVisibilityPrivate,
		JoinMode:    groupJoinModeOpenInvite,
		Policy:      ptrPolicy(entpolicy.Standard()),
		NowMS:       1_009,
	})
	if err != nil {
		t.Fatalf("createGroupLocalState: %v", err)
	}
	t.Cleanup(rollback)
	t.Setenv("ENTMOOT_ESP_URL", "https://esp.example.com")
	deactivateSeen, stop := serveGroupCreateJoinErrorFrameIPC(t, controlSocketPath(dir), gid)
	defer stop()
	_, err = maybeCreateGroupOpenInvite(context.Background(), &globalFlags{data: dir}, state)
	if err == nil || !strings.Contains(err.Error(), "activate group through daemon") {
		t.Fatalf("maybeCreateGroupOpenInvite err = %v, want daemon activation failure", err)
	}
	select {
	case <-deactivateSeen:
	case <-time.After(time.Second):
		t.Fatal("daemon deactivation request was not sent after join error frame")
	}
}

func serveGroupCreateOpenInviteIPC(t *testing.T, sock string, gid entmoot.GroupID) func() {
	t.Helper()
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				_, payload, err := ipc.ReadAndDecode(conn)
				if err != nil {
					return
				}
				switch payload.(type) {
				case *ipc.JoinGroupReq:
					_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{
						Status:  "joined",
						GroupID: gid,
						Members: 1,
					})
				case *ipc.GroupDeactivateReq:
					_ = ipc.EncodeAndWrite(conn, &ipc.GroupDeactivateResp{
						Status:  "deactivated",
						GroupID: gid,
					})
				case *ipc.InviteAuthorityCheckReq:
					req := payload.(*ipc.InviteAuthorityCheckReq)
					if req.CandidateInvite == nil {
						_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "missing candidate invite"})
						return
					}
					_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{
						Status:  "ok",
						GroupID: gid,
						Members: 1,
					})
				default:
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "unexpected request"})
				}
			}(conn)
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
		_ = os.Remove(sock)
	}
}

func serveGroupCreateJoinErrorFrameIPC(t *testing.T, sock string, gid entmoot.GroupID) (<-chan struct{}, func()) {
	t.Helper()
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	deactivateSeen := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				_, payload, err := ipc.ReadAndDecode(conn)
				if err != nil {
					return
				}
				switch payload.(type) {
				case *ipc.InviteAuthorityCheckReq:
					_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{Status: "ok", GroupID: gid, Members: 1})
				case *ipc.JoinGroupReq:
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, Message: "metadata persist failed"})
				case *ipc.GroupDeactivateReq:
					select {
					case <-deactivateSeen:
					default:
						close(deactivateSeen)
					}
					_ = ipc.EncodeAndWrite(conn, &ipc.GroupDeactivateResp{Status: "deactivated", GroupID: gid})
				default:
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "unexpected request"})
				}
			}(conn)
		}
	}()
	return deactivateSeen, func() {
		_ = ln.Close()
		<-done
		_ = os.Remove(sock)
	}
}

func serveGroupCreateOpenInviteCancelAfterJoinIPC(t *testing.T, sock string, gid entmoot.GroupID, cancel func()) (<-chan struct{}, func()) {
	t.Helper()
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	deactivateSeen := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				_, payload, err := ipc.ReadAndDecode(conn)
				if err != nil {
					return
				}
				switch payload.(type) {
				case *ipc.InviteAuthorityCheckReq:
					_ = ipc.EncodeAndWrite(conn, &ipc.InviteAuthorityCheckResp{Status: "ok", GroupID: gid, Members: 1})
				case *ipc.JoinGroupReq:
					_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: gid, Members: 1})
					cancel()
				case *ipc.GroupDeactivateReq:
					select {
					case <-deactivateSeen:
					default:
						close(deactivateSeen)
					}
					_ = ipc.EncodeAndWrite(conn, &ipc.GroupDeactivateResp{Status: "deactivated", GroupID: gid})
				default:
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "unexpected request"})
				}
			}(conn)
		}
	}()
	return deactivateSeen, func() {
		_ = ln.Close()
		<-done
		_ = os.Remove(sock)
	}
}

func serveGroupCreateAuthorityFailureIPC(t *testing.T, sock string) (<-chan struct{}, func()) {
	t.Helper()
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	joinSeen := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				_, payload, err := ipc.ReadAndDecode(conn)
				if err != nil {
					return
				}
				switch payload.(type) {
				case *ipc.InviteAuthorityCheckReq:
					req := payload.(*ipc.InviteAuthorityCheckReq)
					if req.CandidateInvite == nil {
						_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "missing candidate invite"})
						return
					}
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{
						Type:    "error",
						Code:    ipc.CodeUnavailable,
						Message: "group is not active",
					})
				case *ipc.JoinGroupReq:
					select {
					case <-joinSeen:
					default:
						close(joinSeen)
					}
					_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined"})
				default:
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "unexpected request"})
				}
			}(conn)
		}
	}()
	return joinSeen, func() {
		_ = ln.Close()
		<-done
		_ = os.Remove(sock)
	}
}

func assertStoredPolicy(t *testing.T, dir string, gid entmoot.GroupID, want entpolicy.Policy, wantOK bool) {
	t.Helper()
	store, err := entpolicy.OpenFileStore(dir)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	got, ok, err := store.Get(context.Background(), gid)
	if err != nil {
		t.Fatalf("policy Get: %v", err)
	}
	if ok != wantOK {
		t.Fatalf("policy ok = %t, want %t", ok, wantOK)
	}
	if ok && got != want {
		t.Fatalf("policy = %+v, want %+v", got, want)
	}
}

func readGroupCreateMetadata(t *testing.T, dir string, gid entmoot.GroupID) map[string]any {
	t.Helper()
	meta, ok := readGroupCreateMetadataOptional(t, dir, gid)
	if !ok {
		t.Fatal("metadata missing")
	}
	return meta
}

func readGroupCreateMetadataOptional(t *testing.T, dir string, gid entmoot.GroupID) (map[string]any, bool) {
	t.Helper()
	state, err := esphttp.OpenSQLiteStateStore(dir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	raw, ok, err := state.GetGroupMetadata(context.Background(), gid)
	if err != nil {
		t.Fatalf("GetGroupMetadata: %v", err)
	}
	if !ok {
		return nil, false
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("metadata JSON: %v", err)
	}
	return meta, true
}

func mustGenerateGroupCreateIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	return id
}

func testGroupCreateID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = seed
	}
	return gid
}

func ptrPolicy(p entpolicy.Policy) *entpolicy.Policy {
	return &p
}

func serveGroupCreatePilotInfoOnly(t *testing.T, sock string, nodeID uint32) (<-chan struct{}, func()) {
	t.Helper()
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen pilot unix: %v", err)
	}
	done := make(chan struct{})
	bindSeen := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		payload, err := readGroupCreatePilotTestFrame(conn)
		if err != nil || len(payload) == 0 {
			return
		}
		switch payload[0] {
		case 0x0D: // Info
			body, _ := json.Marshal(map[string]any{"node_id": nodeID})
			_ = writeGroupCreatePilotTestFrame(conn, append([]byte{0x0E}, body...)) // InfoOK
		case 0x01: // Bind
			close(bindSeen)
		}
	}()
	return bindSeen, func() {
		_ = ln.Close()
		<-done
		_ = os.Remove(sock)
	}
}

func readGroupCreatePilotTestFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writeGroupCreatePilotTestFrame(w io.Writer, payload []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}
