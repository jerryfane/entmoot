package main

import (
	"bytes"
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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/defaultmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

func TestLoadJoinInputClassifiesSignedInvite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invite.json")
	raw := `{
		"group_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		"founder":{"pilot_node_id":1,"entmoot_pubkey":"` + base64.StdEncoding.EncodeToString(make([]byte, 32)) + `"},
		"roster_head":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		"merkle_root":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		"bootstrap_peers":[],
		"issued_at":1,
		"valid_until":0,
		"issuer":{"pilot_node_id":1,"entmoot_pubkey":"` + base64.StdEncoding.EncodeToString(make([]byte, 32)) + `"}
	}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	input, err := loadJoinInput(path)
	if err != nil {
		t.Fatalf("loadJoinInput: %v", err)
	}
	if input.invite == nil || input.openInvite != nil {
		t.Fatalf("input = %+v, want signed invite", input)
	}
}

func TestLoadJoinInputClassifiesFleetInviteDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet-invite.json")
	gid := testESPGroupID(77)
	raw, err := json.Marshal(fleetInviteDescriptor{
		Type:           fleetInviteDescriptorType,
		FleetID:        "fleet-a",
		FleetName:      "Fleet A",
		ControlGroupID: gid,
		Invite: entmoot.Invite{
			GroupID:    gid,
			Founder:    entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: make([]byte, 32)},
			Issuer:     entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: make([]byte, 32)},
			IssuedAt:   1,
			ValidUntil: 0,
		},
	})
	if err != nil {
		t.Fatalf("Marshal descriptor: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	input, err := loadJoinInput(path)
	if err != nil {
		t.Fatalf("loadJoinInput: %v", err)
	}
	if input.invite == nil || input.openInvite != nil {
		t.Fatalf("input = %+v, want fleet invite descriptor", input)
	}
	if !fleetControlMetadataMatches(input.groupMetadata, "fleet-a") {
		t.Fatalf("group metadata = %s, want Fleet control metadata", input.groupMetadata)
	}
}

func TestLoadJoinInputClassifiesOpenInviteDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open-invite.json")
	raw := `{"issuer_url":"https://esp.example.com/base","token":"open-token","group_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","max_uses":5}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	input, err := loadJoinInput(path)
	if err != nil {
		t.Fatalf("loadJoinInput: %v", err)
	}
	if input.openInvite == nil || input.invite != nil {
		t.Fatalf("input = %+v, want open invite", input)
	}
	if input.openInvite.IssuerURL != "https://esp.example.com/base" || input.openInvite.Token != "open-token" {
		t.Fatalf("open invite = %+v", input.openInvite)
	}
}

func TestLoadJoinInputVerifiesDefaultMootDescriptor(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	t.Setenv(defaultmoot.EnvDescriptorPubKey, base64.StdEncoding.EncodeToString(pub))

	desc := defaultmoot.Descriptor{
		Type:    defaultmoot.DescriptorType,
		Name:    defaultmoot.Name,
		GroupID: testESPGroupID(91),
		OpenInvite: defaultmoot.OpenInviteDescriptor{
			IssuerURL: "https://esp.example.com/base",
			Token:     "open-token",
		},
		Issuer: entmoot.NodeInfo{
			PilotNodeID:   45491,
			EntmootPubKey: make([]byte, ed25519.PublicKeySize),
		},
		DefaultTopics: []string{"chat/general", "introductions"},
		RecommendedLiveConfig: defaultmoot.RecommendedLiveConfig{
			Mode:           "converse",
			AllowedActions: []string{"reply"},
			MaxActions:     policy.DefaultLiveMaxActionsPerScan,
			MaxActionBytes: policy.DefaultLiveMaxActionBytes,
		},
		Policy:     policy.TheEntMootDefault(),
		IssuedAtMS: 1_700_000_000_000,
	}
	signed, err := defaultmoot.Sign(desc, priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	raw, err := json.Marshal(signed)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	path := filepath.Join(t.TempDir(), "the-ent-moot.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	input, err := loadJoinInput(path)
	if err != nil {
		t.Fatalf("loadJoinInput: %v", err)
	}
	if input.openInvite == nil || input.openInvite.IssuerURL != "https://esp.example.com/base" || input.openInvite.Token != "open-token" {
		t.Fatalf("open invite = %+v", input.openInvite)
	}
	if input.expectedGroup == nil || *input.expectedGroup != desc.GroupID {
		t.Fatalf("expected group = %v, want %s", input.expectedGroup, desc.GroupID)
	}
	if input.expectedIssuer == nil || !nodeInfoEqual(*input.expectedIssuer, desc.Issuer) {
		t.Fatalf("expected issuer = %+v, want %+v", input.expectedIssuer, desc.Issuer)
	}
	if input.groupPolicy == nil || input.groupPolicy.MaxMessageBytes != policy.DefaultMaxMessageBytes {
		t.Fatalf("group policy = %+v, want default moot policy", input.groupPolicy)
	}
	if !bytes.Contains(input.groupMetadata, []byte(`"name":"The Ent Moot"`)) {
		t.Fatalf("group metadata = %s, want default moot name", input.groupMetadata)
	}
}

func TestLoadJoinInputRejectsDefaultMootDescriptorWrongKey(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey signer: %v", err)
	}
	wrongPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey wrong: %v", err)
	}
	t.Setenv(defaultmoot.EnvDescriptorPubKey, base64.StdEncoding.EncodeToString(wrongPub))

	desc := defaultmoot.Descriptor{
		Type:    defaultmoot.DescriptorType,
		Name:    defaultmoot.Name,
		GroupID: testESPGroupID(92),
		OpenInvite: defaultmoot.OpenInviteDescriptor{
			Link: "entmoot://open-invite?issuer=https://esp.example.com/base&token=open-token",
		},
		Issuer: entmoot.NodeInfo{
			PilotNodeID:   45491,
			EntmootPubKey: make([]byte, ed25519.PublicKeySize),
		},
		DefaultTopics: []string{"chat/general", "introductions"},
		RecommendedLiveConfig: defaultmoot.RecommendedLiveConfig{
			Mode:           "converse",
			AllowedActions: []string{"reply"},
			MaxActions:     policy.DefaultLiveMaxActionsPerScan,
			MaxActionBytes: policy.DefaultLiveMaxActionBytes,
		},
		Policy:     policy.TheEntMootDefault(),
		IssuedAtMS: 1_700_000_000_000,
	}
	signed, err := defaultmoot.Sign(desc, priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	raw, err := json.Marshal(signed)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	path := filepath.Join(t.TempDir(), "the-ent-moot.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = loadJoinInput(path)
	if err == nil || !errors.Is(err, defaultmoot.ErrDescriptorSignature) {
		t.Fatalf("loadJoinInput err = %v, want ErrDescriptorSignature", err)
	}
}

func TestLoadJoinInputClassifiesOpenInviteLink(t *testing.T) {
	input, err := loadJoinInput("entmoot://open-invite?issuer=https://esp.example.com/esp&token=open-token")
	if err != nil {
		t.Fatalf("loadJoinInput: %v", err)
	}
	if input.openInvite == nil {
		t.Fatal("openInvite = nil")
	}
	if input.openInvite.IssuerURL != "https://esp.example.com/esp" || input.openInvite.Token != "open-token" {
		t.Fatalf("open invite = %+v", input.openInvite)
	}
}

func TestLoadJoinInputRejectsRawOpenInviteToken(t *testing.T) {
	_, err := loadJoinInput("vWWMheXCvw07IY27Kj6KLRNUwJR18vpgtBxK3a30ttY")
	if err == nil || !strings.Contains(err.Error(), "raw open invite token is not enough") {
		t.Fatalf("err = %v, want raw token guidance", err)
	}
}

func TestLoadJoinInputClassifiesFetchedOpenInviteDescriptor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"issuer_url":"https://esp.example.com","token":"open-token"}`))
	}))
	defer srv.Close()
	input, err := loadJoinInput(srv.URL)
	if err != nil {
		t.Fatalf("loadJoinInput: %v", err)
	}
	if input.openInvite == nil || input.openInvite.Token != "open-token" {
		t.Fatalf("input = %+v, want fetched open invite", input)
	}
}

func TestRedeemJoinOpenInviteLivePilotCallbacksUseDeadlines(t *testing.T) {
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate identity: %v", err)
	}
	pilotPub, pilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey pilot: %v", err)
	}
	gid := testESPGroupID(42)
	challengeID := "challenge-1"
	challengePayload, err := canonical.Encode(openInvitePilotProofEnvelope{
		Type:          "entmoot.open_invite.redeem.v1",
		TokenHash:     esphttp.HashOpenInviteToken("open-token"),
		GroupID:       gid,
		ChallengeID:   challengeID,
		Nonce:         "nonce-1",
		IssuedAtMS:    time.Now().UnixMilli(),
		ExpiresAtMS:   time.Now().Add(time.Minute).UnixMilli(),
		PilotNodeID:   45981,
		PilotPubKey:   pilotPub,
		EntmootPubKey: identity.PublicKey,
	})
	if err != nil {
		t.Fatalf("Encode challenge payload: %v", err)
	}

	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/open-invites/open-token/challenge":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"challenge_id":           challengeID,
				"signing_payload":        base64.StdEncoding.EncodeToString(challengePayload),
				"signing_payload_sha256": sha256Base64(challengePayload),
				"expires_at_ms":          time.Now().Add(time.Minute).UnixMilli(),
			})
		case "/v1/open-invites/open-token/redeem":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode redeem request: %v", err)
			}
			sigText, _ := body["pilot_signature"].(string)
			sig, _ := base64.StdEncoding.DecodeString(sigText)
			if !ed25519.Verify(pilotPub, pilotChallengeSigningBytes(challengePayload), sig) {
				t.Errorf("pilot signature did not verify")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":   "redeemed",
				"group_id": gid,
				"invite":   entmoot.Invite{GroupID: gid},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer issuer.Close()

	pilot := &deadlineCheckingPilot{
		t:         t,
		nodeID:    45981,
		publicKey: base64.StdEncoding.EncodeToString(pilotPub),
		sign: func(payload []byte) string {
			if !bytes.Equal(payload, challengePayload) {
				t.Fatalf("challenge payload = %q, want %q", payload, challengePayload)
			}
			return base64.StdEncoding.EncodeToString(ed25519.Sign(pilotPriv, pilotChallengeSigningBytes(payload)))
		},
	}
	invite, err := redeemJoinOpenInvite(context.Background(), &openInviteAcceptPayload{
		IssuerURL: issuer.URL,
		Token:     "open-token",
	}, groupDaemonLoadContext{
		identity: identity,
		pilot:    pilot,
	})
	if err != nil {
		t.Fatalf("redeemJoinOpenInvite: %v", err)
	}
	if invite == nil || invite.GroupID != gid {
		t.Fatalf("invite = %+v, want group %s", invite, gid)
	}
	if pilot.infoDeadline.IsZero() || pilot.signDeadline.IsZero() {
		t.Fatalf("deadlines not captured: info=%v sign=%v", pilot.infoDeadline, pilot.signDeadline)
	}
	if time.Until(pilot.infoDeadline) <= 0 || time.Until(pilot.infoDeadline) > 31*time.Second {
		t.Fatalf("info deadline = %v, want about 30s", pilot.infoDeadline)
	}
	if time.Until(pilot.signDeadline) <= 0 || time.Until(pilot.signDeadline) > 31*time.Second {
		t.Fatalf("sign deadline = %v, want about 30s", pilot.signDeadline)
	}
}

func TestJoinInviteOverIPCSendsJoinGroupRequest(t *testing.T) {
	sock := shortTestUnixSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer ln.Close()

	gid := testESPGroupID(77)
	reqCh := make(chan *ipc.JoinGroupReq, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		_, payload, err := ipc.ReadAndDecode(conn)
		if err != nil {
			errCh <- err
			return
		}
		req, ok := payload.(*ipc.JoinGroupReq)
		if !ok {
			errCh <- fmt.Errorf("payload = %T, want *ipc.JoinGroupReq", payload)
			return
		}
		reqCh <- req
		_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: req.Invite.GroupID, Members: 2})
	}()

	resp, frame, err := joinInviteOverIPC(context.Background(), sock, entmoot.Invite{GroupID: gid}, time.Second)
	if err != nil {
		t.Fatalf("joinInviteOverIPC: %v", err)
	}
	if frame != nil {
		t.Fatalf("frame = %+v, want nil", frame)
	}
	if resp.Status != "joined" || resp.GroupID != gid || resp.Members != 2 {
		t.Fatalf("resp = %+v", resp)
	}
	select {
	case req := <-reqCh:
		if req.Invite.GroupID != gid {
			t.Fatalf("req group = %s, want %s", req.Invite.GroupID, gid)
		}
		if req.TimeoutMS != 1_000 {
			t.Fatalf("req timeout_ms = %d, want 1000", req.TimeoutMS)
		}
	case err := <-errCh:
		t.Fatalf("server error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for join request")
	}
}

func TestJoinInviteOverIPCWaitsForResponseMargin(t *testing.T) {
	sock := shortTestUnixSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer ln.Close()

	gid := testESPGroupID(78)
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		_, payload, err := ipc.ReadAndDecode(conn)
		if err != nil {
			errCh <- err
			return
		}
		req, ok := payload.(*ipc.JoinGroupReq)
		if !ok {
			errCh <- fmt.Errorf("payload = %T, want *ipc.JoinGroupReq", payload)
			return
		}
		time.Sleep(150 * time.Millisecond)
		_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: req.Invite.GroupID, Members: 2})
	}()

	resp, frame, err := joinInviteOverIPC(context.Background(), sock, entmoot.Invite{GroupID: gid}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("joinInviteOverIPC: %v", err)
	}
	if frame != nil {
		t.Fatalf("frame = %+v, want nil", frame)
	}
	if resp.Status != "joined" || resp.GroupID != gid {
		t.Fatalf("resp = %+v", resp)
	}
	select {
	case err := <-errCh:
		t.Fatalf("server error: %v", err)
	default:
	}
}

func TestRemainingJoinBootstrapTimeoutUsesExistingDeadline(t *testing.T) {
	now := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(time.Second))
	defer cancel()

	remaining, err := remainingJoinBootstrapTimeout(ctx, 30*time.Second, now.Add(250*time.Millisecond))
	if err != nil {
		t.Fatalf("remainingJoinBootstrapTimeout: %v", err)
	}
	if remaining != 750*time.Millisecond {
		t.Fatalf("remaining = %v, want 750ms", remaining)
	}
}

func TestRemainingJoinBootstrapTimeoutRejectsExpiredDeadline(t *testing.T) {
	now := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(time.Second))
	defer cancel()

	remaining, err := remainingJoinBootstrapTimeout(ctx, 30*time.Second, now.Add(2*time.Second))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, remaining = %v; want DeadlineExceeded", err, remaining)
	}
}

func TestJoinInputOverIPCSendsOpenInviteDescriptor(t *testing.T) {
	sock := shortTestUnixSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer ln.Close()

	gid := testESPGroupID(79)
	expectedIssuer := entmoot.NodeInfo{
		PilotNodeID:   45491,
		EntmootPubKey: []byte("issuer-key"),
	}
	groupPolicy := policy.TheEntMootDefault()
	reqCh := make(chan *ipc.JoinGroupReq, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		_, payload, err := ipc.ReadAndDecode(conn)
		if err != nil {
			errCh <- err
			return
		}
		req, ok := payload.(*ipc.JoinGroupReq)
		if !ok {
			errCh <- fmt.Errorf("payload = %T, want *ipc.JoinGroupReq", payload)
			return
		}
		reqCh <- req
		_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: gid, Issuer: &expectedIssuer, Members: 2})
	}()

	resp, frame, err := joinInputOverIPC(context.Background(), sock, joinInput{
		source: "open",
		openInvite: &openInviteAcceptPayload{
			IssuerURL: "https://esp.example.com",
			Token:     "open-token",
		},
		expectedGroup:  &gid,
		expectedIssuer: &expectedIssuer,
		groupPolicy:    &groupPolicy,
	}, time.Second)
	if err != nil {
		t.Fatalf("joinInputOverIPC: %v", err)
	}
	if frame != nil {
		t.Fatalf("frame = %+v, want nil", frame)
	}
	if resp.Status != "joined" || resp.GroupID != gid {
		t.Fatalf("resp = %+v", resp)
	}
	select {
	case req := <-reqCh:
		if req.OpenInvite == nil {
			t.Fatalf("req.OpenInvite = nil")
		}
		if req.TimeoutMS != 1_000 {
			t.Fatalf("req timeout_ms = %d, want 1000", req.TimeoutMS)
		}
		if req.Invite.GroupID != (entmoot.GroupID{}) {
			t.Fatalf("req.Invite.GroupID = %s, want zero", req.Invite.GroupID)
		}
		if req.OpenInvite.IssuerURL != "https://esp.example.com" || req.OpenInvite.Token != "open-token" {
			t.Fatalf("req.OpenInvite = %+v", req.OpenInvite)
		}
		if req.OpenInvite.ExpectedGroupID == nil || *req.OpenInvite.ExpectedGroupID != gid {
			t.Fatalf("req.OpenInvite.ExpectedGroupID = %v, want %s", req.OpenInvite.ExpectedGroupID, gid)
		}
		if req.OpenInvite.ExpectedIssuer == nil || !nodeInfoEqual(*req.OpenInvite.ExpectedIssuer, expectedIssuer) {
			t.Fatalf("req.OpenInvite.ExpectedIssuer = %+v, want %+v", req.OpenInvite.ExpectedIssuer, expectedIssuer)
		}
		if req.GroupPolicy == nil || req.GroupPolicy.LiveTriggerRate != groupPolicy.LiveTriggerRate {
			t.Fatalf("req.GroupPolicy = %+v, want %+v", req.GroupPolicy, groupPolicy)
		}
	case err := <-errCh:
		t.Fatalf("server error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for join request")
	}
}

func TestJoinInputOverIPCRejectsUnexpectedDefaultMootGroup(t *testing.T) {
	sock := shortTestUnixSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer ln.Close()

	wantGID := testESPGroupID(77)
	gotGID := testESPGroupID(78)
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		if _, _, err := ipc.ReadAndDecode(conn); err != nil {
			errCh <- err
			return
		}
		_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: gotGID, Members: 2})
	}()

	resp, frame, err := joinInputOverIPC(context.Background(), sock, joinInput{
		source: "default-moot",
		openInvite: &openInviteAcceptPayload{
			IssuerURL: "https://esp.example.com",
			Token:     "open-token",
		},
		expectedGroup: &wantGID,
	}, time.Second)
	if err == nil || !errors.Is(err, errInviteMalformed) || !strings.Contains(err.Error(), "want signed descriptor group") {
		t.Fatalf("joinInputOverIPC err = %v, want expected group mismatch", err)
	}
	if resp != nil || frame != nil {
		t.Fatalf("resp/frame = %+v/%+v, want nil on mismatch", resp, frame)
	}
	select {
	case err := <-errCh:
		t.Fatalf("server error: %v", err)
	default:
	}
}

func TestJoinInputOverIPCRejectsMissingExpectedDefaultMootIssuer(t *testing.T) {
	sock := shortTestUnixSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer ln.Close()

	gid := testESPGroupID(83)
	expectedIssuer := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("issuer-key")}
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		if _, _, err := ipc.ReadAndDecode(conn); err != nil {
			errCh <- err
			return
		}
		_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: gid, Members: 2})
	}()

	resp, frame, err := joinInputOverIPC(context.Background(), sock, joinInput{
		source: "default-moot",
		openInvite: &openInviteAcceptPayload{
			IssuerURL: "https://esp.example.com",
			Token:     "open-token",
		},
		expectedGroup:  &gid,
		expectedIssuer: &expectedIssuer,
	}, time.Second)
	if err == nil || !errors.Is(err, errInviteMalformed) || !strings.Contains(err.Error(), "issuer does not match signed descriptor issuer") {
		t.Fatalf("joinInputOverIPC err = %v, want expected issuer mismatch", err)
	}
	if resp != nil || frame != nil {
		t.Fatalf("resp/frame = %+v/%+v, want nil on mismatch", resp, frame)
	}
	select {
	case err := <-errCh:
		t.Fatalf("server error: %v", err)
	default:
	}
}

func TestJoinInputsOverIPCClassifiesUnexpectedDefaultMootGroupAsInvalidArgument(t *testing.T) {
	sock := shortTestUnixSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer ln.Close()

	wantGID := testESPGroupID(81)
	gotGID := testESPGroupID(82)
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		if _, _, err := ipc.ReadAndDecode(conn); err != nil {
			errCh <- err
			return
		}
		_ = ipc.EncodeAndWrite(conn, &ipc.JoinGroupResp{Status: "joined", GroupID: gotGID, Members: 2})
	}()

	code := joinInputsOverIPC(sock, []joinInput{{
		source: "default-moot",
		openInvite: &openInviteAcceptPayload{
			IssuerURL: "https://esp.example.com",
			Token:     "open-token",
		},
		expectedGroup: &wantGID,
	}}, time.Second)
	if code != exitInvalidArgument {
		t.Fatalf("joinInputsOverIPC exit = %d, want %d", code, exitInvalidArgument)
	}
	select {
	case err := <-errCh:
		t.Fatalf("server error: %v", err)
	default:
	}
}

func TestJoinInviteOverIPCReturnsErrorFrame(t *testing.T) {
	sock := shortTestUnixSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = ipc.ReadAndDecode(conn)
		_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "bad invite"})
	}()

	resp, frame, err := joinInviteOverIPC(context.Background(), sock, entmoot.Invite{GroupID: testESPGroupID(78)}, time.Second)
	if err != nil {
		t.Fatalf("joinInviteOverIPC: %v", err)
	}
	if resp != nil {
		t.Fatalf("resp = %+v, want nil", resp)
	}
	if frame == nil || frame.Code != ipc.CodeInvalidArgument || frame.Message != "bad invite" {
		t.Fatalf("frame = %+v", frame)
	}
}

func TestIPCServerResolveJoinGroupInviteRedeemsOpenInviteWithDaemonIdentity(t *testing.T) {
	daemonIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate daemon identity: %v", err)
	}
	pilotPub, pilotPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey pilot: %v", err)
	}
	gid := testESPGroupID(80)
	challengeID := "challenge-daemon"
	challengePayload, err := canonical.Encode(openInvitePilotProofEnvelope{
		Type:          "entmoot.open_invite.redeem.v1",
		TokenHash:     esphttp.HashOpenInviteToken("open-token"),
		GroupID:       gid,
		ChallengeID:   challengeID,
		Nonce:         "nonce-daemon",
		IssuedAtMS:    time.Now().UnixMilli(),
		ExpiresAtMS:   time.Now().Add(time.Minute).UnixMilli(),
		PilotNodeID:   133053,
		PilotPubKey:   pilotPub,
		EntmootPubKey: daemonIdentity.PublicKey,
	})
	if err != nil {
		t.Fatalf("Encode challenge payload: %v", err)
	}

	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/open-invites/open-token/challenge":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode challenge request: %v", err)
			}
			if body["pilot_node_id"] != float64(133053) {
				t.Errorf("pilot_node_id = %v, want daemon node", body["pilot_node_id"])
			}
			if body["entmoot_pubkey"] != base64.StdEncoding.EncodeToString(daemonIdentity.PublicKey) {
				t.Errorf("entmoot_pubkey = %v, want daemon key", body["entmoot_pubkey"])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"challenge_id":           challengeID,
				"signing_payload":        base64.StdEncoding.EncodeToString(challengePayload),
				"signing_payload_sha256": sha256Base64(challengePayload),
				"expires_at_ms":          time.Now().Add(time.Minute).UnixMilli(),
			})
		case "/v1/open-invites/open-token/redeem":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode redeem request: %v", err)
			}
			if body["pilot_node_id"] != float64(133053) {
				t.Errorf("redeem pilot_node_id = %v, want daemon node", body["pilot_node_id"])
			}
			if body["entmoot_pubkey"] != base64.StdEncoding.EncodeToString(daemonIdentity.PublicKey) {
				t.Errorf("redeem entmoot_pubkey = %v, want daemon key", body["entmoot_pubkey"])
			}
			sigText, _ := body["pilot_signature"].(string)
			sig, _ := base64.StdEncoding.DecodeString(sigText)
			if !ed25519.Verify(pilotPub, pilotChallengeSigningBytes(challengePayload), sig) {
				t.Errorf("pilot signature did not verify")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":   "redeemed",
				"group_id": gid,
				"invite":   entmoot.Invite{GroupID: gid},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer issuer.Close()

	pilot := &deadlineCheckingPilot{
		t:         t,
		nodeID:    133053,
		publicKey: base64.StdEncoding.EncodeToString(pilotPub),
		sign: func(payload []byte) string {
			return base64.StdEncoding.EncodeToString(ed25519.Sign(pilotPriv, pilotChallengeSigningBytes(payload)))
		},
	}
	invite, err := resolveJoinGroupInviteWithContext(context.Background(), &ipc.JoinGroupReq{
		OpenInvite: &ipc.OpenInviteJoin{
			IssuerURL: issuer.URL,
			Token:     "open-token",
		},
	}, daemonIdentity, pilot, "/daemon/pilot.sock")
	if err != nil {
		t.Fatalf("resolveJoinGroupInvite: %v", err)
	}
	if invite.GroupID != gid {
		t.Fatalf("invite.GroupID = %s, want %s", invite.GroupID, gid)
	}

	wrongGID := testESPGroupID(81)
	_, err = resolveJoinGroupInviteWithContext(context.Background(), &ipc.JoinGroupReq{
		OpenInvite: &ipc.OpenInviteJoin{
			IssuerURL:       issuer.URL,
			Token:           "open-token",
			ExpectedGroupID: &wrongGID,
		},
	}, daemonIdentity, pilot, "/daemon/pilot.sock")
	if err == nil || !errors.Is(err, errInviteMalformed) || !strings.Contains(err.Error(), "want signed descriptor group") {
		t.Fatalf("resolveJoinGroupInvite wrong expected group err = %v, want malformed group mismatch", err)
	}

	expectedIssuer := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("issuer-key")}
	_, err = resolveJoinGroupInviteWithContext(context.Background(), &ipc.JoinGroupReq{
		OpenInvite: &ipc.OpenInviteJoin{
			IssuerURL:      issuer.URL,
			Token:          "open-token",
			ExpectedIssuer: &expectedIssuer,
		},
	}, daemonIdentity, pilot, "/daemon/pilot.sock")
	if err == nil || !errors.Is(err, errInviteMalformed) || !strings.Contains(err.Error(), "issuer does not match signed descriptor issuer") {
		t.Fatalf("resolveJoinGroupInvite wrong expected issuer err = %v, want malformed issuer mismatch", err)
	}
}

func shortTestUnixSocket(t *testing.T) string {
	t.Helper()
	path := filepath.Join(os.TempDir(), fmt.Sprintf("entmoot-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

type deadlineCheckingPilot struct {
	t            *testing.T
	nodeID       uint32
	publicKey    string
	sign         func([]byte) string
	infoDeadline time.Time
	signDeadline time.Time
}

func (p *deadlineCheckingPilot) InfoStruct(ctx context.Context) (ipcclient.Info, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		p.t.Fatal("InfoStruct context has no deadline")
	}
	p.infoDeadline = deadline
	return ipcclient.Info{NodeID: p.nodeID, PublicKey: p.publicKey}, nil
}

func (p *deadlineCheckingPilot) SignChallenge(ctx context.Context, payload []byte) (ipcclient.ChallengeSignature, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		p.t.Fatal("SignChallenge context has no deadline")
	}
	p.signDeadline = deadline
	return ipcclient.ChallengeSignature{Signature: p.sign(payload)}, nil
}
