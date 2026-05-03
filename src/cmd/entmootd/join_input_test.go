package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
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
