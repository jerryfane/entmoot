package esphttp

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
)

// testMemberSigner is a member identity that can sign ESP requests the way the
// iPhone app does: one Ed25519 key, the member id derived from it, and the
// libp2p peer id derived from the same key.
type testMemberSigner struct {
	memberID entmoot.MemberID
	peerID   string
	pub      ed25519.PublicKey
	priv     ed25519.PrivateKey
}

func newTestMemberSigner(t *testing.T, seed byte) testMemberSigner {
	t.Helper()
	seedBytes := make([]byte, ed25519.SeedSize)
	for i := range seedBytes {
		seedBytes[i] = seed
	}
	priv := ed25519.NewKeyFromSeed(seedBytes)
	pub := priv.Public().(ed25519.PublicKey)
	memberID, err := entmoot.MemberIDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("MemberIDFromPublicKey: %v", err)
	}
	libp2pPub, err := libp2pcrypto.UnmarshalEd25519PublicKey(pub)
	if err != nil {
		t.Fatalf("UnmarshalEd25519PublicKey: %v", err)
	}
	pid, err := peer.IDFromPublicKey(libp2pPub)
	if err != nil {
		t.Fatalf("IDFromPublicKey: %v", err)
	}
	return testMemberSigner{memberID: memberID, peerID: pid.String(), pub: pub, priv: priv}
}

// signedSessionRequest builds a correctly member-signed GET /v1/session.
// claimedMemberID is the id put on the wire; it is the signer's own id for a
// well-formed request and a different id for the impersonation case.
func (s testMemberSigner) signedSessionRequest(claimedMemberID entmoot.MemberID, timestampMillis int64, nonce string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/v1/session", nil)
	input := MemberSigningInput(http.MethodGet, req.URL.RequestURI(), claimedMemberID, s.peerID, s.pub, timestampMillis, nonce, nil)
	sig := ed25519.Sign(s.priv, []byte(input))
	req.Header.Set(memberIDHeader, claimedMemberID.String())
	req.Header.Set(memberPeerHeader, s.peerID)
	req.Header.Set(memberPubKeyHeader, base64.StdEncoding.EncodeToString(s.pub))
	req.Header.Set(timestampHeader, strconv.FormatInt(timestampMillis, 10))
	req.Header.Set(nonceHeader, nonce)
	req.Header.Set(memberSignatureHeader, base64.StdEncoding.EncodeToString(sig))
	return req
}

func memberSessionUnauthorizedMessage(t *testing.T, handler http.Handler, req *http.Request) string {
	t.Helper()
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d\nbody=%s", resp.Code, http.StatusUnauthorized, resp.Body.String())
	}
	var env errorEnvelope
	if err := json.Unmarshal(resp.Body.Bytes(), &env); err != nil {
		t.Fatalf("Unmarshal error envelope: %v\n%s", err, resp.Body.String())
	}
	if env.Error.Code != "unauthorized" {
		t.Fatalf("error code = %q, want %q", env.Error.Code, "unauthorized")
	}
	return env.Error.Message
}

// TestMemberSignedSessionRoundTrip is the only exercise of a *successful*
// member-signed request: the scheme the iPhone app authenticates with. It
// proves the identity the server echoes back and the three refusals that stop
// a captured request from being replayed, back-dated, or reused under someone
// else's member id.
//
// It does NOT pin the signing input: it signs with the same
// MemberSigningInput the server verifies with, so a change to the canonical
// layout keeps this green while invalidating every deployed client's
// signature. TestMemberSigningInputIsFrozen is what defends that.
func TestMemberSignedSessionRoundTrip(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	handler := testMobileHandlerFull(t, testGroupID(21), nil, nil, func() time.Time { return now }, nil, NewMemoryStateStore(), nil)
	signer := newTestMemberSigner(t, 0x11)

	t.Run("signed request is accepted and the member is echoed", func(t *testing.T) {
		req := signer.signedSessionRequest(signer.memberID, now.UnixMilli(), "member-nonce-accept")
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d\nbody=%s", resp.Code, http.StatusOK, resp.Body.String())
		}
		var out struct {
			Authenticated bool `json:"authenticated"`
			Member        struct {
				MemberID      string `json:"member_id"`
				PeerID        string `json:"peer_id"`
				EntmootPubKey string `json:"entmoot_pubkey"`
			} `json:"member"`
		}
		if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
			t.Fatalf("Unmarshal session: %v\n%s", err, resp.Body.String())
		}
		if !out.Authenticated {
			t.Error("session response is not authenticated")
		}
		if out.Member.MemberID != signer.memberID.String() {
			t.Errorf("member_id = %q, want %q", out.Member.MemberID, signer.memberID.String())
		}
		if out.Member.PeerID != signer.peerID {
			t.Errorf("peer_id = %q, want %q", out.Member.PeerID, signer.peerID)
		}
		if want := base64.StdEncoding.EncodeToString(signer.pub); out.Member.EntmootPubKey != want {
			t.Errorf("entmoot_pubkey = %q, want %q", out.Member.EntmootPubKey, want)
		}
	})

	t.Run("replayed nonce is refused", func(t *testing.T) {
		const nonce = "member-nonce-replay"
		first := httptest.NewRecorder()
		handler.ServeHTTP(first, signer.signedSessionRequest(signer.memberID, now.UnixMilli(), nonce))
		if first.Code != http.StatusOK {
			t.Fatalf("first use status = %d, want %d\nbody=%s", first.Code, http.StatusOK, first.Body.String())
		}
		msg := memberSessionUnauthorizedMessage(t, handler, signer.signedSessionRequest(signer.memberID, now.UnixMilli(), nonce))
		if msg != "replayed nonce" {
			t.Errorf("replay message = %q, want %q", msg, "replayed nonce")
		}
	})

	t.Run("timestamp outside the window is refused", func(t *testing.T) {
		stale := now.Add(-deviceAuthSkew - time.Second).UnixMilli()
		msg := memberSessionUnauthorizedMessage(t, handler, signer.signedSessionRequest(signer.memberID, stale, "member-nonce-stale"))
		if msg != "request timestamp outside allowed window" {
			t.Errorf("stale message = %q, want %q", msg, "request timestamp outside allowed window")
		}
		future := now.Add(deviceAuthSkew + time.Second).UnixMilli()
		msg = memberSessionUnauthorizedMessage(t, handler, signer.signedSessionRequest(signer.memberID, future, "member-nonce-future"))
		if msg != "request timestamp outside allowed window" {
			t.Errorf("future message = %q, want %q", msg, "request timestamp outside allowed window")
		}
	})

	t.Run("member id that does not derive from the key is refused", func(t *testing.T) {
		other := newTestMemberSigner(t, 0x22)
		msg := memberSessionUnauthorizedMessage(t, handler, signer.signedSessionRequest(other.memberID, now.UnixMilli(), "member-nonce-impersonate"))
		if msg != "member id does not match public key" {
			t.Errorf("impersonation message = %q, want %q", msg, "member id does not match public key")
		}
	})
}

// A golden vector over MemberSigningInput. The round-trip test above signs
// with the same function the server verifies with, so it stays green if the
// canonical layout changes - field order, the version prefix, the body-hash
// encoding - while every signature a deployed iPhone client produces becomes
// invalid. These bytes were produced by this code and must not move without a
// client release: the fixed key, timestamp and nonce make the expected
// signature exact.
func TestMemberSigningInputIsFrozen(t *testing.T) {
	const (
		wantMemberID  = "mpPB7OcXItIdNQKxrg2PnaqGlxnA4b3OrePAjZUQuYo="
		wantPeerID    = "12D3KooWRawPbxPtP1eZaJpumGnyWX2DcUyd3RQnydr3eAto4Az7"
		wantPubKey    = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw="
		wantSignature = "ceBe+/ezCHpmAN99yODSEIZ8GcAakPgdmENAcLy6G3vhR+WDPUHAlrAC0qMSQykdu8619XHjy8VM7uV+fQcjBQ=="
		wantInput     = "ENTMOOT-ESP-MEMBER-AUTH-V2\n" +
			"GET\n" +
			"/v1/session\n" +
			wantMemberID + "\n" +
			wantPeerID + "\n" +
			wantPubKey + "\n" +
			"1700000000000\n" +
			"golden-nonce\n" +
			"47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
	)

	signer := newTestMemberSigner(t, 0x07)
	if got := signer.memberID.String(); got != wantMemberID {
		t.Fatalf("member id = %q, want %q (the vector's key changed)", got, wantMemberID)
	}
	if signer.peerID != wantPeerID {
		t.Fatalf("peer id = %q, want %q", signer.peerID, wantPeerID)
	}
	if got := base64.StdEncoding.EncodeToString(signer.pub); got != wantPubKey {
		t.Fatalf("public key = %q, want %q", got, wantPubKey)
	}

	got := MemberSigningInput("GET", "/v1/session", signer.memberID, signer.peerID, signer.pub, 1700000000000, "golden-nonce", nil)
	if got != wantInput {
		t.Fatalf("signing input changed:\n got %q\nwant %q", got, wantInput)
	}
	gotSignature := base64.StdEncoding.EncodeToString(ed25519.Sign(signer.priv, []byte(got)))
	if gotSignature != wantSignature {
		t.Fatalf("signature = %q, want %q", gotSignature, wantSignature)
	}
}
