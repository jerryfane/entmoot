package defaultmoot

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/policy"
)

func TestDescriptorVerifyValid(t *testing.T) {
	pub, priv := testDescriptorKey(t)
	desc := testSignedDescriptor(t, priv)

	if err := Verify(desc, pub); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestDescriptorVerifyRejectsTamperedDescriptor(t *testing.T) {
	pub, priv := testDescriptorKey(t)
	desc := testSignedDescriptor(t, priv)
	desc.OpenInvite.Token = "tampered-token"

	err := Verify(desc, pub)
	if err == nil || !errors.Is(err, ErrDescriptorSignature) {
		t.Fatalf("Verify tampered err = %v, want ErrDescriptorSignature", err)
	}
}

func TestDescriptorVerifyRejectsWrongKey(t *testing.T) {
	_, priv := testDescriptorKey(t)
	wrongPub, _ := testDescriptorKey(t)
	desc := testSignedDescriptor(t, priv)

	err := Verify(desc, wrongPub)
	if err == nil || !errors.Is(err, ErrDescriptorSignature) {
		t.Fatalf("Verify wrong key err = %v, want ErrDescriptorSignature", err)
	}
}

func TestDescriptorVerifyRejectsMissingRequiredFields(t *testing.T) {
	pub, priv := testDescriptorKey(t)
	desc := testSignedDescriptor(t, priv)
	desc.Name = ""
	resigned, err := Sign(desc, priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	err = Verify(resigned, pub)
	if err == nil || !strings.Contains(err.Error(), "name must be") {
		t.Fatalf("Verify missing name err = %v, want name validation", err)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	pub, _ := testDescriptorKey(t)
	pubRaw := base64.StdEncoding.EncodeToString(pub)

	cfg, err := ConfigFromEnv(func(key string) string {
		switch key {
		case EnvDescriptorURL:
			return "http://127.0.0.1/descriptor.json"
		case EnvDescriptorPubKey:
			return pubRaw
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.URL != "http://127.0.0.1/descriptor.json" {
		t.Fatalf("URL = %q", cfg.URL)
	}
	if string(cfg.PinnedPublicKey) != string(pub) {
		t.Fatalf("PinnedPublicKey mismatch")
	}
}

func TestConfigFromEnvUsesCompiledPinnedKey(t *testing.T) {
	cfg, err := ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.URL != DefaultDescriptorURL {
		t.Fatalf("URL = %q, want %q", cfg.URL, DefaultDescriptorURL)
	}
	if len(cfg.PinnedPublicKey) != ed25519.PublicKeySize {
		t.Fatalf("PinnedPublicKey length = %d, want %d", len(cfg.PinnedPublicKey), ed25519.PublicKeySize)
	}
}

func TestDescriptorVerifyRejectsMalformedOpenInviteLink(t *testing.T) {
	pub, priv := testDescriptorKey(t)
	desc := testSignedDescriptor(t, priv)
	desc.OpenInvite = OpenInviteDescriptor{Link: "entmoot://open-invite?issuer=https://esp.example.com"}
	resigned, err := Sign(desc, priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	err = Verify(resigned, pub)
	if err == nil || !strings.Contains(err.Error(), "token is required") {
		t.Fatalf("Verify malformed link err = %v, want token validation", err)
	}
}

func TestDescriptorVerifyRejectsConflictingOpenInviteFields(t *testing.T) {
	pub, priv := testDescriptorKey(t)
	desc := testSignedDescriptor(t, priv)
	desc.OpenInvite.Link = "entmoot://open-invite?issuer=https%3A%2F%2Fother.example.com&token=test-token"
	resigned, err := Sign(desc, priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	err = Verify(resigned, pub)
	if err == nil || !strings.Contains(err.Error(), "does not match issuer_url/token fields") {
		t.Fatalf("Verify conflicting fields err = %v, want mismatch validation", err)
	}
}

func TestDescriptorVerifyRejectsIssuerURLQueryOrFragment(t *testing.T) {
	pub, priv := testDescriptorKey(t)
	for _, tc := range []struct {
		name       string
		openInvite OpenInviteDescriptor
	}{
		{
			name: "direct issuer url query",
			openInvite: OpenInviteDescriptor{
				IssuerURL: "https://esp.example.com/base?tenant=a",
				Token:     "test-token",
			},
		},
		{
			name: "link issuer url fragment",
			openInvite: OpenInviteDescriptor{
				Link: "entmoot://open-invite?issuer=https%3A%2F%2Fesp.example.com%2Fbase%23tenant-a&token=test-token",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desc := testSignedDescriptor(t, priv)
			desc.OpenInvite = tc.openInvite
			resigned, err := Sign(desc, priv)
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}

			err = Verify(resigned, pub)
			if err == nil || !strings.Contains(err.Error(), "query or fragment") {
				t.Fatalf("Verify issuer URL err = %v, want query/fragment validation", err)
			}
		})
	}
}

func TestFetchAndVerifyUsesOverrides(t *testing.T) {
	pub, priv := testDescriptorKey(t)
	desc := testSignedDescriptor(t, priv)
	raw, err := json.Marshal(desc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	}))
	t.Cleanup(srv.Close)

	cfg, err := ConfigFromEnv(func(key string) string {
		switch key {
		case EnvDescriptorURL:
			return srv.URL
		case EnvDescriptorPubKey:
			return base64.StdEncoding.EncodeToString(pub)
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}

	got, err := FetchAndVerify(context.Background(), srv.Client(), cfg)
	if err != nil {
		t.Fatalf("FetchAndVerify: %v", err)
	}
	if got.GroupID != desc.GroupID || got.Name != Name {
		t.Fatalf("descriptor = %+v, want group %s name %q", got, desc.GroupID, Name)
	}
}

func testDescriptorKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

func testSignedDescriptor(t *testing.T, priv ed25519.PrivateKey) Descriptor {
	t.Helper()
	desc := Descriptor{
		Type:    DescriptorType,
		Name:    Name,
		GroupID: testGroupID(0x42),
		OpenInvite: OpenInviteDescriptor{
			IssuerURL: "https://entmoot.xyz",
			Token:     "test-token",
		},
		Issuer: entmoot.NodeInfo{
			PilotNodeID:   45491,
			EntmootPubKey: make([]byte, ed25519.PublicKeySize),
		},
		DefaultTopics: []string{"chat/general", "introductions"},
		RecommendedLiveConfig: RecommendedLiveConfig{
			Mode:           "converse",
			AllowedActions: []string{"reply"},
			MaxActions:     policy.DefaultLiveMaxActionsPerScan,
			MaxActionBytes: policy.DefaultLiveMaxActionBytes,
		},
		Policy:     policy.TheEntMootDefault(),
		IssuedAtMS: 1_700_000_000_000,
	}
	signed, err := Sign(desc, priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return signed
}

func testGroupID(fill byte) entmoot.GroupID {
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = fill
	}
	return gid
}
