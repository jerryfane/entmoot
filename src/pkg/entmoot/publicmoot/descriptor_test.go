package publicmoot

import (
	"encoding/json"
	"errors"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/policy"
)

func TestSignVerifyAndParseDescriptor(t *testing.T) {
	id := mustPublicMootIdentity(t)
	desc := mustPublicMootSign(t, id, validPublicMootDescriptor())
	if err := Verify(desc); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	raw, err := SigningBytes(desc)
	if err != nil {
		t.Fatalf("SigningBytes: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("SigningBytes returned empty input")
	}
	data, err := marshalPublicMootDescriptor(desc)
	if err != nil {
		t.Fatalf("marshalPublicMootDescriptor: %v", err)
	}
	parsed, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.OpenInvite == nil || parsed.OpenInvite.Link != desc.OpenInvite.Link {
		t.Fatalf("parsed open_invite = %+v, want preserved link", parsed.OpenInvite)
	}
	if err := Verify(parsed); err != nil {
		t.Fatalf("Verify parsed: %v", err)
	}
}

func TestVerifyRejectsTamperedDescriptor(t *testing.T) {
	id := mustPublicMootIdentity(t)
	desc := mustPublicMootSign(t, id, validPublicMootDescriptor())
	desc.Name = "Different Moot"
	if err := Verify(desc); !errors.Is(err, ErrDescriptorSignature) {
		t.Fatalf("Verify tampered err = %v, want ErrDescriptorSignature", err)
	}
}

func TestVerifyRejectsWrongFounderKey(t *testing.T) {
	id := mustPublicMootIdentity(t)
	other := mustPublicMootIdentity(t)
	desc := mustPublicMootSign(t, id, validPublicMootDescriptor())
	desc.Founder.EntmootPubKey = append([]byte(nil), other.PublicKey...)
	if err := Verify(desc); !errors.Is(err, ErrDescriptorSignature) {
		t.Fatalf("Verify wrong founder key err = %v, want ErrDescriptorSignature", err)
	}
}

func TestValidateRejectsNonPublicVisibility(t *testing.T) {
	id := mustPublicMootIdentity(t)
	for _, visibility := range []string{"private", "unlisted"} {
		desc := validPublicMootDescriptor()
		desc.Visibility = visibility
		desc = mustPublicMootSign(t, id, desc)
		if err := Verify(desc); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatalf("Verify visibility %q err = %v, want ErrInvalidDescriptor", visibility, err)
		}
	}
}

func TestValidateRejectsInvalidIndexingFlags(t *testing.T) {
	id := mustPublicMootIdentity(t)
	t.Run("directory false", func(t *testing.T) {
		desc := validPublicMootDescriptor()
		desc.Indexing.Directory = false
		desc = mustPublicMootSign(t, id, desc)
		if err := Verify(desc); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatalf("Verify directory=false err = %v, want ErrInvalidDescriptor", err)
		}
	})
	t.Run("messages true", func(t *testing.T) {
		desc := validPublicMootDescriptor()
		desc.Indexing.Messages = true
		desc = mustPublicMootSign(t, id, desc)
		if err := Verify(desc); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatalf("Verify messages=true err = %v, want ErrInvalidDescriptor", err)
		}
	})
}

func TestValidateRequiresOpenInviteOnlyForOpenInviteMode(t *testing.T) {
	id := mustPublicMootIdentity(t)
	t.Run("open invite missing descriptor", func(t *testing.T) {
		desc := validPublicMootDescriptor()
		desc.OpenInvite = nil
		desc = mustPublicMootSign(t, id, desc)
		if err := Verify(desc); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatalf("Verify open_invite missing err = %v, want ErrInvalidDescriptor", err)
		}
	})
	t.Run("invite only with open invite", func(t *testing.T) {
		desc := validPublicMootDescriptor()
		desc.JoinMode = JoinModeInviteOnly
		desc = mustPublicMootSign(t, id, desc)
		if err := Verify(desc); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatalf("Verify invite_only open_invite err = %v, want ErrInvalidDescriptor", err)
		}
	})
}

func validPublicMootDescriptor() Descriptor {
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = byte(i + 1)
	}
	return Descriptor{
		Type:        DescriptorType,
		GroupID:     gid,
		Name:        "Mars Hub",
		Description: "Public Mars coordination moot",
		Tags:        []string{"mars", "ops"},
		Visibility:  VisibilityPublic,
		JoinMode:    JoinModeOpenInvite,
		OpenInvite: &OpenInviteDescriptor{
			IssuerURL: "https://esp.example.com",
			Token:     "invite-token",
			Link:      "entmoot://open-invite?issuer=https%3A%2F%2Fesp.example.com&token=invite-token",
		},
		Policy: policy.Standard(),
		Founder: entmoot.NodeInfo{
			PilotNodeID: 42,
		},
		Indexing: Indexing{
			Directory: true,
			Messages:  false,
		},
		UpdatedAtMS: 1_000,
	}
}

func mustPublicMootIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return id
}

func mustPublicMootSign(t *testing.T, id *keystore.Identity, desc Descriptor) Descriptor {
	t.Helper()
	signed, err := Sign(desc, id)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return signed
}

func marshalPublicMootDescriptor(desc Descriptor) ([]byte, error) {
	return json.Marshal(desc)
}
