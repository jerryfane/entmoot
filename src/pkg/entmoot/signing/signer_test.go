package signing

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

func TestLocalSignerSignsVerifiableMessage(t *testing.T) {
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	author := entmoot.NodeInfo{PilotNodeID: 10, EntmootPubKey: id.PublicKey}
	signer, err := NewLocalSigner(author, id)
	if err != nil {
		t.Fatalf("NewLocalSigner: %v", err)
	}

	msg, err := SignMessage(context.Background(), signer, entmoot.Message{
		GroupID:   groupID(1),
		Timestamp: 1000,
		Topics:    []string{"agents/status"},
		Content:   []byte("hello"),
	})
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	if msg.ID == (entmoot.MessageID{}) {
		t.Fatalf("message id was not populated")
	}
	if len(msg.Signature) != ed25519.SignatureSize {
		t.Fatalf("signature length = %d", len(msg.Signature))
	}
	if err := VerifyMessage(msg, author); err != nil {
		t.Fatalf("VerifyMessage: %v", err)
	}
}

func TestExternalSignerAllowsPhoneHeldIdentity(t *testing.T) {
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	author := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: id.PublicKey}
	signer, err := NewExternalSigner(author, func(_ context.Context, payload []byte) ([]byte, error) {
		return id.Sign(payload), nil
	})
	if err != nil {
		t.Fatalf("NewExternalSigner: %v", err)
	}

	msg, err := signer.SignMessage(context.Background(), entmoot.Message{
		GroupID:   groupID(2),
		Timestamp: 2000,
		Topics:    []string{"mobile/inbox"},
		Content:   []byte("signed off-device"),
	})
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	if err := VerifyMessage(msg, author); err != nil {
		t.Fatalf("VerifyMessage: %v", err)
	}

	msg.Content[0] ^= 0xff
	if err := VerifyMessage(msg, author); err == nil {
		t.Fatalf("VerifyMessage accepted tampered content")
	}
}

func TestVerifyMessageCanonicalIDMismatchWrapsSigInvalid(t *testing.T) {
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	author := entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: id.PublicKey}
	signer, err := NewExternalSigner(author, func(_ context.Context, payload []byte) ([]byte, error) {
		return id.Sign(payload), nil
	})
	if err != nil {
		t.Fatalf("NewExternalSigner: %v", err)
	}
	msg, err := signer.SignMessage(context.Background(), entmoot.Message{
		GroupID:   groupID(2),
		Timestamp: 2000,
		Topics:    []string{"mobile/inbox"},
		Content:   []byte("signed off-device"),
	})
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	msg.ID[0] ^= 0xff
	err = VerifyMessage(msg, author)
	if !errors.Is(err, entmoot.ErrSigInvalid) {
		t.Fatalf("VerifyMessage err = %v, want ErrSigInvalid", err)
	}
}

func TestLocalSignerRejectsMismatchedAuthorKey(t *testing.T) {
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	other, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate other: %v", err)
	}
	_, err = NewLocalSigner(entmoot.NodeInfo{PilotNodeID: 10, EntmootPubKey: other.PublicKey}, id)
	if err == nil {
		t.Fatalf("NewLocalSigner accepted mismatched author key")
	}
}

func groupID(seed byte) entmoot.GroupID {
	var g entmoot.GroupID
	g[0] = seed
	return g
}
