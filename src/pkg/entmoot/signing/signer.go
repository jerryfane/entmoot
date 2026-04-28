// Package signing owns Entmoot message signing and verification primitives.
//
// The package deliberately separates authorship from the process that happens
// to run entmootd. The default LocalSigner preserves the existing on-disk
// keystore behavior, while ExternalSigner lets a future mobile/client signer
// produce the same canonical signatures without giving the service peer the
// author's private key.
package signing

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// ErrInvalidSigner is returned when a signer is missing identity material or
// cannot produce a valid Ed25519 signature.
var ErrInvalidSigner = errors.New("signing: invalid signer")

// Signer produces Entmoot author signatures for canonical message payloads.
type Signer interface {
	Author() entmoot.NodeInfo
	SignMessage(ctx context.Context, msg entmoot.Message) (entmoot.Message, error)
}

// LocalSigner signs with the existing Entmoot keystore.
type LocalSigner struct {
	author entmoot.NodeInfo
	id     *keystore.Identity
}

// NewLocalSigner returns a signer backed by id. author is the roster identity
// peers should verify against; its EntmootPubKey must match id.PublicKey.
func NewLocalSigner(author entmoot.NodeInfo, id *keystore.Identity) (*LocalSigner, error) {
	if id == nil {
		return nil, fmt.Errorf("%w: nil identity", ErrInvalidSigner)
	}
	if len(author.EntmootPubKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: author pubkey length %d", ErrInvalidSigner, len(author.EntmootPubKey))
	}
	if !equalBytes(author.EntmootPubKey, id.PublicKey) {
		return nil, fmt.Errorf("%w: author pubkey does not match local identity", ErrInvalidSigner)
	}
	return &LocalSigner{author: cloneNodeInfo(author), id: id}, nil
}

func (s *LocalSigner) Author() entmoot.NodeInfo {
	return cloneNodeInfo(s.author)
}

func (s *LocalSigner) SignMessage(_ context.Context, msg entmoot.Message) (entmoot.Message, error) {
	msg.Author = s.Author()
	return signWith(msg, func(payload []byte) ([]byte, error) {
		return s.id.Sign(payload), nil
	})
}

// SignFunc is the external/mobile signing callback used by ExternalSigner.
type SignFunc func(ctx context.Context, payload []byte) ([]byte, error)

// ExternalSigner signs through a caller-supplied callback. It is intentionally
// small: the callback might be an HTTPS bridge, a local secure enclave bridge,
// or a test fake, but the canonical payload and returned signature semantics
// stay identical.
type ExternalSigner struct {
	author entmoot.NodeInfo
	sign   SignFunc
}

// NewExternalSigner returns a signer that asks sign to sign canonical message
// bytes for author.
func NewExternalSigner(author entmoot.NodeInfo, sign SignFunc) (*ExternalSigner, error) {
	if len(author.EntmootPubKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: author pubkey length %d", ErrInvalidSigner, len(author.EntmootPubKey))
	}
	if sign == nil {
		return nil, fmt.Errorf("%w: nil external sign func", ErrInvalidSigner)
	}
	return &ExternalSigner{author: cloneNodeInfo(author), sign: sign}, nil
}

func (s *ExternalSigner) Author() entmoot.NodeInfo {
	return cloneNodeInfo(s.author)
}

func (s *ExternalSigner) SignMessage(ctx context.Context, msg entmoot.Message) (entmoot.Message, error) {
	msg.Author = s.Author()
	return signWith(msg, func(payload []byte) ([]byte, error) {
		return s.sign(ctx, payload)
	})
}

// SignMessage fills Author, ID, and Signature using signer.
func SignMessage(ctx context.Context, signer Signer, msg entmoot.Message) (entmoot.Message, error) {
	if signer == nil {
		return entmoot.Message{}, fmt.Errorf("%w: nil signer", ErrInvalidSigner)
	}
	return signer.SignMessage(ctx, msg)
}

// VerifyMessage verifies msg against the supplied author identity.
func VerifyMessage(msg entmoot.Message, author entmoot.NodeInfo) error {
	if len(author.EntmootPubKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: author pubkey length %d", entmoot.ErrSigInvalid, len(author.EntmootPubKey))
	}
	signingBytes, err := MessageSigningBytes(msg)
	if err != nil {
		return err
	}
	if !keystore.Verify(author.EntmootPubKey, signingBytes, msg.Signature) {
		return fmt.Errorf("%w: message %s", entmoot.ErrSigInvalid, msg.ID)
	}
	if canonical.MessageID(msg) != msg.ID {
		return fmt.Errorf("%w: message id does not match canonical hash", entmoot.ErrSigInvalid)
	}
	return nil
}

// MessageSigningBytes returns the canonical bytes covered by a message
// signature.
func MessageSigningBytes(msg entmoot.Message) ([]byte, error) {
	signing := msg
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil
	return canonical.Encode(signing)
}

func signWith(msg entmoot.Message, sign func([]byte) ([]byte, error)) (entmoot.Message, error) {
	msg.ID = canonical.MessageID(msg)
	signingBytes, err := MessageSigningBytes(msg)
	if err != nil {
		return entmoot.Message{}, err
	}
	msg.Signature, err = sign(signingBytes)
	if err != nil {
		return entmoot.Message{}, fmt.Errorf("signing: sign: %w", err)
	}
	if len(msg.Signature) != ed25519.SignatureSize {
		return entmoot.Message{}, fmt.Errorf("%w: signature length %d", ErrInvalidSigner, len(msg.Signature))
	}
	if err := VerifyMessage(msg, msg.Author); err != nil {
		return entmoot.Message{}, fmt.Errorf("%w: produced unverifiable message: %w", ErrInvalidSigner, err)
	}
	return msg, nil
}

func cloneNodeInfo(in entmoot.NodeInfo) entmoot.NodeInfo {
	out := in
	out.EntmootPubKey = append([]byte(nil), in.EntmootPubKey...)
	return out
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
