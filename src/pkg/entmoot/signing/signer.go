// Package signing owns Entmoot message signing and verification primitives.
//
// The package deliberately separates authorship from the process that happens
// to run entmootd: LocalSigner holds the on-disk keystore identity, and
// signWith produces the canonical payload every verifier re-derives.
package signing

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
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
	author, err := operationalAuthor(author)
	if err != nil {
		return nil, err
	}
	return &LocalSigner{author: author, id: id}, nil
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

func operationalAuthor(author entmoot.NodeInfo) (entmoot.NodeInfo, error) {
	if err := entmoot.ValidateOperationalMemberInfo(author); err != nil {
		return entmoot.NodeInfo{}, fmt.Errorf("%w: %v", ErrInvalidSigner, err)
	}
	return cloneNodeInfo(author), nil
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
	if entmoot.MessageID(sha256.Sum256(signingBytes)) != msg.ID {
		return fmt.Errorf("%w: message id does not match canonical hash", entmoot.ErrSigInvalid)
	}
	return nil
}

// MessageSigningBytes returns the canonical bytes covered by an author's
// message signature.
func MessageSigningBytes(msg entmoot.Message) ([]byte, error) {
	return canonical.MessageSigningBytes(msg)
}

func signWith(msg entmoot.Message, sign func([]byte) ([]byte, error)) (entmoot.Message, error) {
	signingBytes, err := MessageSigningBytes(msg)
	if err != nil {
		return entmoot.Message{}, err
	}
	msg.ID = entmoot.MessageID(sha256.Sum256(signingBytes))
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
	if in.MemberID != nil {
		memberID := *in.MemberID
		out.MemberID = &memberID
	}
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
