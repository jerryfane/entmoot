// Package keystore manages the on-disk Ed25519 identity used by an Entmoot
// node. Identities are persisted as JSON with base64-encoded keys, mirroring
// Pilot's identity.json shape so operators can inspect either file the same
// way. The package provides generation, atomic save, load, and sign/verify
// helpers; it has no runtime state or globals.
package keystore

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Identity holds an Ed25519 keypair for an Entmoot node. The zero value is
// not usable; construct via Generate, Load, or LoadOrGenerate.
type Identity struct {
	// PrivateKey is the 64-byte Ed25519 private key (seed || public key).
	PrivateKey ed25519.PrivateKey
	// PublicKey is the 32-byte Ed25519 public key. It equals
	// PrivateKey.Public().(ed25519.PublicKey).
	PublicKey ed25519.PublicKey
}

// identityFile is the on-disk JSON shape. Keys are std-base64 (not URL-safe)
// to match Pilot's identity.json for operator familiarity.
type identityFile struct {
	PrivateKey string `json:"private_key"`
	PublicKey  string `json:"public_key"`
}

// Generate creates a fresh Ed25519 identity using crypto/rand as the entropy
// source. It returns an error only if the system random source fails.
func Generate() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("keystore: generate ed25519 key: %w", err)
	}
	return &Identity{PrivateKey: priv, PublicKey: pub}, nil
}

// Load reads an identity from path. If the file does not exist, the returned
// error wraps os.ErrNotExist (check with errors.Is). Malformed JSON, wrong
// key lengths, or a private/public-key mismatch produce descriptive errors.
func Load(path string) (*Identity, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		// os.ReadFile returns an error that already satisfies errors.Is(err, os.ErrNotExist)
		// for missing files; wrap with %w to preserve that.
		return nil, fmt.Errorf("keystore: read %s: %w", path, err)
	}

	var f identityFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("keystore: parse %s: %w", path, err)
	}

	privBytes, err := base64.StdEncoding.DecodeString(f.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("keystore: decode private_key: %w", err)
	}
	pubBytes, err := base64.StdEncoding.DecodeString(f.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("keystore: decode public_key: %w", err)
	}

	if len(privBytes) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("keystore: private_key length = %d, want %d",
			len(privBytes), ed25519.PrivateKeySize)
	}
	if len(pubBytes) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("keystore: public_key length = %d, want %d",
			len(pubBytes), ed25519.PublicKeySize)
	}

	priv := ed25519.PrivateKey(privBytes)
	pub := ed25519.PublicKey(pubBytes)

	// Defensively verify the stored pubkey matches the one derived from
	// the private key. A mismatch indicates a corrupt or tampered file.
	derived, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("keystore: private_key did not yield ed25519 public key")
	}
	if string(derived) != string(pub) {
		return nil, errors.New("keystore: public_key does not match private_key")
	}

	return &Identity{PrivateKey: priv, PublicKey: pub}, nil
}

// LoadOrGenerate returns the identity stored at path. If the file does not
// exist, it generates a new identity and saves it to path (creating the
// parent directory with 0700 and the file with 0600). Any other load error
// is returned unchanged.
func LoadOrGenerate(path string) (*Identity, error) {
	id, err := Load(path)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	id, err = Generate()
	if err != nil {
		return nil, err
	}
	if err := id.Save(path); err != nil {
		return nil, err
	}
	return id, nil
}

// Save writes the identity to path atomically: it writes to "<path>.tmp"
// with 0600 permissions and then renames to path. The parent directory is
// created with 0700 permissions if missing. On any error the temp file is
// best-effort removed.
func (id *Identity) Save(path string) error {
	if id == nil || len(id.PrivateKey) != ed25519.PrivateKeySize ||
		len(id.PublicKey) != ed25519.PublicKeySize {
		return errors.New("keystore: cannot save invalid identity")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("keystore: mkdir %s: %w", dir, err)
	}

	f := identityFile{
		PrivateKey: base64.StdEncoding.EncodeToString(id.PrivateKey),
		PublicKey:  base64.StdEncoding.EncodeToString(id.PublicKey),
	}
	data, err := json.MarshalIndent(&f, "", "  ")
	if err != nil {
		return fmt.Errorf("keystore: marshal identity: %w", err)
	}
	// Trailing newline for tidiness — matches what editors produce.
	data = append(data, '\n')

	tmp := path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("keystore: open temp %s: %w", tmp, err)
	}
	cleanup := func() { _ = os.Remove(tmp) }
	if _, err := tf.Write(data); err != nil {
		_ = tf.Close()
		cleanup()
		return fmt.Errorf("keystore: write temp %s: %w", tmp, err)
	}
	if err := tf.Sync(); err != nil {
		_ = tf.Close()
		cleanup()
		return fmt.Errorf("keystore: fsync temp %s: %w", tmp, err)
	}
	if err := tf.Close(); err != nil {
		cleanup()
		return fmt.Errorf("keystore: close temp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		cleanup()
		return fmt.Errorf("keystore: rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}

// Sign signs msg with the identity's private key and returns the 64-byte
// Ed25519 signature. It panics only if the identity is misconstructed
// (wrong private-key length), which Generate/Load prevent.
func (id *Identity) Sign(msg []byte) []byte {
	return ed25519.Sign(id.PrivateKey, msg)
}

// Verify reports whether sig is a valid Ed25519 signature over msg under
// pub. It returns false for any malformed input (wrong key length, wrong
// signature length, etc.) rather than panicking.
func Verify(pub ed25519.PublicKey, msg, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, msg, sig)
}
