package keystore

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// helperPath returns a path inside a per-test temp dir.
func helperPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

func TestGenerate_SignVerifyRoundTrip(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(id.PrivateKey) != ed25519.PrivateKeySize {
		t.Fatalf("private key size = %d, want %d", len(id.PrivateKey), ed25519.PrivateKeySize)
	}
	if len(id.PublicKey) != ed25519.PublicKeySize {
		t.Fatalf("public key size = %d, want %d", len(id.PublicKey), ed25519.PublicKeySize)
	}

	msg := []byte("hello entmoot")
	sig := id.Sign(msg)
	if len(sig) != ed25519.SignatureSize {
		t.Fatalf("signature size = %d, want %d", len(sig), ed25519.SignatureSize)
	}
	if !Verify(id.PublicKey, msg, sig) {
		t.Fatalf("Verify returned false for valid signature")
	}
}

func TestSaveThenLoad_Roundtrip(t *testing.T) {
	path := helperPath(t, "identity.json")

	orig, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := orig.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if string(loaded.PublicKey) != string(orig.PublicKey) {
		t.Fatalf("public key mismatch after round-trip")
	}
	if string(loaded.PrivateKey) != string(orig.PrivateKey) {
		t.Fatalf("private key mismatch after round-trip")
	}

	// Functional round-trip: sign with loaded, verify with original pubkey.
	msg := []byte("round-trip payload")
	sig := loaded.Sign(msg)
	if !Verify(orig.PublicKey, msg, sig) {
		t.Fatalf("signature from loaded key did not verify under original public key")
	}
}

func TestLoad_MissingFileWrapsNotExist(t *testing.T) {
	path := helperPath(t, "does_not_exist.json")
	_, err := Load(path)
	if err == nil {
		t.Fatalf("Load on missing file: expected error, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load error does not wrap os.ErrNotExist: %v", err)
	}
}

func TestLoadOrGenerate_CreatesWhenMissing(t *testing.T) {
	// Use a nested path to exercise MkdirAll.
	path := filepath.Join(t.TempDir(), "nested", "dir", "identity.json")

	id, err := LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("LoadOrGenerate (missing): %v", err)
	}
	if id == nil {
		t.Fatalf("LoadOrGenerate returned nil identity")
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after LoadOrGenerate: %v", err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("file perms = %o, want 0600", st.Mode().Perm())
	}
}

func TestLoadOrGenerate_ReturnsSameOnExisting(t *testing.T) {
	path := helperPath(t, "identity.json")

	first, err := LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("first LoadOrGenerate: %v", err)
	}

	second, err := LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("second LoadOrGenerate: %v", err)
	}

	if string(first.PublicKey) != string(second.PublicKey) {
		t.Fatalf("LoadOrGenerate did not return the same identity on existing file")
	}
	if string(first.PrivateKey) != string(second.PrivateKey) {
		t.Fatalf("LoadOrGenerate private keys differ on existing file")
	}
}

func TestSave_FilePerms0600(t *testing.T) {
	path := helperPath(t, "identity.json")

	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := id.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := st.Mode().Perm(); got != 0o600 {
		t.Fatalf("file perms = %o, want 0600", got)
	}

	// Also verify the parent directory perm contract is at-most 0700
	// when we created it ourselves.
	nestedPath := filepath.Join(t.TempDir(), "fresh-subdir", "identity.json")
	if err := id.Save(nestedPath); err != nil {
		t.Fatalf("Save nested: %v", err)
	}
	dirSt, err := os.Stat(filepath.Dir(nestedPath))
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !dirSt.IsDir() {
		t.Fatalf("parent is not a directory")
	}
	if got := dirSt.Mode().Perm(); got != 0o700 {
		t.Fatalf("created dir perms = %o, want 0700", got)
	}
}

func TestVerify_WrongSignatureFails(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	msg := []byte("payload")
	sig := id.Sign(msg)

	// Flip a byte in the signature.
	tampered := make([]byte, len(sig))
	copy(tampered, sig)
	tampered[0] ^= 0xFF

	if Verify(id.PublicKey, msg, tampered) {
		t.Fatalf("Verify accepted tampered signature")
	}
}

func TestVerify_WrongPubkeyFails(t *testing.T) {
	a, err := Generate()
	if err != nil {
		t.Fatalf("Generate a: %v", err)
	}
	b, err := Generate()
	if err != nil {
		t.Fatalf("Generate b: %v", err)
	}
	msg := []byte("payload")
	sig := a.Sign(msg)

	if Verify(b.PublicKey, msg, sig) {
		t.Fatalf("Verify accepted signature under wrong public key")
	}
}

func TestVerify_TamperedMessageFails(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	msg := []byte("original message")
	sig := id.Sign(msg)

	tamperedMsg := []byte("original messagf") // last byte flipped
	if Verify(id.PublicKey, tamperedMsg, sig) {
		t.Fatalf("Verify accepted signature over tampered message")
	}
}

func TestVerify_RejectsMalformedInputs(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	msg := []byte("payload")
	sig := id.Sign(msg)

	// Short public key.
	if Verify(ed25519.PublicKey{0x00}, msg, sig) {
		t.Fatalf("Verify accepted short public key")
	}
	// Short signature.
	if Verify(id.PublicKey, msg, []byte{0x00}) {
		t.Fatalf("Verify accepted short signature")
	}
}

func TestLoad_RejectsMalformedFiles(t *testing.T) {
	dir := t.TempDir()

	// Not JSON.
	badJSONPath := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badJSONPath, []byte("not-json"), 0o600); err != nil {
		t.Fatalf("write bad json: %v", err)
	}
	if _, err := Load(badJSONPath); err == nil {
		t.Fatalf("Load accepted non-JSON file")
	}

	// Wrong key length.
	shortKeyPath := filepath.Join(dir, "short.json")
	payload := identityFile{
		PrivateKey: "AAA=",
		PublicKey:  "AAA=",
	}
	b, _ := json.Marshal(&payload)
	if err := os.WriteFile(shortKeyPath, b, 0o600); err != nil {
		t.Fatalf("write short key file: %v", err)
	}
	if _, err := Load(shortKeyPath); err == nil {
		t.Fatalf("Load accepted short-key identity file")
	}

	// Mismatched pub/priv (valid lengths, but pub doesn't match priv).
	a, _ := Generate()
	bID, _ := Generate()
	mismatchPath := filepath.Join(dir, "mismatch.json")
	mismatchPayload := identityFile{
		PrivateKey: encodeForTest(a.PrivateKey),
		PublicKey:  encodeForTest(bID.PublicKey),
	}
	raw, _ := json.Marshal(&mismatchPayload)
	if err := os.WriteFile(mismatchPath, raw, 0o600); err != nil {
		t.Fatalf("write mismatch file: %v", err)
	}
	if _, err := Load(mismatchPath); err == nil {
		t.Fatalf("Load accepted mismatched pub/priv keys")
	}
}

func TestSave_AtomicRename_LeavesNoTempOnSuccess(t *testing.T) {
	path := helperPath(t, "identity.json")
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := id.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file still exists after successful Save: err=%v", err)
	}
}

// encodeForTest base64-StdEncoding-encodes a byte slice, matching the
// on-disk format used by Save.
func encodeForTest(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
