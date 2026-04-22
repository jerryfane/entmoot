package reconcile

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"entmoot/pkg/entmoot"
)

// Fingerprint is the 16-byte compact summary of a set of message ids
// produced by Accumulator.Finalize. Two peers whose fingerprints agree for
// the same half-open range [lo, hi) can conclude their local sets for that
// range are equal with overwhelming probability (collision resistance of
// the truncated SHA-256 output).
type Fingerprint [16]byte

// String returns the base64 (standard encoding, with padding) form of the
// fingerprint. Useful in logs and test diagnostics.
func (f Fingerprint) String() string {
	return base64.StdEncoding.EncodeToString(f[:])
}

// MarshalJSON encodes the fingerprint as a base64 JSON string rather than
// the default numeric-array form, mirroring wire.MerkleRoot so wire frames
// stay debuggable.
func (f Fingerprint) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.String())
}

// UnmarshalJSON decodes a base64 JSON string into the 16-byte fingerprint.
func (f *Fingerprint) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("reconcile.Fingerprint: expected base64 string: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("reconcile.Fingerprint: invalid base64: %w", err)
	}
	if len(raw) != len(f) {
		return fmt.Errorf("reconcile.Fingerprint: expected %d bytes, got %d", len(f), len(raw))
	}
	copy(f[:], raw)
	return nil
}

// Accumulator incrementally builds a range fingerprint by folding each
// member id through an inner SHA-256 (to defend against XOR-cancellation
// and duplicate-erasure attacks) and then summing the results as a 256-bit
// big-endian integer modulo 2^256. The count and sum are finalised together
// via an outer SHA-256 so that adding the same id twice produces a
// different fingerprint than adding it once.
//
// The zero value of Accumulator is a valid empty accumulator.
type Accumulator struct {
	count uint64
	sum   [32]byte
}

// Add incorporates id into the accumulator. It hashes id via SHA-256 first
// so the commutative (sum, count) accumulator cannot be cancelled by a
// crafted preimage, then adds the 256-bit big-endian hash to a.sum with
// carry and increments count.
func (a *Accumulator) Add(id entmoot.MessageID) {
	h := sha256.Sum256(id[:])
	addUint256BE(&a.sum, h)
	a.count++
}

// Finalize returns the fingerprint for the currently accumulated set:
// the first 16 bytes of SHA-256( count_u64_le || sum_be32 ).
//
// Finalize does not mutate the accumulator, so callers can take a
// fingerprint, continue to Add, and take another fingerprint.
func (a *Accumulator) Finalize() Fingerprint {
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], a.count)
	h := sha256.New()
	h.Write(header[:])
	h.Write(a.sum[:])
	digest := h.Sum(nil)
	var fp Fingerprint
	copy(fp[:], digest[:16])
	return fp
}

// Count returns the number of ids currently folded into the accumulator.
func (a *Accumulator) Count() uint64 {
	return a.count
}

// addUint256BE computes *dst = (*dst + addend) mod 2^256 treating both as
// 32-byte big-endian unsigned integers.
func addUint256BE(dst *[32]byte, addend [32]byte) {
	var carry uint16
	for i := 31; i >= 0; i-- {
		s := uint16(dst[i]) + uint16(addend[i]) + carry
		dst[i] = byte(s & 0xff)
		carry = s >> 8
	}
	// Any remaining carry is discarded: arithmetic is modulo 2^256.
}
