package reconcile

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"testing"

	"entmoot/pkg/entmoot"
)

// mkID fills a MessageID with the given byte. Convenient for test fixtures
// with obviously-distinct ids.
func mkID(fill byte) entmoot.MessageID {
	var m entmoot.MessageID
	for i := range m {
		m[i] = fill
	}
	return m
}

// TestFingerprintPermutationInvariant asserts that the accumulator produces
// the same fingerprint regardless of insertion order. Fifty shuffles over
// fifty ids, each seeded deterministically so failures are reproducible.
func TestFingerprintPermutationInvariant(t *testing.T) {
	const n = 50
	base := make([]entmoot.MessageID, n)
	for i := range base {
		base[i] = mkID(byte(i + 1))
	}

	var ref Accumulator
	for _, id := range base {
		ref.Add(id)
	}
	want := ref.Finalize()

	for seed := int64(1); seed <= 50; seed++ {
		r := rand.New(rand.NewSource(seed))
		shuf := make([]entmoot.MessageID, n)
		copy(shuf, base)
		r.Shuffle(n, func(i, j int) { shuf[i], shuf[j] = shuf[j], shuf[i] })

		var acc Accumulator
		for _, id := range shuf {
			acc.Add(id)
		}
		got := acc.Finalize()
		if got != want {
			t.Fatalf("seed=%d: fingerprint mismatch under shuffle: got %s want %s",
				seed, got, want)
		}
	}
}

// TestFingerprintDuplicateChanges asserts that adding the same id twice
// produces a different fingerprint than adding it once. This is the
// regression for the naive-XOR accumulator bug: a hypothetical
// implementation that XORs raw ids would erase duplicates and give
// identical fingerprints for {id} and {id, id}.
func TestFingerprintDuplicateChanges(t *testing.T) {
	id := mkID(0x42)

	var once Accumulator
	once.Add(id)
	onceFP := once.Finalize()

	var twice Accumulator
	twice.Add(id)
	twice.Add(id)
	twiceFP := twice.Finalize()

	if onceFP == twiceFP {
		t.Fatalf("duplicate id yielded same fingerprint as single id — XOR regression?")
	}
	if once.Count() != 1 {
		t.Fatalf("once.Count() = %d, want 1", once.Count())
	}
	if twice.Count() != 2 {
		t.Fatalf("twice.Count() = %d, want 2", twice.Count())
	}
}

// TestFingerprintEmptyIsZero asserts that the empty accumulator's
// fingerprint equals the documented SHA-256 of (count_u64_le(0) || 0^32),
// truncated to 16 bytes.
func TestFingerprintEmptyIsZero(t *testing.T) {
	var a Accumulator
	got := a.Finalize()

	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], 0)
	var sum [32]byte
	h := sha256.New()
	h.Write(header[:])
	h.Write(sum[:])
	digest := h.Sum(nil)
	var want Fingerprint
	copy(want[:], digest[:16])

	if got != want {
		t.Fatalf("empty fingerprint = %s, want %s", got, want)
	}
	if a.Count() != 0 {
		t.Fatalf("empty Count() = %d, want 0", a.Count())
	}
}

// TestFingerprintGoldenVector pins the accumulator's output to a specific
// set of 10 ids so that an unintended change in the hashing scheme is
// caught at review time rather than on the wire between peers.
func TestFingerprintGoldenVector(t *testing.T) {
	// Ten ids with a deterministic sha256-derived shape: id_i = sha256("golden-i").
	ids := make([]entmoot.MessageID, 10)
	for i := range ids {
		h := sha256.Sum256([]byte{'g', 'o', 'l', 'd', 'e', 'n', '-', byte('0' + i)})
		ids[i] = entmoot.MessageID(h)
	}

	var acc Accumulator
	for _, id := range ids {
		acc.Add(id)
	}
	got := acc.Finalize()
	gotHex := hex.EncodeToString(got[:])

	// Computed from the above construction with this implementation; if
	// this changes any v1.2.1 peer upgrading will disagree with an
	// un-upgraded peer on every range, so the value is intentionally
	// pinned here.
	const want = "3081720d30373f326ce6eb0c794d8b4c"
	if gotHex != want {
		t.Fatalf("golden fingerprint mismatch\n  got:  %s\n  want: %s", gotHex, want)
	}
}
