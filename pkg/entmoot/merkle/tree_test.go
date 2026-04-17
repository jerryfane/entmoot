package merkle

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"entmoot/pkg/entmoot"
)

// mkID produces a deterministic MessageID from a small integer, so tests are
// readable.
func mkID(n int) entmoot.MessageID {
	var id entmoot.MessageID
	// Fill with sha256(n) for variety.
	s := sha256.Sum256([]byte(fmt.Sprintf("id-%d", n)))
	copy(id[:], s[:])
	return id
}

func mkIDs(n int) []entmoot.MessageID {
	out := make([]entmoot.MessageID, n)
	for i := 0; i < n; i++ {
		out[i] = mkID(i)
	}
	return out
}

// manualLeaf and manualInternal mirror the construction rule so tests can
// compute expected hashes without going through the Tree.
func manualLeaf(id entmoot.MessageID) [32]byte {
	h := sha256.New()
	h.Write([]byte{0x00})
	h.Write(id[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func manualInternal(l, r [32]byte) [32]byte {
	h := sha256.New()
	h.Write([]byte{0x01})
	h.Write(l[:])
	h.Write(r[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func TestEmptyTreeHasZeroRoot(t *testing.T) {
	tr := New(nil)
	if tr.Root() != ([32]byte{}) {
		t.Fatalf("empty tree root = %x, want all zeros", tr.Root())
	}
	if tr.Len() != 0 {
		t.Fatalf("empty tree Len = %d, want 0", tr.Len())
	}
	tr2 := New([]entmoot.MessageID{})
	if tr2.Root() != ([32]byte{}) {
		t.Fatalf("empty []MessageID tree root = %x, want all zeros", tr2.Root())
	}
}

func TestSingleLeafRootIsDomainSeparatedLeafHash(t *testing.T) {
	id := mkID(0)
	tr := New([]entmoot.MessageID{id})
	want := manualLeaf(id)
	if tr.Root() != want {
		t.Fatalf("single-leaf root = %x, want %x", tr.Root(), want)
	}
}

func TestTwoLeavesRoot(t *testing.T) {
	ids := mkIDs(2)
	tr := New(ids)
	l0 := manualLeaf(ids[0])
	l1 := manualLeaf(ids[1])
	want := manualInternal(l0, l1)
	if tr.Root() != want {
		t.Fatalf("two-leaf root = %x, want %x", tr.Root(), want)
	}
}

// TestThreeLeavesRootHandComputed locks in the promotion rule for odd
// trailing nodes: leaf 2 is promoted unchanged into layer 1, then combined
// with H(L0,L1) to form the root.
func TestThreeLeavesRootHandComputed(t *testing.T) {
	ids := mkIDs(3)
	tr := New(ids)

	l0 := manualLeaf(ids[0])
	l1 := manualLeaf(ids[1])
	l2 := manualLeaf(ids[2])

	// Layer 1: [H(L0, L1), L2]  (L2 promoted)
	n01 := manualInternal(l0, l1)

	// Layer 2: [H(H(L0,L1), L2)]
	want := manualInternal(n01, l2)

	if tr.Root() != want {
		t.Fatalf("three-leaf root = %x, want %x", tr.Root(), want)
	}
}

func TestProofVerifyRoundTrip(t *testing.T) {
	for _, n := range []int{1, 2, 3, 5, 10, 100} {
		n := n
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			ids := mkIDs(n)
			tr := New(ids)
			root := tr.Root()
			for i, id := range ids {
				p, err := tr.Proof(id)
				if err != nil {
					t.Fatalf("Proof(%d): unexpected error: %v", i, err)
				}
				if p.Index != i {
					t.Fatalf("Proof(%d).Index = %d, want %d", i, p.Index, i)
				}
				if p.LeafCount != n {
					t.Fatalf("Proof(%d).LeafCount = %d, want %d", i, p.LeafCount, n)
				}
				if !Verify(root, id, p) {
					t.Fatalf("Verify failed for id %d in tree of size %d (proof: %+v)", i, n, p)
				}
			}
		})
	}
}

func TestTamperingWithRootFailsVerify(t *testing.T) {
	ids := mkIDs(10)
	tr := New(ids)
	root := tr.Root()

	for i, id := range ids {
		p, err := tr.Proof(id)
		if err != nil {
			t.Fatalf("Proof(%d): %v", i, err)
		}
		// Flip every byte in turn; each flipped root must fail.
		for b := 0; b < len(root); b++ {
			tampered := root
			tampered[b] ^= 0xff
			if Verify(tampered, id, p) {
				t.Fatalf("Verify accepted tampered root (byte %d flipped) for id %d", b, i)
			}
		}
	}
}

func TestTamperingWithSiblingFailsVerify(t *testing.T) {
	ids := mkIDs(10)
	tr := New(ids)
	root := tr.Root()

	for i, id := range ids {
		p, err := tr.Proof(id)
		if err != nil {
			t.Fatalf("Proof(%d): %v", i, err)
		}
		if len(p.Siblings) == 0 {
			// A leaf whose path is all promotions has no siblings to
			// tamper with — skip (can happen for tiny trees, but not
			// at n=10 for any leaf).
			continue
		}
		for s := range p.Siblings {
			// Make a deep copy so we don't affect other iterations.
			bad := Proof{Index: p.Index, LeafCount: p.LeafCount}
			bad.Siblings = make([][32]byte, len(p.Siblings))
			copy(bad.Siblings, p.Siblings)
			bad.Siblings[s][0] ^= 0xff
			if Verify(root, id, bad) {
				t.Fatalf("Verify accepted tampered sibling %d for id %d", s, i)
			}
		}
	}
}

func TestProofUnknownIDReturnsError(t *testing.T) {
	ids := mkIDs(5)
	tr := New(ids)
	// Pick an id that is not in the set.
	unknown := mkID(999)
	_, err := tr.Proof(unknown)
	if err == nil {
		t.Fatalf("Proof(unknown) err = nil, want non-nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Proof(unknown) err = %v, want ErrNotFound", err)
	}
}

func TestVerifyRejectsMalformedProofs(t *testing.T) {
	ids := mkIDs(5)
	tr := New(ids)
	root := tr.Root()
	id := ids[2]
	good, err := tr.Proof(id)
	if err != nil {
		t.Fatalf("Proof: %v", err)
	}

	cases := []struct {
		name string
		p    Proof
	}{
		{"negative index", Proof{Index: -1, LeafCount: 5, Siblings: good.Siblings}},
		{"zero leaf count", Proof{Index: 0, LeafCount: 0, Siblings: nil}},
		{"index >= leaf count", Proof{Index: 5, LeafCount: 5, Siblings: good.Siblings}},
		{"too few siblings", Proof{Index: good.Index, LeafCount: good.LeafCount, Siblings: good.Siblings[:len(good.Siblings)-1]}},
		{"too many siblings", Proof{Index: good.Index, LeafCount: good.LeafCount, Siblings: append(append([][32]byte{}, good.Siblings...), [32]byte{})}},
		{"single-leaf with spurious siblings", Proof{Index: 0, LeafCount: 1, Siblings: [][32]byte{{}}}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if Verify(root, id, c.p) {
				t.Fatalf("Verify accepted malformed proof: %+v", c.p)
			}
		})
	}
}

// TestWrongLeafFailsVerify confirms that a valid-shape proof against the
// wrong leaf id is rejected.
func TestWrongLeafFailsVerify(t *testing.T) {
	ids := mkIDs(5)
	tr := New(ids)
	root := tr.Root()
	p, err := tr.Proof(ids[2])
	if err != nil {
		t.Fatalf("Proof: %v", err)
	}
	if Verify(root, ids[3], p) {
		t.Fatal("Verify accepted proof against wrong leaf")
	}
}

// TestDefensiveCopyOfInput asserts that mutating the caller's slice after
// New() does not change the tree's Root.
func TestDefensiveCopyOfInput(t *testing.T) {
	ids := mkIDs(4)
	tr := New(ids)
	rootBefore := tr.Root()

	// Mutate caller's slice in-place.
	ids[0] = mkID(777)

	if tr.Root() != rootBefore {
		t.Fatal("mutating caller's ids slice changed Tree.Root(); input not defensively copied")
	}
}

// TestLargeTreeRootStable re-builds the same tree twice and checks the root
// matches. Cheap sanity for determinism.
func TestLargeTreeRootStable(t *testing.T) {
	ids := mkIDs(100)
	a := New(ids).Root()
	b := New(ids).Root()
	if !bytes.Equal(a[:], b[:]) {
		t.Fatalf("non-deterministic root: %x vs %x", a, b)
	}
}
