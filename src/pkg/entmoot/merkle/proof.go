package merkle

import (
	"fmt"

	"entmoot/pkg/entmoot"
)

// Proof is a positive-inclusion proof for a single leaf.
//
// Siblings lists the sibling hashes encountered on the path from the leaf
// upward to the root, bottom-first. A "promoted" node on an odd-sized layer
// contributes no sibling entry — the verifier carries the running hash
// through that layer unchanged — so Proof.Siblings has one entry per layer
// at which the leaf's subtree actually met a sibling on the way up.
//
// Because a promoted node has no sibling, the layer count is NOT recoverable
// from len(Siblings) alone; the verifier also needs to know the original
// tree's leaf count so it can detect which layers were promotions. LeafCount
// carries that value.
type Proof struct {
	// Index is the 0-based position of the leaf in the input slice used to
	// build the tree.
	Index int `json:"index"`

	// LeafCount is the total number of leaves in the tree the proof was
	// produced from. Verifiers use it together with Index to detect
	// promotion layers.
	LeafCount int `json:"leaf_count"`

	// Siblings are sibling hashes from the leaf layer toward the root.
	// Siblings[0] is the sibling on the leaf layer; the last entry is the
	// sibling on the layer just below the root. Promotion layers are not
	// represented in this slice.
	Siblings [][32]byte `json:"siblings"`
}

// Proof returns an inclusion proof for id. It returns ErrNotFound if id is
// not present in the tree.
func (t *Tree) Proof(id entmoot.MessageID) (Proof, error) {
	i, ok := t.index[id]
	if !ok {
		return Proof{}, fmt.Errorf("%w: %x", ErrNotFound, id[:])
	}

	p := Proof{Index: i, LeafCount: len(t.ids)}
	pos := i
	// Walk from the leaf layer up to (but not including) the root layer,
	// collecting siblings. On a layer where pos points at a lone trailing
	// odd node, there is no sibling: we simply carry the hash up unchanged
	// without appending anything to Siblings, mirroring the construction
	// rule.
	for layer := 0; layer < len(t.layers)-1; layer++ {
		cur := t.layers[layer]
		if pos == len(cur)-1 && len(cur)%2 == 1 {
			// Promoted unchanged; no sibling recorded.
			pos /= 2
			continue
		}
		var sibIdx int
		if pos%2 == 0 {
			sibIdx = pos + 1
		} else {
			sibIdx = pos - 1
		}
		p.Siblings = append(p.Siblings, cur[sibIdx])
		pos /= 2
	}
	return p, nil
}

// Verify reconstructs a root from (id, proof) and reports whether it matches
// the expected root. It returns false for any malformed proof (negative
// index, index out of range, wrong number of siblings, etc.).
//
// Verify uses proof.LeafCount together with proof.Index to replay the exact
// shape of the original tree, detecting which layers promoted a lone
// trailing node rather than combining a sibling pair. This matches the
// construction rule in buildLayers exactly.
func Verify(root [32]byte, id entmoot.MessageID, proof Proof) bool {
	if proof.Index < 0 || proof.LeafCount <= 0 || proof.Index >= proof.LeafCount {
		return false
	}
	// Special-case the single-leaf tree: root is just the leaf hash.
	if proof.LeafCount == 1 {
		if len(proof.Siblings) != 0 {
			return false
		}
		return hashLeaf(id) == root
	}

	running := hashLeaf(id)
	pos := proof.Index
	layerSize := proof.LeafCount
	used := 0
	for layerSize > 1 {
		isLastOddOut := pos == layerSize-1 && layerSize%2 == 1
		if isLastOddOut {
			// Promoted unchanged; no sibling consumed.
			pos /= 2
			layerSize = (layerSize + 1) / 2
			continue
		}
		if used >= len(proof.Siblings) {
			return false
		}
		sib := proof.Siblings[used]
		used++
		if pos%2 == 0 {
			running = hashInternal(running, sib)
		} else {
			running = hashInternal(sib, running)
		}
		pos /= 2
		layerSize = (layerSize + 1) / 2
	}
	if used != len(proof.Siblings) {
		return false
	}
	return running == root
}
