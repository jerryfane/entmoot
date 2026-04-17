// Package merkle builds a Merkle tree over an ordered slice of message ids
// and produces positive-inclusion proofs for individual leaves.
//
// # Construction rules
//
// The hash function is SHA-256. The tree is domain-separated in the style of
// RFC 6962 (Certificate Transparency) to defend against second-preimage and
// length-extension concerns that arise when leaves and internal nodes share a
// hash domain:
//
//   - Leaf hash:     leaf_i = sha256(0x00 || id_i)
//   - Internal node: node   = sha256(0x01 || left || right)
//
// Odd-sized layers are handled in the "Bitcoin-style-but-not-quite" way: a
// lone trailing node is PROMOTED UNCHANGED to the next layer (it is NOT
// duplicated and re-hashed with itself). This differs from both RFC 6962
// (which uses an unbalanced tree with true depth) and Bitcoin (which
// duplicates the final hash). We pick promotion explicitly because it keeps
// proofs minimal — a promoted node contributes no sibling on its layer — and
// is simple to reason about. Peers MUST follow this rule exactly for roots
// to agree.
//
// # Edge cases
//
//   - Empty input:     Root() returns the zero hash ([32]byte{}).
//   - Single id:       Root() returns the domain-separated leaf hash
//     sha256(0x00 || id).
//
// # Ordering
//
// The caller is responsible for passing ids in canonical order. Use
// pkg/entmoot/order.Topological to derive that order from a message set.
package merkle

import (
	"crypto/sha256"
	"errors"

	"entmoot/pkg/entmoot"
)

// Domain-separation prefixes. See the package doc.
const (
	leafPrefix     byte = 0x00
	internalPrefix byte = 0x01
)

// ErrNotFound is returned by Tree.Proof when the requested message id is not
// present in the tree.
var ErrNotFound = errors.New("merkle: id not in tree")

// Tree is a Merkle tree over an ordered slice of MessageIDs.
//
// A Tree is immutable once built. All methods are safe for concurrent use by
// multiple readers.
//
// The caller must pass ids already in canonical order (see
// pkg/entmoot/order).
type Tree struct {
	// ids is the input slice in the order given by the caller. A defensive
	// copy, so later mutation of the caller's slice cannot change proofs.
	ids []entmoot.MessageID

	// index maps a message id to its leaf index, for O(1) Proof lookup.
	index map[entmoot.MessageID]int

	// layers holds one slice of hashes per tree level, starting at layer 0
	// (the leaves) and ending at the layer containing the root. For a tree
	// with zero leaves, layers is nil.
	layers [][][32]byte
}

// New builds a Merkle tree over ids.
//
// An empty ids slice produces a tree whose Root is the zero hash. A
// single-element slice produces a tree whose Root is the domain-separated
// leaf hash sha256(0x00 || id).
//
// New takes a defensive copy of ids, so subsequent mutation of the input
// slice does not affect the tree.
func New(ids []entmoot.MessageID) *Tree {
	t := &Tree{}
	if len(ids) == 0 {
		return t
	}

	t.ids = make([]entmoot.MessageID, len(ids))
	copy(t.ids, ids)

	t.index = make(map[entmoot.MessageID]int, len(ids))
	for i, id := range t.ids {
		// If the caller passes duplicates, the last index wins for Proof
		// lookup. The tree structure still reflects every supplied leaf.
		t.index[id] = i
	}

	leaves := make([][32]byte, len(t.ids))
	for i, id := range t.ids {
		leaves[i] = hashLeaf(id)
	}
	t.layers = buildLayers(leaves)
	return t
}

// Root returns the Merkle root. For an empty tree the zero hash is returned.
func (t *Tree) Root() [32]byte {
	if len(t.layers) == 0 {
		return [32]byte{}
	}
	top := t.layers[len(t.layers)-1]
	// buildLayers is guaranteed to terminate with a single-node top layer
	// whenever there is at least one leaf.
	return top[0]
}

// Len returns the number of leaves in the tree.
func (t *Tree) Len() int {
	return len(t.ids)
}

// hashLeaf returns sha256(0x00 || id).
func hashLeaf(id entmoot.MessageID) [32]byte {
	h := sha256.New()
	h.Write([]byte{leafPrefix})
	h.Write(id[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// hashInternal returns sha256(0x01 || left || right).
func hashInternal(left, right [32]byte) [32]byte {
	h := sha256.New()
	h.Write([]byte{internalPrefix})
	h.Write(left[:])
	h.Write(right[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// buildLayers constructs the per-layer hash table bottom-up. It is called
// with the leaf layer (len >= 1) and returns a slice of layers ending with a
// single-element top layer (the root).
//
// Odd-sized layers promote the lone trailing node unchanged; see package doc.
func buildLayers(leaves [][32]byte) [][][32]byte {
	layers := [][][32]byte{leaves}
	cur := leaves
	for len(cur) > 1 {
		next := make([][32]byte, 0, (len(cur)+1)/2)
		for i := 0; i < len(cur); i += 2 {
			if i+1 == len(cur) {
				// Odd trailing node: promote unchanged.
				next = append(next, cur[i])
				continue
			}
			next = append(next, hashInternal(cur[i], cur[i+1]))
		}
		layers = append(layers, next)
		cur = next
	}
	return layers
}
