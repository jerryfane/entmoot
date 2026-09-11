// Package order defines the deterministic topological ordering of messages
// used for Merkle-tree construction and any other place the protocol needs a
// total, peer-independent sequence over the message DAG.
//
// # Ordering rule (this is the spec; every peer MUST follow it)
//
// Given a set of messages M linked by Parents edges (child -> parent), the
// order produced by Topological is:
//
//  1. Parent messages always come before their children. The graph is a DAG;
//     if it contains a cycle, Topological returns a non-nil error.
//  2. When two messages are not ordered by the DAG (siblings, or unrelated
//     nodes at the same topological level), they are tie-broken by, in order:
//     a. Timestamp ascending (earlier first),
//     b. Author.PilotNodeID ascending,
//     c. Message ID lexicographically ascending (byte-wise compare over the
//     32-byte id).
//  3. Messages whose Parents reference IDs not present in M are still included
//     at the root of the topological order (they have no in-set ancestors).
//     Cross-set edges are ignored; they cannot create cycles and they do not
//     re-order anything.
//
// The returned slice contains each message's ID exactly once, in the total
// order defined above.
package order

import (
	"bytes"
	"container/heap"
	"fmt"

	"entmoot/pkg/entmoot"
)

// Topological returns message ids in deterministic topological order. See
// the package doc for the exact rule.
//
// If the input contains a cycle (which should never happen for well-formed
// message DAGs but indicates upstream corruption if it does), Topological
// returns a non-nil error.
func Topological(msgs []entmoot.Message) ([]entmoot.MessageID, error) {
	if len(msgs) == 0 {
		return []entmoot.MessageID{}, nil
	}

	// Index messages by id. If duplicates are supplied, the later entry
	// wins — callers shouldn't pass duplicates but we don't error on them
	// because that's not a cycle.
	index := make(map[entmoot.MessageID]entmoot.Message, len(msgs))
	for _, m := range msgs {
		index[m.ID] = m
	}

	// For Kahn's algorithm we need in-degree (only counting edges whose
	// parent is actually in the set) and the reverse adjacency list
	// (parent -> []child) so we can decrement children's in-degree when we
	// emit a parent.
	inDegree := make(map[entmoot.MessageID]int, len(index))
	children := make(map[entmoot.MessageID][]entmoot.MessageID, len(index))
	for id := range index {
		inDegree[id] = 0
	}
	for _, m := range msgs {
		for _, p := range m.Parents {
			if _, ok := index[p]; !ok {
				// Cross-set edge; skip.
				continue
			}
			inDegree[m.ID]++
			children[p] = append(children[p], m.ID)
		}
	}

	ready := &messageHeap{index: index}
	for id, degree := range inDegree {
		if degree == 0 {
			heap.Push(ready, id)
		}
	}

	out := make([]entmoot.MessageID, 0, len(index))
	for ready.Len() > 0 {
		chosenID := heap.Pop(ready).(entmoot.MessageID)
		out = append(out, chosenID)

		for _, child := range children[chosenID] {
			inDegree[child]--
			if inDegree[child] == 0 {
				heap.Push(ready, child)
			}
		}
	}

	if len(out) != len(index) {
		return nil, fmt.Errorf("order: cycle detected (emitted %d of %d messages)", len(out), len(index))
	}
	return out, nil
}

// less reports whether a sorts before b under the tie-breaker rule.
func less(a, b entmoot.Message) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp < b.Timestamp
	}
	if a.Author.PilotNodeID != b.Author.PilotNodeID {
		return a.Author.PilotNodeID < b.Author.PilotNodeID
	}
	return bytes.Compare(a.ID[:], b.ID[:]) < 0
}

type messageHeap struct {
	ids   []entmoot.MessageID
	index map[entmoot.MessageID]entmoot.Message
}

func (h messageHeap) Len() int { return len(h.ids) }

func (h messageHeap) Less(i, j int) bool {
	return less(h.index[h.ids[i]], h.index[h.ids[j]])
}

func (h messageHeap) Swap(i, j int) {
	h.ids[i], h.ids[j] = h.ids[j], h.ids[i]
}

func (h *messageHeap) Push(value any) {
	h.ids = append(h.ids, value.(entmoot.MessageID))
}

func (h *messageHeap) Pop() any {
	last := len(h.ids) - 1
	value := h.ids[last]
	h.ids = h.ids[:last]
	return value
}
