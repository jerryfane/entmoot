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

	// Ready set: messages with in-degree 0 right now. We keep it sorted by
	// the tie-breaker rule every time we pick the next emission; with a
	// small v0 canary this is fine and keeps the code obvious. Swap for a
	// heap if we ever need it.
	ready := make([]entmoot.MessageID, 0)
	for id, d := range inDegree {
		if d == 0 {
			ready = append(ready, id)
		}
	}

	out := make([]entmoot.MessageID, 0, len(msgs))
	for len(ready) > 0 {
		// Select the ready id with the smallest (timestamp, node_id, id).
		pickIdx := 0
		for i := 1; i < len(ready); i++ {
			if less(index[ready[i]], index[ready[pickIdx]]) {
				pickIdx = i
			}
		}
		chosenID := ready[pickIdx]
		// Remove ready[pickIdx] by swapping with last.
		ready[pickIdx] = ready[len(ready)-1]
		ready = ready[:len(ready)-1]

		out = append(out, chosenID)

		for _, child := range children[chosenID] {
			inDegree[child]--
			if inDegree[child] == 0 {
				ready = append(ready, child)
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
