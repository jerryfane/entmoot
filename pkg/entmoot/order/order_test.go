package order

import (
	"testing"

	"entmoot/pkg/entmoot"
)

// TestChain verifies A -> B -> C always returns [A, B, C] regardless of input
// order.
func TestChain(t *testing.T) {
	a := entmoot.Message{ID: entmoot.MessageID{0xA}, Timestamp: 100}
	b := entmoot.Message{ID: entmoot.MessageID{0xB}, Timestamp: 200, Parents: []entmoot.MessageID{a.ID}}
	c := entmoot.Message{ID: entmoot.MessageID{0xC}, Timestamp: 300, Parents: []entmoot.MessageID{b.ID}}

	permutations := [][]entmoot.Message{
		{a, b, c},
		{c, b, a},
		{b, a, c},
		{c, a, b},
		{a, c, b},
		{b, c, a},
	}
	want := []entmoot.MessageID{a.ID, b.ID, c.ID}

	for i, p := range permutations {
		got, err := Topological(p)
		if err != nil {
			t.Fatalf("perm %d: Topological: %v", i, err)
		}
		if !equalIDs(got, want) {
			t.Fatalf("perm %d: got %v want %v", i, got, want)
		}
	}
}

// TestSiblingsTieBreakTimestamp: two siblings at the same level sort by
// timestamp ascending.
func TestSiblingsTieBreakTimestamp(t *testing.T) {
	root := entmoot.Message{ID: entmoot.MessageID{0x01}, Timestamp: 0}
	early := entmoot.Message{
		ID:        entmoot.MessageID{0x02},
		Timestamp: 10,
		Parents:   []entmoot.MessageID{root.ID},
	}
	late := entmoot.Message{
		ID:        entmoot.MessageID{0x03},
		Timestamp: 20,
		Parents:   []entmoot.MessageID{root.ID},
	}

	got, err := Topological([]entmoot.Message{late, early, root})
	if err != nil {
		t.Fatalf("Topological: %v", err)
	}
	want := []entmoot.MessageID{root.ID, early.ID, late.ID}
	if !equalIDs(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// TestSiblingsTieBreakNodeID: equal timestamps fall through to node id
// ascending.
func TestSiblingsTieBreakNodeID(t *testing.T) {
	root := entmoot.Message{ID: entmoot.MessageID{0x01}, Timestamp: 0}
	sibA := entmoot.Message{
		ID:        entmoot.MessageID{0x02},
		Timestamp: 10,
		Author:    entmoot.NodeInfo{PilotNodeID: 5},
		Parents:   []entmoot.MessageID{root.ID},
	}
	sibB := entmoot.Message{
		ID:        entmoot.MessageID{0x03},
		Timestamp: 10,
		Author:    entmoot.NodeInfo{PilotNodeID: 2},
		Parents:   []entmoot.MessageID{root.ID},
	}

	got, err := Topological([]entmoot.Message{sibA, sibB, root})
	if err != nil {
		t.Fatalf("Topological: %v", err)
	}
	// Lower node id (sibB, 2) wins over sibA (5).
	want := []entmoot.MessageID{root.ID, sibB.ID, sibA.ID}
	if !equalIDs(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// TestSiblingsTieBreakID: equal timestamps and node ids fall through to id
// lex ascending.
func TestSiblingsTieBreakID(t *testing.T) {
	root := entmoot.Message{ID: entmoot.MessageID{0x01}, Timestamp: 0}
	low := entmoot.Message{
		ID:        entmoot.MessageID{0x10},
		Timestamp: 10,
		Author:    entmoot.NodeInfo{PilotNodeID: 5},
		Parents:   []entmoot.MessageID{root.ID},
	}
	high := entmoot.Message{
		ID:        entmoot.MessageID{0x20},
		Timestamp: 10,
		Author:    entmoot.NodeInfo{PilotNodeID: 5},
		Parents:   []entmoot.MessageID{root.ID},
	}

	got, err := Topological([]entmoot.Message{high, low, root})
	if err != nil {
		t.Fatalf("Topological: %v", err)
	}
	want := []entmoot.MessageID{root.ID, low.ID, high.ID}
	if !equalIDs(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// TestCycle: a cycle A -> B -> A returns an error.
func TestCycle(t *testing.T) {
	a := entmoot.Message{ID: entmoot.MessageID{0xA}, Timestamp: 1}
	b := entmoot.Message{ID: entmoot.MessageID{0xB}, Timestamp: 2, Parents: []entmoot.MessageID{a.ID}}
	// Introduce cycle: pretend A lists B as parent (bogus, but that's the
	// test: Topological must refuse it).
	a.Parents = []entmoot.MessageID{b.ID}

	got, err := Topological([]entmoot.Message{a, b})
	if err == nil {
		t.Fatalf("expected cycle error; got result %v", got)
	}
}

// TestEmpty returns an empty slice, no error.
func TestEmpty(t *testing.T) {
	got, err := Topological(nil)
	if err != nil {
		t.Fatalf("Topological(nil): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

// TestUnknownParentEdgesIgnored: a message referencing a parent not present in
// msgs is still emitted (edges to out-of-set ids are ignored).
func TestUnknownParentEdgesIgnored(t *testing.T) {
	orphan := entmoot.Message{
		ID:        entmoot.MessageID{0xAA},
		Timestamp: 100,
		Parents:   []entmoot.MessageID{{0x99}}, // not in input set
	}
	got, err := Topological([]entmoot.Message{orphan})
	if err != nil {
		t.Fatalf("Topological: %v", err)
	}
	want := []entmoot.MessageID{orphan.ID}
	if !equalIDs(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func equalIDs(a, b []entmoot.MessageID) bool {
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
