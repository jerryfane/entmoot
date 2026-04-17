package canonical

import (
	"bytes"
	"testing"

	"entmoot/pkg/entmoot"
)

// TestEncodeStability verifies byte-identical output across 100 calls.
func TestEncodeStability(t *testing.T) {
	m := sampleMessage()
	want, err := Encode(m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for i := 0; i < 100; i++ {
		got, err := Encode(m)
		if err != nil {
			t.Fatalf("iteration %d: Encode: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("iteration %d: bytes differ\nwant=%s\n got=%s", i, want, got)
		}
	}
}

// TestEncodeMapOrderInsensitive verifies that differently-ordered map inputs
// produce the same canonical bytes.
func TestEncodeMapOrderInsensitive(t *testing.T) {
	a := map[string]any{
		"zeta":  1,
		"alpha": 2,
		"mu":    3,
		"beta":  map[string]any{"y": 4, "x": 5},
	}
	b := map[string]any{
		"beta":  map[string]any{"x": 5, "y": 4},
		"alpha": 2,
		"mu":    3,
		"zeta":  1,
	}
	aBytes, err := Encode(a)
	if err != nil {
		t.Fatalf("Encode(a): %v", err)
	}
	bBytes, err := Encode(b)
	if err != nil {
		t.Fatalf("Encode(b): %v", err)
	}
	if !bytes.Equal(aBytes, bBytes) {
		t.Fatalf("map-order outputs differ:\na=%s\nb=%s", aBytes, bBytes)
	}
}

// TestEncodeNoHTMLEscape verifies that <, >, and & are not rewritten.
func TestEncodeNoHTMLEscape(t *testing.T) {
	v := map[string]any{"s": "a<b>c&d"}
	got, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	want := `{"s":"a<b>c&d"}`
	if string(got) != want {
		t.Fatalf("unexpected encoding\ngot=%s\nwant=%s", got, want)
	}
}

// TestMessageIDFieldSensitivity verifies that flipping any contributing field
// changes the MessageID.
func TestMessageIDFieldSensitivity(t *testing.T) {
	base := sampleMessage()
	baseID := MessageID(base)

	// ID and Signature must NOT affect MessageID (they're zeroed in signing
	// form).
	mID := base
	mID.ID = entmoot.MessageID{1, 2, 3}
	if got := MessageID(mID); got != baseID {
		t.Fatalf("ID must not affect MessageID: got %x vs base %x", got, baseID)
	}

	mSig := base
	mSig.Signature = []byte{9, 9, 9}
	if got := MessageID(mSig); got != baseID {
		t.Fatalf("Signature must not affect MessageID: got %x vs base %x", got, baseID)
	}

	// Every other field must affect it.
	type mutation struct {
		name  string
		mutat func(*entmoot.Message)
	}
	mutations := []mutation{
		{"GroupID", func(m *entmoot.Message) { m.GroupID[0] ^= 0x55 }},
		{"Author.PilotNodeID", func(m *entmoot.Message) { m.Author.PilotNodeID++ }},
		{"Author.EntmootPubKey", func(m *entmoot.Message) {
			m.Author.EntmootPubKey = append([]byte{}, m.Author.EntmootPubKey...)
			m.Author.EntmootPubKey[0] ^= 0x01
		}},
		{"Timestamp", func(m *entmoot.Message) { m.Timestamp++ }},
		{"Topics", func(m *entmoot.Message) { m.Topics = append(m.Topics, "extra") }},
		{"Parents", func(m *entmoot.Message) {
			m.Parents = append(m.Parents, entmoot.MessageID{7})
		}},
		{"Content", func(m *entmoot.Message) {
			m.Content = append([]byte{}, m.Content...)
			m.Content[0] ^= 0x01
		}},
		{"References", func(m *entmoot.Message) {
			m.References = append(m.References, entmoot.MessageID{8})
		}},
	}
	for _, mu := range mutations {
		t.Run(mu.name, func(t *testing.T) {
			clone := cloneMessage(base)
			mu.mutat(&clone)
			if got := MessageID(clone); got == baseID {
				t.Fatalf("mutation of %s did not change MessageID", mu.name)
			}
		})
	}
}

// TestMessageIDDeterministic verifies the id computation itself is stable.
func TestMessageIDDeterministic(t *testing.T) {
	m := sampleMessage()
	first := MessageID(m)
	for i := 0; i < 10; i++ {
		if got := MessageID(m); got != first {
			t.Fatalf("iteration %d: MessageID not stable", i)
		}
	}
}

// TestRosterEntryIDFieldSensitivity verifies that flipping any contributing
// field changes the RosterEntryID, and that ID/Signature do not contribute.
func TestRosterEntryIDFieldSensitivity(t *testing.T) {
	base := sampleRosterEntry()
	baseID := RosterEntryID(base)

	// ID and Signature must NOT affect RosterEntryID (they're zeroed in
	// signing form).
	mID := base
	mID.ID = entmoot.RosterEntryID{1, 2, 3}
	if got := RosterEntryID(mID); got != baseID {
		t.Fatalf("ID must not affect RosterEntryID: got %x vs base %x", got, baseID)
	}

	mSig := base
	mSig.Signature = []byte{9, 9, 9}
	if got := RosterEntryID(mSig); got != baseID {
		t.Fatalf("Signature must not affect RosterEntryID: got %x vs base %x", got, baseID)
	}

	// Every other field must affect it.
	type mutation struct {
		name  string
		mutat func(*entmoot.RosterEntry)
	}
	mutations := []mutation{
		{"Op", func(e *entmoot.RosterEntry) { e.Op = "remove" }},
		{"Subject.PilotNodeID", func(e *entmoot.RosterEntry) { e.Subject.PilotNodeID++ }},
		{"Subject.EntmootPubKey", func(e *entmoot.RosterEntry) {
			e.Subject.EntmootPubKey = append([]byte{}, e.Subject.EntmootPubKey...)
			e.Subject.EntmootPubKey[0] ^= 0x01
		}},
		{"Policy", func(e *entmoot.RosterEntry) { e.Policy = []byte(`{"admins":3}`) }},
		{"Actor", func(e *entmoot.RosterEntry) { e.Actor++ }},
		{"Timestamp", func(e *entmoot.RosterEntry) { e.Timestamp++ }},
		{"Parents", func(e *entmoot.RosterEntry) {
			e.Parents = append(e.Parents, entmoot.RosterEntryID{7})
		}},
	}
	for _, mu := range mutations {
		t.Run(mu.name, func(t *testing.T) {
			clone := cloneRosterEntry(base)
			mu.mutat(&clone)
			if got := RosterEntryID(clone); got == baseID {
				t.Fatalf("mutation of %s did not change RosterEntryID", mu.name)
			}
		})
	}
}

// TestRosterEntryIDDeterministic verifies the id computation itself is stable
// across 100 calls.
func TestRosterEntryIDDeterministic(t *testing.T) {
	e := sampleRosterEntry()
	first := RosterEntryID(e)
	for i := 0; i < 100; i++ {
		if got := RosterEntryID(e); got != first {
			t.Fatalf("iteration %d: RosterEntryID not stable", i)
		}
	}
}

func sampleRosterEntry() entmoot.RosterEntry {
	return entmoot.RosterEntry{
		ID: entmoot.RosterEntryID{0xff, 0xee},
		Op: "add",
		Subject: entmoot.NodeInfo{
			PilotNodeID:   7,
			EntmootPubKey: []byte{0xca, 0xfe, 0xba, 0xbe},
		},
		Policy:    []byte(`{"admins":1}`),
		Actor:     42,
		Timestamp: 1_700_000_000_000,
		Parents:   []entmoot.RosterEntryID{{1}, {2}},
		Signature: []byte{0xaa, 0xbb},
	}
}

func cloneRosterEntry(e entmoot.RosterEntry) entmoot.RosterEntry {
	out := e
	out.Subject.EntmootPubKey = append([]byte{}, e.Subject.EntmootPubKey...)
	out.Policy = append([]byte{}, e.Policy...)
	out.Parents = append([]entmoot.RosterEntryID{}, e.Parents...)
	out.Signature = append([]byte{}, e.Signature...)
	return out
}

func sampleMessage() entmoot.Message {
	return entmoot.Message{
		ID: entmoot.MessageID{0xff, 0xee},
		GroupID: entmoot.GroupID{1, 2, 3, 4, 5, 6, 7, 8,
			9, 10, 11, 12, 13, 14, 15, 16,
			17, 18, 19, 20, 21, 22, 23, 24,
			25, 26, 27, 28, 29, 30, 31, 32},
		Author: entmoot.NodeInfo{
			PilotNodeID:   42,
			EntmootPubKey: []byte{0xde, 0xad, 0xbe, 0xef},
		},
		Timestamp:  1_700_000_000_000,
		Topics:     []string{"entmoot/security/cve"},
		Parents:    []entmoot.MessageID{{1}, {2}},
		Content:    []byte("hello, group"),
		References: []entmoot.MessageID{{3}},
		Signature:  []byte{0xaa, 0xbb},
	}
}

func cloneMessage(m entmoot.Message) entmoot.Message {
	out := m
	out.Author.EntmootPubKey = append([]byte{}, m.Author.EntmootPubKey...)
	out.Topics = append([]string{}, m.Topics...)
	out.Parents = append([]entmoot.MessageID{}, m.Parents...)
	out.Content = append([]byte{}, m.Content...)
	out.References = append([]entmoot.MessageID{}, m.References...)
	out.Signature = append([]byte{}, m.Signature...)
	return out
}
