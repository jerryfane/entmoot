package canonical

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
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
		{"Version", func(m *entmoot.Message) { m.Version = 2 }},
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
		{"RosterHead", func(m *entmoot.Message) {
			head := entmoot.RosterEntryID{9}
			m.RosterHead = &head
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

func TestLegacyMessageSigningFixtureUnchanged(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	var groupID entmoot.GroupID
	for i := range groupID {
		groupID[i] = byte(31 - i)
	}
	message := entmoot.Message{
		GroupID:    groupID,
		Author:     entmoot.NodeInfo{PilotNodeID: 42, EntmootPubKey: publicKey},
		Timestamp:  1_700_000_000_123,
		Topics:     []string{"chat/general"},
		Parents:    []entmoot.MessageID{{1}},
		Content:    []byte("legacy history"),
		References: []entmoot.MessageID{{2}},
	}
	gotBytes, err := MessageSigningBytes(message)
	if err != nil {
		t.Fatalf("MessageSigningBytes: %v", err)
	}
	const wantBytes = `{"author":{"entmoot_pubkey":"A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg=","pilot_node_id":42},"content":"bGVnYWN5IGhpc3Rvcnk=","group_id":"Hx4dHBsaGRgXFhUUExIREA8ODQwLCgkIBwYFBAMCAQA=","id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","parents":["AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="],"references":["AgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="],"timestamp":1700000000123,"topics":["chat/general"]}`
	if string(gotBytes) != wantBytes {
		t.Fatalf("legacy signing bytes changed\nwant=%s\n got=%s", wantBytes, gotBytes)
	}
	wantID, err := hex.DecodeString("677e46b5bce9707712023f29929a4cb526e4a57b00e0873db864a6d91efcfedd")
	if err != nil {
		t.Fatal(err)
	}
	gotID := MessageID(message)
	if !bytes.Equal(gotID[:], wantID) {
		t.Fatalf("legacy message id changed: %x", gotID[:])
	}
	wantSignature, err := hex.DecodeString("3bed74d9016664c473a7ef21009763833195439367f9560f1e4c9af312d01c7deb464fd99bd7a61b286495fd1b5512ee06becf3375c13feaabc1d071d9bbec0b")
	if err != nil {
		t.Fatal(err)
	}
	gotSignature := ed25519.Sign(privateKey, gotBytes)
	if !bytes.Equal(gotSignature, wantSignature) {
		t.Fatalf("legacy message signature changed: %x", gotSignature)
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

func TestLegacyRosterEntrySigningFixtureUnchanged(t *testing.T) {
	entry := sampleRosterEntry()
	gotBytes, err := RosterEntrySigningBytes(entry)
	if err != nil {
		t.Fatalf("RosterEntrySigningBytes: %v", err)
	}
	const wantBytes = `{"actor":42,"id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","op":"add","parents":["AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","AgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="],"policy":{"admins":1},"subject":{"entmoot_pubkey":"yv66vg==","pilot_node_id":7},"timestamp":1700000000000}`
	if string(gotBytes) != wantBytes {
		t.Fatalf("legacy signing bytes changed\nwant=%s\n got=%s", wantBytes, gotBytes)
	}
	wantID, err := hex.DecodeString("806523f72f49705f5166908a7197cf69653944c8ce6beda932363dfd7883bdb0")
	if err != nil {
		t.Fatal(err)
	}
	gotID := RosterEntryID(entry)
	if !bytes.Equal(gotID[:], wantID) {
		t.Fatalf("legacy roster id changed: %x", gotID)
	}
}

func TestVersion2RosterEntryIDBindsGroupAndDomain(t *testing.T) {
	entry := sampleRosterEntry()
	entry.Version = 2
	entry.Sequence = 7
	groupA := entmoot.GroupID{1}
	entry.GroupID = &groupA
	idA := RosterEntryID(entry)
	groupB := entmoot.GroupID{2}
	entry.GroupID = &groupB
	if idB := RosterEntryID(entry); idB == idA {
		t.Fatal("version-2 roster id did not bind group_id")
	}
	entry.GroupID = &groupA
	entry.Version = 0
	entry.Sequence = 0
	if legacyID := RosterEntryID(entry); legacyID == idA {
		t.Fatal("version-2 roster id was not domain separated")
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
	if m.RosterHead != nil {
		head := *m.RosterHead
		out.RosterHead = &head
	}
	return out
}
