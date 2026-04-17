package roster

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// mkEntry constructs, canonicalizes, and signs a roster entry. The resulting
// entry has its ID set to canonical.RosterEntryID and its Signature set to
// actor.Sign over the canonical signing form (id/sig zeroed). Tests use this
// helper so the validity of an entry tracks the validation code exactly.
func mkEntry(
	t *testing.T,
	actor *keystore.Identity,
	actorNodeID entmoot.NodeID,
	op string,
	subject entmoot.NodeInfo,
	ts int64,
	parents []entmoot.RosterEntryID,
) entmoot.RosterEntry {
	t.Helper()
	entry := entmoot.RosterEntry{
		Op:        op,
		Subject:   subject,
		Actor:     actorNodeID,
		Timestamp: ts,
		Parents:   parents,
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		t.Fatalf("mkEntry: canonical encode: %v", err)
	}
	entry.Signature = actor.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)
	return entry
}

// newFounder returns a fresh identity and NodeInfo for a founder-like actor.
func newFounder(t *testing.T, nodeID entmoot.NodeID) (*keystore.Identity, entmoot.NodeInfo) {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate: %v", err)
	}
	return id, entmoot.NodeInfo{
		PilotNodeID:   nodeID,
		EntmootPubKey: []byte(id.PublicKey),
	}
}

func testGroupID() entmoot.GroupID {
	var g entmoot.GroupID
	for i := range g {
		g[i] = byte(i + 1)
	}
	return g
}

// 1. Genesis on empty log: succeeds; founder is a member.
func TestGenesisHappyPath(t *testing.T) {
	t.Parallel()
	id, info := newFounder(t, 100)

	r := New(testGroupID())
	if err := r.Genesis(id, info, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	if !r.IsMember(100) {
		t.Fatalf("expected founder to be a member after Genesis")
	}
	got := r.Members()
	if len(got) != 1 || got[0] != 100 {
		t.Fatalf("Members() = %v, want [100]", got)
	}
	if r.Head() == (entmoot.RosterEntryID{}) {
		t.Fatalf("Head() unexpectedly zero after Genesis")
	}
	fi, ok := r.Founder()
	if !ok || fi.PilotNodeID != 100 {
		t.Fatalf("Founder() = %#v, ok=%v", fi, ok)
	}
}

// 2. Genesis twice returns an error and leaves the log unchanged.
func TestGenesisTwiceRejected(t *testing.T) {
	t.Parallel()
	id, info := newFounder(t, 100)
	r := New(testGroupID())
	if err := r.Genesis(id, info, 1_000); err != nil {
		t.Fatalf("first Genesis: %v", err)
	}
	headBefore := r.Head()
	if err := r.Genesis(id, info, 2_000); err == nil {
		t.Fatalf("expected error from second Genesis, got nil")
	}
	if r.Head() != headBefore {
		t.Fatalf("head changed after failed second Genesis")
	}
	if got := r.Members(); len(got) != 1 || got[0] != 100 {
		t.Fatalf("Members after failed second Genesis = %v, want [100]", got)
	}
}

// 3. Apply add(bob) signed by founder: bob becomes a member.
func TestApplyAddBobByFounder(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)

	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	entry := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !r.IsMember(200) {
		t.Fatalf("expected bob to be a member after add")
	}
	got := r.Members()
	want := []entmoot.NodeID{100, 200}
	if !equalNodeIDs(got, want) {
		t.Fatalf("Members() = %v, want %v", got, want)
	}
	info, ok := r.MemberInfo(200)
	if !ok {
		t.Fatalf("MemberInfo(200) returned ok=false")
	}
	if len(info.EntmootPubKey) != len(bobInfo.EntmootPubKey) {
		t.Fatalf("MemberInfo pubkey length mismatch")
	}
}

// 4. Apply add(bob) signed by bob (not founder): ErrRosterReject.
func TestApplyNonFounderRejected(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	bob, bobInfo := newFounder(t, 200)

	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	headBefore := r.Head()
	entry := mkEntry(t, bob, bobInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{headBefore})
	err := r.Apply(entry)
	if err == nil {
		t.Fatalf("expected ErrRosterReject, got nil")
	}
	if !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("expected ErrRosterReject, got %v", err)
	}
	if r.Head() != headBefore {
		t.Fatalf("head changed after rejected Apply")
	}
	if r.IsMember(200) {
		t.Fatalf("bob should not be a member after rejected Apply")
	}
}

// 5. Apply add(bob) signed by founder but with wrong ID hash: ErrRosterReject.
func TestApplyWrongIDRejected(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)

	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	entry := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	entry.ID[0] ^= 0xFF // corrupt the id

	err := r.Apply(entry)
	if !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("expected ErrRosterReject, got %v", err)
	}
	if r.IsMember(200) {
		t.Fatalf("bob should not be a member after rejected Apply")
	}
}

// 6. Apply add(bob) with timestamp <= head timestamp: ErrRosterReject.
func TestApplyMonotonicityRejected(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)

	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 5_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}

	// equal timestamp: rejected.
	eq := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 5_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(eq); !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("equal timestamp: expected ErrRosterReject, got %v", err)
	}

	// earlier timestamp: rejected.
	earlier := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 4_999,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(earlier); !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("earlier timestamp: expected ErrRosterReject, got %v", err)
	}

	if r.IsMember(200) {
		t.Fatalf("bob should not be a member after monotonicity rejections")
	}
}

// 7. Apply remove(bob): bob no longer a member.
func TestApplyRemoveMember(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)

	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	add := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(add); err != nil {
		t.Fatalf("Apply add: %v", err)
	}
	remove := mkEntry(t, founder, founderInfo.PilotNodeID, "remove", bobInfo, 3_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(remove); err != nil {
		t.Fatalf("Apply remove: %v", err)
	}
	if r.IsMember(200) {
		t.Fatalf("bob should not be a member after remove")
	}
	got := r.Members()
	if len(got) != 1 || got[0] != 100 {
		t.Fatalf("Members after remove = %v, want [100]", got)
	}
}

// 8. Apply sequence of 3 entries: Head() matches the third's ID.
func TestHeadMatchesLastApplied(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)
	_, carolInfo := newFounder(t, 300)

	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	a := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(a); err != nil {
		t.Fatalf("Apply a: %v", err)
	}
	b := mkEntry(t, founder, founderInfo.PilotNodeID, "add", carolInfo, 3_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(b); err != nil {
		t.Fatalf("Apply b: %v", err)
	}
	c := mkEntry(t, founder, founderInfo.PilotNodeID, "remove", bobInfo, 4_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(c); err != nil {
		t.Fatalf("Apply c: %v", err)
	}
	if r.Head() != c.ID {
		t.Fatalf("Head() = %x, want %x (last applied)", r.Head(), c.ID)
	}
	if got := r.Entries(); len(got) != 4 {
		t.Fatalf("Entries() len = %d, want 4", len(got))
	}
}

// 9. OpenJSONL reloads state after close+reopen with multiple entries.
func TestJSONLRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)
	_, carolInfo := newFounder(t, 300)

	r, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	addBob := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(addBob); err != nil {
		t.Fatalf("Apply addBob: %v", err)
	}
	addCarol := mkEntry(t, founder, founderInfo.PilotNodeID, "add", carolInfo, 3_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(addCarol); err != nil {
		t.Fatalf("Apply addCarol: %v", err)
	}
	wantHead := r.Head()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r2, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL reopen: %v", err)
	}
	defer r2.Close()
	if r2.Head() != wantHead {
		t.Fatalf("reopened Head() = %x, want %x", r2.Head(), wantHead)
	}
	got := r2.Members()
	want := []entmoot.NodeID{100, 200, 300}
	if !equalNodeIDs(got, want) {
		t.Fatalf("reopened Members() = %v, want %v", got, want)
	}
	fi, ok := r2.Founder()
	if !ok || fi.PilotNodeID != 100 {
		t.Fatalf("reopened Founder() = %#v, ok=%v", fi, ok)
	}
}

//  10. OpenJSONL with a malformed line in the middle skips it, loads valid
//     entries before and after.
func TestJSONLSkipsMalformedLine(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)

	// Produce two valid entries via a first open, then inject a malformed
	// line between them and append a third valid line.
	r, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	addBob := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(addBob); err != nil {
		t.Fatalf("Apply addBob: %v", err)
	}
	// Build a third valid entry keyed off the current head BEFORE we corrupt
	// the file, then inject garbage between lines 2 and 3.
	removeBob := mkEntry(t, founder, founderInfo.PilotNodeID, "remove", bobInfo, 3_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, "groups", encodeGroupDirName(gid), rosterFileName)

	// Append a malformed line directly, then append the encoded valid entry.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("{this is not json\n")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	encoded, err := canonical.Encode(removeBob)
	if err != nil {
		t.Fatalf("canonical.Encode: %v", err)
	}
	if _, err := f.Write(append(encoded, '\n')); err != nil {
		t.Fatalf("write valid: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close inject: %v", err)
	}

	r2, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL reopen: %v", err)
	}
	defer r2.Close()
	// All three valid entries should be present despite the garbage line.
	got := r2.Entries()
	if len(got) != 3 {
		t.Fatalf("reopened Entries() len = %d, want 3", len(got))
	}
	if r2.Head() != removeBob.ID {
		t.Fatalf("Head() = %x, want removeBob.ID = %x", r2.Head(), removeBob.ID)
	}
	if r2.IsMember(200) {
		t.Fatalf("bob should not be a member after replayed remove")
	}
}

// 11. Subscribe receives events for successful Apply, not for rejected ones.
func TestSubscribeReceivesSuccessfulApplies(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	bob, bobInfo := newFounder(t, 200)

	r := New(testGroupID())
	ch, cancel := r.Subscribe()
	defer cancel()

	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	// Rejected: non-founder signer.
	bad := mkEntry(t, bob, bobInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(bad); !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("bad Apply expected ErrRosterReject, got %v", err)
	}
	good := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(good); err != nil {
		t.Fatalf("good Apply: %v", err)
	}

	// We expect exactly 2 events (Genesis, good Apply) and nothing else.
	got := drainEvents(t, ch, 2, 250*time.Millisecond)
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
	if got[0].Entry.Op != "add" || got[0].Entry.Subject.PilotNodeID != 100 {
		t.Fatalf("first event not Genesis add(founder): %+v", got[0])
	}
	if got[1].Entry.Op != "add" || got[1].Entry.Subject.PilotNodeID != 200 {
		t.Fatalf("second event not add(bob): %+v", got[1])
	}
	for _, ev := range got {
		if len(ev.Heads) != 1 {
			t.Fatalf("expected single head, got %d", len(ev.Heads))
		}
	}
	// No further event should arrive within a short window.
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("unexpected extra event: %+v", ev)
		}
	case <-time.After(50 * time.Millisecond):
	}
}

//  12. Subscribe cancel() closes the channel and is idempotent; events after
//     cancel do not panic.
func TestSubscribeCancelIdempotent(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)

	r := New(testGroupID())
	ch, cancel := r.Subscribe()

	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	// Drain the one event we expect.
	select {
	case <-ch:
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("did not receive genesis event")
	}

	cancel()
	cancel() // must not panic; idempotent.

	// Channel must be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("channel yielded a value after cancel")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("channel not closed after cancel")
	}

	// Further Applies must not panic even though the subscriber is gone.
	add := mkEntry(t, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(add); err != nil {
		t.Fatalf("Apply after cancel: %v", err)
	}
}

// mkGenesisEntry constructs a canonical self-signed genesis entry for the
// supplied founder identity+info, suitable for feeding into AcceptGenesis.
// The Actor equals founderInfo.PilotNodeID and Parents is nil — the signing
// form roster.AcceptGenesis verifies against.
func mkGenesisEntry(
	t *testing.T,
	founder *keystore.Identity,
	founderInfo entmoot.NodeInfo,
	ts int64,
) entmoot.RosterEntry {
	t.Helper()
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   founderInfo,
		Actor:     founderInfo.PilotNodeID,
		Timestamp: ts,
		Parents:   nil,
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		t.Fatalf("mkGenesisEntry: canonical encode: %v", err)
	}
	entry.Signature = founder.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)
	return entry
}

// 13. AcceptGenesis with a valid self-signed genesis entry: founder is
// adopted from entry.Subject and membership reflects the founder.
func TestAcceptGenesis_Valid(t *testing.T) {
	t.Parallel()
	id, info := newFounder(t, 100)
	entry := mkGenesisEntry(t, id, info, 1_000)

	r := New(testGroupID())
	if err := r.AcceptGenesis(entry); err != nil {
		t.Fatalf("AcceptGenesis: %v", err)
	}
	if !r.IsMember(100) {
		t.Fatalf("expected founder to be a member after AcceptGenesis")
	}
	got := r.Members()
	if len(got) != 1 || got[0] != 100 {
		t.Fatalf("Members() = %v, want [100]", got)
	}
	if r.Head() != entry.ID {
		t.Fatalf("Head() = %x, want %x", r.Head(), entry.ID)
	}
	fi, ok := r.Founder()
	if !ok || fi.PilotNodeID != 100 {
		t.Fatalf("Founder() = %#v, ok=%v", fi, ok)
	}
}

// 14. AcceptGenesis on a log that already has a genesis: ErrRosterReject.
func TestAcceptGenesis_NotEmpty(t *testing.T) {
	t.Parallel()
	id, info := newFounder(t, 100)

	r := New(testGroupID())
	if err := r.Genesis(id, info, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	headBefore := r.Head()
	entry := mkGenesisEntry(t, id, info, 2_000)
	err := r.AcceptGenesis(entry)
	if !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("expected ErrRosterReject, got %v", err)
	}
	if r.Head() != headBefore {
		t.Fatalf("head changed after rejected AcceptGenesis")
	}
}

// 15. AcceptGenesis with a tampered signature: ErrRosterReject.
func TestAcceptGenesis_BadSignature(t *testing.T) {
	t.Parallel()
	id, info := newFounder(t, 100)
	entry := mkGenesisEntry(t, id, info, 1_000)
	entry.Signature[0] ^= 0xFF // tamper the signature; ID still matches signing form

	r := New(testGroupID())
	err := r.AcceptGenesis(entry)
	if !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("expected ErrRosterReject, got %v", err)
	}
	if r.Head() != (entmoot.RosterEntryID{}) {
		t.Fatalf("log unexpectedly seeded after rejected AcceptGenesis")
	}
	if _, ok := r.Founder(); ok {
		t.Fatalf("Founder() reported ok=true on an empty log")
	}
}

// 16. AcceptGenesis with a correctly-signed entry but corrupted ID field:
// ErrRosterReject.
func TestAcceptGenesis_WrongID(t *testing.T) {
	t.Parallel()
	id, info := newFounder(t, 100)
	entry := mkGenesisEntry(t, id, info, 1_000)
	entry.ID[0] ^= 0xFF // corrupt the id

	r := New(testGroupID())
	err := r.AcceptGenesis(entry)
	if !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("expected ErrRosterReject, got %v", err)
	}
}

// 17. AcceptGenesis with Actor != Subject.PilotNodeID (not self-signed):
// ErrRosterReject. We build a well-formed entry signed by someone-else and
// point Actor at a different node id than the subject.
func TestAcceptGenesis_NotSelfSigned(t *testing.T) {
	t.Parallel()
	_, founderInfo := newFounder(t, 100)
	otherID, _ := newFounder(t, 200)

	// Entry looks like a genesis for founder 100 but is signed by other and
	// claims Actor=200.
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   founderInfo,
		Actor:     200, // != Subject.PilotNodeID (100)
		Timestamp: 1_000,
		Parents:   nil,
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		t.Fatalf("canonical encode: %v", err)
	}
	entry.Signature = otherID.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)

	r := New(testGroupID())
	gotErr := r.AcceptGenesis(entry)
	if !errors.Is(gotErr, entmoot.ErrRosterReject) {
		t.Fatalf("expected ErrRosterReject, got %v", gotErr)
	}
}

// 18. AcceptGenesis with non-empty Parents: ErrRosterReject. We must reject
// before signature verification because a genesis with parents makes no
// semantic sense even if the bytes are otherwise valid.
func TestAcceptGenesis_NonEmptyParents(t *testing.T) {
	t.Parallel()
	id, info := newFounder(t, 100)

	bogusParent := entmoot.RosterEntryID{0x01, 0x02, 0x03}
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   info,
		Actor:     info.PilotNodeID,
		Timestamp: 1_000,
		Parents:   []entmoot.RosterEntryID{bogusParent},
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		t.Fatalf("canonical encode: %v", err)
	}
	entry.Signature = id.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)

	r := New(testGroupID())
	gotErr := r.AcceptGenesis(entry)
	if !errors.Is(gotErr, entmoot.ErrRosterReject) {
		t.Fatalf("expected ErrRosterReject, got %v", gotErr)
	}
}

// drainEvents collects up to n events from ch or returns whatever it got
// within deadline.
func drainEvents(t *testing.T, ch <-chan RosterEvent, n int, deadline time.Duration) []RosterEvent {
	t.Helper()
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	out := make([]RosterEvent, 0, n)
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-timer.C:
			return out
		}
	}
	return out
}

// equalNodeIDs reports whether two slices of NodeIDs are equal after sort.
func equalNodeIDs(a, b []entmoot.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]entmoot.NodeID(nil), a...)
	bb := append([]entmoot.NodeID(nil), b...)
	sort.Slice(aa, func(i, j int) bool { return aa[i] < aa[j] })
	sort.Slice(bb, func(i, j int) bool { return bb[i] < bb[j] })
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
