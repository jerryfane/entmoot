package roster

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	rlog *RosterLog,
	actor *keystore.Identity,
	actorNodeID entmoot.NodeID,
	op string,
	subject entmoot.NodeInfo,
	ts int64,
	_ []entmoot.RosterEntryID,
) entmoot.RosterEntry {
	t.Helper()
	entry, err := rlog.SignEntry(actor, op, subject, nil, ts)
	if err != nil {
		t.Fatalf("mkEntry: %v", err)
	}
	return entry
}

// newFounder returns a fresh same-key operational identity.
func newFounder(t *testing.T, _ entmoot.NodeID) (*keystore.Identity, entmoot.NodeInfo) {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate: %v", err)
	}
	memberID, err := entmoot.MemberIDFromPublicKey(id.PublicKey)
	if err != nil {
		t.Fatalf("MemberIDFromPublicKey: %v", err)
	}
	peerID, err := entmoot.PeerIDFromPublicKey(id.PublicKey)
	if err != nil {
		t.Fatalf("PeerIDFromPublicKey: %v", err)
	}
	return id, entmoot.NodeInfo{EntmootPubKey: []byte(id.PublicKey), MemberID: &memberID, PeerID: peerID}
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
	memberID := *info.MemberID
	if !r.IsMemberID(memberID) {
		t.Fatalf("expected founder to be a member after Genesis")
	}
	got := r.MemberIDs()
	if len(got) != 1 || got[0] != memberID {
		t.Fatalf("MemberIDs() = %v, want [%v]", got, memberID)
	}
	if r.Head() == (entmoot.RosterEntryID{}) {
		t.Fatalf("Head() unexpectedly zero after Genesis")
	}
	fi, ok := r.Founder()
	if !ok || fi.MemberID == nil || *fi.MemberID != memberID || fi.PeerID != info.PeerID {
		t.Fatalf("Founder() = %#v, ok=%v", fi, ok)
	}
}
func TestGenesisAndSignEntryUseGroupBoundVersion2(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	genesis := r.Entries()[0]
	if genesis.Version != CurrentEntryVersion || genesis.GroupID == nil || *genesis.GroupID != testGroupID() || genesis.Sequence != 1 {
		t.Fatalf("genesis version fields = version %d group %v sequence %d", genesis.Version, genesis.GroupID, genesis.Sequence)
	}
	member, memberInfo := newFounder(t, 200)
	_ = member
	entry, err := r.SignEntry(founder, "add", memberInfo, nil, 2_000)
	if err != nil {
		t.Fatalf("SignEntry: %v", err)
	}
	if entry.Version != CurrentEntryVersion || entry.GroupID == nil || *entry.GroupID != testGroupID() || entry.Sequence != 2 {
		t.Fatalf("entry version fields = version %d group %v sequence %d", entry.Version, entry.GroupID, entry.Sequence)
	}
	if err := r.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

func TestFullWidthMembersDoNotCollideAtZeroLegacyNodeID(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 0)
	founderID, err := entmoot.MemberIDFromPublicKey(founder.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	founderInfo.MemberID = &founderID
	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatal(err)
	}
	member, memberInfo := newFounder(t, 0)
	memberID, err := entmoot.MemberIDFromPublicKey(member.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	memberInfo.MemberID = &memberID
	entry := mkEntry(t, r, founder, 0, "add", memberInfo, 2_000, nil)
	if err := r.Apply(entry); err != nil {
		t.Fatal(err)
	}
	if !r.IsMemberID(founderID) || !r.IsMemberID(memberID) {
		t.Fatalf("full-width membership missing: %v", r.MemberIDs())
	}
	if got := len(r.MemberIDs()); got != 2 {
		t.Fatalf("full-width member count = %d, want 2", got)
	}
	_, forgedInfo := newFounder(t, 0)
	forgedEntry := mkEntry(t, r, founder, 0, "add", forgedInfo, 3_000, nil)
	forgedEntry.Subject.MemberID = &memberID
	signingBytes, err := canonical.RosterEntrySigningBytes(forgedEntry)
	if err != nil {
		t.Fatal(err)
	}
	forgedEntry.Signature = founder.Sign(signingBytes)
	forgedEntry.ID = canonical.RosterEntryID(forgedEntry)
	if err := r.Apply(forgedEntry); err == nil {
		t.Fatal("colliding MemberID with another public key accepted")
	}
}

func TestVersion2RosterEntryRejectedInAnotherGroup(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	groupA := testGroupID()
	groupB := groupA
	groupB[0] ^= 0xFF
	a := New(groupA)
	b := New(groupB)
	if err := a.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis A: %v", err)
	}
	if err := b.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis B: %v", err)
	}
	_, memberInfo := newFounder(t, 200)
	entry, err := a.SignEntry(founder, "add", memberInfo, nil, 2_000)
	if err != nil {
		t.Fatalf("SignEntry: %v", err)
	}
	if err := b.Apply(entry); !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("cross-group Apply = %v, want ErrRosterReject", err)
	}
}

func TestMemberInfoAtPreservesHistoricalKeyAfterRemoval(t *testing.T) {
	t.Parallel()
	founder, founderInfo := newFounder(t, 100)
	_, memberInfo := newFounder(t, 200)
	r := New(testGroupID())
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	add, err := r.SignEntry(founder, "add", memberInfo, nil, 2_000)
	if err != nil {
		t.Fatalf("SignEntry add: %v", err)
	}
	if err := r.Apply(add); err != nil {
		t.Fatalf("Apply add: %v", err)
	}
	historicalHead := r.Head()
	remove, err := r.SignEntry(founder, "remove", memberInfo, nil, 3_000)
	if err != nil {
		t.Fatalf("SignEntry remove: %v", err)
	}
	if err := r.Apply(remove); err != nil {
		t.Fatalf("Apply remove: %v", err)
	}
	memberID := *memberInfo.MemberID
	if _, current := r.MemberInfoByID(memberID); current {
		t.Fatal("removed member remains current")
	}
	got, historical, known := r.MemberInfoAtID(memberID, historicalHead)
	if !known || !historical || !bytes.Equal(got.EntmootPubKey, memberInfo.EntmootPubKey) {
		t.Fatalf("historical lookup = (%+v, %v, %v)", got, historical, known)
	}
	if _, historical, known := r.MemberInfoAtID(memberID, r.Head()); !known || historical {
		t.Fatalf("post-removal lookup = member %v known %v", historical, known)
	}
	if _, _, known := r.MemberInfoAtID(memberID, entmoot.RosterEntryID{0xFF}); known {
		t.Fatal("unknown head reported known")
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
	if got := r.MemberIDs(); len(got) != 1 || got[0] != *info.MemberID {
		t.Fatalf("MemberIDs after failed second Genesis = %v, want [%v]", got, *info.MemberID)
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
	entry := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !r.IsMemberID(*bobInfo.MemberID) {
		t.Fatalf("expected bob to be a member after add")
	}
	got := r.MemberIDs()
	if len(got) != 2 || !r.IsMemberID(*founderInfo.MemberID) {
		t.Fatalf("MemberIDs() = %v, want founder and bob", got)
	}
	info, ok := r.MemberInfoByID(*bobInfo.MemberID)
	if !ok {
		t.Fatalf("MemberInfoByID(bob) returned ok=false")
	}
	if len(info.EntmootPubKey) != len(bobInfo.EntmootPubKey) {
		t.Fatalf("MemberInfoByID pubkey length mismatch")
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
	entry := mkEntry(t, r, bob, bobInfo.PilotNodeID, "add", bobInfo, 2_000,
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
	if r.IsMemberID(*bobInfo.MemberID) {
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
	entry := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	entry.ID[0] ^= 0xFF // corrupt the id

	err := r.Apply(entry)
	if !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("expected ErrRosterReject, got %v", err)
	}
	if r.IsMemberID(*bobInfo.MemberID) {
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
	eq := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", bobInfo, 5_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(eq); !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("equal timestamp: expected ErrRosterReject, got %v", err)
	}

	// earlier timestamp: rejected.
	earlier := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", bobInfo, 4_999,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(earlier); !errors.Is(err, entmoot.ErrRosterReject) {
		t.Fatalf("earlier timestamp: expected ErrRosterReject, got %v", err)
	}

	if r.IsMemberID(*bobInfo.MemberID) {
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
	add := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(add); err != nil {
		t.Fatalf("Apply add: %v", err)
	}
	remove := mkEntry(t, r, founder, founderInfo.PilotNodeID, "remove", bobInfo, 3_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(remove); err != nil {
		t.Fatalf("Apply remove: %v", err)
	}
	if r.IsMemberID(*bobInfo.MemberID) {
		t.Fatalf("bob should not be a member after remove")
	}
	got := r.MemberIDs()
	if len(got) != 1 || got[0] != *founderInfo.MemberID {
		t.Fatalf("MemberIDs after remove = %v, want founder", got)
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
	a := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(a); err != nil {
		t.Fatalf("Apply a: %v", err)
	}
	b := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", carolInfo, 3_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(b); err != nil {
		t.Fatalf("Apply b: %v", err)
	}
	c := mkEntry(t, r, founder, founderInfo.PilotNodeID, "remove", bobInfo, 4_000,
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
	addBob := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{r.Head()})
	if err := r.Apply(addBob); err != nil {
		t.Fatalf("Apply addBob: %v", err)
	}
	addCarol := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", carolInfo, 3_000,
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
	got := r2.MemberIDs()
	if len(got) != 3 || !r2.IsMemberID(*founderInfo.MemberID) || !r2.IsMemberID(*bobInfo.MemberID) || !r2.IsMemberID(*carolInfo.MemberID) {
		t.Fatalf("reopened MemberIDs() = %v, want founder, bob, and carol", got)
	}
	fi, ok := r2.Founder()
	if !ok || fi.MemberID == nil || *fi.MemberID != *founderInfo.MemberID || fi.PeerID != founderInfo.PeerID {
		t.Fatalf("reopened Founder() = %#v, ok=%v", fi, ok)
	}
}

// OpenJSONL fails closed when any legacy line is malformed and preserves the
// source file byte-for-byte for explicit repair.
func TestJSONLRejectsMalformedLineWithoutChangingSource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	_, bobInfo := newFounder(t, 200)

	source := New(gid)
	if err := source.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	addBob := mkEntry(t, source, founder, founderInfo.PilotNodeID, "add", bobInfo, 2_000,
		[]entmoot.RosterEntryID{source.Head()})
	if err := source.Apply(addBob); err != nil {
		t.Fatalf("Apply addBob: %v", err)
	}
	removeBob := mkEntry(t, source, founder, founderInfo.PilotNodeID, "remove", bobInfo, 3_000,
		[]entmoot.RosterEntryID{source.Head()})
	entries := source.Entries()

	groupDir := filepath.Join(dir, "groups", encodeGroupDirName(gid))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	path := filepath.Join(groupDir, rosterFileName)
	var raw []byte
	for _, entry := range entries {
		encoded, err := canonical.Encode(entry)
		if err != nil {
			t.Fatalf("canonical.Encode: %v", err)
		}
		raw = append(raw, encoded...)
		raw = append(raw, '\n')
	}
	validPrefixLen := len(raw)
	raw = append(raw, []byte("{this is not json\n")...)
	encoded, err := canonical.Encode(removeBob)
	if err != nil {
		t.Fatalf("canonical.Encode remove: %v", err)
	}
	raw = append(raw, encoded...)
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := OpenJSONL(dir, gid); err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("OpenJSONL malformed error = %v, want line 3 diagnostic", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile after failure: %v", err)
	}
	if !bytes.Equal(after, raw) {
		t.Fatal("failed import changed legacy source")
	}
	repaired := append([]byte(nil), raw[:validPrefixLen]...)
	repaired = append(repaired, encoded...)
	repaired = append(repaired, '\n')
	if err := os.WriteFile(path, repaired, 0o600); err != nil {
		t.Fatalf("repair WriteFile: %v", err)
	}
	imported, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL after repair: %v", err)
	}
	defer imported.Close()
	if got := len(imported.Entries()); got != 3 {
		t.Fatalf("repaired import entries = %d, want 3", got)
	}
}

func TestConcurrentApplyAcceptsOneChildAndPersistsHead(t *testing.T) {
	dir := t.TempDir()
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	r, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL: %v", err)
	}
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	parent := r.Head()

	const contenders = 64
	entries := make([]entmoot.RosterEntry, contenders)
	for i := range entries {
		_, subject := newFounder(t, 0)
		entries[i] = mkEntry(t, r, founder, 0, "add", subject, int64(2_000+i),
			[]entmoot.RosterEntryID{parent})
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := range entries {
		wg.Add(1)
		go func(entry entmoot.RosterEntry) {
			defer wg.Done()
			if err := r.Apply(entry); err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, entmoot.ErrRosterReject) {
				t.Errorf("Apply unexpected error: %v", err)
			}
		}(entries[i])
	}
	wg.Wait()
	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted = %d, want 1", got)
	}
	wantHead := r.Head()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if reopened.Head() != wantHead || len(reopened.Entries()) != 2 {
		t.Fatalf("reopened head/entries = (%s, %d), want (%s, 2)", reopened.Head(), len(reopened.Entries()), wantHead)
	}
}

func TestPersistentWriterLeaseFailsPromptlyAndAllowsReaders(t *testing.T) {
	dir := t.TempDir()
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	writer, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL writer: %v", err)
	}
	if err := writer.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	reader, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL reader: %v", err)
	}
	defer reader.Close()
	if got, ok := reader.Founder(); !ok || got.PilotNodeID != founderInfo.PilotNodeID {
		t.Fatalf("reader Founder = (%+v, %v)", got, ok)
	}
	start := time.Now()
	if err := reader.ClaimWriter(); !errors.Is(err, ErrWriterActive) {
		t.Fatalf("second ClaimWriter = %v, want ErrWriterActive", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("second ClaimWriter blocked for %s", elapsed)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer Close: %v", err)
	}
}

func TestLegacyImportRejectsTruncatedEntryAndPreservesSource(t *testing.T) {
	dir := t.TempDir()
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	source := New(gid)
	if err := source.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	encoded, err := canonical.Encode(source.Entries()[0])
	if err != nil {
		t.Fatalf("canonical.Encode: %v", err)
	}
	groupDir := filepath.Join(dir, "groups", encodeGroupDirName(gid))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	path := filepath.Join(groupDir, rosterFileName)
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := OpenJSONL(dir, gid); err == nil || !strings.Contains(err.Error(), "truncated legacy log") {
		t.Fatalf("OpenJSONL truncated error = %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(after, encoded) {
		t.Fatal("truncated import changed source")
	}
}

func TestLegacyImportRetainsExactSignedEntries(t *testing.T) {
	dir := t.TempDir()
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	_, memberInfo := newFounder(t, 200)
	source := New(gid)
	if err := source.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	entry := mkEntry(t, source, founder, founderInfo.PilotNodeID, "add", memberInfo, 2_000,
		[]entmoot.RosterEntryID{source.Head()})
	if err := source.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	groupDir := filepath.Join(dir, "groups", encodeGroupDirName(gid))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	path := filepath.Join(groupDir, rosterFileName)
	var original []byte
	for _, item := range source.Entries() {
		encoded, err := canonical.Encode(item)
		if err != nil {
			t.Fatalf("canonical.Encode: %v", err)
		}
		original = append(original, encoded...)
		original = append(original, '\n')
	}
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	imported, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("OpenJSONL import: %v", err)
	}
	defer imported.Close()
	got := imported.Entries()
	want := source.Entries()
	if len(got) != len(want) {
		t.Fatalf("imported entries = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || !bytes.Equal(got[i].Signature, want[i].Signature) {
			t.Fatalf("entry %d changed across import", i)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(after, original) {
		t.Fatal("valid import changed legacy source")
	}
}

func TestPersistFailureDoesNotAdvanceProjection(t *testing.T) {
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	_, memberInfo := newFounder(t, 200)
	r := New(gid)
	if err := r.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	head := r.Head()
	entry := mkEntry(t, r, founder, founderInfo.PilotNodeID, "add", memberInfo, 2_000,
		[]entmoot.RosterEntryID{head})
	r.persist = func(entmoot.RosterEntry) error { return errors.New("injected commit failure") }

	if err := r.Apply(entry); err == nil || !strings.Contains(err.Error(), "injected commit failure") {
		t.Fatalf("Apply error = %v, want injected failure", err)
	}
	if r.Head() != head || r.IsMemberID(*memberInfo.MemberID) || len(r.Entries()) != 1 {
		t.Fatal("persist failure advanced in-memory roster")
	}
}

func TestPersistentWriterLeaseAcrossProcesses(t *testing.T) {
	if root := os.Getenv("ENTMOOT_ROSTER_LOCK_HELPER"); root != "" {
		r, err := OpenJSONL(root, testGroupID())
		if err != nil {
			t.Fatalf("child OpenJSONL: %v", err)
		}
		defer r.Close()
		if err := r.ClaimWriter(); err != nil {
			t.Fatalf("child ClaimWriter: %v", err)
		}
		fmt.Fprintln(os.Stdout, "ready")
		_, _ = bufio.NewReader(os.Stdin).ReadByte()
		return
	}

	dir := t.TempDir()
	gid := testGroupID()
	founder, founderInfo := newFounder(t, 100)
	seed, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("seed OpenJSONL: %v", err)
	}
	if err := seed.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatalf("seed Genesis: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestPersistentWriterLeaseAcrossProcesses$")
	cmd.Env = append(os.Environ(), "ENTMOOT_ROSTER_LOCK_HELPER="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start child: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "ready" {
		t.Fatalf("child ready = (%q, %v)", line, err)
	}

	reader, err := OpenJSONL(dir, gid)
	if err != nil {
		t.Fatalf("parent reader OpenJSONL: %v", err)
	}
	if err := reader.ClaimWriter(); !errors.Is(err, ErrWriterActive) {
		t.Fatalf("parent ClaimWriter = %v, want ErrWriterActive", err)
	}
	_ = reader.Close()
	if _, err := stdin.Write([]byte{'\n'}); err != nil {
		t.Fatalf("release child: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child Wait: %v", err)
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
	id, _ := newFounder(t, 100)
	info := entmoot.NodeInfo{PilotNodeID: 100, EntmootPubKey: append([]byte(nil), id.PublicKey...)}
	entry := mkGenesisEntry(t, id, info, 1_000)

	r := New(testGroupID())
	if err := r.AcceptGenesis(entry); err != nil {
		t.Fatalf("AcceptGenesis: %v", err)
	}
	legacyMemberID, err := entmoot.MemberIDFromPublicKey(id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.IsMemberID(legacyMemberID) {
		t.Fatalf("expected converted legacy founder to be a member after AcceptGenesis")
	}
	got := r.MemberIDs()
	if len(got) != 1 || got[0] != legacyMemberID {
		t.Fatalf("MemberIDs() = %v, want [%v]", got, legacyMemberID)
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
