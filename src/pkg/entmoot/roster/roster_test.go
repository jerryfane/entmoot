package roster

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// legacyLog builds a signed legacy chain the way the retired writer did, so a
// test's fixture is authentic by construction and the validator is the only
// thing under test. The writer itself is gone: conversion never appends.
type legacyLog struct {
	t       *testing.T
	groupID entmoot.GroupID
	entries []entmoot.RosterEntry
}

func newLegacyLog(t *testing.T) *legacyLog {
	t.Helper()
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = byte(i + 1)
	}
	return &legacyLog{t: t, groupID: gid}
}

func newIdentity(t *testing.T) (*keystore.Identity, entmoot.NodeInfo) {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate: %v", err)
	}
	memberID, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatalf("MemberIDFromPublicKey: %v", err)
	}
	peerID, err := entmoot.PeerIDFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatalf("PeerIDFromPublicKey: %v", err)
	}
	return identity, entmoot.NodeInfo{EntmootPubKey: []byte(identity.PublicKey), MemberID: &memberID, PeerID: peerID}
}

func (l *legacyLog) sign(signer *keystore.Identity, entry entmoot.RosterEntry) entmoot.RosterEntry {
	l.t.Helper()
	groupID := l.groupID
	entry.Version = CurrentEntryVersion
	entry.GroupID = &groupID
	entry.Sequence = uint64(len(l.entries) + 1)
	if len(l.entries) > 0 {
		entry.Parents = []entmoot.RosterEntryID{l.entries[len(l.entries)-1].ID}
	}
	sigInput, err := canonical.RosterEntrySigningBytes(entry)
	if err != nil {
		l.t.Fatalf("canonical signing bytes: %v", err)
	}
	entry.Signature = signer.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)
	return entry
}

// genesis appends the founder's self-signed add(founder).
func (l *legacyLog) genesis(founder *keystore.Identity, info entmoot.NodeInfo, timestamp int64) entmoot.RosterEntry {
	l.t.Helper()
	entry := l.sign(founder, entmoot.RosterEntry{
		Op: "add", Subject: info, ActorMemberID: info.MemberID, Timestamp: timestamp,
	})
	l.entries = append(l.entries, entry)
	return entry
}

// add appends an entry signed by actor, whose authority the validator decides.
func (l *legacyLog) op(actor *keystore.Identity, operation string, subject entmoot.NodeInfo, policy []byte, timestamp int64) entmoot.RosterEntry {
	l.t.Helper()
	actorID, err := entmoot.MemberIDFromPublicKey(actor.PublicKey)
	if err != nil {
		l.t.Fatalf("MemberIDFromPublicKey: %v", err)
	}
	entry := l.sign(actor, entmoot.RosterEntry{
		Op: operation, Subject: subject, Policy: policy, ActorMemberID: &actorID, Timestamp: timestamp,
	})
	l.entries = append(l.entries, entry)
	return entry
}

// write serialises the chain the way a legacy node left it on disk.
func (l *legacyLog) write(root string) string {
	l.t.Helper()
	dir := filepath.Join(root, "groups", "legacy")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		l.t.Fatal(err)
	}
	path := filepath.Join(dir, "roster.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		l.t.Fatal(err)
	}
	defer file.Close()
	for _, entry := range l.entries {
		// Exactly the bytes the reader demands: a legacy line is canonical
		// JSON, not whatever an encoder happens to emit.
		raw, err := canonical.Encode(entry)
		if err != nil {
			l.t.Fatal(err)
		}
		if _, err := file.Write(append(raw, '\n')); err != nil {
			l.t.Fatal(err)
		}
	}
	return path
}

func adminPolicy(t *testing.T, admins ...entmoot.MemberID) []byte {
	t.Helper()
	raw, err := json.Marshal(AdminPolicy{Type: AdminPolicyType, Admins: admins})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// A chain the founder wrote is what conversion is allowed to adopt.
func TestValidateEntriesAcceptsAFounderSignedChain(t *testing.T) {
	log := newLegacyLog(t)
	founder, founderInfo := newIdentity(t)
	_, memberInfo := newIdentity(t)
	log.genesis(founder, founderInfo, 1_000)
	log.op(founder, "add", memberInfo, nil, 2_000)
	log.op(founder, "remove", memberInfo, nil, 3_000)
	if err := ValidateEntries(log.groupID, log.entries); err != nil {
		t.Fatalf("a founder-signed chain was refused: %v", err)
	}
}

// Every rule that decides authenticity, one mutation each. A legacy log is
// adopted wholesale, so a rule that stopped firing would import somebody
// else's membership.
func TestValidateEntriesRefusesAnInauthenticChain(t *testing.T) {
	founderKey, founderInfo := newIdentity(t)
	strangerKey, strangerInfo := newIdentity(t)
	_, memberInfo := newIdentity(t)

	cases := map[string]func(l *legacyLog){
		"genesis signed by somebody else": func(l *legacyLog) {
			entry := l.sign(strangerKey, entmoot.RosterEntry{
				Op: "add", Subject: founderInfo, ActorMemberID: founderInfo.MemberID, Timestamp: 1_000,
			})
			l.entries = append(l.entries, entry)
		},
		"genesis with a parent": func(l *legacyLog) {
			entry := entmoot.RosterEntry{
				Op: "add", Subject: founderInfo, ActorMemberID: founderInfo.MemberID, Timestamp: 1_000,
			}
			entry = l.sign(founderKey, entry)
			entry.Parents = []entmoot.RosterEntryID{{0x01}}
			l.entries = append(l.entries, entry)
		},
		"entry id that does not match its content": func(l *legacyLog) {
			l.genesis(founderKey, founderInfo, 1_000)
			entry := l.op(founderKey, "add", memberInfo, nil, 2_000)
			entry.ID[0] ^= 0xff
			l.entries[len(l.entries)-1] = entry
		},
		"a non-member author": func(l *legacyLog) {
			l.genesis(founderKey, founderInfo, 1_000)
			l.op(strangerKey, "add", memberInfo, nil, 2_000)
		},
		"a timestamp that goes backwards": func(l *legacyLog) {
			l.genesis(founderKey, founderInfo, 5_000)
			l.op(founderKey, "add", memberInfo, nil, 4_000)
		},
		"a parent that is not the head": func(l *legacyLog) {
			l.genesis(founderKey, founderInfo, 1_000)
			entry := l.op(founderKey, "add", memberInfo, nil, 2_000)
			entry.Parents = []entmoot.RosterEntryID{{0x02}}
			entry.Signature = nil
			sigInput, err := canonical.RosterEntrySigningBytes(entry)
			if err != nil {
				t.Fatal(err)
			}
			entry.Signature = founderKey.Sign(sigInput)
			entry.ID = canonical.RosterEntryID(entry)
			l.entries[len(l.entries)-1] = entry
		},
		"an unknown op": func(l *legacyLog) {
			l.genesis(founderKey, founderInfo, 1_000)
			l.op(founderKey, "rekey", memberInfo, nil, 2_000)
		},
		"an admin delegated by somebody other than the founder": func(l *legacyLog) {
			l.genesis(founderKey, founderInfo, 1_000)
			l.op(founderKey, "add", strangerInfo, nil, 2_000)
			l.op(strangerKey, "policy_change", entmoot.NodeInfo{}, adminPolicy(t, *strangerInfo.MemberID), 3_000)
		},
		"an admin policy nobody can decode": func(l *legacyLog) {
			l.genesis(founderKey, founderInfo, 1_000)
			l.op(founderKey, "policy_change", entmoot.NodeInfo{}, []byte(`{"type":"admins/v1","admins":"not-a-list"}`), 2_000)
		},
		"an admin policy in a version this build cannot read": func(l *legacyLog) {
			l.genesis(founderKey, founderInfo, 1_000)
			l.op(founderKey, "policy_change", entmoot.NodeInfo{}, []byte(`{"type":"admins/v2","admins":[]}`), 2_000)
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			log := newLegacyLog(t)
			build(log)
			if err := ValidateEntries(log.groupID, log.entries); err == nil {
				t.Fatalf("an inauthentic chain was accepted: %s", name)
			} else if !errors.Is(err, entmoot.ErrRosterReject) {
				t.Fatalf("refused with %v, want an ErrRosterReject so conversion can tell rejection from a read failure", err)
			}
		})
	}
}

// A delegated admin may admit and evict ordinary members, which is the only
// reason the admin machinery is still here: a legacy log written by one has to
// validate.
func TestValidateEntriesAcceptsADelegatedAdminsWork(t *testing.T) {
	log := newLegacyLog(t)
	founder, founderInfo := newIdentity(t)
	admin, adminInfo := newIdentity(t)
	_, memberInfo := newIdentity(t)
	log.genesis(founder, founderInfo, 1_000)
	log.op(founder, "add", adminInfo, nil, 2_000)
	log.op(founder, "policy_change", entmoot.NodeInfo{}, adminPolicy(t, *adminInfo.MemberID), 3_000)
	log.op(admin, "add", memberInfo, nil, 4_000)
	log.op(admin, "remove", memberInfo, nil, 5_000)
	if err := ValidateEntries(log.groupID, log.entries); err != nil {
		t.Fatalf("a delegated admin's chain was refused: %v", err)
	}

	// What the admin may not do: touch another admin, or change the set.
	second := newLegacyLog(t)
	secondAdmin, secondAdminInfo := newIdentity(t)
	second.genesis(founder, founderInfo, 1_000)
	second.op(founder, "add", adminInfo, nil, 2_000)
	second.op(founder, "add", secondAdminInfo, nil, 3_000)
	second.op(founder, "policy_change", entmoot.NodeInfo{}, adminPolicy(t, *adminInfo.MemberID, *secondAdminInfo.MemberID), 4_000)
	second.op(admin, "remove", secondAdminInfo, nil, 5_000)
	if err := ValidateEntries(second.groupID, second.entries); err == nil {
		t.Fatal("an admin evicted another admin")
	}
	_ = secondAdmin
}

// A group id is part of what a version 2 entry signs, so a chain lifted out of
// another group is not authentic here even though every signature verifies.
func TestValidateEntriesRefusesAnotherGroupsChain(t *testing.T) {
	log := newLegacyLog(t)
	founder, founderInfo := newIdentity(t)
	log.genesis(founder, founderInfo, 1_000)
	other := log.groupID
	other[0] ^= 0xff
	if err := ValidateEntries(other, log.entries); err == nil {
		t.Fatal("a chain signed for another group was accepted")
	}
}

// The JSONL reader is the on-disk half of the same answer, and the file it
// reads is somebody's only copy: it must not be rewritten to be read.
func TestValidateLegacyJSONLReadsTheFileItIsGiven(t *testing.T) {
	root := t.TempDir()
	log := newLegacyLog(t)
	founder, founderInfo := newIdentity(t)
	_, memberInfo := newIdentity(t)
	log.genesis(founder, founderInfo, 1_000)
	log.op(founder, "add", memberInfo, nil, 2_000)
	path := log.write(root)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := ValidateLegacyJSONL(path, log.groupID)
	if err != nil {
		t.Fatalf("a well-formed legacy log was refused: %v", err)
	}
	if len(entries) != len(log.entries) {
		t.Fatalf("read %d entries, want %d", len(entries), len(log.entries))
	}
	for i, entry := range entries {
		if entry.ID != log.entries[i].ID {
			t.Fatalf("entry %d id changed in the round trip", i+1)
		}
	}

	// A truncated final line is the crash case, and the source stays put.
	if err := os.WriteFile(path, before[:len(before)-1], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateLegacyJSONL(path, log.groupID); err == nil {
		t.Fatal("a truncated legacy log was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before)-1 {
		t.Fatalf("the validator rewrote the source it was given: %d bytes, want %d", len(after), len(before)-1)
	}
}
