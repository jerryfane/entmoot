package main

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/profile"
	"entmoot/pkg/entmoot/signing"
)

// TestPublishedProfileBecomesADisplayName is the whole point of the feature:
// a member publishes its name into the group, and the member list the phone
// reads shows that name instead of a truncated MemberID.
//
// Before this path existed the reader was live and nothing wrote, so every
// member was shown as its own key forever.
func TestPublishedProfileBecomesADisplayName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	identity, member := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	mustCreateGroup(t, root, gid, identity, membership.DefaultPolicy())

	state, err := esphttp.OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()

	runtime, session, host := startTestRuntimeWithProfiles(t, ctx, root, identity, gid, state)
	defer host.Close()
	defer runtime.Close()

	memberID := *member.MemberID

	// Before publishing, the display name is the MemberID fallback.
	before := mustDisplayName(t, ctx, state, root, gid, memberID)
	if before != "member-"+memberID.String() {
		t.Fatalf("fallback display name = %q, want the member-id fallback", before)
	}

	now := time.Now()
	content, err := profile.Encode(profile.Profile{
		DisplayName: "pi-burj",
		IssuedAtMS:  now.UnixMilli(),
		ExpiresAtMS: now.Add(time.Hour).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("profile.Encode: %v", err)
	}
	head := session.group.Canonical().ID
	message := entmoot.Message{
		Version: 2, GroupID: gid, Author: member, Timestamp: now.UnixMilli(),
		Topics: []string{profile.Topic}, Content: content, RosterHead: &head,
	}
	signer, err := signing.NewLocalSigner(member, identity)
	if err != nil {
		t.Fatalf("NewLocalSigner: %v", err)
	}
	signed, err := signing.SignMessage(ctx, signer, message)
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	if _, err := session.live.Publish(ctx, signed); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The name is always shown paired with the full MemberID, so a member
	// cannot impersonate another by choosing its name.
	got := mustDisplayName(t, ctx, state, root, gid, memberID)
	if want := "pi-burj#" + memberID.String(); got != want {
		t.Fatalf("display name = %q, want %q", got, want)
	}
	if !strings.HasPrefix(got, "pi-burj#") {
		t.Fatalf("display name = %q, want the published name first", got)
	}
}

// TestProfileOnAnUnrelatedTopicIsIgnored pins that the topic is what selects a
// profile: the same payload on a chat topic must not set anyone's name, or any
// member could rename another by quoting a payload.
func TestProfileOnAnUnrelatedTopicIsIgnored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	identity, member := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	mustCreateGroup(t, root, gid, identity, membership.DefaultPolicy())

	state, err := esphttp.OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()

	runtime, session, host := startTestRuntimeWithProfiles(t, ctx, root, identity, gid, state)
	defer host.Close()
	defer runtime.Close()

	memberID := *member.MemberID
	content, err := profile.Encode(profile.Profile{DisplayName: "not-my-name", IssuedAtMS: time.Now().UnixMilli()})
	if err != nil {
		t.Fatalf("profile.Encode: %v", err)
	}
	head := session.group.Canonical().ID
	message := entmoot.Message{
		Version: 2, GroupID: gid, Author: member, Timestamp: time.Now().UnixMilli(),
		Topics: []string{"chat"}, Content: content, RosterHead: &head,
	}
	signer, err := signing.NewLocalSigner(member, identity)
	if err != nil {
		t.Fatalf("NewLocalSigner: %v", err)
	}
	signed, err := signing.SignMessage(ctx, signer, message)
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	if _, err := session.live.Publish(ctx, signed); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "member-"+memberID.String() {
		t.Fatalf("display name = %q, want the member-id fallback: a payload on an unrelated topic must not set a name", got)
	}
}

func mustDisplayName(t *testing.T, ctx context.Context, state esphttp.StateStore, root string, gid entmoot.GroupID, memberID entmoot.MemberID) string {
	t.Helper()
	members, err := localGroupCatalog{dataDir: root, state: state}.ListMembers(ctx, gid)
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	enriched, err := esphttp.EnrichMemberDisplayNames(ctx, state, gid, members)
	if err != nil {
		t.Fatalf("EnrichMemberDisplayNames: %v", err)
	}
	for _, m := range enriched {
		if m.MemberID == memberID {
			return m.DisplayName
		}
	}
	t.Fatalf("member %s not in the list", memberID)
	return ""
}

// TestProfileClearWithdrawsTheName pins the defect the review found: `profile
// clear` published an empty name, printed success, and changed nothing,
// because an empty hostname is not a storable record. Withdrawal must delete.
func TestProfileClearWithdrawsTheName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	identity, member := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	mustCreateGroup(t, root, gid, identity, membership.DefaultPolicy())

	state, err := esphttp.OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	runtime, session, host := startTestRuntimeWithProfiles(t, ctx, root, identity, gid, state)
	defer host.Close()
	defer runtime.Close()

	memberID := *member.MemberID
	publishProfile(t, ctx, session, identity, member, gid, profile.Profile{
		DisplayName: "pi-burj", IssuedAtMS: time.Now().UnixMilli(),
	})
	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "pi-burj#"+memberID.String() {
		t.Fatalf("display name after set = %q", got)
	}

	publishProfile(t, ctx, session, identity, member, gid, profile.Profile{
		DisplayName: "", IssuedAtMS: time.Now().UnixMilli() + 1,
	})
	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "member-"+memberID.String() {
		t.Fatalf("display name after clear = %q, want the member-id fallback", got)
	}
}

// TestFutureDatedProfileCannotPinAName reproduces the attack the review
// executed: a profile dated in the year 3000 used to win every comparison, so
// the member's real name could never be updated again, on any node that saw
// the message. Ordering is by receipt, so the later honest name must win.
func TestFutureDatedProfileCannotPinAName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	identity, member := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	mustCreateGroup(t, root, gid, identity, membership.DefaultPolicy())

	state, err := esphttp.OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	runtime, session, host := startTestRuntimeWithProfiles(t, ctx, root, identity, gid, state)
	defer host.Close()
	defer runtime.Close()

	memberID := *member.MemberID
	// Year 3000, and an expiry far beyond any sane lifetime.
	publishProfile(t, ctx, session, identity, member, gid, profile.Profile{
		DisplayName: "pinned", IssuedAtMS: 32_503_680_000_000, ExpiresAtMS: 64_000_000_000_000,
	})
	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "pinned#"+memberID.String() {
		t.Fatalf("display name = %q, want the crafted name to be recorded normally", got)
	}

	// The crafted expiry must already be clamped on the record it was written
	// with — checking after a later publish would only see the later record.
	crafted, ok, err := state.GetNodeProfile(ctx, memberID)
	if err != nil || !ok {
		t.Fatalf("GetNodeProfile ok/err = %v/%v", ok, err)
	}
	if limit := time.Now().UnixMilli() + maxProfileLifetimeMS + 1000; crafted.ExpiresAtMS > limit {
		t.Fatalf("crafted expiry %d exceeds the clamp %d", crafted.ExpiresAtMS, limit)
	}

	publishProfile(t, ctx, session, identity, member, gid, profile.Profile{
		DisplayName: "second", IssuedAtMS: time.Now().UnixMilli(),
	})
	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "second#"+memberID.String() {
		t.Fatalf("display name = %q, want the later name to win over a future-dated one", got)
	}

}

func publishProfile(t *testing.T, ctx context.Context, session *groupSession, identity *keystore.Identity, member entmoot.NodeInfo, gid entmoot.GroupID, p profile.Profile) {
	t.Helper()
	content, err := profile.Encode(p)
	if err != nil {
		// A withdrawal has an empty name, which Encode accepts.
		t.Fatalf("profile.Encode: %v", err)
	}
	head := session.group.Canonical().ID
	message := entmoot.Message{
		Version: 2, GroupID: gid, Author: member, Timestamp: time.Now().UnixMilli(),
		Topics: []string{profile.Topic}, Content: content, RosterHead: &head,
	}
	signer, err := signing.NewLocalSigner(member, identity)
	if err != nil {
		t.Fatalf("NewLocalSigner: %v", err)
	}
	signed, err := signing.SignMessage(ctx, signer, message)
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	if _, err := session.live.Publish(ctx, signed); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}
