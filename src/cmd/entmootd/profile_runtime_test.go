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

// TestWithdrawalSurvivesAnOlderProfileArrivingLate is the defect receipt-time
// ordering introduced. A withdrawal must be durable: an older profile that
// arrives afterwards — history catch-up, or a peer re-gossiping — must not
// resurrect the withdrawn name, or two nodes disagree about it forever.
func TestWithdrawalSurvivesAnOlderProfileArrivingLate(t *testing.T) {
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

	base := time.Now().UnixMilli()
	publishProfile(t, ctx, session, identity, member, gid, profile.Profile{DisplayName: "pi-burj", IssuedAtMS: base})
	publishProfile(t, ctx, session, identity, member, gid, profile.Profile{DisplayName: "", IssuedAtMS: base + 1000})
	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "member-"+memberID.String() {
		t.Fatalf("display name after withdrawal = %q", got)
	}

	// The older profile arrives last. It must lose.
	runtime.observeMemberProfile(ctx, gid, mustProfileMessage(t, ctx, session, identity, member, gid,
		profile.Profile{DisplayName: "pi-burj", IssuedAtMS: base}))
	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "member-"+memberID.String() {
		t.Fatalf("an older profile resurrected a withdrawn name: %q", got)
	}
}

// TestFutureDatedProfileIsRefused pins the clock bound. Ordering uses the
// author's timestamp, so a profile from beyond the skew window must be refused
// outright rather than recorded — recording it would pin the name.
func TestFutureDatedProfileIsRefused(t *testing.T) {
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
		DisplayName: "pinned", IssuedAtMS: 32_503_680_000_000,
	})
	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "member-"+memberID.String() {
		t.Fatalf("a future-dated profile was recorded: %q", got)
	}

	// Inside the window it is accepted, so the bound is not simply refusing
	// everything.
	publishProfile(t, ctx, session, identity, member, gid, profile.Profile{
		DisplayName: "ok", IssuedAtMS: time.Now().Add(time.Minute).UnixMilli(),
	})
	if got := mustDisplayName(t, ctx, state, root, gid, memberID); got != "ok#"+memberID.String() {
		t.Fatalf("a profile inside the skew window was refused: %q", got)
	}
}

func mustProfileMessage(t *testing.T, ctx context.Context, session *groupSession, identity *keystore.Identity, member entmoot.NodeInfo, gid entmoot.GroupID, p profile.Profile) entmoot.Message {
	t.Helper()
	content, err := profile.Encode(p)
	if err != nil {
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
	return signed
}
