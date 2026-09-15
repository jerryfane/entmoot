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

// TestHistoryReconciliationIsNotStarvedByOneMember pins the bound that matters.
// The first version counted messages, so one member republishing on the topic
// pushed every other member's name out of the window and those names were
// never learned. The walk now stops per member, not per message.
func TestHistoryReconciliationIsNotStarvedByOneMember(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, root, gid, founder, policy)

	state, err := esphttp.OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	runtime, session, host := startTestRuntimeWithProfiles(t, ctx, root, founder, gid, state)
	defer host.Close()
	defer runtime.Close()

	// A second member joins itself in, which is the ordinary path under an
	// open join rule, and publishes its name long ago.
	quiet, quietInfo := mustDaemonIdentity(t)
	if _, err := session.group.SignRecord(quiet, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if !session.group.IsMemberID(*quietInfo.MemberID) {
		t.Fatalf("the second member did not join, so the flood below would prove nothing")
	}

	quietAt := time.Now().Add(-48 * time.Hour).UnixMilli()
	mustStoreProfileAt(t, ctx, runtime, session, quiet, quietInfo, gid,
		profile.Profile{DisplayName: "quiet-node", IssuedAtMS: quietAt}, quietAt)

	// Now the founder floods the topic with more messages than one page holds.
	for i := 0; i < profileReconcilePageSize+8; i++ {
		publishProfile(t, ctx, session, founder, founderInfo, gid, profile.Profile{
			DisplayName: "loud", IssuedAtMS: time.Now().UnixMilli(),
		})
	}

	// Wipe what the live hook learned, so reconciliation has to find it again.
	if err := esphttp.WithdrawMemberProfileNodeProfile(ctx, state, gid, *quietInfo.MemberID,
		encodeBase64(quietInfo.EntmootPubKey), quietAt-1); err != nil {
		t.Fatalf("reset: %v", err)
	}
	runtime.reconcileProfilesFromHistory(ctx, session)

	if got := mustDisplayName(t, ctx, state, root, gid, *quietInfo.MemberID); got != "quiet-node#"+quietInfo.MemberID.String() {
		t.Fatalf("quiet member's name = %q, want it recovered from history despite the flood", got)
	}
}

func mustStoreProfileAt(t *testing.T, ctx context.Context, runtime *groupRuntime, session *groupSession, identity *keystore.Identity, member entmoot.NodeInfo, gid entmoot.GroupID, p profile.Profile, timestampMS int64) entmoot.Message {
	t.Helper()
	content, err := profile.Encode(p)
	if err != nil {
		t.Fatalf("profile.Encode: %v", err)
	}
	head := session.group.Canonical().ID
	message := entmoot.Message{
		Version: 2, GroupID: gid, Author: member, Timestamp: timestampMS,
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
	// History sync inserts straight into the store, bypassing the live path;
	// that is exactly the case reconciliation has to cover.
	if _, err := runtime.store.Put(ctx, gid, signed); err != nil {
		t.Fatalf("Put: %v", err)
	}
	return signed
}
