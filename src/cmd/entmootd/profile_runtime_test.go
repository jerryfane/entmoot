package main

import (
	"context"
	"crypto/rand"
	"fmt"
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
// because an empty hostname was dropped before any write. A withdrawal is a
// record, not a delete — a tombstone carrying its own issue time, so an older
// profile arriving later cannot resurrect the name.
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
// never learned. The walk now visits every message of every page in the
// window and keeps a per-member best, so a flood SMALLER than the window — as
// here, three pages and change against a sixteen-page window — cannot crowd
// another member out. A flood larger than the window still can; that bound is
// documented where the constants are.
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
	// More than one page, by a margin no single-page walk could cover: a
	// boundary bug that advanced one message per page passed the old
	// page-size+8 flood.
	for i := 0; i < 3*profileReconcilePageSize+44; i++ {
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

// TestReconciliationAdoptsTheNewestNameInTheWindow pins which message in the
// window is believed. The store returns each page in topological order, ties
// ascending, so "the first message from this member in the page" is its
// OLDEST — adopting that silently rolled a member's name back to a superseded
// one on every catch-up.
func TestReconciliationAdoptsTheNewestNameInTheWindow(t *testing.T) {
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

	base := time.Now().Add(-time.Hour).UnixMilli()
	mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
		profile.Profile{DisplayName: "old-name", IssuedAtMS: base}, base)
	mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
		profile.Profile{DisplayName: "new-name", IssuedAtMS: base + 1000}, base+1000)

	runtime.reconcileProfilesFromHistory(ctx, session)

	want := "new-name#" + founderInfo.MemberID.String()
	if got := mustDisplayName(t, ctx, state, root, gid, *founderInfo.MemberID); got != want {
		t.Fatalf("display name = %q, want the newest name in the window (%q)", got, want)
	}
}

// TestReconciliationDoesNotSkipAWithdrawal is the same defect with the worst
// payload: when the newest message is a withdrawal, adopting an older profile
// republishes a name its owner retracted.
func TestReconciliationDoesNotSkipAWithdrawal(t *testing.T) {
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

	base := time.Now().Add(-time.Hour).UnixMilli()
	mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
		profile.Profile{DisplayName: "retracted", IssuedAtMS: base}, base)
	mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
		profile.Profile{DisplayName: "", IssuedAtMS: base + 1000}, base+1000)

	runtime.reconcileProfilesFromHistory(ctx, session)

	if got := mustDisplayName(t, ctx, state, root, gid, *founderInfo.MemberID); got != "member-"+founderInfo.MemberID.String() {
		t.Fatalf("display name = %q, want the fallback: the withdrawal is the newest message", got)
	}
}

// TestReconciliationLetsTheStoreOrderAMillisecondTie is the defect selection
// introduced: the walk ranked messages by (timestamp, author, id) while the
// store ranks records by issue time, tombstone, hostname. A set and a clear
// published in the same millisecond have an equal message key, so "the newest
// message" was whichever id sorted higher — and when that was the profile, the
// retracted name came back on a history-only node. Observing both and letting
// the store decide removes the second ordering rule entirely.
func TestReconciliationLetsTheStoreOrderAMillisecondTie(t *testing.T) {
	for _, tc := range []struct {
		name        string
		clearOffset int64
		clearIssue  int64
	}{
		{name: "same_millisecond", clearOffset: 0, clearIssue: 1},
		{name: "clear_message_older_issue_newer", clearOffset: -1000, clearIssue: 1},
		// Equal issue times: nothing but the store's tombstone rule can
		// decide this, so a ranking that compares issue times alone picks by
		// encounter order and the retracted name comes back.
		{name: "equal_issue_times", clearOffset: -1000, clearIssue: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			at := time.Now().Add(-time.Hour).UnixMilli()
			// The profile: message timestamp and issue time agree.
			mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
				profile.Profile{DisplayName: "retracted", IssuedAtMS: at}, at)
			// The withdrawal: issued strictly later, but its MESSAGE may carry
			// the same or an older timestamp, which is what broke selection.
			mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
				profile.Profile{DisplayName: "", IssuedAtMS: at + tc.clearIssue}, at+tc.clearOffset)

			runtime.reconcileProfilesFromHistory(ctx, session)

			if got := mustDisplayName(t, ctx, state, root, gid, *founderInfo.MemberID); got != "member-"+founderInfo.MemberID.String() {
				t.Fatalf("display name = %q, want the fallback: the withdrawal was issued later", got)
			}
		})
	}
}

// TestReconciliationFindsARetractionADeeperPageAway is the defect that
// survived observing every message in a page: the member was dropped from the
// walk as soon as ANY of its messages appeared, so a withdrawal sitting one
// page deeper — older by message timestamp, NEWER by issue time — was never
// read. Message order and issue order are independent, so the walk cannot use
// the message key to decide it is finished with a member.
func TestReconciliationFindsARetractionADeeperPageAway(t *testing.T) {
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

	base := time.Now().Add(-2 * time.Hour).UnixMilli()
	// Oldest by message timestamp, newest by issue time: the retraction.
	mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
		profile.Profile{DisplayName: "", IssuedAtMS: base + 100_000}, base)
	// A page and a half of unrelated traffic between the two.
	for i := 0; i < profileReconcilePageSize+44; i++ {
		mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
			profile.Profile{DisplayName: fmt.Sprintf("noise-%d", i), IssuedAtMS: base + 1_000}, base+int64(1_000+i))
	}
	// Newest by message timestamp, older by issue time: the superseded name.
	mustStoreProfileAt(t, ctx, runtime, session, founder, founderInfo, gid,
		profile.Profile{DisplayName: "retracted", IssuedAtMS: base + 2_000}, base+5_000_000)

	runtime.reconcileProfilesFromHistory(ctx, session)

	if got := mustDisplayName(t, ctx, state, root, gid, *founderInfo.MemberID); got != "member-"+founderInfo.MemberID.String() {
		t.Fatalf("display name = %q, want the fallback: the withdrawal has the newest issue time in the window", got)
	}
}
