package libp2ptransport

import (
	"context"
	"errors"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/signing"
)

// signAtHead signs a version-2 message citing an explicit roster head and
// timestamp, the two coordinates a historical message commits to.
func signAtHead(t *testing.T, identity *keystore.Identity, groupID entmoot.GroupID, head entmoot.RosterEntryID, timestamp int64, content string) entmoot.Message {
	t.Helper()
	signer, err := signing.NewLocalSigner(mustNodeInfo(t, identity.PublicKey), identity)
	if err != nil {
		t.Fatal(err)
	}
	message, err := signer.SignMessage(context.Background(), entmoot.Message{
		Version: 2, GroupID: groupID, Timestamp: timestamp,
		Topics: []string{"history"}, Content: []byte(content), RosterHead: &head,
	})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

// historicalLeaverGroup is issue #198's shape: a member admitted after the
// group's checkpoint posts while it is in, then leaves on its own signature.
type historicalLeaverGroup struct {
	groupID         entmoot.GroupID
	group           *membership.Group
	founder, member *keystore.Identity
	head            entmoot.RosterEntryID
	clockMS         int64
	joinMS, leaveMS int64
	during          entmoot.Message
}

func newHistoricalLeaverGroup(t *testing.T) *historicalLeaverGroup {
	t.Helper()
	h := &historicalLeaverGroup{founder: mustIdentity(t), member: mustIdentity(t)}
	h.groupID, h.group = mustOpenGroup(t, h.founder)
	h.clockMS = h.group.Canonical().Timestamp
	h.group.SetNow(func() time.Time { return time.UnixMilli(h.clockMS) })
	h.head = h.group.Canonical().ID

	h.clockMS += 1_000
	join, err := h.group.SignRecord(h.member, membership.Record{Kind: membership.KindJoin})
	if err != nil {
		t.Fatal(err)
	}
	h.joinMS = join.Timestamp
	h.clockMS += 1_000
	h.during = signAtHead(t, h.member, h.groupID, h.head, h.clockMS, "posted while a member")
	// The live path is the one a message takes on arrival: current membership.
	if err := VerifyLiveMessage(h.group, h.during, time.UnixMilli(h.clockMS)); err != nil {
		t.Fatalf("member's live message rejected: %v", err)
	}
	h.clockMS += 1_000
	leave, err := h.group.SignRecord(h.member, membership.Record{Kind: membership.KindLeave})
	if err != nil {
		t.Fatal(err)
	}
	h.leaveMS = leave.Timestamp
	return h
}

func requireNotMember(t *testing.T, err error, what string) {
	t.Helper()
	if !errors.Is(err, entmoot.ErrNotMember) {
		t.Fatalf("%s: got %v, want ErrNotMember", what, err)
	}
}

// A member that joined after the cited checkpoint and later left still
// authored what it signed while it was in: its history stays verifiable.
func TestHistoricalMessageFromDepartedPostCheckpointJoinerVerifies(t *testing.T) {
	h := newHistoricalLeaverGroup(t)
	if err := VerifyHistoricalMessage(h.group, h.during, time.UnixMilli(h.clockMS)); err != nil {
		t.Fatalf("departed member's history rejected: %v", err)
	}
	// The live rule is unchanged: once out, the same identity cannot publish.
	requireNotMember(t, VerifyLiveMessage(h.group, h.during, time.UnixMilli(h.clockMS)), "live message after leave")
}

// Leaving is a boundary: no message dated at or after the leave verifies, nor
// one dated before the join, at whatever head it cites. Signature checks stay.
func TestHistoricalMessageOutsideMembershipIntervalRejected(t *testing.T) {
	h := newHistoricalLeaverGroup(t)
	requireNotMember(t, VerifyHistoricalMessage(h.group,
		signAtHead(t, h.member, h.groupID, h.head, h.leaveMS, "signed at the leave"), time.UnixMilli(h.clockMS)),
		"message at the leave instant")
	requireNotMember(t, VerifyHistoricalMessage(h.group,
		signAtHead(t, h.member, h.groupID, h.head, h.leaveMS+500, "signed after leaving"), time.UnixMilli(h.clockMS)),
		"message after the leave")
	requireNotMember(t, VerifyHistoricalMessage(h.group,
		signAtHead(t, h.member, h.groupID, h.head, h.joinMS-1, "dated before joining"), time.UnixMilli(h.clockMS)),
		"message before the join")

	stranger := mustIdentity(t)
	requireNotMember(t, VerifyHistoricalMessage(h.group,
		signAtHead(t, stranger, h.groupID, h.head, h.joinMS+500, "never a member"), time.UnixMilli(h.clockMS)),
		"message from a never-member")

	tampered := h.during
	tampered.Content = []byte("altered after signing")
	if err := VerifyHistoricalMessage(h.group, tampered, time.UnixMilli(h.clockMS)); !errors.Is(err, entmoot.ErrSigInvalid) {
		t.Fatalf("tampered history: got %v, want ErrSigInvalid", err)
	}

	// A checkpoint signed after the leave closes the door from both sides:
	// citing it names a roster without the member, and citing the old head
	// with a later date is judged at the newer checkpoint.
	h.clockMS += 1_000
	next, _, err := h.group.SignCheckpoint(h.founder, true)
	if err != nil {
		t.Fatal(err)
	}
	h.clockMS += 1_000
	requireNotMember(t, VerifyHistoricalMessage(h.group,
		signAtHead(t, h.member, h.groupID, next.ID, h.clockMS, "cites the post-leave checkpoint"), time.UnixMilli(h.clockMS)),
		"message citing the post-leave checkpoint")
	requireNotMember(t, VerifyHistoricalMessage(h.group,
		signAtHead(t, h.member, h.groupID, h.head, h.clockMS, "old head, new date"), time.UnixMilli(h.clockMS)),
		"message citing the old head after the checkpoint")
	if err := VerifyHistoricalMessage(h.group, h.during, time.UnixMilli(h.clockMS)); err != nil {
		t.Fatalf("departed member's history rejected after a checkpoint: %v", err)
	}
}

// A member that joined after a checkpoint and is still in verifies at that
// checkpoint after the next one lands and the join record is retired: the
// checkpoint that closes the window is the signed evidence of the join.
func TestHistoricalMessageFromJoinerVerifiesAfterRecordsRetire(t *testing.T) {
	founder, member := mustIdentity(t), mustIdentity(t)
	groupID, group := mustOpenGroup(t, founder)
	clockMS := group.Canonical().Timestamp
	group.SetNow(func() time.Time { return time.UnixMilli(clockMS) })
	head, headMS := group.Canonical().ID, group.Canonical().Timestamp
	clockMS += 1_000
	join, err := group.SignRecord(member, membership.Record{Kind: membership.KindJoin})
	if err != nil {
		t.Fatal(err)
	}
	clockMS += 1_000
	posted := signAtHead(t, member, groupID, head, clockMS, "posted before the next checkpoint")
	preJoin := signAtHead(t, member, groupID, head, join.Timestamp-1, "dated before the join")
	for i := range 2 {
		clockMS += 1_000
		if _, _, err := group.SignCheckpoint(founder, true); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			// The next checkpoint names the member, but the join record is
			// still held and says exactly when it was admitted.
			if !group.HasRecord(join.ID) {
				t.Fatalf("fixture: want the join held behind one checkpoint of lag")
			}
			requireNotMember(t, VerifyHistoricalMessage(group, preJoin, time.UnixMilli(clockMS)), "message dated before a held join")
		}
	}
	if group.HasRecord(join.ID) || !group.HasCheckpoint(head) {
		t.Fatalf("fixture: want the join retired and the cited checkpoint held")
	}
	if err := VerifyHistoricalMessage(group, posted, time.UnixMilli(clockMS)); err != nil {
		t.Fatalf("joiner's history rejected after records retired: %v", err)
	}
	// The closing checkpoint places the join inside its window and no
	// earlier: a message dated before the cited checkpoint is before it.
	requireNotMember(t, VerifyHistoricalMessage(group,
		signAtHead(t, member, groupID, head, headMS-1, "dated before the cited checkpoint"), time.UnixMilli(clockMS)),
		"message dated before the window the join is known to fall in")
}

// A member a checkpoint names, who then leaves, cannot keep authoring by citing
// that checkpoint: its leave is part of the roster position any later date
// commits to, whether the leave record is still held or a checkpoint has
// folded it in.
func TestHistoricalMessageFromCheckpointMemberAfterLeaveRejected(t *testing.T) {
	founder, member := mustIdentity(t), mustIdentity(t)
	groupID, group := mustOpenGroup(t, founder)
	clockMS := group.Canonical().Timestamp
	group.SetNow(func() time.Time { return time.UnixMilli(clockMS) })
	clockMS += 1_000
	if _, err := group.SignRecord(member, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatal(err)
	}
	clockMS += 1_000
	withMember, _, err := group.SignCheckpoint(founder, true)
	if err != nil {
		t.Fatal(err)
	}
	clockMS += 1_000
	before := signAtHead(t, member, groupID, withMember.ID, clockMS, "posted while a member")
	clockMS += 1_000
	leave, err := group.SignRecord(member, membership.Record{Kind: membership.KindLeave})
	if err != nil {
		t.Fatal(err)
	}
	clockMS += 1_000
	after := signAtHead(t, member, groupID, withMember.ID, clockMS, "posted after leaving")
	if err := VerifyHistoricalMessage(group, before, time.UnixMilli(clockMS)); err != nil {
		t.Fatalf("history from before the leave rejected: %v", err)
	}
	requireNotMember(t, VerifyHistoricalMessage(group, after, time.UnixMilli(clockMS)), "post-leave message citing the member's checkpoint, leave held")
	for range 2 {
		clockMS += 1_000
		if _, _, err := group.SignCheckpoint(founder, true); err != nil {
			t.Fatal(err)
		}
	}
	if group.HasRecord(leave.ID) || !group.HasCheckpoint(withMember.ID) {
		t.Fatalf("fixture: want the leave retired and the cited checkpoint held")
	}
	if err := VerifyHistoricalMessage(group, before, time.UnixMilli(clockMS)); err != nil {
		t.Fatalf("history from before the leave rejected after retirement: %v", err)
	}
	requireNotMember(t, VerifyHistoricalMessage(group, after, time.UnixMilli(clockMS)), "post-leave message citing the member's checkpoint, leave retired")
}
