package libp2ptransport

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
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
// retired join, kept in membership history, still says when it was admitted.
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
	// A message dated before the cited checkpoint is judged at it, where
	// the member was not yet in.
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

// retiringGroup is an open group rooted in a directory the test can reopen,
// on a clock the test advances, so a scenario can drive checkpoints through
// the production retirement path and then read membership back from disk.
type retiringGroup struct {
	t       *testing.T
	dir     string
	groupID entmoot.GroupID
	founder *keystore.Identity
	group   *membership.Group
	clockMS int64
}

func newRetiringGroup(t *testing.T) *retiringGroup {
	t.Helper()
	r := &retiringGroup{t: t, dir: t.TempDir(), groupID: mustGroupID(t), founder: mustIdentity(t)}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	r.clockMS = time.Now().UnixMilli()
	group, err := membership.Create(r.dir, r.founder, mustNode(t, r.founder), r.groupID, policy, r.clockMS)
	if err != nil {
		t.Fatal(err)
	}
	r.adopt(group)
	return r
}

func (r *retiringGroup) adopt(group *membership.Group) {
	r.group = group
	r.group.SetNow(func() time.Time { return time.UnixMilli(r.clockMS) })
	r.t.Cleanup(func() { _ = group.Close() })
}

func (r *retiringGroup) tick() int64 {
	r.clockMS += 1_000
	return r.clockMS
}

func (r *retiringGroup) record(identity *keystore.Identity, kind membership.Kind) membership.Record {
	r.t.Helper()
	r.tick()
	rec, err := r.group.SignRecord(identity, membership.Record{Kind: kind})
	if err != nil {
		r.t.Fatal(err)
	}
	return rec
}

// checkpoints signs n founder checkpoints; two or more retire every record
// before the first of them, through settleCanonicalLocked.
func (r *retiringGroup) checkpoints(n int) {
	r.t.Helper()
	for range n {
		r.tick()
		if _, _, err := r.group.SignCheckpoint(r.founder, true); err != nil {
			r.t.Fatal(err)
		}
	}
}

func (r *retiringGroup) reopen() {
	r.t.Helper()
	if err := r.group.Close(); err != nil {
		r.t.Fatal(err)
	}
	group, err := membership.Open(r.dir, r.groupID)
	if err != nil {
		r.t.Fatal(err)
	}
	r.adopt(group)
}

func (r *retiringGroup) verify(message entmoot.Message) error {
	return VerifyHistoricalMessage(r.group, message, time.UnixMilli(r.clockMS))
}

// Issue #198's lifecycle run to completion: a member joins, posts and leaves
// between two checkpoints, so no checkpoint ever names it, and then later
// checkpoints retire its join and leave. Its message must still verify, from
// memory and after a restart, and nothing dated outside its membership may.
func TestHistoricalMessageFromDepartedJoinerVerifiesAfterRetirement(t *testing.T) {
	r := newRetiringGroup(t)
	head := r.group.Canonical().ID
	member := mustIdentity(t)
	join := r.record(member, membership.KindJoin)
	posted := signAtHead(t, member, r.groupID, head, r.tick(), "posted while a member")
	leave := r.record(member, membership.KindLeave)
	afterLeave := signAtHead(t, member, r.groupID, head, leave.Timestamp+1, "dated after leaving")
	// A checkpoint is dated at the newest record it folds in, so a later
	// record keeps afterLeave inside the window C0 opens rather than at the
	// next checkpoint, which no longer names the member.
	r.record(mustIdentity(t), membership.KindJoin)
	beforeJoin := signAtHead(t, member, r.groupID, head, join.Timestamp-1, "dated before joining")

	r.checkpoints(3)
	if r.group.HasRecord(join.ID) || r.group.HasRecord(leave.ID) || !r.group.HasCheckpoint(head) {
		t.Fatal("fixture: want the join and leave retired and the cited checkpoint held")
	}
	for _, phase := range []string{"in memory", "after reopening"} {
		if phase == "after reopening" {
			r.reopen()
		}
		if err := r.verify(posted); err != nil {
			t.Fatalf("%s: departed joiner's history rejected after retirement: %v", phase, err)
		}
		requireNotMember(t, r.verify(afterLeave), phase+": message dated after a retired leave")
		requireNotMember(t, r.verify(beforeJoin), phase+": message dated before a retired join")
	}
}

// A checkpoint proves membership at its own position only. A member a later
// checkpoint names was not a member throughout the window before it, so a
// message dated before its join stays rejected once the join is retired.
func TestHistoricalMessageDatedBeforeRetiredJoinRejected(t *testing.T) {
	r := newRetiringGroup(t)
	head := r.group.Canonical().ID
	member := mustIdentity(t)
	early := signAtHead(t, member, r.groupID, head, r.tick(), "dated before admission")
	join := r.record(member, membership.KindJoin)
	requireNotMember(t, r.verify(early), "message dated before a held join")

	r.checkpoints(2)
	if r.group.HasRecord(join.ID) || !r.group.HasCheckpoint(head) {
		t.Fatal("fixture: want the join retired and the cited checkpoint held")
	}
	requireNotMember(t, r.verify(early), "message dated before a retired join")
	r.reopen()
	requireNotMember(t, r.verify(early), "message dated before a retired join, after reopening")
}

// A node whose history lacks part of a window - a store retired before history
// was kept, or one that lost a row - must not guess. Holding the join but not
// the leave would place the member in the group past its leave; the window no
// longer reproduces the checkpoint that closes it, so the node refuses to place
// the member at all.
func TestHistoricalMessageWithIncompleteRetiredWindowRejected(t *testing.T) {
	r := newRetiringGroup(t)
	head := r.group.Canonical().ID
	member := mustIdentity(t)
	r.record(member, membership.KindJoin)
	posted := signAtHead(t, member, r.groupID, head, r.tick(), "posted while a member")
	leave := r.record(member, membership.KindLeave)
	afterLeave := signAtHead(t, member, r.groupID, head, leave.Timestamp+1, "dated after leaving")
	// A checkpoint is dated at the newest record it folds in, so a later
	// record keeps afterLeave inside the window C0 opens rather than at the
	// next checkpoint, which no longer names the member.
	r.record(mustIdentity(t), membership.KindJoin)
	r.checkpoints(3)
	if err := r.verify(posted); err != nil {
		t.Fatalf("fixture: complete history rejected: %v", err)
	}

	if err := r.group.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(r.dir, "groups", r.groupID.DirName(), "membership.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(`DELETE FROM membership_history WHERE record_id = ?;`, leave.ID[:])
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := result.RowsAffected(); n != 1 {
		t.Fatalf("fixture: removed %d history rows, want the retired leave", n)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	group, err := membership.Open(r.dir, r.groupID)
	if err != nil {
		t.Fatal(err)
	}
	r.adopt(group)

	requireNotMember(t, r.verify(afterLeave), "message dated after a leave this node no longer holds")
	requireNotMember(t, r.verify(posted), "message in a window this node cannot reconstruct")
}
