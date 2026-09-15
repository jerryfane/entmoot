package libp2ptransport

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// quarantineFixture is a live group with no router: the quarantine, the drain
// and the membership state are exercised directly, so the test does not depend
// on GossipSub timing.
//
// ahead is a second copy of the same group, standing in for a publisher whose
// membership has already moved: records and checkpoints are minted there and
// handed to local only when a test wants the synchronization to happen.
type quarantineFixture struct {
	group    *LiveGroup
	local    *membership.Group
	ahead    *membership.Group
	founder  *keystore.Identity
	member   *keystore.Identity
	groupID  entmoot.GroupID
	store    store.MessageStore
	ingested []entmoot.Message
	clock    time.Time
}

func newQuarantineFixture(t *testing.T) *quarantineFixture {
	t.Helper()
	founder := mustIdentity(t)
	groupID, ahead := mustOpenGroup(t, founder)
	local, err := membership.Adopt(t.TempDir(), ahead.Canonical())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	f := &quarantineFixture{
		local:   local,
		ahead:   ahead,
		founder: founder,
		member:  mustIdentity(t),
		groupID: groupID,
		store:   store.NewMemory(),
		clock:   time.Now(),
	}
	t.Cleanup(func() { f.store.Close() })
	localHost, _, err := NewHost(context.Background(), founder, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = localHost.Close() })
	cfg := LiveConfig{
		GroupID: groupID,
		Group:   local,
		Store:   f.store,
		Host:    localHost,
		Now:     func() time.Time { return f.clock },
		OnIngest: func(message entmoot.Message) {
			f.ingested = append(f.ingested, message)
		},
	}
	f.group = &LiveGroup{cfg: cfg, quarantine: newRosterAheadQuarantine(cfg.Now)}
	return f
}

// ingest is what the router does with an arriving message: the live group's
// validator decides whether to accept it, hold it for a later retry or refuse
// it outright. Going through it rather than calling the quarantine directly
// keeps the test on the path a real publisher reaches.
func (f *quarantineFixture) ingest(t *testing.T, author *keystore.Identity, message entmoot.Message) pubsub.ValidationResult {
	t.Helper()
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := BindingFromPublicKey(author.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	from := []byte(binding.PeerID)
	return f.group.validate(context.Background(), binding.PeerID, &pubsub.Message{
		Message: &pb.Message{From: from, Data: data},
	})
}

// admitAhead admits an identity on the publisher's copy only. The group's join
// rule is open, so the joiner signs its own admission.
func (f *quarantineFixture) admitAhead(t *testing.T, identity *keystore.Identity) membership.Record {
	t.Helper()
	record, err := f.ahead.SignRecord(identity, membership.Record{Kind: membership.KindJoin})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// checkpointAhead folds the publisher's pending records into a new checkpoint,
// which is the head its messages will name.
func (f *quarantineFixture) checkpointAhead(t *testing.T) membership.Checkpoint {
	t.Helper()
	checkpoint, signed, err := f.ahead.SignCheckpoint(f.founder, true)
	if err != nil {
		t.Fatal(err)
	}
	if !signed {
		t.Fatal("forced checkpoint was declined")
	}
	return checkpoint
}

// syncLocal is what a membership sync does to our copy: apply the checkpoints
// the peer offered, then its records.
func (f *quarantineFixture) syncLocal(t *testing.T, checkpoints []membership.Checkpoint, records []membership.Record) {
	t.Helper()
	for _, checkpoint := range checkpoints {
		if _, err := f.local.ApplyCheckpoint(checkpoint); err != nil {
			t.Fatalf("apply checkpoint %d: %v", checkpoint.Sequence, err)
		}
	}
	for _, record := range records {
		if _, err := f.local.Apply(record); err != nil {
			t.Fatalf("apply record %s: %v", record.Kind, err)
		}
	}
}

// signedAt signs a message naming an explicit head, which is how a publisher
// whose membership is ahead of ours looks on the wire.
func (f *quarantineFixture) signedAt(t *testing.T, identity *keystore.Identity, head entmoot.RosterEntryID, content string) entmoot.Message {
	t.Helper()
	author := mustNodeInfo(t, identity.PublicKey)
	signer, err := signing.NewLocalSigner(author, identity)
	if err != nil {
		t.Fatal(err)
	}
	message, err := signer.SignMessage(context.Background(), entmoot.Message{
		Version: 2, GroupID: f.groupID, Timestamp: f.clock.UnixMilli(),
		Topics: []string{"live"}, Content: []byte(content), RosterHead: &head,
	})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

// A membership change and a publish race constantly: the publisher's group
// state moves first, so its message names a checkpoint the receiver has not
// synchronized. Dropping it loses the message; holding and retrying delivers
// it.
func TestMembershipAheadMessageIsHeldAndIngestedAfterSync(t *testing.T) {
	f := newQuarantineFixture(t)
	newcomer := mustIdentity(t)

	// The publisher already holds the checkpoint that admitted the newcomer.
	f.admitAhead(t, newcomer)
	ahead := f.checkpointAhead(t)
	if f.local.HasCheckpoint(ahead.ID) {
		t.Fatal("fixture already knows the ahead checkpoint")
	}

	message := f.signedAt(t, newcomer, ahead.ID, "published while ahead")
	err := VerifyLiveMessage(f.local, message, f.clock)
	if err == nil {
		t.Fatal("a message naming an unknown checkpoint verified")
	}
	if !isUnknownRosterHead(err) {
		t.Fatalf("unknown checkpoint reported as %v, want ErrRosterHeadUnknown", err)
	}
	// The router is told to ignore it rather than reject it, so the publisher
	// is not penalised for being ahead of us, and the message waits.
	if got := f.ingest(t, newcomer, message); got != pubsub.ValidationIgnore {
		t.Fatalf("validation of a message naming an unknown checkpoint = %v, want ignore", got)
	}
	if f.group.QuarantinedMessages() != 1 {
		t.Fatalf("quarantined = %d, want 1", f.group.QuarantinedMessages())
	}
	// Draining before membership catches up must keep the message waiting.
	if ingested, dropped := f.group.DrainQuarantine(context.Background()); ingested != 0 || dropped != 0 {
		t.Fatalf("drained %d and dropped %d before membership caught up", ingested, dropped)
	}
	if f.group.QuarantinedMessages() != 1 {
		t.Fatal("a still-unknown checkpoint stopped waiting")
	}

	// The membership sync brings in the checkpoint the publisher already had.
	f.syncLocal(t, []membership.Checkpoint{ahead}, nil)
	ingested, dropped := f.group.DrainQuarantine(context.Background())
	if ingested != 1 || dropped != 0 {
		t.Fatalf("drained %d dropped %d after sync, want 1 and 0", ingested, dropped)
	}
	if f.group.QuarantinedMessages() != 0 {
		t.Fatal("drained messages stayed in the quarantine")
	}
	if has, err := f.store.Has(context.Background(), f.groupID, message.ID); err != nil || !has {
		t.Fatalf("held message was not stored: has=%t err=%v", has, err)
	}
	if len(f.ingested) != 1 || f.ingested[0].ID != message.ID {
		t.Fatalf("ingest callbacks = %+v", f.ingested)
	}
}

// The buffer must not become a memory sink for a peer inventing heads, and a
// message whose head never arrives must not be held forever.
func TestQuarantineIsBoundedByMessagesHeadsAndTime(t *testing.T) {
	f := newQuarantineFixture(t)

	held := 0
	for i := range maxQuarantinedHeads + 4 {
		var head entmoot.RosterEntryID
		head[0] = byte(i + 1)
		message := f.signedAt(t, f.member, head, "invented head")
		if f.ingest(t, f.member, message) == pubsub.ValidationIgnore {
			held++
		}
	}
	if held != maxQuarantinedHeads {
		t.Fatalf("held %d distinct heads, want the cap of %d", held, maxQuarantinedHeads)
	}

	// Same head, many messages: the message cap applies.
	var single entmoot.RosterEntryID
	single[0] = 1
	for range maxQuarantinedMessages + 16 {
		f.clock = f.clock.Add(time.Millisecond)
		f.ingest(t, f.member, f.signedAt(t, f.member, single, "flood"))
	}
	if count := f.group.QuarantinedMessages(); count != maxQuarantinedMessages {
		t.Fatalf("quarantined %d messages, want the cap of %d", count, maxQuarantinedMessages)
	}

	// A message offered twice must not consume another slot. The buffer is at
	// its message cap, so make room first: without the dedup check the
	// re-offered message would take the freed slot and the count would climb
	// back to the cap.
	f.clock = f.clock.Add(quarantineTTL + time.Second)
	if count := f.group.QuarantinedMessages(); count != 0 {
		t.Fatalf("expired messages still held: %d", count)
	}
	f.clock = f.clock.Add(time.Millisecond)
	first := f.signedAt(t, f.member, single, "only message")
	if got := f.ingest(t, f.member, first); got != pubsub.ValidationIgnore {
		t.Fatalf("an empty quarantine validated a message naming an unknown head as %v, want ignore", got)
	}
	if got := f.ingest(t, f.member, first); got == pubsub.ValidationIgnore {
		t.Fatal("the same message was held a second time")
	}
	if count := f.group.QuarantinedMessages(); count != 1 {
		t.Fatalf("a duplicate consumed a slot: quarantined %d, want 1", count)
	}

	// Time expires the buffer: nothing is held past the TTL.
	f.clock = f.clock.Add(quarantineTTL + time.Second)
	if ingested, dropped := f.group.DrainQuarantine(context.Background()); ingested != 0 || dropped != 0 {
		t.Fatalf("expired messages were drained: ingested=%d dropped=%d", ingested, dropped)
	}
	if count := f.group.QuarantinedMessages(); count != 0 {
		t.Fatalf("quarantine still holds %d expired messages", count)
	}
}

// Only an unknown head earns a hold. A message from someone who is not a
// member, at a checkpoint we do hold, is refused outright.
func TestQuarantineDoesNotCoverInvalidMessages(t *testing.T) {
	f := newQuarantineFixture(t)
	stranger := mustIdentity(t)
	head := f.local.Canonical().ID

	strangerMessage := f.signedAt(t, stranger, head, "not a member")
	if err := VerifyLiveMessage(f.local, strangerMessage, f.clock); err == nil {
		t.Fatal("a non-member message verified")
	} else if isUnknownRosterHead(err) {
		t.Fatalf("non-member rejection was classified as an unknown head: %v", err)
	}

	// A member that was removed is in the same position: the head it names is
	// known, so there is nothing to wait for.
	if _, err := f.local.SignRecord(f.member, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatal(err)
	}
	memberMessage := f.signedAt(t, f.member, f.local.Canonical().ID, "still a member")
	if err := VerifyLiveMessage(f.local, memberMessage, f.clock); err != nil {
		t.Fatalf("member message did not verify: %v", err)
	}
	if _, err := f.local.SignRecord(f.founder, membership.Record{
		Kind: membership.KindRemove, Subject: mustNode(t, f.member),
	}); err != nil {
		t.Fatal(err)
	}
	if err := VerifyLiveMessage(f.local, memberMessage, f.clock); err == nil {
		t.Fatal("a removed member's message verified")
	} else if isUnknownRosterHead(err) {
		t.Fatalf("removed member was classified as an unknown head: %v", err)
	}
}

// isUnknownRosterHead keeps the classification assertions readable.
func isUnknownRosterHead(err error) bool {
	return errors.Is(err, entmoot.ErrRosterHeadUnknown)
}

// The buffer is a retry queue for a real race, not a parking lot for junk. A
// message naming an invented head with a broken signature is refused outright,
// so a stranger cannot occupy the slots a genuine publisher needs.
func TestUnsignedUnknownHeadMessageIsNotHeld(t *testing.T) {
	f := newQuarantineFixture(t)
	stranger := mustIdentity(t)
	var invented entmoot.RosterEntryID
	invented[0] = 0x77

	forged := f.signedAt(t, stranger, invented, "forged")
	forged.Content = []byte("tampered after signing")
	err := VerifyLiveMessage(f.local, forged, f.clock)
	if isUnknownRosterHead(err) {
		t.Fatalf("a tampered message was classified as a membership-ahead race: %v", err)
	}
	if !errors.Is(err, entmoot.ErrSigInvalid) {
		t.Fatalf("tampered message rejected as %v, want ErrSigInvalid", err)
	}

	// A genuinely signed non-member still names a head we do not have: that is
	// the race the buffer exists for, and membership can only be checked once
	// the checkpoint arrives.
	honest := f.signedAt(t, stranger, invented, "honest but unknown to us")
	if err := VerifyLiveMessage(f.local, honest, f.clock); !isUnknownRosterHead(err) {
		t.Fatalf("a signed message naming an unknown head was rejected as %v", err)
	}
}

// A membership sync adopts every checkpoint and record it fetched before the
// drain runs, so by then a held message can name a checkpoint that is no
// longer canonical. It is authentic history, authorized at the checkpoint it
// names; dropping it loses the message the buffer existed to save.
func TestHeldMessageSurvivesABatchedMembershipAdvance(t *testing.T) {
	f := newQuarantineFixture(t)
	newcomer := mustIdentity(t)
	later := mustIdentity(t)

	// The publisher is one checkpoint ahead of us when it writes.
	f.admitAhead(t, newcomer)
	middle := f.checkpointAhead(t)
	message := f.signedAt(t, newcomer, middle.ID, "published at the middle checkpoint")
	if got := f.ingest(t, newcomer, message); got != pubsub.ValidationIgnore {
		t.Fatalf("validation of the ahead message = %v, want ignore so it waits", got)
	}

	// It moves on again, and further records are still pending there.
	f.admitAhead(t, later)
	newest := f.checkpointAhead(t)
	trailing := []membership.Record{
		f.admitAhead(t, mustIdentity(t)),
		f.admitAhead(t, mustIdentity(t)),
		f.admitAhead(t, mustIdentity(t)),
	}

	// Our sync applies both checkpoints and every trailing record at once, so
	// the held message's checkpoint is known but no longer canonical.
	f.syncLocal(t, []membership.Checkpoint{middle, newest}, trailing)
	if f.local.Canonical().ID == middle.ID {
		t.Fatal("the batched advance did not move the canonical checkpoint")
	}
	ingested, dropped := f.group.DrainQuarantine(context.Background())
	if ingested != 1 || dropped != 0 {
		t.Fatalf("batched advance drained ingested=%d dropped=%d, want 1 and 0", ingested, dropped)
	}
	if has, err := f.store.Has(context.Background(), f.groupID, message.ID); err != nil || !has {
		t.Fatalf("held message was lost to a batched advance: has=%t err=%v", has, err)
	}
}

// Held messages are delivered in arrival order. Map iteration over the held
// heads is random, so without an explicit ordering a drain reorders a
// conversation that arrived while membership was catching up.
func TestDrainDeliversHeldMessagesInArrivalOrder(t *testing.T) {
	f := newQuarantineFixture(t)
	newcomer := mustIdentity(t)
	f.admitAhead(t, newcomer)

	var checkpoints []membership.Checkpoint
	var contents []string
	for step := range 6 {
		checkpoint := f.checkpointAhead(t)
		checkpoints = append(checkpoints, checkpoint)
		// Each message names a different checkpoint, so they land in separate
		// buckets: exactly the case map iteration would shuffle.
		f.clock = f.clock.Add(time.Millisecond)
		content := "step " + string(rune('a'+step))
		if got := f.ingest(t, newcomer, f.signedAt(t, newcomer, checkpoint.ID, content)); got != pubsub.ValidationIgnore {
			t.Fatalf("message %q validated as %v, want ignore so it waits", content, got)
		}
		contents = append(contents, content)
		f.admitAhead(t, mustIdentity(t))
	}

	f.syncLocal(t, checkpoints, nil)
	f.ingested = nil
	if ingested, dropped := f.group.DrainQuarantine(context.Background()); ingested != len(contents) || dropped != 0 {
		t.Fatalf("drained ingested=%d dropped=%d, want %d and 0", ingested, dropped, len(contents))
	}
	if len(f.ingested) != len(contents) {
		t.Fatalf("ingest callbacks = %d, want %d", len(f.ingested), len(contents))
	}
	for i, want := range contents {
		if got := string(f.ingested[i].Content); got != want {
			t.Fatalf("position %d delivered %q, want %q", i, got, want)
		}
	}
}
