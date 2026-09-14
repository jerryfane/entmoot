package libp2ptransport

import (
	"context"
	"errors"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// quarantineFixture is a live group with no router: the quarantine, the drain
// and the roster are exercised directly, so the test does not depend on
// GossipSub timing.
type quarantineFixture struct {
	group    *LiveGroup
	log      *roster.RosterLog
	founder  *keystore.Identity
	member   *keystore.Identity
	groupID  entmoot.GroupID
	store    store.MessageStore
	ingested []entmoot.Message
	clock    time.Time
}

func newQuarantineFixture(t *testing.T) *quarantineFixture {
	t.Helper()
	f := &quarantineFixture{
		founder: mustIdentity(t),
		member:  mustIdentity(t),
		groupID: entmoot.GroupID{0x5a},
		store:   store.NewMemory(),
		clock:   time.UnixMilli(20_000),
	}
	t.Cleanup(func() { f.store.Close() })
	f.log = roster.New(f.groupID)
	if err := f.log.Genesis(f.founder, mustNodeInfo(t, f.founder.PublicKey), 1_000); err != nil {
		t.Fatal(err)
	}
	cfg := LiveConfig{
		GroupID: f.groupID,
		Roster:  f.log,
		Store:   f.store,
		Now:     func() time.Time { return f.clock },
		OnIngest: func(message entmoot.Message) {
			f.ingested = append(f.ingested, message)
		},
	}
	f.group = &LiveGroup{cfg: cfg, quarantine: newRosterAheadQuarantine(cfg.Now)}
	return f
}

// admit adds an identity to the roster, advancing the head.
func (f *quarantineFixture) admit(t *testing.T, identity *keystore.Identity, timestamp int64) entmoot.NodeInfo {
	t.Helper()
	subject := mustNodeInfo(t, identity.PublicKey)
	entry, err := f.log.SignEntry(f.founder, "add", subject, nil, timestamp)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.log.Apply(entry); err != nil {
		t.Fatal(err)
	}
	return subject
}

// signedAt signs a message naming an explicit roster head, which is how a
// publisher whose roster is ahead of ours looks on the wire.
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

// A membership change and a publish race constantly: the publisher's roster
// moves first, so its message names a head the receiver has not synchronized.
// Dropping it loses the message; holding and retrying delivers it.
func TestRosterAheadMessageIsHeldAndIngestedAfterRosterSync(t *testing.T) {
	f := newQuarantineFixture(t)
	newcomer := mustIdentity(t)

	// Build the entry the publisher already has, without applying it locally.
	ahead := roster.New(f.groupID)
	if err := ahead.Genesis(f.founder, mustNodeInfo(t, f.founder.PublicKey), 1_000); err != nil {
		t.Fatal(err)
	}
	addNewcomer, err := ahead.SignEntry(f.founder, "add", mustNodeInfo(t, newcomer.PublicKey), nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := ahead.Apply(addNewcomer); err != nil {
		t.Fatal(err)
	}
	aheadHead := ahead.Head()
	if f.log.HasEntry(aheadHead) {
		t.Fatal("fixture roster already knows the ahead head")
	}

	message := f.signedAt(t, newcomer, aheadHead, "published while ahead")
	err = VerifyLiveMessage(f.log, message, f.clock)
	if err == nil {
		t.Fatal("a message naming an unknown head verified")
	}
	if !isUnknownRosterHead(err) {
		t.Fatalf("unknown head reported as %v, want ErrRosterHeadUnknown", err)
	}
	if !f.group.quarantine.hold(message) {
		t.Fatal("message was not held for retry")
	}
	if f.group.QuarantinedMessages() != 1 {
		t.Fatalf("quarantined = %d, want 1", f.group.QuarantinedMessages())
	}
	// Draining before the roster catches up must keep the message waiting.
	if ingested, dropped := f.group.DrainQuarantine(context.Background()); ingested != 0 || dropped != 0 {
		t.Fatalf("drained %d and dropped %d before the roster caught up", ingested, dropped)
	}
	if f.group.QuarantinedMessages() != 1 {
		t.Fatal("a still-unknown head stopped waiting")
	}

	// Roster sync brings in the entry the publisher already had.
	if err := f.log.Apply(addNewcomer); err != nil {
		t.Fatal(err)
	}
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
	member := f.admit(t, f.member, 2_000)
	_ = member

	held := 0
	for i := 0; i < maxQuarantinedHeads+4; i++ {
		var head entmoot.RosterEntryID
		head[0] = byte(i + 1)
		message := f.signedAt(t, f.member, head, "invented head")
		if f.group.quarantine.hold(message) {
			held++
		}
	}
	if held != maxQuarantinedHeads {
		t.Fatalf("held %d distinct heads, want the cap of %d", held, maxQuarantinedHeads)
	}

	// Same head, many messages: the message cap applies.
	var single entmoot.RosterEntryID
	single[0] = 1
	for i := 0; i < maxQuarantinedMessages+16; i++ {
		f.clock = f.clock.Add(time.Millisecond)
		f.group.quarantine.hold(f.signedAt(t, f.member, single, "flood"))
	}
	if count := f.group.QuarantinedMessages(); count != maxQuarantinedMessages {
		t.Fatalf("quarantined %d messages, want the cap of %d", count, maxQuarantinedMessages)
	}

	// A duplicate of a held message must not consume another slot. The buffer
	// is at its message cap, so make room first: without the dedup check the
	// re-offered message would take the freed slot and the count would climb
	// back to the cap.
	f.clock = f.clock.Add(quarantineTTL + time.Second)
	if count := f.group.QuarantinedMessages(); count != 0 {
		t.Fatalf("expired messages still held: %d", count)
	}
	f.clock = f.clock.Add(time.Millisecond)
	first := f.signedAt(t, f.member, single, "only message")
	if !f.group.quarantine.hold(first) {
		t.Fatal("a message was refused by an empty quarantine")
	}
	if f.group.quarantine.hold(first) {
		t.Fatal("the same message was held twice")
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

// Only an unknown head earns a hold. A message from a non-member, or naming a
// head we know is superseded, is refused outright.
func TestQuarantineDoesNotCoverInvalidMessages(t *testing.T) {
	f := newQuarantineFixture(t)
	stranger := mustIdentity(t)
	head := f.log.Head()

	strangerMessage := f.signedAt(t, stranger, head, "not a member")
	if err := VerifyLiveMessage(f.log, strangerMessage, f.clock); err == nil {
		t.Fatal("a non-member message verified")
	} else if isUnknownRosterHead(err) {
		t.Fatalf("non-member rejection was classified as an unknown head: %v", err)
	}

	f.admit(t, f.member, 2_000)
	staleMessage := f.signedAt(t, f.member, head, "stale head")
	if err := VerifyLiveMessage(f.log, staleMessage, f.clock); err == nil {
		t.Fatal("a message naming a superseded head verified")
	} else if isUnknownRosterHead(err) {
		t.Fatalf("superseded head was classified as unknown: %v", err)
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
	err := VerifyLiveMessage(f.log, forged, f.clock)
	if isUnknownRosterHead(err) {
		t.Fatalf("a tampered message was classified as a roster-ahead race: %v", err)
	}
	if !errors.Is(err, entmoot.ErrSigInvalid) {
		t.Fatalf("tampered message rejected as %v, want ErrSigInvalid", err)
	}

	// A genuinely signed non-member still names a head we do not have: that is
	// the race the buffer exists for, and membership can only be checked once
	// the head arrives.
	honest := f.signedAt(t, stranger, invented, "honest but unknown to us")
	if err := VerifyLiveMessage(f.log, honest, f.clock); !isUnknownRosterHead(err) {
		t.Fatalf("a signed message naming an unknown head was rejected as %v", err)
	}
}

// A roster sync applies every entry it fetched before the drain runs, so by
// then a held message can name a head that is already superseded. It is
// authentic history authorized at the head it names; dropping it loses the
// message the buffer existed to save.
func TestHeldMessageSurvivesABatchedRosterAdvance(t *testing.T) {
	f := newQuarantineFixture(t)
	newcomer := mustIdentity(t)
	later := mustIdentity(t)

	// The publisher's roster is one entry ahead of ours.
	ahead := roster.New(f.groupID)
	if err := ahead.Genesis(f.founder, mustNodeInfo(t, f.founder.PublicKey), 1_000); err != nil {
		t.Fatal(err)
	}
	addNewcomer, err := ahead.SignEntry(f.founder, "add", mustNodeInfo(t, newcomer.PublicKey), nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := ahead.Apply(addNewcomer); err != nil {
		t.Fatal(err)
	}
	addLater, err := ahead.SignEntry(f.founder, "add", mustNodeInfo(t, later.PublicKey), nil, 3_000)
	if err != nil {
		t.Fatal(err)
	}

	message := f.signedAt(t, newcomer, ahead.Head(), "published at the middle head")
	if !f.group.quarantine.hold(message) {
		t.Fatal("message was not held")
	}

	// Our sync pulls both entries at once, so the held message's head is known
	// but no longer current when the drain runs.
	if err := f.log.Apply(addNewcomer); err != nil {
		t.Fatal(err)
	}
	if err := ahead.Apply(addLater); err != nil {
		t.Fatal(err)
	}
	if err := f.log.Apply(addLater); err != nil {
		t.Fatal(err)
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
// conversation that arrived while the roster was catching up.
func TestDrainDeliversHeldMessagesInArrivalOrder(t *testing.T) {
	f := newQuarantineFixture(t)
	newcomer := mustIdentity(t)

	ahead := roster.New(f.groupID)
	if err := ahead.Genesis(f.founder, mustNodeInfo(t, f.founder.PublicKey), 1_000); err != nil {
		t.Fatal(err)
	}
	var contents []string
	for step := 0; step < 6; step++ {
		subject := mustNodeInfo(t, mustIdentity(t).PublicKey)
		if step == 0 {
			subject = mustNodeInfo(t, newcomer.PublicKey)
		}
		entry, err := ahead.SignEntry(f.founder, "add", subject, nil, int64(2_000+step))
		if err != nil {
			t.Fatal(err)
		}
		if err := ahead.Apply(entry); err != nil {
			t.Fatal(err)
		}
		if err := f.log.Apply(entry); err != nil {
			t.Fatal(err)
		}
		// Each message names a different head, so they land in separate
		// buckets: exactly the case map iteration would shuffle.
		f.clock = f.clock.Add(time.Millisecond)
		content := "step " + string(rune('a'+step))
		if !f.group.quarantine.hold(f.signedAt(t, newcomer, ahead.Head(), content)) {
			t.Fatalf("message %q was not held", content)
		}
		contents = append(contents, content)
	}

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
