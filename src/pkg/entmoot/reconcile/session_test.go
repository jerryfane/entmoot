package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"entmoot/pkg/entmoot"
)

// randomIDs returns n random-looking MessageIDs derived from the given seed
// and prefix. Deterministic for a given (seed, prefix, n) triple.
func randomIDs(seed int64, prefix string, n int) []entmoot.MessageID {
	r := rand.New(rand.NewSource(seed))
	out := make([]entmoot.MessageID, n)
	for i := range out {
		buf := make([]byte, 16)
		r.Read(buf)
		h := sha256.Sum256(append([]byte(prefix), buf...))
		out[i] = entmoot.MessageID(h)
	}
	return out
}

// driveToConvergence alternates calls to a.Next / b.Next until both sides
// report done or a round cap is reached. a starts with initiatorFrames as
// its first outgoing frame; the loop feeds a's output into b and vice
// versa. Returns the cumulative missing sets observed by each side.
func driveToConvergence(t *testing.T, a, b *Session, initiatorFrames []Range) (
	aMissing, bMissing []entmoot.MessageID, rounds int,
) {
	t.Helper()
	ctx := context.Background()
	// First hand-off: a has already emitted initiatorFrames; deliver them to b.
	aOut := initiatorFrames
	var bOut []Range
	const hardCap = 32 // independent safety net beyond each session's MaxRounds
	for i := 0; i < hardCap; i++ {
		rounds++
		if len(aOut) > 0 || !b.Done() {
			out, missing, _, err := b.Next(ctx, aOut)
			if err != nil {
				t.Fatalf("round %d: b.Next: %v", rounds, err)
			}
			bMissing = append(bMissing, missing...)
			bOut = out
		} else {
			bOut = nil
		}

		if len(bOut) > 0 || !a.Done() {
			out, missing, _, err := a.Next(ctx, bOut)
			if err != nil {
				t.Fatalf("round %d: a.Next: %v", rounds, err)
			}
			aMissing = append(aMissing, missing...)
			aOut = out
		} else {
			aOut = nil
		}

		if a.Done() && b.Done() {
			return aMissing, bMissing, rounds
		}
	}
	t.Fatalf("driveToConvergence hit hard cap %d without converging", hardCap)
	return
}

// newSessionPair wires up an initiator/responder pair over the two given
// storages and returns them along with the initiator's opening frame.
func newSessionPair(t *testing.T, cfg Config, as, bs Storage) (*Session, *Session, []Range) {
	t.Helper()
	a, initFrame, err := NewInitiator(cfg, as)
	if err != nil {
		t.Fatalf("NewInitiator: %v", err)
	}
	b := NewResponder(cfg, bs)
	return a, b, initFrame
}

// assertMissingSet asserts that got (any order) equals want as sets of
// MessageID.
func assertMissingSet(t *testing.T, label string, got, want []entmoot.MessageID) {
	t.Helper()
	gset := make(map[entmoot.MessageID]struct{}, len(got))
	for _, id := range got {
		gset[id] = struct{}{}
	}
	wset := make(map[entmoot.MessageID]struct{}, len(want))
	for _, id := range want {
		wset[id] = struct{}{}
	}
	if len(gset) != len(wset) {
		t.Fatalf("%s: size differs: got %d want %d", label, len(gset), len(wset))
	}
	for id := range wset {
		if _, ok := gset[id]; !ok {
			t.Fatalf("%s: missing expected id %s", label, id)
		}
	}
	for id := range gset {
		if _, ok := wset[id]; !ok {
			t.Fatalf("%s: unexpected extra id %s", label, id)
		}
	}
}

func TestSessionConvergeNoDiff(t *testing.T) {
	ids := randomIDs(1, "eq", 20)
	as := newMemStorage()
	bs := newMemStorage()
	as.addMany(ids)
	bs.addMany(ids)

	a, b, initFrame := newSessionPair(t, DefaultConfig(), as, bs)
	aMissing, bMissing, rounds := driveToConvergence(t, a, b, initFrame)

	assertMissingSet(t, "a", aMissing, nil)
	assertMissingSet(t, "b", bMissing, nil)
	if rounds > 2 {
		t.Fatalf("no-diff converged in %d rounds, want <= 2", rounds)
	}
}

func TestSessionConvergeGapInMiddle(t *testing.T) {
	shared := randomIDs(2, "shared", 10)
	// Extra ids engineered to sort in the middle of the byte order so
	// the gap is not at either edge of the keyspace.
	extras := []entmoot.MessageID{
		idWithPrefix(0x70, 'a'),
		idWithPrefix(0x80, 'b'),
	}

	as := newMemStorage()
	bs := newMemStorage()
	as.addMany(shared)
	bs.addMany(shared)
	as.addMany(extras)

	a, b, initFrame := newSessionPair(t, DefaultConfig(), as, bs)
	aMissing, bMissing, rounds := driveToConvergence(t, a, b, initFrame)

	assertMissingSet(t, "a", aMissing, nil)
	assertMissingSet(t, "b", bMissing, extras)
	if rounds > 5 {
		t.Fatalf("middle-gap converged in %d rounds, want <= 5", rounds)
	}
}

func TestSessionConvergeGapAtEdge(t *testing.T) {
	shared := randomIDs(3, "sharedTail", 10)
	extras := []entmoot.MessageID{
		idWithPrefix(0xFD, 'x'),
		idWithPrefix(0xFE, 'y'),
		idWithPrefix(0xFF, 'z'),
	}

	as := newMemStorage()
	bs := newMemStorage()
	as.addMany(shared)
	bs.addMany(shared)
	as.addMany(extras)

	a, b, initFrame := newSessionPair(t, DefaultConfig(), as, bs)
	aMissing, bMissing, _ := driveToConvergence(t, a, b, initFrame)

	assertMissingSet(t, "a", aMissing, nil)
	assertMissingSet(t, "b", bMissing, extras)
}

func TestSessionConvergePeerAhead(t *testing.T) {
	shared := randomIDs(4, "peerAhead", 10)
	extras := randomIDs(5, "peerAhead-extras", 20)

	as := newMemStorage()
	bs := newMemStorage()
	as.addMany(shared)
	bs.addMany(shared)
	bs.addMany(extras)

	a, b, initFrame := newSessionPair(t, DefaultConfig(), as, bs)
	aMissing, bMissing, _ := driveToConvergence(t, a, b, initFrame)

	assertMissingSet(t, "a", aMissing, extras)
	assertMissingSet(t, "b", bMissing, nil)
}

func TestSessionConvergePeerBehind(t *testing.T) {
	shared := randomIDs(6, "peerBehind", 10)
	extras := randomIDs(7, "peerBehind-extras", 20)

	as := newMemStorage()
	bs := newMemStorage()
	as.addMany(shared)
	as.addMany(extras)
	bs.addMany(shared)

	a, b, initFrame := newSessionPair(t, DefaultConfig(), as, bs)
	aMissing, bMissing, _ := driveToConvergence(t, a, b, initFrame)

	assertMissingSet(t, "a", aMissing, nil)
	assertMissingSet(t, "b", bMissing, extras)
}

func TestSessionConcurrentInsert(t *testing.T) {
	shared := randomIDs(8, "concurrentInsert", 30)
	as := newMemStorage()
	bs := newMemStorage()
	as.addMany(shared)
	bs.addMany(shared)

	a, b, initFrame := newSessionPair(t, DefaultConfig(), as, bs)

	// Drive one synchronous round, then insert a new id into A mid-run.
	ctx := context.Background()
	aOut := initFrame
	bOut, _, _, err := b.Next(ctx, aOut)
	if err != nil {
		t.Fatalf("first b.Next: %v", err)
	}

	late := idWithPrefix(0x42, 'L')
	as.add(late)

	// Continue the drive loop.
	const hardCap = 32
	for i := 0; i < hardCap; i++ {
		aOut2, _, _, err := a.Next(ctx, bOut)
		if err != nil {
			t.Fatalf("round %d: a.Next: %v", i, err)
		}
		bOut2, _, _, err := b.Next(ctx, aOut2)
		if err != nil {
			t.Fatalf("round %d: b.Next: %v", i, err)
		}
		bOut = bOut2
		if a.Done() && b.Done() {
			return
		}
	}
	t.Fatalf("concurrent-insert session did not converge within %d rounds", hardCap)
}

func TestSessionMaxRounds(t *testing.T) {
	// Craft a pathological case: fanout=2, leaf threshold=1, and a big
	// diff so the split tree is tall. Cap rounds at 3.
	cfg := Config{LeafThreshold: 1, MaxRounds: 3, FanoutPerRound: 2}

	as := newMemStorage()
	bs := newMemStorage()
	bulkA := randomIDs(9, "maxRoundsA", 500)
	bulkB := randomIDs(10, "maxRoundsB", 500)
	as.addMany(bulkA)
	bs.addMany(bulkB)

	a, initFrame, err := NewInitiator(cfg, as)
	if err != nil {
		t.Fatalf("NewInitiator: %v", err)
	}
	b := NewResponder(cfg, bs)

	ctx := context.Background()
	aOut := initFrame
	var bOut []Range
	for i := 0; i < 20; i++ {
		out, _, _, err := b.Next(ctx, aOut)
		if errors.Is(err, ErrMaxRounds) {
			return
		}
		if err != nil {
			t.Fatalf("b.Next: %v", err)
		}
		bOut = out
		out, _, _, err = a.Next(ctx, bOut)
		if errors.Is(err, ErrMaxRounds) {
			return
		}
		if err != nil {
			t.Fatalf("a.Next: %v", err)
		}
		aOut = out
	}
	t.Fatalf("expected ErrMaxRounds under pathological split, never observed")
}

// idWithPrefix builds a deterministic MessageID whose first byte is
// prefix and whose remaining bytes are derived from a tag for uniqueness.
// The resulting ids sort by their first byte, which makes it easy to place
// "middle of keyspace" or "tail of keyspace" test fixtures.
func idWithPrefix(prefix byte, tag byte) entmoot.MessageID {
	h := sha256.Sum256([]byte{prefix, tag})
	var id entmoot.MessageID
	id[0] = prefix
	copy(id[1:], h[1:])
	return id
}

// fmtIDs turns a slice of ids into a readable list for failure messages.
func fmtIDs(ids []entmoot.MessageID) string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	return fmt.Sprintf("%v", strs)
}

var _ = fmtIDs
