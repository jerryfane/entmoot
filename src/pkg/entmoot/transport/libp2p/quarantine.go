package libp2ptransport

import (
	"context"
	"sort"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
)

const (
	// maxQuarantinedMessages bounds how many roster-ahead messages one group
	// holds while it catches up. A publisher that is ahead of us is a normal
	// race, but it must never become a way to make a receiver buffer without
	// limit.
	maxQuarantinedMessages = 64
	// maxQuarantinedHeads bounds how many distinct unknown heads are held, so
	// fabricated heads cannot fan the buffer out.
	maxQuarantinedHeads = 8
	// quarantineTTL bounds how long a held message waits for the roster entry
	// that would authorize it. Roster sync runs every two seconds, so a head
	// still unknown after this is not a race.
	quarantineTTL = 2 * time.Minute
)

// quarantinedMessage is one live message whose roster head this node does not
// hold yet, kept with the time it arrived so it can expire.
type quarantinedMessage struct {
	message  entmoot.Message
	received time.Time
}

// rosterAheadQuarantine holds live messages that name a roster head this node
// has not synchronized yet. Rejecting them outright loses messages whenever a
// membership change and a publish race, and penalises the sender's GossipSub
// score for being correct; holding them unbounded would be a memory sink. The
// buffer is bounded in messages, distinct heads and time, and is drained after
// every roster synchronization.
type rosterAheadQuarantine struct {
	mu    sync.Mutex
	byID  map[entmoot.MessageID]struct{}
	heads map[entmoot.RosterEntryID][]quarantinedMessage
	count int
	now   func() time.Time
}

func newRosterAheadQuarantine(now func() time.Time) *rosterAheadQuarantine {
	if now == nil {
		now = time.Now
	}
	return &rosterAheadQuarantine{
		byID:  make(map[entmoot.MessageID]struct{}),
		heads: make(map[entmoot.RosterEntryID][]quarantinedMessage),
		now:   now,
	}
}

// hold stores a message for later retry. It reports false when the message is
// already held or a bound is reached, so the caller can tell "kept for retry"
// from "dropped".
func (q *rosterAheadQuarantine) hold(message entmoot.Message) bool {
	if message.RosterHead == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.expireLocked()
	if _, held := q.byID[message.ID]; held {
		return false
	}
	head := *message.RosterHead
	if _, known := q.heads[head]; !known && len(q.heads) >= maxQuarantinedHeads {
		return false
	}
	if q.count >= maxQuarantinedMessages {
		return false
	}
	q.heads[head] = append(q.heads[head], quarantinedMessage{message: message, received: q.now()})
	q.byID[message.ID] = struct{}{}
	q.count++
	return true
}

// expireLocked drops messages whose head stayed unknown past the TTL. q.mu must
// be held.
func (q *rosterAheadQuarantine) expireLocked() {
	cutoff := q.now().Add(-quarantineTTL)
	for head, held := range q.heads {
		kept := held[:0]
		for _, item := range held {
			if item.received.Before(cutoff) {
				delete(q.byID, item.message.ID)
				q.count--
				continue
			}
			kept = append(kept, item)
		}
		if len(kept) == 0 {
			delete(q.heads, head)
			continue
		}
		q.heads[head] = kept
	}
}

// take removes and returns every held message whose head the roster now holds,
// oldest arrival first so a drain does not reorder what a publisher sent.
// Messages for heads that are still unknown stay until they expire.
func (q *rosterAheadQuarantine) take(known func(entmoot.RosterEntryID) bool) []entmoot.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.expireLocked()
	var ready []quarantinedMessage
	for head, held := range q.heads {
		if !known(head) {
			continue
		}
		for _, item := range held {
			ready = append(ready, item)
			delete(q.byID, item.message.ID)
			q.count--
		}
		delete(q.heads, head)
	}
	sort.SliceStable(ready, func(i, j int) bool {
		return ready[i].received.Before(ready[j].received)
	})
	out := make([]entmoot.Message, 0, len(ready))
	for _, item := range ready {
		out = append(out, item.message)
	}
	return out
}

// len reports how many messages are waiting, expiring stale entries first so a
// status reader never sees a gap that has already timed out.
func (q *rosterAheadQuarantine) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.expireLocked()
	return q.count
}

// DrainQuarantine re-validates messages held for a roster head this node did
// not have and stores the ones the synchronized roster authorizes. It returns
// how many were ingested and how many were dropped as unauthorized. Callers
// run it after a roster synchronization; messages whose head is still unknown
// keep waiting until they expire.
//
// A membership sync can adopt a checkpoint and several records at once, so by
// drain time a held message may name a checkpoint that is no longer canonical.
// It is still authentic history, authorised at the checkpoint it names, so it
// is verified as historical rather than discarded for being late.
func (g *LiveGroup) DrainQuarantine(ctx context.Context) (ingested int, dropped int) {
	if g == nil || g.quarantine == nil {
		return 0, 0
	}
	ready := g.quarantine.take(g.known)
	for _, message := range ready {
		// Membership moved on since the message arrived, so re-run the full
		// check rather than trusting the earlier partial result.
		if err := g.authorizeDrained(message); err != nil {
			dropped++
			continue
		}
		inserted, err := g.cfg.Store.Put(ctx, g.cfg.GroupID, message)
		if err != nil {
			dropped++
			continue
		}
		if !inserted {
			continue
		}
		ingested++
		if g.cfg.OnIngest != nil {
			g.cfg.OnIngest(message)
		}
	}
	return ingested, dropped
}

// authorizeDrained applies the live rule when the held message still names the
// canonical checkpoint, and the historical rule once membership has moved on. A
// message is never accepted on weaker grounds than it would have been at
// arrival: both paths verify author authority at the named head and the author
// signature.
// known reports whether this node can now place a cited head: either a
// checkpoint it holds, or a record it holds, which is what the live rule needs
// to tell a synchronisation gap from a fabricated head.
func (g *LiveGroup) known(head entmoot.RosterEntryID) bool {
	return g.cfg.Group.HasCheckpoint(head) || g.cfg.Group.HasRecord(head)
}

func (g *LiveGroup) authorizeDrained(message entmoot.Message) error {
	if message.RosterHead != nil && *message.RosterHead != g.cfg.Group.Canonical().ID {
		if err := VerifyHistoricalMessage(g.cfg.Group, message, g.now()); err != nil {
			return err
		}
	} else if err := VerifyLiveMessage(g.cfg.Group, message, g.now()); err != nil {
		return err
	}
	if g.cfg.Authorize != nil {
		return g.cfg.Authorize(message)
	}
	return nil
}

// QuarantinedMessages reports how many roster-ahead messages are waiting. It
// exists so status output can show a synchronization gap instead of silence.
func (g *LiveGroup) QuarantinedMessages() int {
	if g == nil || g.quarantine == nil {
		return 0
	}
	return g.quarantine.len()
}
