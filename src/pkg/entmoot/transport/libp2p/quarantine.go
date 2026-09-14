package libp2ptransport

import (
	"context"
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

// take removes and returns every held message whose head the roster now holds.
// Messages for heads that are still unknown stay until they expire.
func (q *rosterAheadQuarantine) take(known func(entmoot.RosterEntryID) bool) []entmoot.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.expireLocked()
	var ready []entmoot.Message
	for head, held := range q.heads {
		if !known(head) {
			continue
		}
		for _, item := range held {
			ready = append(ready, item.message)
			delete(q.byID, item.message.ID)
			q.count--
		}
		delete(q.heads, head)
	}
	return ready
}

func (q *rosterAheadQuarantine) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// DrainQuarantine re-validates messages held for a roster head this node did
// not have, stores the ones the synchronized roster now authorizes, and emits
// them locally. It returns how many were ingested. Callers run it after a
// roster synchronization; messages whose head is still unknown keep waiting
// until they expire.
func (g *LiveGroup) DrainQuarantine(ctx context.Context) int {
	if g == nil || g.quarantine == nil {
		return 0
	}
	ready := g.quarantine.take(g.cfg.Roster.HasEntry)
	ingested := 0
	for _, message := range ready {
		// The roster moved on since the message arrived, so re-run the full
		// live check rather than trusting the earlier partial result.
		if err := VerifyLiveMessage(g.cfg.Roster, message, g.now()); err != nil {
			continue
		}
		if g.cfg.Authorize != nil {
			if err := g.cfg.Authorize(message); err != nil {
				continue
			}
		}
		inserted, err := g.cfg.Store.Put(ctx, g.cfg.GroupID, message)
		if err != nil || !inserted {
			continue
		}
		ingested++
		if g.cfg.OnIngest != nil {
			g.cfg.OnIngest(message)
		}
	}
	return ingested
}

// QuarantinedMessages reports how many roster-ahead messages are waiting. It
// exists so status output can show a synchronization gap instead of silence.
func (g *LiveGroup) QuarantinedMessages() int {
	if g == nil || g.quarantine == nil {
		return 0
	}
	return g.quarantine.len()
}
