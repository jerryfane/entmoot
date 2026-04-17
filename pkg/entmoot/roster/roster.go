// Package roster maintains the signed append-only membership log for a group.
//
// Each group has a single RosterLog; the log's head is the group's current
// membership. v0 is founder-only admin: only entries signed by the founder are
// accepted by Apply. The log is strictly linear in v0 — the single-admin
// constraint means branches cannot arise — so Head() always refers to the
// most-recently-applied entry.
//
// This package is independent of pkg/entmoot/store. Persistence is provided by
// a parallel JSONL implementation (see jsonl.go) that deliberately does not
// share code with the store package even though the two follow the same
// append-only, base64url-named pattern on disk.
package roster

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// subscribeBufferCap is the per-subscriber channel buffer size. Slow readers
// drop events once the buffer fills; see Subscribe.
const subscribeBufferCap = 16

// RosterEvent is emitted on state-changing Apply calls — that is, on Genesis
// and on every subsequent Apply that succeeds.
type RosterEvent struct {
	// Entry is the roster entry that was just applied.
	Entry entmoot.RosterEntry
	// Heads is the new set of log heads after the apply. In v0 this is
	// always a single-element slice because founder-only admin is linear.
	Heads []entmoot.RosterEntryID
}

// RosterLog is the in-memory projection of a single group's signed roster log.
//
// Concurrency: Apply is NOT safe for concurrent use (the caller must serialize
// mutating calls). Read methods (IsMember, Members, MemberInfo, Head, Founder)
// are safe for concurrent use; they take the read lock.
type RosterLog struct {
	groupID entmoot.GroupID

	mu sync.RWMutex

	// entries are all applied entries in apply order.
	entries []entmoot.RosterEntry
	// byID indexes entries by id for quick lookup.
	byID map[entmoot.RosterEntryID]int
	// members is the current membership projection.
	members map[entmoot.NodeID]entmoot.NodeInfo
	// founder is set on Genesis; empty until then.
	founder entmoot.NodeInfo
	// head is the id of the most-recently-applied entry; zero on empty log.
	head entmoot.RosterEntryID

	// sinks is the set of active subscribers; guarded by subsMu.
	subsMu sync.Mutex
	sinks  map[*subscriber]struct{}

	// persist is called after a successful Apply (including the Genesis
	// entry). In-memory logs leave this nil. JSONL logs set it to append
	// the entry to disk.
	persist func(entmoot.RosterEntry) error

	// logger is used for subscribe-drop warnings and malformed-line skips
	// (the latter only from the JSONL loader path).
	logger *slog.Logger

	// closeOnce guards Close so the file-handle teardown only runs once.
	closeOnce sync.Once
	// closeFn is set by OpenJSONL; nil for in-memory logs.
	closeFn func() error
}

// subscriber is one live subscription. cancel is idempotent.
type subscriber struct {
	ch         chan RosterEvent
	cancelOnce sync.Once
	cancelled  chan struct{}
}

// New constructs an empty in-memory RosterLog for the given group.
//
// The returned log is NOT yet valid for queries (Members is empty, Head is
// the zero id) until Genesis is called or entries are Applied. The zero
// GroupID is accepted; it is the caller's responsibility to pass a real id.
func New(groupID entmoot.GroupID) *RosterLog {
	return &RosterLog{
		groupID: groupID,
		byID:    make(map[entmoot.RosterEntryID]int),
		members: make(map[entmoot.NodeID]entmoot.NodeInfo),
		sinks:   make(map[*subscriber]struct{}),
		logger:  slog.Default(),
	}
}

// Genesis writes the founder's self-signed add(founder) entry. It must be
// called exactly once on an empty log; a second call returns an error and does
// not mutate the log. The supplied identity signs the entry; founderInfo is
// recorded as the entry's Subject and is the NodeInfo returned by later
// Founder() / MemberInfo() queries.
//
// Timestamp is supplied in unix milliseconds; zero and negative values are
// allowed (Genesis is the only entry that may carry any timestamp — later
// Apply calls require strict monotonic growth).
func (r *RosterLog) Genesis(founder *keystore.Identity, founderInfo entmoot.NodeInfo, timestampMillis int64) error {
	if founder == nil {
		return fmt.Errorf("roster: Genesis requires a non-nil identity")
	}
	if len(founderInfo.EntmootPubKey) == 0 {
		return fmt.Errorf("roster: Genesis requires founderInfo.EntmootPubKey")
	}

	r.mu.Lock()
	if len(r.entries) != 0 {
		r.mu.Unlock()
		return fmt.Errorf("roster: Genesis called on non-empty log")
	}
	r.mu.Unlock()

	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   founderInfo,
		Actor:     founderInfo.PilotNodeID,
		Timestamp: timestampMillis,
		Parents:   nil, // genesis has no parents
	}

	sigInput, err := canonical.Encode(entry)
	if err != nil {
		return fmt.Errorf("roster: canonical encode for signing: %w", err)
	}
	entry.Signature = founder.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)

	// Persist before updating in-memory state so a write failure leaves the
	// log untouched.
	if r.persist != nil {
		if err := r.persist(entry); err != nil {
			return fmt.Errorf("roster: persist genesis: %w", err)
		}
	}

	r.mu.Lock()
	r.founder = founderInfo
	r.applyLocked(entry)
	heads := []entmoot.RosterEntryID{r.head}
	r.mu.Unlock()

	r.emit(RosterEvent{Entry: entry, Heads: heads})
	return nil
}

// AcceptGenesis seeds an empty log from an already-signed genesis entry
// received from a peer (e.g., via RosterResp during Join). It verifies the
// entry's self-signature against the pubkey declared in Subject.EntmootPubKey
// and, on success, adopts the entry as the genesis.
//
// Returns a wrapped entmoot.ErrRosterReject if:
//   - the log is not empty,
//   - entry is not a well-formed genesis (must have Op="add",
//     len(Parents)==0, Subject.PilotNodeID==Actor),
//   - entry.ID does not match canonical.RosterEntryID of its signing form,
//   - signature verification fails.
//
// On success, emits a RosterEvent identical to what Genesis would have. This
// mirrors the JSONL loader's first-entry path: the founder is adopted from
// entry.Subject rather than being supplied separately by the caller.
func (r *RosterLog) AcceptGenesis(entry entmoot.RosterEntry) error {
	r.mu.RLock()
	empty := len(r.entries) == 0
	r.mu.RUnlock()
	if !empty {
		return fmt.Errorf("%w: AcceptGenesis on non-empty log", entmoot.ErrRosterReject)
	}

	// Well-formed genesis: add, no parents, Actor == Subject.PilotNodeID —
	// that's how we recognize a self-signed genesis vs. a normal add.
	if entry.Op != "add" {
		return fmt.Errorf("%w: genesis op must be \"add\", got %q",
			entmoot.ErrRosterReject, entry.Op)
	}
	if len(entry.Parents) != 0 {
		return fmt.Errorf("%w: genesis must have no parents",
			entmoot.ErrRosterReject)
	}
	if entry.Actor != entry.Subject.PilotNodeID {
		return fmt.Errorf("%w: genesis must be self-signed (actor %d != subject %d)",
			entmoot.ErrRosterReject, entry.Actor, entry.Subject.PilotNodeID)
	}
	if len(entry.Subject.EntmootPubKey) == 0 {
		return fmt.Errorf("%w: genesis subject has no pubkey",
			entmoot.ErrRosterReject)
	}

	// Verify the supplied id matches the canonical hash of the signing form.
	if canonical.RosterEntryID(entry) != entry.ID {
		return fmt.Errorf("%w: genesis entry id does not match canonical hash",
			entmoot.ErrRosterReject)
	}

	// Verify the self-signature against the subject's pubkey.
	signing := entry
	signing.ID = entmoot.RosterEntryID{}
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return fmt.Errorf("%w: canonical encode: %v", entmoot.ErrRosterReject, err)
	}
	if !keystore.Verify(entry.Subject.EntmootPubKey, sigInput, entry.Signature) {
		return fmt.Errorf("%w: genesis signature does not verify",
			entmoot.ErrRosterReject)
	}

	// Persist before updating in-memory state so a write failure leaves the
	// log untouched. Matches Genesis semantics.
	if r.persist != nil {
		if err := r.persist(entry); err != nil {
			return fmt.Errorf("roster: persist accepted genesis: %w", err)
		}
	}

	r.mu.Lock()
	// Re-check under the write lock in case a concurrent caller won the race.
	if len(r.entries) != 0 {
		r.mu.Unlock()
		return fmt.Errorf("%w: AcceptGenesis on non-empty log", entmoot.ErrRosterReject)
	}
	r.founder = entry.Subject
	r.applyLocked(entry)
	heads := []entmoot.RosterEntryID{r.head}
	r.mu.Unlock()

	r.emit(RosterEvent{Entry: entry, Heads: heads})
	return nil
}

// Apply validates and appends a single entry. On any validation failure the
// returned error wraps entmoot.ErrRosterReject and the log is unchanged.
//
// Validation (v0):
//   - log must already have a genesis entry (founder recorded),
//   - Op must be one of "add", "remove", or "policy_change",
//   - Actor must equal the founder's PilotNodeID,
//   - Signature must verify against the founder's EntmootPubKey,
//   - Entry.ID must equal canonical.RosterEntryID of the entry with id/sig
//     zeroed,
//   - Entry.Timestamp must be strictly greater than the current head's
//     timestamp (monotonicity),
//   - Parents must reference the current head (v0 linear log).
//
// Apply does NOT check wire-layer replay windows — that is the caller's job.
func (r *RosterLog) Apply(entry entmoot.RosterEntry) error {
	if err := r.validate(entry); err != nil {
		return err
	}

	if r.persist != nil {
		if err := r.persist(entry); err != nil {
			return fmt.Errorf("roster: persist: %w", err)
		}
	}

	r.mu.Lock()
	r.applyLocked(entry)
	heads := []entmoot.RosterEntryID{r.head}
	r.mu.Unlock()

	r.emit(RosterEvent{Entry: entry, Heads: heads})
	return nil
}

// validate performs every check required to accept entry. It returns a wrapped
// entmoot.ErrRosterReject on any failure so callers can errors.Is against it.
func (r *RosterLog) validate(entry entmoot.RosterEntry) error {
	r.mu.RLock()
	founder := r.founder
	head := r.head
	var headTimestamp int64
	if len(r.entries) > 0 {
		headTimestamp = r.entries[len(r.entries)-1].Timestamp
	}
	empty := len(r.entries) == 0
	r.mu.RUnlock()

	if empty {
		return fmt.Errorf("%w: Apply on empty log; call Genesis first", entmoot.ErrRosterReject)
	}

	switch entry.Op {
	case "add", "remove", "policy_change":
	default:
		return fmt.Errorf("%w: invalid op %q", entmoot.ErrRosterReject, entry.Op)
	}

	if entry.Actor != founder.PilotNodeID {
		return fmt.Errorf("%w: actor %d is not founder %d",
			entmoot.ErrRosterReject, entry.Actor, founder.PilotNodeID)
	}

	// Verify the id the caller supplied matches what we would compute.
	if canonical.RosterEntryID(entry) != entry.ID {
		return fmt.Errorf("%w: entry id does not match canonical hash",
			entmoot.ErrRosterReject)
	}

	// Verify the signature against the founder's pubkey using the signing
	// form (id/sig zeroed).
	signing := entry
	signing.ID = entmoot.RosterEntryID{}
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return fmt.Errorf("%w: canonical encode: %v", entmoot.ErrRosterReject, err)
	}
	if !keystore.Verify(founder.EntmootPubKey, sigInput, entry.Signature) {
		return fmt.Errorf("%w: signature does not verify", entmoot.ErrRosterReject)
	}

	if entry.Timestamp <= headTimestamp {
		return fmt.Errorf("%w: timestamp %d not > head timestamp %d",
			entmoot.ErrRosterReject, entry.Timestamp, headTimestamp)
	}

	// v0 is strictly linear: Parents must be exactly [head].
	if len(entry.Parents) != 1 || entry.Parents[0] != head {
		return fmt.Errorf("%w: parents must reference current head",
			entmoot.ErrRosterReject)
	}

	return nil
}

// applyLocked updates in-memory state for entry. Must be called with r.mu
// held for writing. Does NOT emit events (callers do that after unlocking).
func (r *RosterLog) applyLocked(entry entmoot.RosterEntry) {
	r.byID[entry.ID] = len(r.entries)
	r.entries = append(r.entries, entry)
	r.head = entry.ID

	switch entry.Op {
	case "add":
		r.members[entry.Subject.PilotNodeID] = entry.Subject
	case "remove":
		delete(r.members, entry.Subject.PilotNodeID)
	case "policy_change":
		// v0 does not project policy_change into membership state.
	}
}

// IsMember reports whether nodeID is currently a member of the group.
func (r *RosterLog) IsMember(nodeID entmoot.NodeID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.members[nodeID]
	return ok
}

// Members returns the current members as a slice of NodeIDs sorted ascending.
func (r *RosterLog) Members() []entmoot.NodeID {
	r.mu.RLock()
	out := make([]entmoot.NodeID, 0, len(r.members))
	for id := range r.members {
		out = append(out, id)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// MemberInfo returns the NodeInfo for nodeID, or (zero, false) if not a
// current member. The NodeInfo carries the EntmootPubKey used to verify
// message signatures authored by that member.
func (r *RosterLog) MemberInfo(nodeID entmoot.NodeID) (entmoot.NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.members[nodeID]
	if !ok {
		return entmoot.NodeInfo{}, false
	}
	// Copy the pubkey so callers can't mutate internal state.
	out := info
	out.EntmootPubKey = append([]byte(nil), info.EntmootPubKey...)
	return out, true
}

// Head returns the id of the current head entry, or the zero id if the log is
// empty.
func (r *RosterLog) Head() entmoot.RosterEntryID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.head
}

// Founder returns the founder's NodeInfo, as recorded by Genesis. Returns
// (zero, false) if the log is empty.
func (r *RosterLog) Founder() (entmoot.NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return entmoot.NodeInfo{}, false
	}
	out := r.founder
	out.EntmootPubKey = append([]byte(nil), r.founder.EntmootPubKey...)
	return out, true
}

// Entries returns a copy of the entry slice in apply order. Useful for the
// wire layer's roster_resp payload and for offline diagnostics. The returned
// slice does not share the underlying storage; callers may mutate it freely.
func (r *RosterLog) Entries() []entmoot.RosterEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]entmoot.RosterEntry, len(r.entries))
	copy(out, r.entries)
	return out
}

// Subscribe registers a subscriber for RosterEvent notifications.
//
// Every successful Apply (including Genesis) produces one event delivered to
// every live subscriber. The returned channel has a small buffer
// (subscribeBufferCap); if a subscriber falls behind, new events for that
// subscriber are DROPPED and a warning is logged via slog. Callers that need
// loss-free delivery must keep up.
//
// cancel stops the subscription and closes the channel. It is idempotent.
func (r *RosterLog) Subscribe() (<-chan RosterEvent, func()) {
	s := &subscriber{
		ch:        make(chan RosterEvent, subscribeBufferCap),
		cancelled: make(chan struct{}),
	}
	r.subsMu.Lock()
	r.sinks[s] = struct{}{}
	r.subsMu.Unlock()

	cancel := func() {
		s.cancelOnce.Do(func() {
			r.subsMu.Lock()
			delete(r.sinks, s)
			r.subsMu.Unlock()
			close(s.cancelled)
			close(s.ch)
		})
	}
	return s.ch, cancel
}

// emit delivers ev to every live subscriber. Delivery is non-blocking: if a
// subscriber's channel is full we drop and log rather than stall Apply.
func (r *RosterLog) emit(ev RosterEvent) {
	r.subsMu.Lock()
	sinks := make([]*subscriber, 0, len(r.sinks))
	for s := range r.sinks {
		sinks = append(sinks, s)
	}
	r.subsMu.Unlock()

	for _, s := range sinks {
		select {
		case <-s.cancelled:
			// Subscriber went away between snapshot and send; skip.
			continue
		default:
		}
		select {
		case s.ch <- ev:
		default:
			r.logger.Warn("roster: dropping event for slow subscriber",
				slog.String("group_id", r.groupID.String()),
				slog.String("entry_id", ev.Entry.ID.String()))
		}
	}
}

// Close releases resources held by the log. For in-memory logs it is a no-op.
// For JSONL-backed logs it closes the append file handle. Safe to call
// multiple times.
func (r *RosterLog) Close() error {
	var err error
	r.closeOnce.Do(func() {
		if r.closeFn != nil {
			err = r.closeFn()
		}
	})
	return err
}
