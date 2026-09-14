package roster

import (
	"fmt"

	"entmoot/pkg/entmoot"
)

// ReplaceChain swaps this log's contents for chain, which must be a complete,
// valid, same-genesis roster chain for this group. It exists for one purpose:
// repairing a fork. The log is strictly linear, so two authorised signers who
// write against the same head produce two chains that can never merge; the
// losing side has to adopt the winning chain and re-issue whatever it had
// written. Nothing else in the system replaces committed entries.
//
// Safety rules enforced here:
//   - chain must start at the same genesis entry, so a repair can never move a
//     group to a different founder or a different group id;
//   - chain must validate in order under the ordinary acceptance rules, so a
//     peer cannot hand us a chain we would have refused entry by entry;
//   - the replacement is committed durably before the in-memory projection
//     changes, so a crash mid-repair leaves the old chain intact.
//
// ReplaceChain returns the entries that were dropped, in their original order,
// so the caller can decide what to re-issue.
func (r *RosterLog) ReplaceChain(chain []entmoot.RosterEntry) ([]entmoot.RosterEntry, error) {
	dropped, event, err := r.replaceChainLocked(chain)
	if err != nil || event == nil {
		return dropped, err
	}
	r.emit(*event)
	return dropped, nil
}

// replaceChainLocked does the whole swap inside one critical section and
// returns the event to publish once the lock is released, so a slow subscriber
// cannot stall the log.
func (r *RosterLog) replaceChainLocked(chain []entmoot.RosterEntry) ([]entmoot.RosterEntry, *RosterEvent, error) {
	if len(chain) == 0 {
		return nil, nil, fmt.Errorf("%w: replacement chain is empty", entmoot.ErrRosterReject)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) == 0 {
		return nil, nil, fmt.Errorf("%w: cannot replace an empty log; join or import first", entmoot.ErrRosterReject)
	}
	if chain[0].ID != r.entries[0].ID {
		return nil, nil, fmt.Errorf("%w: replacement chain has a different genesis %s", entmoot.ErrRosterReject, chain[0].ID)
	}
	if err := ValidateEntries(r.groupID, chain); err != nil {
		return nil, nil, fmt.Errorf("%w: replacement chain: %v", entmoot.ErrRosterReject, err)
	}

	// Everything we hold that the replacement does not is lost by the repair.
	// Report it in apply order so the caller re-issues in the order the
	// operator originally made the changes.
	keep := make(map[entmoot.RosterEntryID]struct{}, len(chain))
	for _, entry := range chain {
		keep[entry.ID] = struct{}{}
	}
	var dropped []entmoot.RosterEntry
	for _, entry := range r.entries {
		if _, ok := keep[entry.ID]; !ok {
			dropped = append(dropped, cloneEntry(entry))
		}
	}
	if len(dropped) == 0 && len(chain) == len(r.entries) {
		// Same chain: nothing to repair. Say so rather than rewriting the
		// store for no reason.
		return nil, nil, nil
	}

	if r.replace != nil {
		if err := r.replace(chain); err != nil {
			return nil, nil, fmt.Errorf("roster: persist replacement chain: %w", err)
		}
	}
	r.resetLocked()
	for i, entry := range chain {
		if i == 0 {
			r.founder = entry.Subject
		}
		r.applyLocked(entry)
	}
	event := RosterEvent{Entry: chain[len(chain)-1], Heads: []entmoot.RosterEntryID{r.head}}
	return dropped, &event, nil
}

// resetLocked clears the projection so a validated chain can be replayed into
// it. r.mu must be held for writing.
func (r *RosterLog) resetLocked() {
	r.entries = nil
	r.byID = make(map[entmoot.RosterEntryID]int)
	r.members = make(map[entmoot.NodeID]entmoot.NodeInfo)
	r.membersByID = make(map[entmoot.MemberID]entmoot.NodeInfo)
	r.admins = make(map[entmoot.MemberID]struct{})
	r.founder = entmoot.NodeInfo{}
	r.head = entmoot.RosterEntryID{}
}

// CommonPrefix reports how many leading entries this log shares with chain.
// A repair uses it to tell an operator where the two histories parted.
func (r *RosterLog) CommonPrefix(chain []entmoot.RosterEntry) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	shared := 0
	for shared < len(chain) && shared < len(r.entries) {
		if chain[shared].ID != r.entries[shared].ID {
			break
		}
		shared++
	}
	return shared
}
