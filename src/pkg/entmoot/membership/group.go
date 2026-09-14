package membership

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// Errors a caller distinguishes. All wrap entmoot.ErrRosterReject so existing
// callers that test for a rejected membership change keep working.
var (
	// ErrStale means the record predates the current checkpoint, which already
	// accounts for it. Applying it again could resurrect a discarded change.
	ErrStale = fmt.Errorf("%w: record predates the current checkpoint", entmoot.ErrRosterReject)
	// ErrUnknownPrevious means a checkpoint chains onto one this node does not
	// hold, so its contents cannot be checked yet.
	ErrUnknownPrevious = fmt.Errorf("%w: checkpoint names an unknown previous checkpoint", entmoot.ErrRosterReject)
	// ErrCheckpointMismatch means a checkpoint disagrees with what this node's
	// own records project, so one of the two is wrong.
	ErrCheckpointMismatch = fmt.Errorf("%w: checkpoint contents disagree with local records", entmoot.ErrRosterReject)
	// ErrNotAuthorised means the signer may not author what it signed.
	ErrNotAuthorised = fmt.Errorf("%w: signer is not authorised", entmoot.ErrRosterReject)
	// ErrLegacyOnly means the group exists only as a linear roster chain and
	// awaits its first checkpoint.
	ErrLegacyOnly = errors.New("membership: group has no checkpoint yet")
	// ErrExists means a store is already present where one was to be created.
	ErrExists = errors.New("membership: group store already exists")
)

// Group is a group's membership: the checkpoints and records this node holds,
// and the state they project to. Reads are cheap; every mutation re-projects,
// which keeps one definition of the state rather than an incrementally
// maintained copy that could drift from the records.
type Group struct {
	mu          sync.RWMutex
	groupID     entmoot.GroupID
	dir         string
	db          *sql.DB
	lease       *writerLease
	checkpoints map[entmoot.RosterEntryID]Checkpoint
	// membersAt indexes each retained checkpoint's membership, so asking
	// whether someone was a member at a cited checkpoint is a lookup rather
	// than a scan of the member list. Verifying old messages does this for
	// every message, so it has to be cheap at any group size.
	membersAt   map[entmoot.RosterEntryID]map[entmoot.MemberID]entmoot.NodeInfo
	canonicalID entmoot.RosterEntryID
	records     map[entmoot.RosterEntryID]Record
	state       State
	effective   int
	legacy      *LegacyChain
	now         func() time.Time
	logger      *slog.Logger
	closeOnce   sync.Once
}

// Open loads a group's membership store. A group that still has only the
// linear roster chain returns ErrLegacyOnly, so the caller can wait for its
// first checkpoint instead of inventing one.
func Open(root string, groupID entmoot.GroupID) (*Group, error) {
	dir, err := groupDir(root, groupID)
	if err != nil {
		return nil, err
	}
	if !Exists(root, groupID) {
		if LegacyExists(root, groupID) {
			return nil, fmt.Errorf("%w: group %s", ErrLegacyOnly, groupID.String())
		}
		return nil, fmt.Errorf("membership: group %s has no membership store", groupID.String())
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("membership: mkdir %q: %w", dir, err)
	}
	db, err := openStoreDB(filepath.Join(dir, storeFileName))
	if err != nil {
		return nil, err
	}
	group, err := newGroup(dir, groupID, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return group, nil
}

// Create writes a group's first checkpoint, signed by its founder.
func Create(root string, founder *keystore.Identity, founderInfo entmoot.NodeInfo, groupID entmoot.GroupID, policy Policy, nowMS int64) (*Group, error) {
	if founder == nil {
		return nil, errors.New("membership: founder identity is required")
	}
	if err := entmoot.ValidateOperationalMemberInfo(founderInfo); err != nil {
		return nil, fmt.Errorf("membership: invalid founder: %w", err)
	}
	if policy.CheckpointEvery == 0 {
		policy.CheckpointEvery = DefaultCheckpointEvery
	}
	if policy.JoinRule == "" {
		policy.JoinRule = JoinRuleInvite
	}
	policy.Admins = SortAdmins(policy.Admins)
	if err := ValidatePolicy(policy); err != nil {
		return nil, err
	}
	cp := Checkpoint{
		Version:   Version,
		GroupID:   groupID,
		Sequence:  0,
		Founder:   founderInfo,
		Members:   []entmoot.NodeInfo{founderInfo},
		Policy:    policy,
		Timestamp: nowMS,
	}
	signed, err := SignCheckpoint(founder, founderInfo, cp)
	if err != nil {
		return nil, err
	}
	return Adopt(root, signed)
}

// Adopt writes a first checkpoint into a fresh store. Joining a group and
// upgrading a legacy one both start here: the checkpoint is the group's whole
// starting state, so there is nothing else to install.
func Adopt(root string, cp Checkpoint) (*Group, error) {
	if err := VerifyCheckpoint(cp); err != nil {
		return nil, err
	}
	if cp.Sequence != 0 {
		return nil, fmt.Errorf("membership: cannot adopt checkpoint %d as a starting point", cp.Sequence)
	}
	if Exists(root, cp.GroupID) {
		return nil, fmt.Errorf("%w: group %s", ErrExists, cp.GroupID.String())
	}
	dir, err := groupDir(root, cp.GroupID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("membership: mkdir %q: %w", dir, err)
	}
	db, err := openStoreDB(filepath.Join(dir, storeFileName))
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("membership: begin adopt: %w", err)
	}
	if err := insertCheckpointTx(ctx, tx, cp, true); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		return nil, err
	}
	if err := setCanonicalTx(ctx, tx, cp.GroupID, cp.ID); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("membership: commit adopt: %w", err)
	}
	group, err := newGroup(dir, cp.GroupID, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return group, nil
}

func newGroup(dir string, groupID entmoot.GroupID, db *sql.DB) (*Group, error) {
	stored, err := loadStore(context.Background(), db, groupID)
	if err != nil {
		return nil, err
	}
	if !stored.present {
		return nil, fmt.Errorf("membership: group %s has no membership store", groupID.String())
	}
	g := &Group{
		groupID:     groupID,
		dir:         dir,
		db:          db,
		lease:       &writerLease{path: filepath.Join(dir, lockFileName)},
		checkpoints: make(map[entmoot.RosterEntryID]Checkpoint, len(stored.checkpoints)),
		membersAt:   make(map[entmoot.RosterEntryID]map[entmoot.MemberID]entmoot.NodeInfo, len(stored.checkpoints)),
		canonicalID: stored.canonicalID,
		records:     make(map[entmoot.RosterEntryID]Record, len(stored.records)),
		now:         time.Now,
		logger:      slog.Default(),
	}
	for _, cp := range stored.checkpoints {
		if err := VerifyCheckpoint(cp); err != nil {
			return nil, fmt.Errorf("membership: stored checkpoint %s: %w", cp.ID, err)
		}
		g.retainLocked(cp)
	}
	if _, ok := g.checkpoints[g.canonicalID]; !ok {
		return nil, errors.New("membership: stored canonical checkpoint is missing")
	}
	for _, rec := range stored.records {
		if err := VerifyRecord(rec); err != nil {
			return nil, fmt.Errorf("membership: stored record %s: %w", rec.ID, err)
		}
		g.records[rec.ID] = rec
	}
	// A legacy chain may still be present for verifying messages that cite it.
	if legacy, err := LoadLegacyChain(filepath.Dir(filepath.Dir(dir)), groupID); err == nil {
		g.legacy = legacy
	}
	g.reproject()
	return g, nil
}

// SetNow replaces the clock, for tests.
func (g *Group) SetNow(now func() time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if now != nil {
		g.now = now
	}
}

// SetLogger replaces the logger.
func (g *Group) SetLogger(logger *slog.Logger) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if logger != nil {
		g.logger = logger
	}
}

func (g *Group) reproject() {
	base := g.checkpoints[g.canonicalID]
	records := make([]Record, 0, len(g.records))
	for _, rec := range g.records {
		records = append(records, rec)
	}
	state, effective := Project(base, records)
	g.state = state
	g.effective = len(effective)
}

// GroupID identifies the group.
func (g *Group) GroupID() entmoot.GroupID { return g.groupID }

// Canonical returns the checkpoint this node projects from.
func (g *Group) Canonical() Checkpoint {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return cloneCheckpoint(g.checkpoints[g.canonicalID])
}

// CheckpointByID returns a retained checkpoint.
func (g *Group) CheckpointByID(id entmoot.RosterEntryID) (Checkpoint, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	cp, ok := g.checkpoints[id]
	if !ok {
		return Checkpoint{}, false
	}
	return cloneCheckpoint(cp), true
}

// HasCheckpoint reports whether this node can interpret a checkpoint id: one
// it retains, or an entry of the legacy chain a message may still cite.
func (g *Group) HasCheckpoint(id entmoot.RosterEntryID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if _, ok := g.checkpoints[id]; ok {
		return true
	}
	return g.legacy != nil && g.legacy.HasEntry(id)
}

// CheckpointsSince returns retained checkpoints newer than a sequence, oldest
// first, with the canonical one first within a sequence.
func (g *Group) CheckpointsSince(sequence uint64) []Checkpoint {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]Checkpoint, 0, len(g.checkpoints))
	for _, cp := range g.checkpoints {
		if cp.Sequence > sequence || sequence == 0 && cp.Sequence == 0 {
			out = append(out, cloneCheckpoint(cp))
		}
	}
	canonical := g.canonicalID
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence < out[j].Sequence
		}
		if (out[i].ID == canonical) != (out[j].ID == canonical) {
			return out[i].ID == canonical
		}
		return bytes.Compare(out[i].ID[:], out[j].ID[:]) < 0
	})
	return out
}

// Pending returns the records a peer still needs: the ones this node holds
// that the canonical checkpoint has not folded in.
//
// Records covered by the canonical checkpoint are deliberately excluded even
// though they are still on disk. They are kept for one checkpoint of lag, so a
// peer arriving at the previous checkpoint can re-verify the newest one; a
// peer arriving at the newest checkpoint would reject them as stale, and
// serving records a receiver must refuse is worse than serving none.
func (g *Group) Pending() []Record {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]Record, 0, len(g.records))
	for _, rec := range g.records {
		if g.recordCoveredLocked(rec) {
			continue
		}
		out = append(out, cloneRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp != out[j].Timestamp {
			return out[i].Timestamp < out[j].Timestamp
		}
		return bytes.Compare(out[i].ID[:], out[j].ID[:]) < 0
	})
	return out
}

// EffectivePendingCount is how many retained records changed the state, which
// is what the checkpoint cadence counts.
func (g *Group) EffectivePendingCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.effective
}

// HasRecord reports whether this node already holds a record.
func (g *Group) HasRecord(id entmoot.RosterEntryID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.records[id]
	return ok
}

// State returns a copy of the projected membership.
func (g *Group) State() State {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.copyStateLocked()
}

func (g *Group) copyStateLocked() State {
	out := State{
		Founder:        cloneNodeInfo(g.state.Founder),
		Members:        make(map[entmoot.MemberID]entmoot.NodeInfo, len(g.state.Members)),
		Policy:         g.state.Policy.Clone(),
		Banned:         make(map[entmoot.MemberID]struct{}, len(g.state.Banned)),
		RevokedInvites: make(map[[32]byte]struct{}, len(g.state.RevokedInvites)),
		InviteUses:     make(map[[32]byte]int, len(g.state.InviteUses)),
	}
	for id, info := range g.state.Members {
		out.Members[id] = cloneNodeInfo(info)
	}
	for id := range g.state.Banned {
		out.Banned[id] = struct{}{}
	}
	for nonce := range g.state.RevokedInvites {
		out.RevokedInvites[nonce] = struct{}{}
	}
	for nonce, count := range g.state.InviteUses {
		out.InviteUses[nonce] = count
	}
	return out
}

// Founder returns the group's anchor identity.
func (g *Group) Founder() entmoot.NodeInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return cloneNodeInfo(g.state.Founder)
}

// Policy returns the current rules.
func (g *Group) Policy() Policy {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.Policy.Clone()
}

// Admins returns the delegated-admin set.
func (g *Group) Admins() []entmoot.MemberID {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return append([]entmoot.MemberID(nil), g.state.Policy.Admins...)
}

// CanAdminister reports whether id may author authority records.
func (g *Group) CanAdminister(id entmoot.MemberID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.CanAdminister(id)
}

// IsMemberID reports current membership.
func (g *Group) IsMemberID(id entmoot.MemberID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.state.Members[id]
	return ok
}

// IsBanned reports whether id is barred from rejoining.
func (g *Group) IsBanned(id entmoot.MemberID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, banned := g.state.Banned[id]
	return banned
}

// MemberInfoByID returns a current member's record.
func (g *Group) MemberInfoByID(id entmoot.MemberID) (entmoot.NodeInfo, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	info, ok := g.state.Members[id]
	if !ok {
		return entmoot.NodeInfo{}, false
	}
	return cloneNodeInfo(info), true
}

// MemberIDs returns the current membership, sorted.
func (g *Group) MemberIDs() []entmoot.MemberID {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.MemberIDs()
}

// MemberAt answers whether a member was in the group at a cited checkpoint,
// which is what verifying an old message needs. known is false when the id
// means nothing to this node, so the caller can hold the message rather than
// reject it.
func (g *Group) MemberAt(id entmoot.MemberID, checkpoint entmoot.RosterEntryID) (info entmoot.NodeInfo, active bool, known bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	index, ok := g.membersAt[checkpoint]
	if !ok {
		if g.legacy != nil {
			return g.legacy.MemberAt(id, checkpoint)
		}
		return entmoot.NodeInfo{}, false, false
	}
	if member, present := index[id]; present {
		return cloneNodeInfo(member), true, true
	}
	// A member admitted after the cited checkpoint but before the next one is
	// still a legitimate author of a message citing it: the join record was
	// pending when the message was written.
	if info, ok := g.state.Members[id]; ok && checkpoint == g.canonicalID {
		return cloneNodeInfo(info), true, true
	}
	return entmoot.NodeInfo{}, false, true
}

// Legacy exposes the linear chain this group upgraded from, when one is still
// on disk. Historical messages that cite a chain entry are verified against it.
func (g *Group) Legacy() *LegacyChain {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.legacy
}

// ClaimWriter takes the single-writer lease.
func (g *Group) ClaimWriter() error {
	return g.lease.claim()
}

// Close releases the store and the lease.
func (g *Group) Close() error {
	var err error
	g.closeOnce.Do(func() {
		leaseErr := g.lease.close()
		dbErr := g.db.Close()
		if leaseErr != nil {
			err = leaseErr
			return
		}
		if dbErr != nil {
			err = fmt.Errorf("membership: close store: %w", dbErr)
		}
	})
	return err
}

// Apply stores one record and re-projects. Applying a record twice is a no-op,
// so a peer may send the same record repeatedly.
func (g *Group) Apply(rec Record) (bool, error) {
	if err := VerifyRecord(rec); err != nil {
		return false, err
	}
	if rec.GroupID != g.groupID {
		return false, fmt.Errorf("%w: record names group %s", entmoot.ErrRosterReject, rec.GroupID.String())
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.records[rec.ID]; exists {
		return false, nil
	}
	if rec.Timestamp < g.checkpoints[g.canonicalID].Timestamp {
		return false, ErrStale
	}
	if rec.Timestamp == g.checkpoints[g.canonicalID].Timestamp && g.recordCoveredLocked(rec) {
		return false, ErrStale
	}
	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("membership: begin apply: %w", err)
	}
	if err := insertRecordTx(ctx, tx, rec); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("membership: commit record: %w", err)
	}
	g.records[rec.ID] = cloneRecord(rec)
	g.reproject()
	return true, nil
}

// ApplyCheckpoint stores a checkpoint, choosing it as canonical when it
// supersedes the current one. A checkpoint that disagrees with this node's own
// records is refused rather than adopted, so a wrong or hostile checkpoint
// cannot rewrite what this node knows.
func (g *Group) ApplyCheckpoint(cp Checkpoint) (bool, error) {
	if err := VerifyCheckpoint(cp); err != nil {
		return false, err
	}
	if cp.GroupID != g.groupID {
		return false, fmt.Errorf("%w: checkpoint names group %s", entmoot.ErrRosterReject, cp.GroupID.String())
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.checkpoints[cp.ID]; exists {
		return false, nil
	}
	previous, ok := g.checkpoints[cp.Previous]
	if !ok {
		return false, ErrUnknownPrevious
	}
	if cp.Sequence != previous.Sequence+1 {
		return false, fmt.Errorf("%w: checkpoint %d does not follow %d", entmoot.ErrRosterReject, cp.Sequence, previous.Sequence)
	}
	if err := g.verifyCheckpointAuthorityLocked(cp, previous); err != nil {
		return false, err
	}
	if err := g.verifyCheckpointContentsLocked(cp, previous); err != nil {
		return false, err
	}

	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("membership: begin apply checkpoint: %w", err)
	}
	if err := insertCheckpointTx(ctx, tx, cp, false); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("membership: commit checkpoint: %w", err)
	}
	g.retainLocked(cp)
	if err := g.settleCanonicalLocked(); err != nil {
		return true, err
	}
	return true, nil
}

// verifyCheckpointAuthorityLocked requires the signer to have been able to
// author a checkpoint at the previous one: the founder, or an admin that was
// then an unbanned member.
func (g *Group) verifyCheckpointAuthorityLocked(cp, previous Checkpoint) error {
	signer, err := entmoot.ResolvedMemberID(cp.Signer)
	if err != nil {
		return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
	}
	// The signer may have been granted authority by a record this checkpoint
	// folds in, so its own claimed state is the right place to judge it. That
	// claim is not taken on trust: verifyCheckpointContentsLocked requires it
	// to agree with the records this node holds.
	claimed := stateFrom(cp)
	previousState := stateFrom(previous)
	if !claimed.CanAdminister(signer) && !previousState.CanAdminister(signer) {
		return fmt.Errorf("%w: %s may not sign a checkpoint for this group", ErrNotAuthorised, signer.String())
	}
	if !claimed.IsFounder(signer) {
		member, ok := claimed.Members[signer]
		if !ok {
			member, ok = previousState.Members[signer]
		}
		if !ok || !bytes.Equal(member.EntmootPubKey, cp.Signer.EntmootPubKey) {
			return fmt.Errorf("%w: checkpoint signer key does not match the group's record for it", ErrNotAuthorised)
		}
	}
	if founder, err := entmoot.ResolvedMemberID(cp.Founder); err != nil ||
		founder != mustMemberID(previous.Founder) || !bytes.Equal(cp.Founder.EntmootPubKey, previous.Founder.EntmootPubKey) {
		return fmt.Errorf("%w: checkpoint changes the group's founder", entmoot.ErrRosterReject)
	}
	return nil
}

// verifyCheckpointContentsLocked checks the checkpoint against what this
// node's own records project from the same base. A node with no relevant
// records accepts it on the signer's authority; a node that does hold them
// requires agreement.
func (g *Group) verifyCheckpointContentsLocked(cp, previous Checkpoint) error {
	relevant := make([]Record, 0, len(g.records))
	for _, rec := range g.records {
		if rec.Timestamp >= previous.Timestamp && rec.Timestamp <= cp.Timestamp {
			relevant = append(relevant, rec)
		}
	}
	if len(relevant) == 0 {
		return nil
	}
	projected, _ := Project(previous, relevant)
	claimed := stateFrom(cp)
	if !sameMembership(projected, claimed) {
		return ErrCheckpointMismatch
	}
	return nil
}

func sameMembership(left, right State) bool {
	if len(left.Members) != len(right.Members) || len(left.Banned) != len(right.Banned) ||
		len(left.RevokedInvites) != len(right.RevokedInvites) {
		return false
	}
	for id, info := range left.Members {
		other, ok := right.Members[id]
		if !ok || !bytes.Equal(info.EntmootPubKey, other.EntmootPubKey) {
			return false
		}
	}
	for id := range left.Banned {
		if _, ok := right.Banned[id]; !ok {
			return false
		}
	}
	for nonce := range left.RevokedInvites {
		if _, ok := right.RevokedInvites[nonce]; !ok {
			return false
		}
	}
	if left.Policy.JoinRule != right.Policy.JoinRule || left.Policy.CheckpointEvery != right.Policy.CheckpointEvery ||
		len(left.Policy.Admins) != len(right.Policy.Admins) {
		return false
	}
	for i := range left.Policy.Admins {
		if left.Policy.Admins[i] != right.Policy.Admins[i] {
			return false
		}
	}
	for nonce, count := range left.InviteUses {
		if right.InviteUses[nonce] != count {
			return false
		}
	}
	for nonce, count := range right.InviteUses {
		if left.InviteUses[nonce] != count {
			return false
		}
	}
	return true
}

// settleCanonicalLocked picks the canonical checkpoint and retires what it
// covers. Two admins may sign a checkpoint for the same sequence; the earlier
// one wins, ties broken by id, so every node picks the same one without
// negotiating.
func (g *Group) settleCanonicalLocked() error {
	// Walk forward from the group's first checkpoint, not from the current
	// canonical one: a better sibling can arrive after a worse one was already
	// chosen, and every node must end up with the same answer regardless of
	// the order the two showed up in.
	best, ok := g.rootCheckpointLocked()
	if !ok {
		return nil
	}
	for {
		var candidate *Checkpoint
		for id, cp := range g.checkpoints {
			if cp.Previous != best.ID || id == best.ID {
				continue
			}
			if candidate == nil || checkpointBeats(cp, *candidate) {
				next := cp
				candidate = &next
			}
		}
		if candidate == nil {
			break
		}
		best = *candidate
	}
	if best.ID == g.canonicalID {
		return nil
	}

	ctx := context.Background()
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("membership: begin canonical update: %w", err)
	}
	if err := setCanonicalTx(ctx, tx, g.groupID, best.ID); err != nil {
		_ = tx.Rollback()
		return err
	}
	// Retire with one checkpoint of lag: records covered by the newest
	// checkpoint stay until the following one lands, so a node that has to
	// re-verify the newest checkpoint still holds the records behind it.
	var retireBefore int64
	for _, cp := range g.checkpoints {
		if cp.ID == best.Previous {
			retireBefore = cp.Timestamp
		}
	}
	if retireBefore > 0 {
		if err := deleteRecordsThroughTx(ctx, tx, g.groupID, retireBefore); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	// Drop stale forks: a checkpoint two or more sequences behind the
	// canonical one can no longer be chosen.
	var dropped []entmoot.RosterEntryID
	for id, cp := range g.checkpoints {
		if id == best.ID {
			continue
		}
		if cp.Sequence+1 < best.Sequence {
			if err := deleteCheckpointTx(ctx, tx, id); err != nil {
				_ = tx.Rollback()
				return err
			}
			dropped = append(dropped, id)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("membership: commit canonical update: %w", err)
	}

	g.canonicalID = best.ID
	for _, id := range dropped {
		delete(g.checkpoints, id)
		delete(g.membersAt, id)
	}
	if retireBefore > 0 {
		for id, rec := range g.records {
			if rec.Timestamp <= retireBefore {
				delete(g.records, id)
			}
		}
	}
	g.reproject()
	return nil
}

// rootCheckpointLocked returns the sequence-zero checkpoint, which every
// chain of checkpoints descends from.
func (g *Group) rootCheckpointLocked() (Checkpoint, bool) {
	for _, cp := range g.checkpoints {
		if cp.Sequence == 0 {
			return cp, true
		}
	}
	return Checkpoint{}, false
}

func checkpointBeats(candidate, current Checkpoint) bool {
	if candidate.Timestamp != current.Timestamp {
		return candidate.Timestamp < current.Timestamp
	}
	return bytes.Compare(candidate.ID[:], current.ID[:]) < 0
}

// SignRecord fills in the actor, group and timestamp, signs, and applies.
func (g *Group) SignRecord(identity *keystore.Identity, rec Record) (Record, error) {
	if identity == nil {
		return Record{}, errors.New("membership: signing identity is required")
	}
	actor, err := identityInfo(identity)
	if err != nil {
		return Record{}, err
	}
	rec.Actor = actor
	rec.GroupID = g.groupID
	switch rec.Kind {
	case KindJoin, KindLeave:
		rec.Subject = actor
	}
	g.mu.RLock()
	// A record must land after everything this node already holds: after the
	// checkpoint, or it would be refused as stale, and after the newest record
	// too, so two writes made in the same millisecond keep the order they were
	// made in rather than being sorted by their ids.
	floor := g.checkpoints[g.canonicalID].Timestamp
	for _, held := range g.records {
		if held.Timestamp > floor {
			floor = held.Timestamp
		}
	}
	now := g.now().UnixMilli()
	g.mu.RUnlock()
	if now <= floor {
		now = floor + 1
	}
	rec.Timestamp = now
	signed, err := SignRecord(identity, rec)
	if err != nil {
		return Record{}, err
	}
	if _, err := g.Apply(signed); err != nil {
		return Record{}, err
	}
	return signed, nil
}

// SignCheckpoint folds the pending records into a new checkpoint. Without
// force it signs only once the policy's cadence is reached, so the common path
// is to call it every maintenance round and have it decline.
func (g *Group) SignCheckpoint(identity *keystore.Identity, force bool) (Checkpoint, bool, error) {
	if identity == nil {
		return Checkpoint{}, false, errors.New("membership: signing identity is required")
	}
	signer, err := identityInfo(identity)
	if err != nil {
		return Checkpoint{}, false, err
	}
	signerID, err := entmoot.ResolvedMemberID(signer)
	if err != nil {
		return Checkpoint{}, false, err
	}

	g.mu.Lock()
	base := g.checkpoints[g.canonicalID]
	if !g.state.CanAdminister(signerID) {
		g.mu.Unlock()
		return Checkpoint{}, false, fmt.Errorf("%w: %s may not sign a checkpoint", ErrNotAuthorised, signerID.String())
	}
	records := make([]Record, 0, len(g.records))
	for _, rec := range g.records {
		records = append(records, rec)
	}
	cadence := g.state.Policy.CheckpointEvery
	g.mu.Unlock()

	state, effective := Project(base, records)
	if !force && len(effective) < cadence {
		return Checkpoint{}, false, nil
	}
	if len(effective) == 0 && !force {
		return Checkpoint{}, false, nil
	}
	// The timestamp covers every record folded in, not only the ones that
	// changed the state. A record the checkpoint saw and judged ineffective is
	// accounted for too: leaving it uncovered would let it be replayed later,
	// once the context that made it ineffective has been retired.
	timestamp := base.Timestamp
	for _, rec := range records {
		if rec.Timestamp > timestamp {
			timestamp = rec.Timestamp
		}
	}
	if timestamp <= base.Timestamp {
		timestamp = base.Timestamp + 1
	}
	body := state.Checkpoint(g.groupID, base.Sequence+1, base.ID, uint64(len(effective)), timestamp)
	signed, err := SignCheckpoint(identity, signer, body)
	if err != nil {
		return Checkpoint{}, false, err
	}
	if _, err := g.ApplyCheckpoint(signed); err != nil {
		return Checkpoint{}, false, err
	}
	return signed, true, nil
}

func identityInfo(identity *keystore.Identity) (entmoot.NodeInfo, error) {
	memberID, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
	if err != nil {
		return entmoot.NodeInfo{}, err
	}
	peerID, err := entmoot.PeerIDFromPublicKey(identity.PublicKey)
	if err != nil {
		return entmoot.NodeInfo{}, err
	}
	return entmoot.NodeInfo{
		MemberID:      &memberID,
		PeerID:        peerID,
		EntmootPubKey: bytes.Clone(identity.PublicKey),
	}, nil
}

// retainLocked stores a checkpoint and indexes its membership.
func (g *Group) retainLocked(cp Checkpoint) {
	stored := cloneCheckpoint(cp)
	g.checkpoints[stored.ID] = stored
	index := make(map[entmoot.MemberID]entmoot.NodeInfo, len(stored.Members))
	for _, member := range stored.Members {
		if id, err := entmoot.ResolvedMemberID(member); err == nil {
			index[id] = member
		}
	}
	g.membersAt[stored.ID] = index
}

// recordCoveredLocked reports whether a record whose timestamp equals the
// canonical checkpoint's is already accounted for by it, which is true unless
// the checkpoint predates any record at all.
func (g *Group) recordCoveredLocked(rec Record) bool {
	canonical := g.checkpoints[g.canonicalID]
	return canonical.Covered > 0 && rec.Timestamp <= canonical.Timestamp
}

func mustMemberID(info entmoot.NodeInfo) entmoot.MemberID {
	id, err := entmoot.ResolvedMemberID(info)
	if err != nil {
		return entmoot.MemberID{}
	}
	return id
}

// CheckInvite reports whether an invite still authorises its holder to read
// this group's membership. It is the pre-membership gate: a joiner has no
// member record yet, so the invite is the only credential it can present.
//
// The answer comes from the group's own signed state, not from a local
// ledger. That is what makes it the same answer on every node: an invite
// revoked by a record, exhausted by other joins, or signed by a demoted
// admin stops working everywhere, without any node having to be told.
func (g *Group) CheckInvite(invite entmoot.BootstrapCapability, atMS int64) error {
	if err := VerifyInviteSignature(invite); err != nil {
		return err
	}
	if invite.GroupID != g.groupID {
		return fmt.Errorf("%w: invite names another group", ErrInviteDenied)
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if _, revoked := g.state.RevokedInvites[invite.Nonce]; revoked {
		return fmt.Errorf("%w: invite was revoked", ErrInviteDenied)
	}
	if err := InviteValidAt(invite, atMS); err != nil {
		return err
	}
	if g.state.InviteUses[invite.Nonce] >= invite.Uses() {
		return fmt.Errorf("%w: invite has no uses left", ErrInviteDenied)
	}
	authority := invite.SigningAuthority()
	authorityID, err := entmoot.ResolvedMemberID(authority)
	if err != nil {
		return fmt.Errorf("%w: invite issuer is incomplete", ErrInviteDenied)
	}
	if !g.state.CanAdminister(authorityID) {
		return fmt.Errorf("%w: invite issuer cannot administer this group", ErrInviteDenied)
	}
	if g.state.IsFounder(authorityID) {
		if !bytes.Equal(g.state.Founder.EntmootPubKey, authority.EntmootPubKey) {
			return fmt.Errorf("%w: invite issuer key does not match the founder", ErrInviteDenied)
		}
		return nil
	}
	member, ok := g.state.Members[authorityID]
	if !ok || !bytes.Equal(member.EntmootPubKey, authority.EntmootPubKey) {
		return fmt.Errorf("%w: invite issuer key does not match its member record", ErrInviteDenied)
	}
	return nil
}

// InviteUses reports how many identities the group's state records as having
// redeemed one invite. This is the count that decides admission, so an
// operator reading it sees what every other node sees.
func (g *Group) InviteUses(nonce [32]byte) int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.InviteUses[nonce]
}

// IsInviteRevoked reports whether a signed record has withdrawn an invite.
func (g *Group) IsInviteRevoked(nonce [32]byte) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, revoked := g.state.RevokedInvites[nonce]
	return revoked
}
