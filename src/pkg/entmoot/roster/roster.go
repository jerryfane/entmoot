// Package roster maintains the signed append-only membership log for a group.
//
// Each group has a single RosterLog; the log's head is the group's current
// membership. Entries are accepted from the founder or from a delegated admin
// named by a founder-signed policy_change (see admin.go). Admins exist so a
// group can admit and evict members while the founder is away; only the
// founder changes the admin set, removes an admin, or is removed.
//
// The log is strictly linear: every entry must name the current head as its
// only parent, so concurrent authors do not branch — the loser's entry is
// rejected and has to be re-signed against the new head. Head() therefore
// always refers to the most-recently-applied entry. Because several nodes may
// author entries, peers must pull roster state from members generally, not
// only from the founder.
//
// Persistence uses a dedicated transactional SQLite schema. Legacy JSONL logs
// are validated as immutable import sources; they are never replayed
// permissively or appended after migration.
package roster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// CurrentEntryVersion is the group-bound, domain-separated roster format used
// for every newly signed entry.
const CurrentEntryVersion uint8 = 2

// RosterLog is the concurrency-safe in-memory projection of a single group's
// signed roster log. Mutation validation, durable commit, and projection
// update are serialized under mu; read methods use the read lock.
type RosterLog struct {
	groupID entmoot.GroupID

	mu sync.RWMutex

	// entries are all applied entries in apply order.
	entries []entmoot.RosterEntry
	// byID indexes entries by id for quick lookup.
	byID map[entmoot.RosterEntryID]int
	// members is the current membership projection.
	members map[entmoot.NodeID]entmoot.NodeInfo
	// membersByID is the Pilot-independent current membership projection.
	membersByID map[entmoot.MemberID]entmoot.NodeInfo
	// founder is set on Genesis; empty until then.
	founder entmoot.NodeInfo
	// admins is the current delegated-admin set, maintained by founder-signed
	// policy_change entries. The founder is always an admin and is never
	// listed here.
	admins map[entmoot.MemberID]struct{}
	// head is the id of the most-recently-applied entry; zero on empty log.
	head entmoot.RosterEntryID

	// persist commits one validated entry before the in-memory projection
	// advances. Persistent logs store entries and projections transactionally.
	persist func(entmoot.RosterEntry) error
	// replace atomically swaps the whole persisted chain. Only fork repair
	// uses it; in-memory logs leave it nil.
	replace func([]entmoot.RosterEntry) error
	// storedChain reads the committed chain back from the store. Repair uses
	// it to resolve an ambiguous write failure, where the store may or may not
	// have landed the new chain. In-memory logs leave it nil.
	storedChain func() ([]entmoot.RosterEntry, error)
	// claimWriter acquires the persistent writer lease. In-memory logs leave it
	// nil. Persistent logs acquire lazily for offline mutation; daemons call
	// ClaimWriter during startup.
	claimWriter func() error

	// logger is used for subscribe-drop warnings.
	logger *slog.Logger

	// closeOnce guards persistent handle and writer-lease teardown.
	closeOnce sync.Once
	closeFn   func() error
}

// New constructs an empty in-memory RosterLog for the given group.
//
// The returned log is NOT yet valid for queries (Members is empty, Head is
// the zero id) until Genesis is called or entries are Applied. The zero
// GroupID is accepted; it is the caller's responsibility to pass a real id.
func New(groupID entmoot.GroupID) *RosterLog {
	return &RosterLog{
		groupID:     groupID,
		byID:        make(map[entmoot.RosterEntryID]int),
		members:     make(map[entmoot.NodeID]entmoot.NodeInfo),
		membersByID: make(map[entmoot.MemberID]entmoot.NodeInfo),
		logger:      slog.Default(),
		admins:      make(map[entmoot.MemberID]struct{}),
	}
}

// SignEntry builds a version-2 entry against the log's current head. Apply
// still performs authoritative validation, so a concurrent mutation can make
// the returned entry stale and safely reject it.
func (r *RosterLog) SignEntry(
	signer *keystore.Identity,
	op string,
	subject entmoot.NodeInfo,
	policy []byte,
	timestampMillis int64,
) (entmoot.RosterEntry, error) {
	if signer == nil {
		return entmoot.RosterEntry{}, fmt.Errorf("roster: SignEntry requires a non-nil identity")
	}
	actorMemberID, err := entmoot.MemberIDFromPublicKey(signer.PublicKey)
	if err != nil {
		return entmoot.RosterEntry{}, fmt.Errorf("roster: derive signer member id: %w", err)
	}
	if op == "add" || op == "remove" {
		if err := entmoot.ValidateOperationalMemberInfo(subject); err != nil {
			return entmoot.RosterEntry{}, fmt.Errorf("roster: invalid subject identity: %w", err)
		}
	}
	r.mu.RLock()
	if len(r.entries) == 0 {
		r.mu.RUnlock()
		return entmoot.RosterEntry{}, fmt.Errorf("%w: SignEntry on empty log", entmoot.ErrRosterReject)
	}
	if r.entries[len(r.entries)-1].Version != CurrentEntryVersion {
		r.mu.RUnlock()
		return entmoot.RosterEntry{}, fmt.Errorf("%w: legacy roster requires an authenticated upgrade checkpoint", entmoot.ErrRosterReject)
	}
	groupID := r.groupID
	entry := entmoot.RosterEntry{
		Op:            op,
		Subject:       subject,
		Policy:        append([]byte(nil), policy...),
		ActorMemberID: &actorMemberID,
		Timestamp:     timestampMillis,
		Parents:       []entmoot.RosterEntryID{r.head},
		Version:       CurrentEntryVersion,
		GroupID:       &groupID,
		Sequence:      uint64(len(r.entries) + 1),
	}
	r.mu.RUnlock()
	sigInput, err := canonical.RosterEntrySigningBytes(entry)
	if err != nil {
		return entmoot.RosterEntry{}, fmt.Errorf("roster: canonical encode for signing: %w", err)
	}
	entry.Signature = signer.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)
	return entry, nil
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
	if !bytes.Equal(founderInfo.EntmootPubKey, founder.PublicKey) {
		return fmt.Errorf("roster: founder public key does not match signing identity")
	}
	if err := entmoot.ValidateOperationalMemberInfo(founderInfo); err != nil {
		return fmt.Errorf("roster: invalid founder identity: %w", err)
	}
	groupID := r.groupID
	entry := entmoot.RosterEntry{
		Op:            "add",
		Subject:       founderInfo,
		ActorMemberID: founderInfo.MemberID,
		Timestamp:     timestampMillis,
		Parents:       nil,
		Version:       CurrentEntryVersion,
		GroupID:       &groupID,
		Sequence:      1,
	}
	sigInput, err := canonical.RosterEntrySigningBytes(entry)
	if err != nil {
		return fmt.Errorf("roster: canonical encode for signing: %w", err)
	}
	entry.Signature = founder.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)

	r.mu.Lock()
	if len(r.entries) != 0 {
		r.mu.Unlock()
		return fmt.Errorf("roster: Genesis called on non-empty log")
	}
	if err := r.persistLocked(entry, "genesis"); err != nil {
		r.mu.Unlock()
		return err
	}
	r.founder = founderInfo
	r.applyLocked(entry)
	r.mu.Unlock()
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
// On success it leaves the log in exactly the state Genesis would have. This
// mirrors the JSONL loader's first-entry path: the founder is adopted from
// entry.Subject rather than being supplied separately by the caller.
func (r *RosterLog) AcceptGenesis(entry entmoot.RosterEntry) error {
	r.mu.Lock()
	if len(r.entries) != 0 {
		r.mu.Unlock()
		return fmt.Errorf("%w: AcceptGenesis on non-empty log", entmoot.ErrRosterReject)
	}
	if err := validateGenesis(entry, r.groupID); err != nil {
		r.mu.Unlock()
		return err
	}
	if err := r.persistLocked(entry, "accepted genesis"); err != nil {
		r.mu.Unlock()
		return err
	}
	r.founder = entry.Subject
	r.applyLocked(entry)
	r.mu.Unlock()
	return nil
}

// Apply validates and appends a single entry. On any validation failure the
// returned error wraps entmoot.ErrRosterReject and the log is unchanged.
//
// Validation:
//   - the log must already have a genesis entry, so the founder is known;
//   - Op must be one of "add", "remove", or "policy_change";
//   - the signer must be authorised for that op: the founder always, a
//     delegated admin for adds and removals of ordinary members. Only the
//     founder changes the admin set or removes another admin; an admin may
//     remove itself, which gives up its own delegation;
//   - the signature must verify against the signer's current member record,
//     so losing membership or delegation ends the authority at once;
//   - Entry.ID must equal canonical.RosterEntryID of the entry with id and
//     signature zeroed, and version-2 entries must carry this group's id and
//     the next sequence number;
//   - Entry.Timestamp must be strictly greater than the current head's;
//   - Parents must reference the current head: the log stays linear, so two
//     writers racing means one of them retries;
//   - a version-2 policy_change payload must be readable JSON, and one that
//     claims to change the admin set must be a version this build can apply.
//
// Apply does NOT check wire-layer replay windows — that is the caller's job.
func (r *RosterLog) Apply(entry entmoot.RosterEntry) error {
	r.mu.Lock()
	if err := r.validateLocked(entry); err != nil {
		r.mu.Unlock()
		return err
	}
	if err := r.persistLocked(entry, "entry"); err != nil {
		r.mu.Unlock()
		return err
	}
	r.applyLocked(entry)
	r.mu.Unlock()
	return nil
}

// validateLocked performs every non-genesis acceptance check. r.mu must be
// held for writing so validation and projection update share one critical
// section.
func (r *RosterLog) validateLocked(entry entmoot.RosterEntry) error {
	founder := r.founder
	head := r.head
	var headTimestamp int64
	if len(r.entries) > 0 {
		headTimestamp = r.entries[len(r.entries)-1].Timestamp
	}
	empty := len(r.entries) == 0

	if empty {
		return fmt.Errorf("%w: Apply on empty log; call Genesis first", entmoot.ErrRosterReject)
	}

	switch entry.Op {
	case "add", "remove", "policy_change":
	default:
		return fmt.Errorf("%w: invalid op %q", entmoot.ErrRosterReject, entry.Op)
	}

	founderMemberID, founderErr := entmoot.ResolvedMemberID(founder)
	if entry.Version == 0 {
		if entry.Actor != founder.PilotNodeID {
			return fmt.Errorf("%w: legacy actor %d is not founder %d", entmoot.ErrRosterReject, entry.Actor, founder.PilotNodeID)
		}
	} else if founderErr != nil || entry.ActorMemberID == nil {
		return fmt.Errorf("%w: actor member is not resolvable", entmoot.ErrRosterReject)
	}
	// signerKey is the key the entry must verify against: the founder for
	// legacy entries and founder-only operations, or a delegated admin.
	signerKey := founder.EntmootPubKey
	if entry.Version != 0 && *entry.ActorMemberID != founderMemberID {
		actor, isAdmin := r.adminInfoLocked(*entry.ActorMemberID)
		if !isAdmin {
			return fmt.Errorf("%w: actor %s is not the founder or a delegated admin",
				entmoot.ErrRosterReject, entry.ActorMemberID.String())
		}
		if err := r.validateAdminOpLocked(entry); err != nil {
			return err
		}
		signerKey = actor.EntmootPubKey
	}

	// Verify the id the caller supplied matches what we would compute.
	if canonical.RosterEntryID(entry) != entry.ID {
		return fmt.Errorf("%w: entry id does not match canonical hash",
			entmoot.ErrRosterReject)
	}

	if err := validateEntryFormat(entry, r.groupID, uint64(len(r.entries)+1), r.entries[len(r.entries)-1].Version == 0); err != nil {
		return err
	}

	// A version-2 policy payload must be readable JSON, and an admin-set
	// policy must parse: otherwise peers would disagree about who can sign the
	// next entry, and an undecodable payload would silently count as "some
	// other policy". Other policy types travel through the same op and are not
	// interpreted here.
	if entry.Op == "policy_change" && entry.Version != 0 {
		if len(entry.Policy) == 0 || !json.Valid(entry.Policy) {
			return fmt.Errorf("%w: policy_change payload is not valid JSON", entmoot.ErrRosterReject)
		}
		if IsUnknownAdminPolicy(entry.Policy) {
			// It says it changes the admin set, in a version this build cannot
			// read. Accepting it would leave the current admins standing here
			// while a newer peer applied the change: the two nodes would then
			// disagree about who may sign. Refuse and let the operator see it.
			return fmt.Errorf("%w: policy_change names an admin policy version this build cannot apply", entmoot.ErrRosterReject)
		}
		if IsAdminPolicy(entry.Policy) {
			if _, err := ParseAdminPolicy(entry.Policy); err != nil {
				return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
			}
		}
	}

	// A member id is derived from its key, so re-adding one under a different
	// key is an identity substitution. r.members is keyed by legacy Pilot node
	// id and is empty for version-2 groups, so this has to consult the
	// full-width projection.
	if entry.Op == "add" && entry.Subject.MemberID != nil {
		if existing, ok := r.membersByID[*entry.Subject.MemberID]; ok &&
			!bytes.Equal(existing.EntmootPubKey, entry.Subject.EntmootPubKey) {
			return fmt.Errorf("%w: member id is already bound to another public key", entmoot.ErrRosterReject)
		}
	}

	// Verify the signature against the acting authority's pubkey using the
	// versioned signing form.
	sigInput, err := canonical.RosterEntrySigningBytes(entry)
	if err != nil {
		return fmt.Errorf("%w: canonical encode: %v", entmoot.ErrRosterReject, err)
	}
	if !keystore.Verify(signerKey, sigInput, entry.Signature) {
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

func validateGenesis(entry entmoot.RosterEntry, groupID entmoot.GroupID) error {
	if entry.Op != "add" {
		return fmt.Errorf("%w: genesis op must be \"add\", got %q", entmoot.ErrRosterReject, entry.Op)
	}
	if len(entry.Parents) != 0 {
		return fmt.Errorf("%w: genesis must have no parents", entmoot.ErrRosterReject)
	}
	if entry.Version == 0 {
		if entry.Actor != entry.Subject.PilotNodeID {
			return fmt.Errorf("%w: legacy genesis actor is not subject", entmoot.ErrRosterReject)
		}
	} else if entry.ActorMemberID == nil || entry.Subject.MemberID == nil || *entry.ActorMemberID != *entry.Subject.MemberID {
		return fmt.Errorf("%w: genesis actor member is not subject", entmoot.ErrRosterReject)
	}
	if len(entry.Subject.EntmootPubKey) == 0 {
		return fmt.Errorf("%w: genesis subject has no pubkey", entmoot.ErrRosterReject)
	}
	if err := validateEntryFormat(entry, groupID, 1, true); err != nil {
		return err
	}
	if canonical.RosterEntryID(entry) != entry.ID {
		return fmt.Errorf("%w: genesis entry id does not match canonical hash", entmoot.ErrRosterReject)
	}
	sigInput, err := canonical.RosterEntrySigningBytes(entry)
	if err != nil {
		return fmt.Errorf("%w: canonical encode: %v", entmoot.ErrRosterReject, err)
	}
	if !keystore.Verify(entry.Subject.EntmootPubKey, sigInput, entry.Signature) {
		return fmt.Errorf("%w: genesis signature does not verify", entmoot.ErrRosterReject)
	}
	return nil
}

func validateEntryFormat(entry entmoot.RosterEntry, groupID entmoot.GroupID, sequence uint64, allowLegacy bool) error {
	switch entry.Version {
	case 0:
		if !allowLegacy {
			return fmt.Errorf("%w: legacy entry cannot follow a version-2 entry", entmoot.ErrRosterReject)
		}
		if entry.GroupID != nil || entry.Sequence != 0 || entry.ActorMemberID != nil {
			return fmt.Errorf("%w: legacy entry carries version-2 fields", entmoot.ErrRosterReject)
		}
		if entry.Subject.MemberID != nil {
			return fmt.Errorf("%w: legacy entry carries member_id", entmoot.ErrRosterReject)
		}
	case CurrentEntryVersion:
		if entry.GroupID == nil || *entry.GroupID != groupID {
			return fmt.Errorf("%w: version-2 entry group_id mismatch", entmoot.ErrRosterReject)
		}
		if entry.Sequence != sequence {
			return fmt.Errorf("%w: version-2 entry sequence %d, want %d", entmoot.ErrRosterReject, entry.Sequence, sequence)
		}
		if err := entmoot.ValidateMemberInfo(entry.Subject); err != nil {
			return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
		}
		if entry.Op == "add" || entry.Op == "remove" {
			if entry.Subject.MemberID == nil || entry.Subject.PeerID == "" || entry.Subject.PilotNodeID != 0 {
				return fmt.Errorf("%w: version-2 roster member requires member_id and same-key peer_id", entmoot.ErrRosterReject)
			}
		}
		if entry.ActorMemberID == nil || entry.Actor != 0 {
			return fmt.Errorf("%w: version-2 entry has invalid actor identity", entmoot.ErrRosterReject)
		}
	default:
		return fmt.Errorf("%w: unsupported roster entry version %d", entmoot.ErrRosterReject, entry.Version)
	}
	return nil
}

func (r *RosterLog) persistLocked(entry entmoot.RosterEntry, description string) error {
	if r.claimWriter != nil {
		if err := r.claimWriter(); err != nil {
			return err
		}
	}
	if r.persist != nil {
		if err := r.persist(entry); err != nil {
			return fmt.Errorf("roster: persist %s: %w", description, err)
		}
	}
	return nil
}

// ClaimWriter acquires the persistent writer lease without mutating state.
// Daemons use it at startup so a second process fails promptly; in-memory logs
// always succeed.
func (r *RosterLog) ClaimWriter() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claimWriter == nil {
		return nil
	}
	return r.claimWriter()
}

// applyLocked updates in-memory state for entry. Must be called with r.mu
// held for writing. Does NOT emit events (callers do that after unlocking).
func (r *RosterLog) applyLocked(entry entmoot.RosterEntry) {
	stored := cloneEntry(entry)
	r.byID[stored.ID] = len(r.entries)
	r.entries = append(r.entries, stored)
	r.head = stored.ID
	switch stored.Op {
	case "add":
		projected := stored.Subject
		memberID := stored.Subject.MemberID
		if memberID == nil {
			if derived, err := entmoot.MemberIDFromPublicKey(stored.Subject.EntmootPubKey); err == nil {
				memberID = &derived
				projected.MemberID = &derived
			}
		}
		if memberID != nil {
			r.membersByID[*memberID] = projected
		}
		if stored.Subject.PilotNodeID != 0 || stored.Subject.MemberID == nil {
			r.members[stored.Subject.PilotNodeID] = stored.Subject
		}
	case "remove":
		memberID := stored.Subject.MemberID
		if memberID == nil {
			if derived, err := entmoot.MemberIDFromPublicKey(stored.Subject.EntmootPubKey); err == nil {
				memberID = &derived
			}
		}
		if memberID != nil {
			delete(r.membersByID, *memberID)
			// Losing membership loses delegated authority with it.
			delete(r.admins, *memberID)
		}
		if stored.Subject.PilotNodeID != 0 || stored.Subject.MemberID == nil {
			delete(r.members, stored.Subject.PilotNodeID)
		}
	case "policy_change":
		// Membership is unchanged. A readable policy of another family leaves
		// the admin set alone; an admin policy replaces it wholesale. A policy
		// that claims to change the admin set but that this build cannot read
		// is authority-reducing: validation rejects such entries, so reaching
		// one here means the log was loaded without validation, and keeping
		// delegated authority the bytes do not state would be the unsafe
		// reading.
		if len(stored.Policy) > 0 && json.Valid(stored.Policy) &&
			!IsAdminPolicy(stored.Policy) && !IsUnknownAdminPolicy(stored.Policy) {
			break
		}
		policy, err := ParseAdminPolicy(stored.Policy)
		if err != nil {
			r.logger.Warn("roster: unreadable policy_change; clearing delegated admins",
				slog.String("entry_id", stored.ID.String()),
				slog.String("err", err.Error()))
			r.admins = make(map[entmoot.MemberID]struct{})
			break
		}
		r.admins = make(map[entmoot.MemberID]struct{}, len(policy.Admins))
		for _, admin := range policy.Admins {
			r.admins[admin] = struct{}{}
		}
	}
}

// adminInfoLocked resolves a delegated admin's current member record. An admin
// that is no longer a member has no authority. r.mu must be held.
func (r *RosterLog) adminInfoLocked(memberID entmoot.MemberID) (entmoot.NodeInfo, bool) {
	if _, delegated := r.admins[memberID]; !delegated {
		return entmoot.NodeInfo{}, false
	}
	info, member := r.membersByID[memberID]
	if !member {
		return entmoot.NodeInfo{}, false
	}
	return info, true
}

// validateAdminOpLocked enforces what a delegated admin may do. Admins exist
// so members can be invited and evicted without the founder present; changing
// who holds that authority stays with the founder, and an admin cannot remove
// another admin or the founder. r.mu must be held.
func (r *RosterLog) validateAdminOpLocked(entry entmoot.RosterEntry) error {
	switch entry.Op {
	case "add":
		return nil
	case "remove":
		subject, err := entmoot.ResolvedMemberID(entry.Subject)
		if err != nil {
			return fmt.Errorf("%w: remove subject is not resolvable", entmoot.ErrRosterReject)
		}
		if founderMemberID, err := entmoot.ResolvedMemberID(r.founder); err == nil && subject == founderMemberID {
			return fmt.Errorf("%w: an admin cannot remove the founder", entmoot.ErrRosterReject)
		}
		if _, delegated := r.admins[subject]; delegated && subject != *entry.ActorMemberID {
			return fmt.Errorf("%w: an admin cannot remove another admin", entmoot.ErrRosterReject)
		}
		return nil
	case "policy_change":
		return fmt.Errorf("%w: only the founder can change the admin set", entmoot.ErrRosterReject)
	default:
		return fmt.Errorf("%w: invalid op %q", entmoot.ErrRosterReject, entry.Op)
	}
}

// Admins returns the current delegated-admin set sorted lexicographically. The
// founder is always authoritative and is not listed.
func (r *RosterLog) Admins() []entmoot.MemberID {
	r.mu.RLock()
	out := make([]entmoot.MemberID, 0, len(r.admins))
	for id := range r.admins {
		out = append(out, id)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out
}

// CanAdminister reports whether memberID may currently sign membership
// changes: the founder, or a delegated admin that is still a member.
func (r *RosterLog) CanAdminister(memberID entmoot.MemberID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if founderMemberID, err := entmoot.ResolvedMemberID(r.founder); err == nil && memberID == founderMemberID {
		return true
	}
	_, ok := r.adminInfoLocked(memberID)
	return ok
}

// IsMemberID reports whether the full-width identity is a current member.
func (r *RosterLog) IsMemberID(memberID entmoot.MemberID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.membersByID[memberID]
	return ok
}

// MemberInfoByID returns an independent copy of the full-width member record.
func (r *RosterLog) MemberInfoByID(memberID entmoot.MemberID) (entmoot.NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.membersByID[memberID]
	if !ok {
		return entmoot.NodeInfo{}, false
	}
	return cloneNodeInfo(info), true
}

// MemberIDs returns the current full-width members sorted lexicographically.
func (r *RosterLog) MemberIDs() []entmoot.MemberID {
	r.mu.RLock()
	out := make([]entmoot.MemberID, 0, len(r.membersByID))
	for id := range r.membersByID {
		out = append(out, id)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out
}

// MemberInfoAtID resolves a full-width identity in the membership projection
// at head. The third result distinguishes a known head from an unresolved hash.
func (r *RosterLog) MemberInfoAtID(memberID entmoot.MemberID, head entmoot.RosterEntryID) (entmoot.NodeInfo, bool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	index, known := r.byID[head]
	if !known {
		return entmoot.NodeInfo{}, false, false
	}
	for i := index; i >= 0; i-- {
		entry := r.entries[i]
		if entry.Subject.MemberID == nil || *entry.Subject.MemberID != memberID {
			continue
		}
		switch entry.Op {
		case "remove":
			return entmoot.NodeInfo{}, false, true
		case "add":
			return cloneNodeInfo(entry.Subject), true, true
		}
	}
	return entmoot.NodeInfo{}, false, true
}

// HasEntry reports whether id is on this log's accepted linear chain.
func (r *RosterLog) HasEntry(id entmoot.RosterEntryID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.byID[id]
	return ok
}

// RemovedSince reports whether memberID was removed at any entry after since,
// and whether since is a known entry. An invite vouches for admission as of
// its checkpoint, so a later removal has to override it: otherwise an evicted
// member could rejoin with an invite it already holds.
func (r *RosterLog) RemovedSince(memberID entmoot.MemberID, since entmoot.RosterEntryID) (bool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	index, known := r.byID[since]
	if !known {
		return false, false
	}
	for i := index + 1; i < len(r.entries); i++ {
		entry := r.entries[i]
		if entry.Op != "remove" || entry.Subject.MemberID == nil {
			continue
		}
		if *entry.Subject.MemberID == memberID {
			return true, true
		}
	}
	return false, true
}

// Head returns the id of the current head entry, or the zero id if the log is
// empty.
func (r *RosterLog) Head() entmoot.RosterEntryID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.head
}

// HeadTimestamp returns the unix-millisecond timestamp of the current head, or
// zero if the log is empty. Entry timestamps must grow strictly, so a caller
// producing back-to-back entries needs this to pick a valid timestamp when the
// wall clock has not ticked.
func (r *RosterLog) HeadTimestamp() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return 0
	}
	return r.entries[len(r.entries)-1].Timestamp
}

// HeadIsGroupBound reports whether the current head is a version-2 entry
// signed for this log's group.
func (r *RosterLog) HeadIsGroupBound() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return false
	}
	head := r.entries[len(r.entries)-1]
	return head.Version == CurrentEntryVersion && head.GroupID != nil && *head.GroupID == r.groupID
}

// Founder returns the founder identity projected to MemberID. Immutable legacy
// genesis bytes remain untouched; callers receive the same-key operational
// identity.
func (r *RosterLog) Founder() (entmoot.NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return entmoot.NodeInfo{}, false
	}
	founder := cloneNodeInfo(r.founder)
	if founder.MemberID == nil {
		if memberID, err := entmoot.MemberIDFromPublicKey(founder.EntmootPubKey); err == nil {
			founder.MemberID = &memberID
		}
	}
	return founder, true
}

// Entries returns a deep copy of the entry slice in apply order. Useful for
// wire responses and offline diagnostics.
func (r *RosterLog) Entries() []entmoot.RosterEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]entmoot.RosterEntry, len(r.entries))
	for i := range r.entries {
		out[i] = cloneEntry(r.entries[i])
	}
	return out
}

func cloneNodeInfo(info entmoot.NodeInfo) entmoot.NodeInfo {
	out := info
	out.EntmootPubKey = append([]byte(nil), info.EntmootPubKey...)
	if info.MemberID != nil {
		memberID := *info.MemberID
		out.MemberID = &memberID
	}
	return out
}

func cloneEntry(entry entmoot.RosterEntry) entmoot.RosterEntry {
	out := entry
	out.Subject = cloneNodeInfo(entry.Subject)
	out.Policy = append([]byte(nil), entry.Policy...)
	out.Parents = append([]entmoot.RosterEntryID(nil), entry.Parents...)
	out.Signature = append([]byte(nil), entry.Signature...)
	if entry.ActorMemberID != nil {
		actor := *entry.ActorMemberID
		out.ActorMemberID = &actor
	}
	if entry.GroupID != nil {
		groupID := *entry.GroupID
		out.GroupID = &groupID
	}
	return out
}

// Close releases persistent resources and any writer lease. It serializes
// with mutation and is safe to call multiple times.
func (r *RosterLog) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.closeFn != nil {
			err = r.closeFn()
		}
	})
	return err
}
