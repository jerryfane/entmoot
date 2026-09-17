// Package roster validates immutable legacy roster logs during conversion.
//
// The live membership layer is pkg/entmoot/membership: a set of signed records
// projected against a checkpoint, with no head to contend for. This package is
// what remains of the linear chain that preceded it, and it exists for one
// purpose - deciding whether a legacy log found on disk is authentic before
// pkg/entmoot/conversion adopts it. Nothing here appends, signs or persists;
// the exported surface is ValidateEntries, ValidateLegacyJSONL and
// CurrentEntryVersion.
//
// The rules it enforces are the ones the chain was written under: entries come
// from the founder or from a delegated admin named by a founder-signed
// policy_change (see admin.go); only the founder changes the admin set or
// removes an admin; every entry names the current head as its only parent, so
// a legacy log that branched is not authentic.
package roster

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// CurrentEntryVersion is the group-bound, domain-separated roster format. A
// legacy log may hold version 0 entries; conversion records this version for
// the ones it accepts.
const CurrentEntryVersion uint8 = 2

// chain replays a legacy log entry by entry, holding exactly the state the
// acceptance rules consult. It is not concurrent: one chain validates one log
// on the goroutine that built it.
type chain struct {
	groupID entmoot.GroupID

	// entries are the accepted entries in log order.
	entries []entmoot.RosterEntry
	// byID indexes entries by id, so a parent reference can be resolved.
	byID map[entmoot.RosterEntryID]int
	// members is the membership projection keyed by Pilot node id, which is
	// what version 0 entries name.
	members map[entmoot.NodeID]entmoot.NodeInfo
	// membersByID is the same projection keyed by member id, which is what
	// version 2 entries name.
	membersByID map[entmoot.MemberID]entmoot.NodeInfo
	// founder comes from the genesis entry's subject.
	founder entmoot.NodeInfo
	// admins is the delegated-admin set the policy_change entries leave
	// behind. The founder is never listed.
	admins map[entmoot.MemberID]struct{}
	// head is the id of the last accepted entry; zero before genesis.
	head entmoot.RosterEntryID

	// logger carries the unreadable-policy_change warning, its one use.
	logger *slog.Logger
}

func newChain(groupID entmoot.GroupID) *chain {
	return &chain{
		groupID:     groupID,
		byID:        make(map[entmoot.RosterEntryID]int),
		members:     make(map[entmoot.NodeID]entmoot.NodeInfo),
		membersByID: make(map[entmoot.MemberID]entmoot.NodeInfo),
		logger:      slog.Default(),
		admins:      make(map[entmoot.MemberID]struct{}),
	}
}

// validate performs every non-genesis acceptance check. Entry 1 never reaches
// it: both callers route the first entry to validateGenesis, so the log is
// non-empty here by construction.
func (r *chain) validate(entry entmoot.RosterEntry) error {
	founder := r.founder
	head := r.head
	var headTimestamp int64
	if len(r.entries) > 0 {
		headTimestamp = r.entries[len(r.entries)-1].Timestamp
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
		actor, isAdmin := r.adminInfo(*entry.ActorMemberID)
		if !isAdmin {
			return fmt.Errorf("%w: actor %s is not the founder or a delegated admin",
				entmoot.ErrRosterReject, entry.ActorMemberID.String())
		}
		if err := r.validateAdminOp(entry); err != nil {
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

// apply updates the projection for an accepted entry.
func (r *chain) apply(entry entmoot.RosterEntry) {
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

// adminInfo resolves a delegated admin's current member record.
//
// Delegation alone is enough to look up here: apply drops a removed member
// from r.admins in the same step it drops it from the projection (see the
// "remove" case), so a delegated id is always a current member.
func (r *chain) adminInfo(memberID entmoot.MemberID) (entmoot.NodeInfo, bool) {
	if _, delegated := r.admins[memberID]; !delegated {
		return entmoot.NodeInfo{}, false
	}
	info, member := r.membersByID[memberID]
	return info, member
}

// validateAdminOp enforces what a delegated admin may do. Admins exist
// so members can be invited and evicted without the founder present; changing
// who holds that authority stays with the founder, and an admin cannot remove
// another admin or the founder.
func (r *chain) validateAdminOp(entry entmoot.RosterEntry) error {
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
