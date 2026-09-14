package membership

import (
	"bytes"
	"sort"

	"entmoot/pkg/entmoot"
)

// kindRank orders records that share a timestamp. The order is a decision, not
// an accident:
//   - joins first, so somebody admitted in the same instant is a member when
//     the records that follow are judged;
//   - then rekeys, which are a leave and a join of one person;
//   - then authority records, so a removal beats a join made at the same
//     moment: admitting is recoverable, failing to remove is not;
//   - leaves last, so a member who leaves at the same instant as anything else
//     still ends up outside the group.
func kindRank(kind Kind) int {
	switch kind {
	case KindJoin:
		return 0
	case KindRekey:
		return 1
	case KindRemove, KindUnban, KindPolicy, KindRevokeInvite:
		return 2
	case KindLeave:
		return 3
	default:
		return 4
	}
}

// Project folds records onto a checkpoint and returns the resulting state
// together with the records that changed it.
//
// Every node that holds the same records computes the same membership, because
// the records are applied in an order derived from their contents — timestamp,
// then kind, then id — and never in the order they arrived. That is what makes
// concurrent writers safe: there is no head to contend for and nothing to
// repair afterwards.
//
// Records that cannot take effect — unauthorised, superseded, refused by the
// join rule, already true — are ignored rather than rejected, because a peer is
// entitled to send us records we cannot use.
func Project(base Checkpoint, records []Record) (State, []Record) {
	state := stateFrom(base)

	// Deduplicate, and drop anything this base already accounts for. A record
	// at or before the checkpoint's timestamp is inside it; applying it again
	// is exactly what would let a discarded change come back.
	seen := make(map[entmoot.RosterEntryID]struct{}, len(records))
	ordered := make([]Record, 0, len(records))
	for _, rec := range records {
		if rec.GroupID != base.GroupID || rec.Timestamp < base.Timestamp {
			continue
		}
		if _, duplicate := seen[rec.ID]; duplicate {
			continue
		}
		seen[rec.ID] = struct{}{}
		ordered = append(ordered, rec)
	}

	founderID, founderErr := entmoot.ResolvedMemberID(base.Founder)
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if left.Timestamp != right.Timestamp {
			return left.Timestamp < right.Timestamp
		}
		if leftRank, rightRank := kindRank(left.Kind), kindRank(right.Kind); leftRank != rightRank {
			return leftRank < rightRank
		}
		if founderErr == nil {
			// Within one instant and one kind, the founder's decision is read
			// before a delegated admin's.
			leftFounder, rightFounder := actorIs(left, founderID), actorIs(right, founderID)
			if leftFounder != rightFounder {
				return leftFounder
			}
		}
		return bytes.Compare(left.ID[:], right.ID[:]) < 0
	})

	effective := make([]Record, 0, len(ordered))
	for _, rec := range ordered {
		if applyRecord(&state, rec) {
			effective = append(effective, rec)
		}
	}
	return state, effective
}

func actorIs(rec Record, id entmoot.MemberID) bool {
	actor, err := entmoot.ResolvedMemberID(rec.Actor)
	return err == nil && actor == id
}

// applyRecord applies one record and reports whether the state changed.
func applyRecord(state *State, rec Record) bool {
	actor, err := entmoot.ResolvedMemberID(rec.Actor)
	if err != nil {
		return false
	}
	switch rec.Kind {
	case KindJoin:
		return admitJoin(state, rec, actor, rec.Subject, nil)
	case KindLeave:
		if _, member := state.Members[actor]; !member {
			return false
		}
		delete(state.Members, actor)
		return true
	case KindRekey:
		subject, err := entmoot.ResolvedMemberID(rec.Subject)
		if err != nil {
			return false
		}
		// A rekey only moves an existing membership, and a ban follows the
		// person: a banned member cannot rename its way back in.
		if _, member := state.Members[actor]; !member {
			return false
		}
		if _, banned := state.Banned[actor]; banned {
			return false
		}
		if _, banned := state.Banned[subject]; banned {
			return false
		}
		if _, already := state.Members[subject]; already {
			return false
		}
		delete(state.Members, actor)
		state.Members[subject] = cloneNodeInfo(rec.Subject)
		return true
	case KindRemove, KindUnban, KindPolicy, KindRevokeInvite:
		return applyAuthority(state, rec, actor)
	default:
		return false
	}
}

// applyAuthority applies a record whose effect depends on the actor holding
// authority at this point in the order.
func applyAuthority(state *State, rec Record, actor entmoot.MemberID) bool {
	isFounder := state.IsFounder(actor)
	if !isFounder && !state.CanAdminister(actor) {
		return false
	}
	switch rec.Kind {
	case KindPolicy:
		// Who may act for the founder is the founder's decision alone.
		if !isFounder {
			return false
		}
		policy := rec.Policy.Clone()
		policy.Admins = withoutFounder(SortAdmins(policy.Admins), state)
		state.Policy = policy
		return true
	case KindRemove:
		subject, err := entmoot.ResolvedMemberID(rec.Subject)
		if err != nil {
			return false
		}
		if !isFounder {
			// An admin may stand down, but removing the founder or a peer
			// admin is the founder's decision.
			if state.IsFounder(subject) {
				return false
			}
			if subject != actor && state.Policy.HasAdmin(subject) {
				return false
			}
		}
		changed := false
		if _, member := state.Members[subject]; member {
			delete(state.Members, subject)
			changed = true
		}
		if rec.Banned {
			if _, banned := state.Banned[subject]; !banned {
				state.Banned[subject] = struct{}{}
				changed = true
			}
		}
		return changed
	case KindUnban:
		// Lifting a ban is founder-only: an admin that could unban could undo
		// the founder's decision.
		if !isFounder {
			return false
		}
		subject, err := entmoot.ResolvedMemberID(rec.Subject)
		if err != nil {
			return false
		}
		if _, banned := state.Banned[subject]; !banned {
			return false
		}
		delete(state.Banned, subject)
		return true
	case KindRevokeInvite:
		if _, revoked := state.RevokedInvites[rec.InviteNonce]; revoked {
			return false
		}
		state.RevokedInvites[rec.InviteNonce] = struct{}{}
		return true
	default:
		return false
	}
}

func withoutFounder(admins []entmoot.MemberID, state *State) []entmoot.MemberID {
	founder, err := entmoot.ResolvedMemberID(state.Founder)
	if err != nil {
		return admins
	}
	out := make([]entmoot.MemberID, 0, len(admins))
	for _, admin := range admins {
		if admin == founder {
			continue
		}
		out = append(out, admin)
	}
	return out
}

// admitJoin decides whether one join takes effect, and applies it.
func admitJoin(state *State, rec Record, subject entmoot.MemberID, info entmoot.NodeInfo, rekeyFrom *entmoot.MemberID) bool {
	if _, banned := state.Banned[subject]; banned {
		return false
	}
	if _, already := state.Members[subject]; already {
		return false
	}
	if state.Policy.JoinRule != JoinRuleOpen {
		if rec.Invite == nil || !inviteAdmits(state, *rec.Invite, rec.Timestamp, subject) {
			return false
		}
	}
	state.Members[subject] = cloneNodeInfo(info)
	if rec.Invite != nil && state.Policy.JoinRule != JoinRuleOpen {
		state.InviteUses[rec.Invite.Nonce]++
	}
	return true
}

// inviteAdmits reports whether an invite still authorises one more join by this
// subject at this time. The invite's group binding and target binding against
// the signed record are already settled by VerifyRecord.
func inviteAdmits(state *State, invite entmoot.BootstrapCapability, atMS int64, subject entmoot.MemberID) bool {
	if _, revoked := state.RevokedInvites[invite.Nonce]; revoked {
		return false
	}
	if err := InviteValidAt(invite, atMS); err != nil {
		return false
	}
	founderID, err := entmoot.ResolvedMemberID(state.Founder)
	if err != nil {
		return false
	}
	inviteFounder, err := entmoot.ResolvedMemberID(invite.Founder)
	if err != nil || inviteFounder != founderID ||
		!bytes.Equal(invite.Founder.EntmootPubKey, state.Founder.EntmootPubKey) {
		return false
	}
	authority := invite.SigningAuthority()
	authorityID, err := entmoot.ResolvedMemberID(authority)
	if err != nil {
		return false
	}
	if authorityID == founderID {
		if !bytes.Equal(state.Founder.EntmootPubKey, authority.EntmootPubKey) {
			return false
		}
	} else {
		// A delegated admin's invite is worth exactly its authority now: once
		// the admin is demoted or removed, its outstanding invites stop
		// working.
		if !state.CanAdminister(authorityID) {
			return false
		}
		member, ok := state.Members[authorityID]
		if !ok || !bytes.Equal(member.EntmootPubKey, authority.EntmootPubKey) {
			return false
		}
	}
	if !invite.IsOpenInvite() {
		target, err := entmoot.MemberIDFromPublicKey(invite.TargetPublicKey)
		if err != nil || target != subject {
			return false
		}
	}
	return state.InviteUses[invite.Nonce] < invite.Uses()
}

// ExplainJoin says why a join would not take effect, so a joiner is told the
// reason instead of just failing.
func ExplainJoin(state State, rec Record) string {
	subject, err := entmoot.ResolvedMemberID(rec.Subject)
	if err != nil {
		return "the join record names an invalid identity"
	}
	if _, banned := state.Banned[subject]; banned {
		return "this identity is banned from the group"
	}
	if _, member := state.Members[subject]; member {
		return ""
	}
	if state.Policy.JoinRule == JoinRuleOpen {
		return "the group is open but the join was not applied"
	}
	if rec.Invite == nil {
		return "the group requires an invite and the join carried none"
	}
	invite := *rec.Invite
	if _, revoked := state.RevokedInvites[invite.Nonce]; revoked {
		return "the invite was revoked"
	}
	if err := InviteValidAt(invite, rec.Timestamp); err != nil {
		return "the invite is outside its validity window"
	}
	founderID, err := entmoot.ResolvedMemberID(state.Founder)
	if err != nil {
		return "the group state carries no usable founder"
	}
	if inviteFounder, err := entmoot.ResolvedMemberID(invite.Founder); err != nil || inviteFounder != founderID {
		return "the invite names a different founder than this group"
	}
	authorityID, err := entmoot.ResolvedMemberID(invite.SigningAuthority())
	if err != nil {
		return "the invite issuer identity is incomplete"
	}
	if authorityID != founderID && !state.CanAdminister(authorityID) {
		return "the invite issuer may no longer administer this group"
	}
	if !invite.IsOpenInvite() {
		if target, err := entmoot.MemberIDFromPublicKey(invite.TargetPublicKey); err != nil || target != subject {
			return "the invite is bound to a different identity"
		}
	}
	if state.InviteUses[invite.Nonce] >= invite.Uses() {
		return "the invite has no uses left"
	}
	return "the join was not applied"
}
