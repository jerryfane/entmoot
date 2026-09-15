// Package membership holds a group's membership as a set of signed records
// plus periodic signed checkpoints, rather than a single linear chain.
//
// Two properties follow from that shape, and both are the reason it exists.
// A joiner signs its own admission under a rule the founder signed earlier, so
// nobody has to be online to admit anyone. And a checkpoint states the
// complete membership, so the records behind it can be discarded: storage
// follows how many members a group has, not how many changes it has ever made.
//
// Records never conflict in a way that needs repair. Joins are a set union;
// the small number of records that do carry authority are ordered by one fixed
// rule — timestamp, then kind (join, rekey, authority, leave), then the
// founder's record before a delegated admin's, then record id — so every node
// that holds the same records computes the same membership. The kind rank is
// deliberate: it is what makes a removal beat a join issued in the same
// instant.
package membership

import (
	"bytes"
	"sort"

	"entmoot/pkg/entmoot"
)

// Version is the record and checkpoint format this build writes.
const Version uint8 = 3

// DefaultCheckpointEvery is how many effective records accumulate before an
// admin signs a checkpoint. Small enough that a new member downloads little
// history, large enough that checkpoints are not the common case.
const DefaultCheckpointEvery = 64

// MaxAdmins bounds the delegated-admin set, as the linear roster did: admins
// are the only concurrent writers of authority records, and a small set keeps
// their ordering easy to reason about.
const MaxAdmins = 16

// Join rules. A group's rule decides whether a join record needs an invite.
const (
	JoinRuleInvite = "invite"
	JoinRuleOpen   = "open"
)

// Kind is the operation a record performs.
type Kind string

const (
	// KindJoin admits the signer itself. Subject equals Actor.
	KindJoin Kind = "join"
	// KindLeave removes the signer itself. Subject equals Actor.
	KindLeave Kind = "leave"
	// KindRekey moves membership from the signing identity to a new one, so a
	// member can replace its own key without an admin.
	KindRekey Kind = "rekey"
	// KindRemove removes Subject. With Banned set it also bars rejoining.
	KindRemove Kind = "remove"
	// KindUnban clears a ban. Founder only.
	KindUnban Kind = "unban"
	// KindPolicy replaces the group policy wholesale. Founder only.
	KindPolicy Kind = "policy"
	// KindRevokeInvite voids an invite by nonce, so an unredeemed invite can
	// be withdrawn without touching membership.
	KindRevokeInvite Kind = "revoke_invite"
)

// Policy is the group's current rules. A policy record replaces it wholesale,
// so reading one record is enough to know what the rules became.
type Policy struct {
	JoinRule        string             `json:"join_rule"`
	CheckpointEvery int                `json:"checkpoint_every"`
	Admins          []entmoot.MemberID `json:"admins,omitempty"`
}

// DefaultPolicy is what a new group gets: invitation required, and the
// standard checkpoint cadence.
func DefaultPolicy() Policy {
	return Policy{JoinRule: JoinRuleInvite, CheckpointEvery: DefaultCheckpointEvery}
}

// Clone returns a deep copy, so a caller cannot mutate stored policy state.
func (p Policy) Clone() Policy {
	out := p
	out.Admins = append([]entmoot.MemberID(nil), p.Admins...)
	return out
}

// HasAdmin reports whether id is in the delegated-admin set.
func (p Policy) HasAdmin(id entmoot.MemberID) bool {
	for _, admin := range p.Admins {
		if admin == id {
			return true
		}
	}
	return false
}

// SortAdmins sorts and deduplicates the admin set so the same intent always
// produces the same signed bytes.
func SortAdmins(admins []entmoot.MemberID) []entmoot.MemberID {
	out := make([]entmoot.MemberID, 0, len(admins))
	seen := make(map[entmoot.MemberID]struct{}, len(admins))
	for _, admin := range admins {
		if _, duplicate := seen[admin]; duplicate {
			continue
		}
		seen[admin] = struct{}{}
		out = append(out, admin)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

// Record is one signed membership statement. Its id is the hash of its signed
// bytes, so the same statement always has the same id and applying it twice is
// a no-op.
type Record struct {
	ID      entmoot.RosterEntryID `json:"id"`
	Version uint8                 `json:"version"`
	GroupID entmoot.GroupID       `json:"group_id"`
	Kind    Kind                  `json:"kind"`
	// Actor is the signer. Its MemberID and EntmootPubKey are required and
	// must agree with each other.
	Actor entmoot.NodeInfo `json:"actor"`
	// Subject is who the record is about. Self-authored kinds repeat the
	// actor; rekey names the new identity; remove and unban name a target.
	Subject entmoot.NodeInfo `json:"subject,omitempty"`
	// Invite is the grant a join redeems. Absent when the group's rule is
	// open, and on the join half of a rekey.
	Invite *entmoot.BootstrapCapability `json:"invite,omitempty"`
	// Banned makes a removal also bar rejoining.
	Banned bool `json:"banned,omitempty"`
	// InviteNonce names the invite a revoke_invite withdraws.
	InviteNonce [32]byte `json:"invite_nonce,omitempty"`
	// Policy is the replacement policy on a policy record.
	Policy *Policy `json:"policy,omitempty"`
	// Timestamp is the signer's clock in unix milliseconds. It orders
	// authority records and decides which self record about one subject wins.
	Timestamp int64  `json:"timestamp"`
	Signature []byte `json:"signature,omitempty"`
}

// SubjectMemberID resolves who the record is about.
//
// A named member id is authoritative: a removal or unban may carry nothing
// else, because the identity it names is not a member and so no member record
// holds its key. VerifyRecord has already checked that a subject carrying both
// an id and a key agrees with itself.
func (r Record) SubjectMemberID() (entmoot.MemberID, error) {
	if r.Subject.MemberID != nil {
		return *r.Subject.MemberID, nil
	}
	return entmoot.ResolvedMemberID(r.Subject)
}

// ActorMemberID resolves who signed the record.
func (r Record) ActorMemberID() (entmoot.MemberID, error) {
	return entmoot.ResolvedMemberID(r.Actor)
}

// IsAuthority reports whether the record's effect depends on the actor holding
// founder or admin authority, rather than being a statement about itself.
func (r Record) IsAuthority() bool {
	switch r.Kind {
	case KindRemove, KindUnban, KindPolicy, KindRevokeInvite:
		return true
	default:
		return false
	}
}

// InviteUse counts how many identities have redeemed one invite. Checkpoints
// carry these counts so a use limit survives the records being discarded.
type InviteUse struct {
	Nonce [32]byte `json:"nonce"`
	Uses  int      `json:"uses"`
}

// Checkpoint is a signed statement of the complete membership at a point in
// time. It replaces the records it covers: a node that holds a checkpoint does
// not need the history behind it, and a new member downloads one instead of
// replaying a group's whole past.
type Checkpoint struct {
	ID       entmoot.RosterEntryID `json:"id"`
	Version  uint8                 `json:"version"`
	GroupID  entmoot.GroupID       `json:"group_id"`
	Sequence uint64                `json:"sequence"`
	// Previous chains checkpoints together, so a node holding an older one can
	// verify a newer one without re-downloading anything.
	Previous entmoot.RosterEntryID `json:"previous"`
	Founder  entmoot.NodeInfo      `json:"founder"`
	// Members is the complete membership, sorted by member id, founder
	// included.
	Members        []entmoot.NodeInfo `json:"members"`
	Policy         Policy             `json:"policy"`
	Banned         []entmoot.MemberID `json:"banned,omitempty"`
	RevokedInvites [][32]byte         `json:"revoked_invites,omitempty"`
	InviteUses     []InviteUse        `json:"invite_uses,omitempty"`
	// Covered is how many records this checkpoint folded in.
	Covered uint64 `json:"covered"`
	// Timestamp is the newest record timestamp this checkpoint accounts for.
	// A record older than this is already inside it and must not be applied
	// again, which is what stops a discarded change from coming back.
	Timestamp int64 `json:"timestamp"`
	// LegacyHead records the head of the linear roster chain that checkpoint 0
	// replaced, so a node can tell a genuine upgrade from a fabricated one.
	LegacyHead *entmoot.RosterEntryID `json:"legacy_head,omitempty"`
	Signer     entmoot.NodeInfo       `json:"signer"`
	Signature  []byte                 `json:"signature,omitempty"`
}

// State is the membership a checkpoint plus a set of records projects to.
type State struct {
	Founder        entmoot.NodeInfo
	Members        map[entmoot.MemberID]entmoot.NodeInfo
	Policy         Policy
	Banned         map[entmoot.MemberID]struct{}
	RevokedInvites map[[32]byte]struct{}
	InviteUses     map[[32]byte]int
}

// CanAdminister reports whether id may author authority records: the founder
// always, and a delegated admin while it is still an unbanned member.
func (s State) CanAdminister(id entmoot.MemberID) bool {
	if founder, err := entmoot.ResolvedMemberID(s.Founder); err == nil && founder == id {
		return true
	}
	if !s.Policy.HasAdmin(id) {
		return false
	}
	if _, member := s.Members[id]; !member {
		return false
	}
	_, banned := s.Banned[id]
	return !banned
}

// IsFounder reports whether id is the group's founder.
func (s State) IsFounder(id entmoot.MemberID) bool {
	founder, err := entmoot.ResolvedMemberID(s.Founder)
	return err == nil && founder == id
}

// MemberIDs returns the current membership, sorted.
func (s State) MemberIDs() []entmoot.MemberID {
	out := make([]entmoot.MemberID, 0, len(s.Members))
	for id := range s.Members {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

// Checkpoint renders the state as the body of a checkpoint at the given
// sequence. The caller signs the result.
func (s State) Checkpoint(groupID entmoot.GroupID, sequence uint64, previous entmoot.RosterEntryID, covered uint64, timestamp int64) Checkpoint {
	members := make([]entmoot.NodeInfo, 0, len(s.Members))
	for _, id := range s.MemberIDs() {
		members = append(members, s.Members[id])
	}
	banned := make([]entmoot.MemberID, 0, len(s.Banned))
	for id := range s.Banned {
		banned = append(banned, id)
	}
	sort.Slice(banned, func(i, j int) bool { return bytes.Compare(banned[i][:], banned[j][:]) < 0 })
	revoked := make([][32]byte, 0, len(s.RevokedInvites))
	for nonce := range s.RevokedInvites {
		revoked = append(revoked, nonce)
	}
	sort.Slice(revoked, func(i, j int) bool { return bytes.Compare(revoked[i][:], revoked[j][:]) < 0 })
	uses := make([]InviteUse, 0, len(s.InviteUses))
	for nonce, count := range s.InviteUses {
		if count <= 0 {
			continue
		}
		uses = append(uses, InviteUse{Nonce: nonce, Uses: count})
	}
	sort.Slice(uses, func(i, j int) bool { return bytes.Compare(uses[i].Nonce[:], uses[j].Nonce[:]) < 0 })
	return Checkpoint{
		Version:        Version,
		GroupID:        groupID,
		Sequence:       sequence,
		Previous:       previous,
		Founder:        cloneNodeInfo(s.Founder),
		Members:        members,
		Policy:         s.Policy.Clone(),
		Banned:         banned,
		RevokedInvites: revoked,
		InviteUses:     uses,
		Covered:        covered,
		Timestamp:      timestamp,
	}
}

// stateFrom builds the projection base from a checkpoint.
func stateFrom(base Checkpoint) State {
	state := State{
		Founder:        cloneNodeInfo(base.Founder),
		Members:        make(map[entmoot.MemberID]entmoot.NodeInfo, len(base.Members)),
		Policy:         base.Policy.Clone(),
		Banned:         make(map[entmoot.MemberID]struct{}, len(base.Banned)),
		RevokedInvites: make(map[[32]byte]struct{}, len(base.RevokedInvites)),
		InviteUses:     make(map[[32]byte]int, len(base.InviteUses)),
	}
	for _, member := range base.Members {
		if id, err := entmoot.ResolvedMemberID(member); err == nil {
			state.Members[id] = cloneNodeInfo(member)
		}
	}
	for _, id := range base.Banned {
		state.Banned[id] = struct{}{}
	}
	for _, nonce := range base.RevokedInvites {
		state.RevokedInvites[nonce] = struct{}{}
	}
	for _, use := range base.InviteUses {
		state.InviteUses[use.Nonce] = use.Uses
	}
	return state
}

func cloneNodeInfo(info entmoot.NodeInfo) entmoot.NodeInfo {
	out := info
	out.EntmootPubKey = bytes.Clone(info.EntmootPubKey)
	if info.MemberID != nil {
		id := *info.MemberID
		out.MemberID = &id
	}
	return out
}

func cloneRecord(rec Record) Record {
	out := rec
	out.Actor = cloneNodeInfo(rec.Actor)
	out.Subject = cloneNodeInfo(rec.Subject)
	out.Signature = bytes.Clone(rec.Signature)
	if rec.Policy != nil {
		policy := rec.Policy.Clone()
		out.Policy = &policy
	}
	if rec.Invite != nil {
		invite := *rec.Invite
		invite.TargetPublicKey = bytes.Clone(rec.Invite.TargetPublicKey)
		invite.Signature = bytes.Clone(rec.Invite.Signature)
		invite.AllowedPeerIDs = append([]string(nil), rec.Invite.AllowedPeerIDs...)
		invite.AllowedMultiaddrs = append([]string(nil), rec.Invite.AllowedMultiaddrs...)
		invite.Relays = append([]string(nil), rec.Invite.Relays...)
		invite.Founder = cloneNodeInfo(rec.Invite.Founder)
		if rec.Invite.Issuer != nil {
			issuer := cloneNodeInfo(*rec.Invite.Issuer)
			invite.Issuer = &issuer
		}
		out.Invite = &invite
	}
	return out
}

func cloneCheckpoint(cp Checkpoint) Checkpoint {
	out := cp
	out.Founder = cloneNodeInfo(cp.Founder)
	out.Signer = cloneNodeInfo(cp.Signer)
	out.Signature = bytes.Clone(cp.Signature)
	out.Policy = cp.Policy.Clone()
	out.Members = make([]entmoot.NodeInfo, 0, len(cp.Members))
	for _, member := range cp.Members {
		out.Members = append(out.Members, cloneNodeInfo(member))
	}
	out.Banned = append([]entmoot.MemberID(nil), cp.Banned...)
	out.RevokedInvites = append([][32]byte(nil), cp.RevokedInvites...)
	out.InviteUses = append([]InviteUse(nil), cp.InviteUses...)
	if cp.LegacyHead != nil {
		head := *cp.LegacyHead
		out.LegacyHead = &head
	}
	return out
}
