package membership

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
)

// Domain separators keep a record signature from ever being read as a
// checkpoint, a message or a roster entry.
const (
	recordDomain     = "entmoot/membership-record/v3\x00"
	checkpointDomain = "entmoot/membership-checkpoint/v3\x00"
)

// RecordSigningBytes returns the exact bytes a record signature covers: the
// canonical encoding with the id and signature zeroed, behind a domain tag.
func RecordSigningBytes(rec Record) ([]byte, error) {
	signing := rec
	signing.ID = entmoot.RosterEntryID{}
	signing.Signature = nil
	encoded, err := canonical.Encode(signing)
	if err != nil {
		return nil, err
	}
	return append([]byte(recordDomain), encoded...), nil
}

// RecordID is the hash of a record's signing bytes, so an identical statement
// always has an identical id and applying it twice changes nothing.
func RecordID(rec Record) (entmoot.RosterEntryID, error) {
	encoded, err := RecordSigningBytes(rec)
	if err != nil {
		return entmoot.RosterEntryID{}, err
	}
	return entmoot.RosterEntryID(sha256.Sum256(encoded)), nil
}

// CheckpointSigningBytes returns the exact bytes a checkpoint signature covers.
func CheckpointSigningBytes(cp Checkpoint) ([]byte, error) {
	signing := cp
	signing.ID = entmoot.RosterEntryID{}
	signing.Signature = nil
	encoded, err := canonical.Encode(signing)
	if err != nil {
		return nil, err
	}
	return append([]byte(checkpointDomain), encoded...), nil
}

// CheckpointID is the hash of a checkpoint's signing bytes.
func CheckpointID(cp Checkpoint) (entmoot.RosterEntryID, error) {
	encoded, err := CheckpointSigningBytes(cp)
	if err != nil {
		return entmoot.RosterEntryID{}, err
	}
	return entmoot.RosterEntryID(sha256.Sum256(encoded)), nil
}

// SignRecord fills in the version, id and signature. The actor must be the
// signing identity: a record is a statement by whoever signs it.
func SignRecord(identity *keystore.Identity, rec Record) (Record, error) {
	if identity == nil {
		return Record{}, fmt.Errorf("%w: signing identity is required", entmoot.ErrRosterReject)
	}
	if !bytes.Equal(identity.PublicKey, rec.Actor.EntmootPubKey) {
		return Record{}, fmt.Errorf("%w: record actor is not the signing identity", entmoot.ErrRosterReject)
	}
	rec.Version = Version
	rec.Signature = nil
	payload, err := RecordSigningBytes(rec)
	if err != nil {
		return Record{}, err
	}
	rec.ID = entmoot.RosterEntryID(sha256.Sum256(payload))
	rec.Signature = identity.Sign(payload)
	if err := VerifyRecord(rec); err != nil {
		return Record{}, err
	}
	return rec, nil
}

// SignCheckpoint fills in the version, id and signature, with the signer set
// to the signing identity.
func SignCheckpoint(identity *keystore.Identity, signer entmoot.NodeInfo, cp Checkpoint) (Checkpoint, error) {
	if identity == nil {
		return Checkpoint{}, fmt.Errorf("%w: signing identity is required", entmoot.ErrRosterReject)
	}
	if !bytes.Equal(identity.PublicKey, signer.EntmootPubKey) {
		return Checkpoint{}, fmt.Errorf("%w: checkpoint signer is not the signing identity", entmoot.ErrRosterReject)
	}
	cp.Version = Version
	cp.Signer = cloneNodeInfo(signer)
	cp.Signature = nil
	payload, err := CheckpointSigningBytes(cp)
	if err != nil {
		return Checkpoint{}, err
	}
	cp.ID = entmoot.RosterEntryID(sha256.Sum256(payload))
	cp.Signature = identity.Sign(payload)
	if err := VerifyCheckpoint(cp); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}

// VerifyRecord checks everything about a record that does not depend on group
// state: version, id, identity binding, the field rules for its kind, and the
// actor's signature. Whether the actor had the authority its kind requires is
// decided by projection against the group's state, not here.
func VerifyRecord(rec Record) error {
	if rec.Version != Version {
		return fmt.Errorf("%w: unsupported record version %d", entmoot.ErrRosterReject, rec.Version)
	}
	if rec.GroupID == (entmoot.GroupID{}) {
		return fmt.Errorf("%w: record names no group", entmoot.ErrRosterReject)
	}
	if rec.Timestamp <= 0 {
		return fmt.Errorf("%w: record timestamp must be positive", entmoot.ErrRosterReject)
	}
	if err := entmoot.ValidateOperationalMemberInfo(rec.Actor); err != nil {
		return fmt.Errorf("%w: invalid actor: %v", entmoot.ErrRosterReject, err)
	}
	if err := verifyRecordFields(rec); err != nil {
		return err
	}
	want, err := RecordID(rec)
	if err != nil {
		return fmt.Errorf("%w: encode record: %v", entmoot.ErrRosterReject, err)
	}
	if want != rec.ID {
		return fmt.Errorf("%w: record id does not match its contents", entmoot.ErrRosterReject)
	}
	payload, err := RecordSigningBytes(rec)
	if err != nil {
		return fmt.Errorf("%w: encode record: %v", entmoot.ErrRosterReject, err)
	}
	if len(rec.Actor.EntmootPubKey) != ed25519.PublicKeySize ||
		!keystore.Verify(rec.Actor.EntmootPubKey, payload, rec.Signature) {
		return fmt.Errorf("%w: record signature does not verify", entmoot.ErrSigInvalid)
	}
	return nil
}

func verifyRecordFields(rec Record) error {
	sameIdentity := func() error {
		actor, err := entmoot.ResolvedMemberID(rec.Actor)
		if err != nil {
			return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
		}
		subject, err := entmoot.ResolvedMemberID(rec.Subject)
		if err != nil {
			return fmt.Errorf("%w: invalid subject: %v", entmoot.ErrRosterReject, err)
		}
		if actor != subject || !bytes.Equal(rec.Actor.EntmootPubKey, rec.Subject.EntmootPubKey) {
			return fmt.Errorf("%w: %s must be signed by its subject", entmoot.ErrRosterReject, rec.Kind)
		}
		return nil
	}
	noPolicy := func() error {
		if rec.Policy != nil {
			return fmt.Errorf("%w: %s must not carry a policy", entmoot.ErrRosterReject, rec.Kind)
		}
		return nil
	}
	noNonce := func() error {
		if rec.InviteNonce != ([32]byte{}) {
			return fmt.Errorf("%w: %s must not name an invite nonce", entmoot.ErrRosterReject, rec.Kind)
		}
		return nil
	}
	noBan := func() error {
		if rec.Banned {
			return fmt.Errorf("%w: only a removal may set banned", entmoot.ErrRosterReject)
		}
		return nil
	}

	switch rec.Kind {
	case KindJoin:
		if err := sameIdentity(); err != nil {
			return err
		}
		if err := noPolicy(); err != nil {
			return err
		}
		if err := noNonce(); err != nil {
			return err
		}
		if err := noBan(); err != nil {
			return err
		}
		if rec.Invite != nil {
			if err := VerifyInviteSignature(*rec.Invite); err != nil {
				return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
			}
			if rec.Invite.GroupID != rec.GroupID {
				return fmt.Errorf("%w: invite names another group", entmoot.ErrRosterReject)
			}
			if !rec.Invite.IsOpenInvite() && !bytes.Equal(rec.Invite.TargetPublicKey, rec.Subject.EntmootPubKey) {
				return fmt.Errorf("%w: invite is bound to another identity", entmoot.ErrRosterReject)
			}
		}
		return nil
	case KindLeave:
		if err := sameIdentity(); err != nil {
			return err
		}
		if rec.Invite != nil {
			return fmt.Errorf("%w: leave must not carry an invite", entmoot.ErrRosterReject)
		}
		if err := noPolicy(); err != nil {
			return err
		}
		if err := noNonce(); err != nil {
			return err
		}
		return noBan()
	case KindRekey:
		actor, err := entmoot.ResolvedMemberID(rec.Actor)
		if err != nil {
			return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
		}
		if err := entmoot.ValidateOperationalMemberInfo(rec.Subject); err != nil {
			return fmt.Errorf("%w: invalid rekey subject: %v", entmoot.ErrRosterReject, err)
		}
		subject, err := entmoot.ResolvedMemberID(rec.Subject)
		if err != nil {
			return fmt.Errorf("%w: invalid rekey subject: %v", entmoot.ErrRosterReject, err)
		}
		if actor == subject {
			return fmt.Errorf("%w: rekey must name a different identity", entmoot.ErrRosterReject)
		}
		if rec.Invite != nil {
			return fmt.Errorf("%w: rekey must not carry an invite", entmoot.ErrRosterReject)
		}
		if err := noPolicy(); err != nil {
			return err
		}
		if err := noNonce(); err != nil {
			return err
		}
		return noBan()
	case KindRemove, KindUnban:
		if rec.Subject.MemberID == nil {
			return fmt.Errorf("%w: %s must name a member id", entmoot.ErrRosterReject, rec.Kind)
		}
		if err := entmoot.ValidateMemberInfo(rec.Subject); err != nil {
			return fmt.Errorf("%w: invalid subject: %v", entmoot.ErrRosterReject, err)
		}
		if rec.Invite != nil {
			return fmt.Errorf("%w: %s must not carry an invite", entmoot.ErrRosterReject, rec.Kind)
		}
		if err := noPolicy(); err != nil {
			return err
		}
		if err := noNonce(); err != nil {
			return err
		}
		if rec.Kind == KindUnban {
			return noBan()
		}
		return nil
	case KindPolicy:
		if rec.Policy == nil {
			return fmt.Errorf("%w: policy record carries no policy", entmoot.ErrRosterReject)
		}
		if err := ValidatePolicy(*rec.Policy); err != nil {
			return err
		}
		if rec.Subject.MemberID != nil || len(rec.Subject.EntmootPubKey) != 0 {
			return fmt.Errorf("%w: policy record must not name a subject", entmoot.ErrRosterReject)
		}
		if rec.Invite != nil {
			return fmt.Errorf("%w: policy record must not carry an invite", entmoot.ErrRosterReject)
		}
		if err := noNonce(); err != nil {
			return err
		}
		return noBan()
	case KindRevokeInvite:
		if rec.InviteNonce == ([32]byte{}) {
			return fmt.Errorf("%w: revoke_invite must name a nonce", entmoot.ErrRosterReject)
		}
		if rec.Subject.MemberID != nil || len(rec.Subject.EntmootPubKey) != 0 {
			return fmt.Errorf("%w: revoke_invite must not name a subject", entmoot.ErrRosterReject)
		}
		if rec.Invite != nil {
			return fmt.Errorf("%w: revoke_invite must not carry an invite", entmoot.ErrRosterReject)
		}
		if err := noPolicy(); err != nil {
			return err
		}
		return noBan()
	default:
		return fmt.Errorf("%w: unknown record kind %q", entmoot.ErrRosterReject, rec.Kind)
	}
}

// ValidatePolicy enforces the shape every node must agree on, so two nodes
// cannot read the same policy differently.
func ValidatePolicy(policy Policy) error {
	switch policy.JoinRule {
	case JoinRuleInvite, JoinRuleOpen:
	default:
		return fmt.Errorf("%w: unknown join rule %q", entmoot.ErrRosterReject, policy.JoinRule)
	}
	if policy.CheckpointEvery < 1 {
		return fmt.Errorf("%w: checkpoint_every must be at least 1", entmoot.ErrRosterReject)
	}
	if len(policy.Admins) > MaxAdmins {
		return fmt.Errorf("%w: %d admins exceeds the ceiling of %d", entmoot.ErrRosterReject, len(policy.Admins), MaxAdmins)
	}
	for i, admin := range policy.Admins {
		if admin == (entmoot.MemberID{}) {
			return fmt.Errorf("%w: admin set contains the zero member id", entmoot.ErrRosterReject)
		}
		if i > 0 && bytes.Compare(policy.Admins[i-1][:], admin[:]) >= 0 {
			return fmt.Errorf("%w: admin set must be sorted and free of duplicates", entmoot.ErrRosterReject)
		}
	}
	return nil
}

// VerifyCheckpoint checks everything about a checkpoint that does not depend
// on the node's own state. Whether the signer held authority at the previous
// checkpoint is decided when the checkpoint is applied, because only then is
// the previous one known.
func VerifyCheckpoint(cp Checkpoint) error {
	if cp.Version != Version {
		return fmt.Errorf("%w: unsupported checkpoint version %d", entmoot.ErrRosterReject, cp.Version)
	}
	if cp.GroupID == (entmoot.GroupID{}) {
		return fmt.Errorf("%w: checkpoint names no group", entmoot.ErrRosterReject)
	}
	if cp.Timestamp <= 0 {
		return fmt.Errorf("%w: checkpoint timestamp must be positive", entmoot.ErrRosterReject)
	}
	if err := entmoot.ValidateOperationalMemberInfo(cp.Founder); err != nil {
		return fmt.Errorf("%w: invalid founder: %v", entmoot.ErrRosterReject, err)
	}
	if err := entmoot.ValidateOperationalMemberInfo(cp.Signer); err != nil {
		return fmt.Errorf("%w: invalid signer: %v", entmoot.ErrRosterReject, err)
	}
	if err := ValidatePolicy(cp.Policy); err != nil {
		return err
	}
	founderID, err := entmoot.ResolvedMemberID(cp.Founder)
	if err != nil {
		return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
	}
	if cp.Policy.HasAdmin(founderID) {
		return fmt.Errorf("%w: the founder must not be listed as a delegated admin", entmoot.ErrRosterReject)
	}
	founderPresent := false
	var previousID entmoot.MemberID
	for i, member := range cp.Members {
		if err := entmoot.ValidateMemberInfo(member); err != nil {
			return fmt.Errorf("%w: invalid member: %v", entmoot.ErrRosterReject, err)
		}
		id, err := entmoot.ResolvedMemberID(member)
		if err != nil {
			return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
		}
		if i > 0 && bytes.Compare(previousID[:], id[:]) >= 0 {
			return fmt.Errorf("%w: members must be sorted and free of duplicates", entmoot.ErrRosterReject)
		}
		previousID = id
		if id == founderID {
			founderPresent = true
		}
	}
	if !founderPresent {
		return fmt.Errorf("%w: checkpoint omits the founder", entmoot.ErrRosterReject)
	}
	if err := sortedUnique("banned member ids", len(cp.Banned), func(i int) []byte { return cp.Banned[i][:] }); err != nil {
		return err
	}
	if err := sortedUnique("revoked invites", len(cp.RevokedInvites), func(i int) []byte { return cp.RevokedInvites[i][:] }); err != nil {
		return err
	}
	if err := sortedUnique("invite uses", len(cp.InviteUses), func(i int) []byte { return cp.InviteUses[i].Nonce[:] }); err != nil {
		return err
	}
	for _, use := range cp.InviteUses {
		if use.Uses <= 0 {
			return fmt.Errorf("%w: invite use count must be positive", entmoot.ErrRosterReject)
		}
	}
	if cp.Sequence == 0 {
		if cp.Previous != (entmoot.RosterEntryID{}) {
			return fmt.Errorf("%w: the first checkpoint must not name a previous one", entmoot.ErrRosterReject)
		}
		signerID, err := entmoot.ResolvedMemberID(cp.Signer)
		if err != nil {
			return fmt.Errorf("%w: %v", entmoot.ErrRosterReject, err)
		}
		if signerID != founderID {
			return fmt.Errorf("%w: the first checkpoint must be signed by the founder", entmoot.ErrRosterReject)
		}
	} else if cp.Previous == (entmoot.RosterEntryID{}) {
		return fmt.Errorf("%w: checkpoint %d names no previous checkpoint", entmoot.ErrRosterReject, cp.Sequence)
	}
	if cp.Sequence != 0 && cp.LegacyHead != nil {
		return fmt.Errorf("%w: only the first checkpoint may name a legacy head", entmoot.ErrRosterReject)
	}
	want, err := CheckpointID(cp)
	if err != nil {
		return fmt.Errorf("%w: encode checkpoint: %v", entmoot.ErrRosterReject, err)
	}
	if want != cp.ID {
		return fmt.Errorf("%w: checkpoint id does not match its contents", entmoot.ErrRosterReject)
	}
	payload, err := CheckpointSigningBytes(cp)
	if err != nil {
		return fmt.Errorf("%w: encode checkpoint: %v", entmoot.ErrRosterReject, err)
	}
	if len(cp.Signer.EntmootPubKey) != ed25519.PublicKeySize ||
		!keystore.Verify(cp.Signer.EntmootPubKey, payload, cp.Signature) {
		return fmt.Errorf("%w: checkpoint signature does not verify", entmoot.ErrSigInvalid)
	}
	return nil
}

func sortedUnique(what string, n int, at func(int) []byte) error {
	for i := 1; i < n; i++ {
		if bytes.Compare(at(i-1), at(i)) >= 0 {
			return fmt.Errorf("%w: %s must be sorted and free of duplicates", entmoot.ErrRosterReject, what)
		}
	}
	return nil
}
