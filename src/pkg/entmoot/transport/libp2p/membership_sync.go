package libp2ptransport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
)

const (
	// maxMembershipResponse bounds one answer. A checkpoint carries the whole
	// member set, so this is what limits group size over the wire: roughly
	// 20k members at ~200 bytes each. It is a per-response limit, not a
	// lifetime one — a caller that cannot fit a checkpoint cannot join, and
	// says so, rather than silently syncing half a group.
	maxMembershipResponse = 4 << 20
	// maxMembershipRecords bounds the records in one answer. Records are a
	// set, so a truncated answer is still progress: the caller applies what it
	// got and asks again.
	maxMembershipRecords = 512
	// maxMembershipCheckpoints bounds checkpoints per answer. A caller far
	// behind receives them oldest-first and pages forward, because it must
	// chain each one onto the previous to check who was allowed to sign it.
	maxMembershipCheckpoints = 4
	// maxMembershipRounds bounds one pull's pages. It is a cost limit on a
	// single exchange, not on how far a node may catch up: the cursor is
	// durable in the sense that every applied record is, so the next round
	// resumes from what landed.
	maxMembershipRounds = 32
)

// MembershipSyncRequest asks a peer for the membership state this node is
// missing: newer checkpoints, and records outside them that it does not hold.
type MembershipSyncRequest struct {
	Version   uint8           `json:"version"`
	RequestID string          `json:"request_id"`
	GroupID   entmoot.GroupID `json:"group_id"`
	// Capability admits a non-member that holds an invite, so a joiner can
	// read the checkpoint it must sign itself into.
	Capability *entmoot.BootstrapCapability `json:"capability,omitempty"`
	// HaveSequence and HaveCheckpoint describe the checkpoint the caller
	// projects from. The id matters as well as the sequence: two checkpoints
	// can share a sequence, and a caller on the losing one must be told.
	HaveSequence   uint64                `json:"have_sequence,omitempty"`
	HaveCheckpoint entmoot.RosterEntryID `json:"have_checkpoint,omitempty"`
	// AfterTimestamp and AfterID are a cursor into the group's record order
	// (timestamp, then id), which is the same order on every node. The answer
	// carries records strictly after it.
	//
	// A cursor rather than a list of held ids: a list long enough to describe
	// a real backlog did not fit in the request frame, and naming only part of
	// it made the server re-serve records the caller already had, round after
	// round, without ever reaching the ones it lacked.
	AfterTimestamp int64                 `json:"after_timestamp,omitempty"`
	AfterID        entmoot.RosterEntryID `json:"after_id,omitempty"`
	Limit          int                   `json:"limit,omitempty"`
}

// MembershipSyncResponse answers with verifiable objects only: every
// checkpoint and record it carries is signed, so the caller never has to trust
// the peer that served it.
type MembershipSyncResponse struct {
	Version   uint8           `json:"version"`
	RequestID string          `json:"request_id"`
	GroupID   entmoot.GroupID `json:"group_id"`
	// Checkpoints run oldest to newest and end at the server's canonical one.
	Checkpoints []membership.Checkpoint `json:"checkpoints,omitempty"`
	Records     []membership.Record     `json:"records,omitempty"`
	Canonical   entmoot.RosterEntryID   `json:"canonical"`
	// CanonicalSequence lets a caller see how far behind it is without
	// decoding the checkpoint body.
	CanonicalSequence uint64 `json:"canonical_sequence"`
	// NextTimestamp and NextID are the cursor to continue from when a page was
	// cut short, so the next request starts where this one stopped instead of
	// walking the same prefix again.
	NextTimestamp int64                 `json:"next_timestamp,omitempty"`
	NextID        entmoot.RosterEntryID `json:"next_id,omitempty"`
	// Complete is false when checkpoints or records were cut by a page limit.
	Complete bool          `json:"complete"`
	Error    SyncErrorCode `json:"error,omitempty"`
}

// handleMembership serves checkpoints and records. It holds no per-caller
// state: every answer is computed from what this node holds at that moment,
// which is why a caller can stop and resume at any point.
func (s *SyncServer) handleMembership(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	var request MembershipSyncRequest
	if err := decodeJSONLimit(stream, maxSyncRequestBytes, &request); err != nil {
		s.writeMembership(stream, MembershipSyncResponse{Version: 1, Error: SyncMalformed})
		return
	}
	response := MembershipSyncResponse{Version: 1, RequestID: request.RequestID, GroupID: request.GroupID}
	if request.Version != 1 || request.RequestID == "" {
		response.Error = SyncMalformed
		s.writeMembership(stream, response)
		return
	}
	if err := s.authorize(stream, request.GroupID, request.Capability, MembershipProtocol); err != nil {
		// A refused caller may be a member this group removed. It cannot learn
		// that anywhere else: every door is shut to it, so without this it
		// keeps dialling forever with "unauthorized" as its only clue.
		//
		// The answer carries exactly the signed record that names it, and
		// nothing about who else is in the group. That record is verifiable,
		// so it can act on it; the error code alone is not, and no node may
		// evict itself on a peer's unproven word.
		response.Error = SyncUnauthorized
		if proof, found := s.removalRecordFor(stream, request.GroupID); found {
			response.Error = SyncNotMember
			response.Records = proof
		}
		s.writeMembership(stream, response)
		return
	}
	group, ok := s.Group(request.GroupID)
	if !ok {
		response.Error = SyncUnauthorized
		s.writeMembership(stream, response)
		return
	}
	canonical := group.Canonical()
	response.Canonical = canonical.ID
	response.CanonicalSequence = canonical.Sequence

	response.Complete = true

	// A caller already on our canonical checkpoint needs no checkpoint at all.
	// Otherwise send the ones from its sequence forward, OLDEST FIRST: each
	// one is checked against its predecessor, so a caller far behind has to
	// walk them in order rather than being handed the newest few.
	if request.HaveCheckpoint != canonical.ID {
		checkpoints := group.CheckpointsSince(request.HaveSequence)
		if len(checkpoints) > maxMembershipCheckpoints {
			checkpoints = checkpoints[:maxMembershipCheckpoints]
			response.Complete = false
		}
		response.Checkpoints = checkpoints
	}

	// Against the CALLER's bound, not ours. A caller whose bound is lower
	// than ours - behind, or rewound by a branch that reaches further while
	// dated earlier - needs the records we still hold and call covered, and
	// they are the only copy it can get. When we do not know the checkpoint
	// it names we fall back to our own bound, which is what this served
	// before.
	base := canonical
	if caller, known := group.CheckpointByID(request.HaveCheckpoint); known {
		if caller.Timestamp < canonical.Timestamp {
			base = caller
		}
	} else {
		// A checkpoint we do not hold: the caller is on a branch of its own,
		// or has none yet. We cannot read its bound - the request carries no
		// timestamp and adding a field would break every current peer, which
		// refuses unknown fields - so serve the whole window we still hold and
		// let the caller refuse what its own checkpoint covers. The window is
		// one checkpoint of lag, so this is bounded, and it is the only way a
		// node whose bound moved backwards can get those records back.
		base = membership.Checkpoint{}
	}
	limit := boundedLimit(request.Limit, maxMembershipRecords, maxMembershipRecords)
	for _, record := range group.PendingFor(base) {
		if !afterCursor(record, request.AfterTimestamp, request.AfterID) {
			continue
		}
		if len(response.Records) >= limit {
			response.Complete = false
			break
		}
		response.Records = append(response.Records, record)
		response.NextTimestamp = record.Timestamp
		response.NextID = record.ID
	}
	s.writeMembership(stream, response)
}

// afterCursor reports whether a record sorts strictly after a cursor in the
// group's record order. Group.Pending() is already sorted that way, so a page
// plus its closing cursor walks the whole set without repeating or skipping.
func afterCursor(record membership.Record, afterTimestamp int64, afterID entmoot.RosterEntryID) bool {
	if afterTimestamp == 0 && afterID == (entmoot.RosterEntryID{}) {
		return true
	}
	if record.Timestamp != afterTimestamp {
		return record.Timestamp > afterTimestamp
	}
	return bytes.Compare(record.ID[:], afterID[:]) > 0
}

// removalRecordFor finds the signed record by which this group removed the
// peer on the other end of a refused request. It is the only thing a refused
// caller is told, and it is told nothing when no such record is held: a
// checkpoint that has already folded the removal in cannot prove one identity
// is absent without disclosing every identity that is present.
func (s *SyncServer) removalRecordFor(stream network.Stream, groupID entmoot.GroupID) ([]membership.Record, bool) {
	group, ok := s.Group(groupID)
	if !ok {
		return nil, false
	}
	remote := stream.Conn().RemotePeer()
	publicKey, err := remote.ExtractPublicKey()
	if err != nil || publicKey == nil {
		publicKey = s.Host.Peerstore().PubKey(remote)
	}
	if publicKey == nil {
		return nil, false
	}
	raw, err := publicKey.Raw()
	if err != nil {
		return nil, false
	}
	binding, err := BindingFromPublicKey(raw)
	if err != nil || binding.PeerID != remote {
		return nil, false
	}
	// Ask the group which record it acted on. Anybody can sign a record
	// naming this caller, and the projection ignores the ones whose author had
	// no authority when they applied; serving one of those would tell a node
	// "you were removed" with a proof that proves nothing, and it would keep
	// asking for ever. A caller that is still a member gets nothing, because
	// then whatever it was refused for, it was not removal.
	return group.RemovalProof(binding.MemberID)
}

func (s *SyncServer) writeMembership(stream network.Stream, response MembershipSyncResponse) {
	if err := encodeJSONLimit(stream, response, maxMembershipResponse); err != nil {
		// The only honest fallback is the error code itself: the bodies did
		// not fit, and a caller that hears nothing cannot tell this from a
		// dead peer.
		_ = encodeJSONLimit(stream, MembershipSyncResponse{
			Version:   1,
			RequestID: response.RequestID,
			GroupID:   response.GroupID,
			Canonical: response.Canonical,
			Error:     SyncResourceExhausted,
		}, maxMembershipResponse)
	}
}

// RequestMembership performs one membership exchange with a peer.
func RequestMembership(ctx context.Context, h host.Host, remote peer.AddrInfo, request MembershipSyncRequest) (MembershipSyncResponse, error) {
	var response MembershipSyncResponse
	if err := requestResponse(ctx, h, remote, MembershipProtocol, request, maxSyncRequestBytes, &response, maxMembershipResponse); err != nil {
		return MembershipSyncResponse{}, err
	}
	if response.Version != 1 || response.RequestID != request.RequestID || response.GroupID != request.GroupID {
		return MembershipSyncResponse{}, errors.New("libp2p: membership response does not answer the request")
	}
	if response.Error != "" {
		return response, fmt.Errorf("libp2p: membership sync: %s", response.Error)
	}
	return response, nil
}

// ErrRemoved reports that the peer answered with the signed record by which
// the group removed this node. It is a settled answer, not a transient
// failure: retrying cannot change it.
var ErrRemoved = errors.New("libp2p: this node was removed from the group")

// FetchMembership pulls membership from a peer and applies what verifies into
// the local group. It pages until the peer has nothing more to give, or until
// maxMembershipRounds, and reports how many checkpoints and records were
// adopted and whether the peer still had more.
//
// self is this node's member id, needed only to tell a refusal apart from an
// eviction: a peer may serve the signed record that removed us, and only the
// resulting state — not the peer's word, and not the mere fact that a record
// was stored — decides whether this node has really been removed.
//
// Nothing here trusts the peer: the group verifies each checkpoint's signature
// and authority, and each record's signature, before it changes any state. A
// peer serving junk therefore costs bandwidth, not correctness.
func FetchMembership(ctx context.Context, h host.Host, remote peer.AddrInfo, group *membership.Group, self entmoot.MemberID) (checkpoints int, records int, complete bool, err error) {
	if group == nil {
		return 0, 0, false, errors.New("libp2p: membership pull requires a local group")
	}
	var firstErr error
	var cursorTimestamp int64
	var cursorID entmoot.RosterEntryID
	for round := 0; round < maxMembershipRounds; round++ {
		canonical := group.Canonical()
		request := MembershipSyncRequest{
			Version:        1,
			RequestID:      fmt.Sprintf("membership-%d-%d", time.Now().UnixNano(), round),
			GroupID:        group.GroupID(),
			HaveSequence:   canonical.Sequence,
			HaveCheckpoint: canonical.ID,
			AfterTimestamp: cursorTimestamp,
			AfterID:        cursorID,
			Limit:          maxMembershipRecords,
		}
		response, requestErr := RequestMembership(ctx, h, remote, request)
		if requestErr != nil {
			if response.Error == SyncNotMember && group.IsMemberID(self) {
				// The peer refused us and said why, with a record we can
				// check. Applying it makes this node's own view of itself
				// correct, which is the point: a node that has been removed
				// should know. The record only counts as an eviction if the
				// projection actually drops us — a stored record that changed
				// nothing proves nothing.
				for _, record := range response.Records {
					if applied, applyErr := group.Apply(record); applyErr == nil && applied {
						records++
					}
				}
				if !group.IsMemberID(self) {
					return checkpoints, records, true, ErrRemoved
				}
			}
			if firstErr == nil {
				firstErr = requestErr
			}
			return checkpoints, records, false, firstErr
		}
		progressed := false
		for _, checkpoint := range response.Checkpoints {
			applied, applyErr := group.ApplyCheckpoint(checkpoint)
			if applyErr != nil {
				if firstErr == nil {
					firstErr = applyErr
				}
				continue
			}
			if applied {
				checkpoints++
				progressed = true
			}
		}
		for _, record := range response.Records {
			applied, applyErr := group.Apply(record)
			if applyErr != nil {
				// A peer one checkpoint behind still holds records our
				// checkpoint has already folded in, and may serve them. That
				// is not a fault in the peer or in us: the record is
				// accounted for either way.
				if firstErr == nil && !errors.Is(applyErr, membership.ErrStale) {
					firstErr = applyErr
				}
				continue
			}
			if applied {
				records++
				progressed = true
			}
		}
		if response.Complete {
			return checkpoints, records, true, firstErr
		}
		// The page was cut short, so continue from where it stopped. A page
		// that neither advanced the cursor nor applied anything would repeat
		// for ever, so stop and let the next round try another peer.
		advanced := response.NextTimestamp != 0 || response.NextID != (entmoot.RosterEntryID{})
		if !advanced && !progressed {
			return checkpoints, records, false, firstErr
		}
		if advanced {
			cursorTimestamp = response.NextTimestamp
			cursorID = response.NextID
		}
	}
	return checkpoints, records, false, firstErr
}

// JoinGroup is how a non-member gets in. It reads the group's checkpoint with
// the invite, signs its own join record, and hands that record to the peer it
// read from, which is an ordinary membership exchange: there is no enrollment
// authority to ask, because the invite already is the authorisation.
func JoinGroup(ctx context.Context, h host.Host, remote peer.AddrInfo, root string, identity *keystore.Identity, capability entmoot.BootstrapCapability, applicant entmoot.NodeInfo) (*membership.Group, error) {
	request := MembershipSyncRequest{
		Version:    1,
		RequestID:  fmt.Sprintf("join-%d", time.Now().UnixNano()),
		GroupID:    capability.GroupID,
		Capability: &capability,
		Limit:      maxMembershipRecords,
	}
	response, err := RequestMembership(ctx, h, remote, request)
	if err != nil {
		return nil, err
	}
	if len(response.Checkpoints) == 0 {
		return nil, errors.New("libp2p: peer served no membership checkpoint")
	}
	// The invite pins the founder's key, and that is the joiner's only anchor:
	// everything else in the answer is checked against it. A peer that served
	// a group founded by somebody else — its own, say — would otherwise be
	// installed as if it were the group the invite names.
	anchor := response.Checkpoints[0]
	invitedFounder, err := entmoot.ResolvedMemberID(capability.Founder)
	if err != nil {
		return nil, fmt.Errorf("libp2p: invite names an unusable founder: %w", err)
	}
	servedFounder, err := entmoot.ResolvedMemberID(anchor.Founder)
	if err != nil || servedFounder != invitedFounder ||
		!bytes.Equal(anchor.Founder.EntmootPubKey, capability.Founder.EntmootPubKey) {
		return nil, errors.New("libp2p: served checkpoint names a different founder than the invite")
	}
	group, err := membership.Adopt(root, anchor)
	if err != nil {
		return nil, err
	}
	for _, checkpoint := range response.Checkpoints[1:] {
		if _, err := group.ApplyCheckpoint(checkpoint); err != nil {
			_ = group.Close()
			return nil, err
		}
	}
	for _, record := range response.Records {
		if _, err := group.Apply(record); err != nil {
			// The same tolerance a pull has: a record the checkpoint we just
			// adopted already accounts for is not a reason to refuse a join.
			if errors.Is(err, membership.ErrStale) {
				continue
			}
			_ = group.Close()
			return nil, err
		}
	}
	// The first answer may have been cut short by a page limit, and the join
	// has to be signed against the newest membership this node can reach, or a
	// record it has not seen yet could make the join ineffective.
	if !response.Complete && applicant.MemberID != nil {
		if _, _, _, err := FetchMembership(ctx, h, remote, group, *applicant.MemberID); err != nil &&
			!errors.Is(err, ErrRemoved) {
			_ = group.Close()
			return nil, err
		}
	}
	record := membership.Record{Kind: membership.KindJoin, Subject: applicant, Invite: &capability}
	signed, err := group.SignRecord(identity, record)
	if err != nil {
		_ = group.Close()
		return nil, err
	}
	// The record is valid and stored, which is not the same as effective: a
	// banned identity, a spent invite or one signed by a demoted admin all
	// produce a perfectly good record that admits nobody. Say which, in the
	// group's own terms, instead of leaving the caller with "not a member".
	if applicant.MemberID == nil || !group.IsMemberID(*applicant.MemberID) {
		reason := membership.ExplainJoin(group.State(), signed)
		_ = group.Close()
		if reason == "" {
			reason = "the group did not admit this identity"
		}
		return nil, fmt.Errorf("libp2p: join not admitted: %s", reason)
	}
	// Publish the join by handing it back: the peer applies it under the same
	// rules this node just did, and gossip carries it to everyone else.
	if err := PushMembershipRecord(ctx, h, remote, capability.GroupID, signed, &capability); err != nil {
		_ = group.Close()
		return nil, err
	}
	return group, nil
}

// PushMembershipRecord hands one signed record to a peer. A joiner uses it for
// its own join record, and it is also how a member that has just signed a
// change gets it out without waiting for the next pull.
func PushMembershipRecord(ctx context.Context, h host.Host, remote peer.AddrInfo, groupID entmoot.GroupID, record membership.Record, capability *entmoot.BootstrapCapability) error {
	request := MembershipPushRequest{
		Version:   1,
		RequestID: fmt.Sprintf("push-%d", time.Now().UnixNano()),
		GroupID:   groupID,
		Record:    record,
	}
	if capability != nil {
		request.Capability = capability
	}
	var response MembershipPushResponse
	if err := requestResponse(ctx, h, remote, MembershipPushProtocol, request, maxMembershipResponse, &response, maxSyncRequestBytes); err != nil {
		return err
	}
	if response.Version != 1 || response.RequestID != request.RequestID {
		return errors.New("libp2p: membership push response does not answer the request")
	}
	if response.Error != "" {
		return fmt.Errorf("libp2p: membership push: %s", response.Error)
	}
	return nil
}

// MembershipPushRequest carries one signed record to a peer.
type MembershipPushRequest struct {
	Version    uint8                        `json:"version"`
	RequestID  string                       `json:"request_id"`
	GroupID    entmoot.GroupID              `json:"group_id"`
	Capability *entmoot.BootstrapCapability `json:"capability,omitempty"`
	Record     membership.Record            `json:"record"`
}

// MembershipPushResponse reports whether the record was accepted. Applied is
// false for a record the receiver already held, which is not an error: the
// same statement twice is the same statement.
type MembershipPushResponse struct {
	Version   uint8         `json:"version"`
	RequestID string        `json:"request_id"`
	Applied   bool          `json:"applied"`
	Error     SyncErrorCode `json:"error,omitempty"`
	// Reason explains a refusal in the group's own terms, so a joiner learns
	// that its invite expired rather than just "rejected".
	Reason string `json:"reason,omitempty"`
}

// handleMembershipPush accepts a record from a peer. A capability admits a
// joiner pushing its own join; members push under their membership.
func (s *SyncServer) handleMembershipPush(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	var request MembershipPushRequest
	if err := decodeJSONLimit(stream, maxMembershipResponse, &request); err != nil {
		s.writePush(stream, MembershipPushResponse{Version: 1, Error: SyncMalformed})
		return
	}
	response := MembershipPushResponse{Version: 1, RequestID: request.RequestID}
	if request.Version != 1 || request.RequestID == "" {
		response.Error = SyncMalformed
		s.writePush(stream, response)
		return
	}
	if err := s.authorize(stream, request.GroupID, request.Capability, MembershipPushProtocol); err != nil {
		response.Error = SyncUnauthorized
		s.writePush(stream, response)
		return
	}
	group, ok := s.Group(request.GroupID)
	if !ok {
		response.Error = SyncUnauthorized
		s.writePush(stream, response)
		return
	}
	applied, err := group.Apply(request.Record)
	if err != nil {
		response.Error = SyncMalformed
		response.Reason = err.Error()
		if errors.Is(err, entmoot.ErrRosterReject) {
			// The record verified but the group would not have it: say why, in
			// the group's terms, so a joiner can tell a bad invite from a bad
			// signature.
			response.Reason = membership.ExplainJoin(group.State(), request.Record)
		}
		s.writePush(stream, response)
		return
	}
	response.Applied = applied
	if applied && s.MembershipGossip != nil {
		// Forward it once, so a join reaches members the joiner never spoke
		// to without the joiner needing addresses for any of them.
		s.MembershipGossip(request.GroupID, request.Record)
	}
	s.writePush(stream, response)
}

func (s *SyncServer) writePush(stream network.Stream, response MembershipPushResponse) {
	_ = encodeJSONLimit(stream, response, maxSyncRequestBytes)
}
