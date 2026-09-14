package libp2ptransport

import (
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
	// maxMembershipHave bounds the ids a caller may list as already held. A
	// caller holding more sends its newest ones and re-downloads the rest.
	maxMembershipHave = 512
	// maxMembershipCheckpoints bounds checkpoints per answer: a caller far
	// behind needs the newest one, and at most a few older ones to chain it
	// to what it holds.
	maxMembershipCheckpoints = 4
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
	// HaveRecords are record ids the caller already holds, so the answer
	// carries only what is new.
	HaveRecords []entmoot.RosterEntryID `json:"have_records,omitempty"`
	Limit       int                     `json:"limit,omitempty"`
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
	// Complete is false when records were cut by the page limit. The caller
	// applies what arrived and asks again; there is nothing to resume from,
	// because a set has no order to lose.
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
	if request.Version != 1 || request.RequestID == "" || len(request.HaveRecords) > maxMembershipHave {
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
		if record, found := s.removalRecordFor(stream, request.GroupID); found {
			response.Error = SyncNotMember
			response.Records = []membership.Record{record}
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

	// A caller already on our canonical checkpoint needs no checkpoint at all.
	// Otherwise send the ones from its sequence forward: they chain, so it can
	// verify each against the previous instead of trusting this peer.
	if request.HaveCheckpoint != canonical.ID {
		from := request.HaveSequence
		checkpoints := group.CheckpointsSince(from)
		if len(checkpoints) > maxMembershipCheckpoints {
			checkpoints = checkpoints[len(checkpoints)-maxMembershipCheckpoints:]
		}
		response.Checkpoints = checkpoints
	}

	held := make(map[entmoot.RosterEntryID]struct{}, len(request.HaveRecords))
	for _, id := range request.HaveRecords {
		held[id] = struct{}{}
	}
	limit := boundedLimit(request.Limit, maxMembershipRecords, maxMembershipRecords)
	response.Complete = true
	for _, record := range group.Pending() {
		if _, seen := held[record.ID]; seen {
			continue
		}
		if len(response.Records) >= limit {
			response.Complete = false
			break
		}
		response.Records = append(response.Records, record)
	}
	s.writeMembership(stream, response)
}

// removalRecordFor finds the signed record by which this group removed the
// peer on the other end of a refused request. It is the only thing a refused
// caller is told, and it is told nothing when no such record is held: a
// checkpoint that has already folded the removal in cannot prove one identity
// is absent without disclosing every identity that is present.
func (s *SyncServer) removalRecordFor(stream network.Stream, groupID entmoot.GroupID) (membership.Record, bool) {
	group, ok := s.Group(groupID)
	if !ok {
		return membership.Record{}, false
	}
	remote := stream.Conn().RemotePeer()
	publicKey, err := remote.ExtractPublicKey()
	if err != nil || publicKey == nil {
		publicKey = s.Host.Peerstore().PubKey(remote)
	}
	if publicKey == nil {
		return membership.Record{}, false
	}
	raw, err := publicKey.Raw()
	if err != nil {
		return membership.Record{}, false
	}
	binding, err := BindingFromPublicKey(raw)
	if err != nil || binding.PeerID != remote {
		return membership.Record{}, false
	}
	for _, record := range group.Pending() {
		if record.Kind != membership.KindRemove {
			continue
		}
		subject, err := record.SubjectMemberID()
		if err != nil || subject != binding.MemberID {
			continue
		}
		return record, true
	}
	return membership.Record{}, false
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

// FetchMembership pulls once from a peer and applies what verifies into the
// local group. It returns how many checkpoints and records were adopted, and
// whether the peer had more records to give.
//
// Nothing here trusts the peer: the group verifies each checkpoint's signature
// and authority, and each record's signature, before it changes any state. A
// peer serving junk therefore costs bandwidth, not correctness.
func FetchMembership(ctx context.Context, h host.Host, remote peer.AddrInfo, group *membership.Group) (checkpoints int, records int, complete bool, err error) {
	if group == nil {
		return 0, 0, false, errors.New("libp2p: membership pull requires a local group")
	}
	canonical := group.Canonical()
	pending := group.Pending()
	have := make([]entmoot.RosterEntryID, 0, len(pending))
	// Newest first: if the caller holds more records than one request can
	// name, re-downloading the oldest is the cheapest thing to lose.
	for i := len(pending) - 1; i >= 0 && len(have) < maxMembershipHave; i-- {
		have = append(have, pending[i].ID)
	}
	request := MembershipSyncRequest{
		Version:        1,
		RequestID:      fmt.Sprintf("membership-%d", time.Now().UnixNano()),
		GroupID:        group.GroupID(),
		HaveSequence:   canonical.Sequence,
		HaveCheckpoint: canonical.ID,
		HaveRecords:    have,
		Limit:          maxMembershipRecords,
	}
	response, err := RequestMembership(ctx, h, remote, request)
	if err != nil {
		if response.Error == SyncNotMember {
			// The peer refused us and said why, with a record we can check.
			// Applying it makes this node's own view of itself correct, which
			// is the whole point: a node that has been removed should know.
			for _, record := range response.Records {
				if applied, applyErr := group.Apply(record); applyErr == nil && applied {
					records++
				}
			}
			if records > 0 {
				return 0, records, true, ErrRemoved
			}
		}
		return 0, 0, false, err
	}
	var firstErr error
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
		}
	}
	for _, record := range response.Records {
		applied, applyErr := group.Apply(record)
		if applyErr != nil {
			// A peer one checkpoint behind still holds records our checkpoint
			// has already folded in, and may serve them. That is not a fault
			// in the peer or in us: the record is accounted for either way.
			if firstErr == nil && !errors.Is(applyErr, membership.ErrStale) {
				firstErr = applyErr
			}
			continue
		}
		if applied {
			records++
		}
	}
	return checkpoints, records, response.Complete, firstErr
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
	group, err := membership.Adopt(root, response.Checkpoints[0])
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
