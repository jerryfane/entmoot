package libp2ptransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/store"
)

func RequestHistoryPage(ctx context.Context, h host.Host, remote peer.AddrInfo, request HistorySyncRequest) (HistorySyncResponse, error) {
	var response HistorySyncResponse
	limit := maxHistoryListBytes
	if request.Mode == "bodies" {
		limit = maxHistoryBodyBytes
	}
	if err := requestResponse(ctx, h, remote, HistoryProtocol, request, maxSyncRequestBytes, &response, limit); err != nil {
		return response, err
	}
	if response.RequestID != request.RequestID || response.GroupID != request.GroupID || response.Version != 2 {
		return response, errors.New("libp2p: history response binding mismatch")
	}

	if response.Error != "" {
		return response, fmt.Errorf("libp2p: history sync: %s", response.Error)
	}
	return response, nil
}

// RequestPeerRecords returns the signed peer records a member holds for other
// roster members. The bytes are unverified here; InstallPeerRecords validates
// every envelope before any address reaches the peerstore.
func RequestPeerRecords(ctx context.Context, h host.Host, remote peer.AddrInfo, groupID entmoot.GroupID) ([][]byte, error) {
	var response PeerRecordResponse
	request := PeerRecordRequest{Version: 1, GroupID: groupID}
	if err := requestResponse(ctx, h, remote, PeerRecordProtocol, request, maxSyncRequestBytes, &response, maxPeerRecordResponse); err != nil {
		return nil, err
	}
	if response.Version != 1 {
		return nil, errors.New("libp2p: peer record response version mismatch")
	}
	if response.Error != "" {
		return nil, fmt.Errorf("libp2p: peer records: %s", response.Error)
	}
	return response.Records, nil
}

// ValidateRosterChain replays a fetched roster in temporary state. Nothing is
// installed unless the founder anchor, every signature and the advertised head
// all validate.
func equalMemberID(left, right *entmoot.MemberID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// requestResponse performs one bounded request/response exchange on a stream.
func requestResponse(ctx context.Context, h host.Host, remote peer.AddrInfo, protocolID protocol.ID, request any, requestLimit int, response any, responseLimit int) error {
	if err := h.Connect(ctx, remote); err != nil {
		return fmt.Errorf("libp2p: connect %s: %w", remote.ID, err)
	}
	stream, err := h.NewStream(network.WithAllowLimitedConn(ctx, "entmoot-sync"), remote.ID, protocolID)
	if err != nil {
		return fmt.Errorf("libp2p: open %s: %w", protocolID, err)
	}
	defer stream.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}
	if err := encodeJSONLimit(stream, request, requestLimit); err != nil {
		return err
	}
	if err := stream.CloseWrite(); err != nil {
		return err
	}
	return decodeJSONLimit(stream, int64(responseLimit), response)
}

type KeeperProgress struct {
	PeerID        peer.ID
	Available     bool
	Listed        int
	Inserted      int
	MissingBodies int
	// PrunedLocally counts pruned identifiers this pass encountered from this
	// keeper: identifiers retention deliberately dropped here, offered again by
	// a peer with a longer window. It is a coverage difference, not a gap to
	// chase, so it is reported apart from MissingBodies. The same identifier
	// offered by several keepers is counted once per keeper.
	PrunedLocally int
	// UnknownHeads counts historical messages skipped because their roster
	// checkpoint is not on this node's chain yet. They are retried on a later
	// pass, after roster synchronization, rather than failing the keeper.
	UnknownHeads     int
	ConvergedHint    bool
	CoverageFloorMS  int64
	TransferredBytes int
	BudgetExhausted  bool
	Continuation     *HistorySyncRequest
	Err              error
}

// HistorySyncState retains one bounded page per keeper across interrupted and
// budget-limited passes. A caller must serialize access and reuse it for the
// same group; the daemon owns one under each group's catch-up lock.
type HistorySyncState struct {
	groupID entmoot.GroupID
	keepers map[peer.ID]*keeperSyncState
}

type keeperSyncState struct {
	request       HistorySyncRequest
	page          *HistorySyncResponse
	offset        int
	missingBodies int
	// unknownHeads counts messages this pass could not authorize yet because
	// their roster checkpoint is not on our chain. They are a real gap, so a
	// pass that skipped any of them has not converged.
	unknownHeads int
	// batch shrinks when a keeper refuses a body page, so a peer running an
	// older server that cannot truncate still makes progress.
	batch int
}

type KeeperAvailability string

const (
	NoKeeperAvailable        KeeperAvailability = "none"
	OneKeeperAvailable       KeeperAvailability = "one"
	MultipleKeepersAvailable KeeperAvailability = "multiple"
)

type SyncSummary struct {
	Availability   KeeperAvailability
	Eligible       int
	Available      int
	Inserted       int
	MissingBodies  int
	PrunedLocally  int
	UnknownHeads   int
	ConvergedHints int
}

func SummarizeKeeperProgress(progress []KeeperProgress) SyncSummary {
	summary := SyncSummary{Eligible: len(progress)}
	for _, item := range progress {
		if item.Available {
			summary.Available++
		}
		summary.Inserted += item.Inserted
		summary.MissingBodies += item.MissingBodies
		summary.PrunedLocally += item.PrunedLocally
		summary.UnknownHeads += item.UnknownHeads
		if item.ConvergedHint {
			summary.ConvergedHints++
		}
	}
	switch summary.Available {
	case 0:
		summary.Availability = NoKeeperAvailable
	case 1:
		summary.Availability = OneKeeperAvailable
	default:
		summary.Availability = MultipleKeepersAvailable
	}
	return summary
}

// SyncFromKeepers queries every eligible keeper in order. Failure or
// withholding by one keeper does not stop later keepers. A ConvergedHint is
// never reported as proof of global completeness.
func SyncFromKeepers(
	ctx context.Context,
	h host.Host,
	groupID entmoot.GroupID,
	keepers []peer.AddrInfo,
	destination store.MessageStore,
	validate func(entmoot.Message, *merkle.Proof) error,
	state *HistorySyncState,
) []KeeperProgress {
	progress := make([]KeeperProgress, 0, len(keepers))
	if destination == nil || validate == nil || state == nil {
		for _, keeper := range keepers {
			progress = append(progress, KeeperProgress{PeerID: keeper.ID, Err: errors.New("libp2p: destination, historical validator, and sync state are required")})
		}
		return progress
	}
	if state.keepers == nil || state.groupID != groupID {
		state.groupID = groupID
		state.keepers = make(map[peer.ID]*keeperSyncState)
	}
	for id := range state.keepers {
		present := false
		for _, keeper := range keepers {
			if keeper.ID == id {
				present = true
				break
			}
		}
		if !present {
			delete(state.keepers, id)
		}
	}
	// No request floor is sent. A node's coverage floor advances whenever
	// retention runs, including when it deletes nothing and for messages that
	// retention deliberately exempts, so using it as a request lower bound
	// would permanently hide history this node still wants. Only a tombstone
	// means "we dropped this on purpose", and that is checked per identifier
	// below.
	for keeperIndex, keeper := range keepers {
		item := KeeperProgress{PeerID: keeper.ID}
		cursor := state.keepers[keeper.ID]
		if cursor == nil {
			cursor = &keeperSyncState{request: HistorySyncRequest{Version: 2, GroupID: groupID, Mode: "list", Limit: 256}}
			state.keepers[keeper.ID] = cursor
		}
		keeperCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := syncFromKeeper(keeperCtx, h, groupID, keeper, destination, validate, keeperIndex, &item, cursor)
		cancel()
		if err != nil {
			item.Err = err
		} else {
			item.Available = true
		}
		if item.ConvergedHint || (err == nil && cursor.page == nil && !item.BudgetExhausted) {
			delete(state.keepers, keeper.ID)
		} else {
			continuation := cursor.request
			item.Continuation = &continuation
		}
		progress = append(progress, item)
	}
	return progress
}

func syncFromKeeper(ctx context.Context, h host.Host, groupID entmoot.GroupID, keeper peer.AddrInfo, destination store.MessageStore, validate func(entmoot.Message, *merkle.Proof) error, keeperIndex int, progress *KeeperProgress, cursor *keeperSyncState) error {
	const transferBudget = 16 << 20
	for pageNumber := 0; pageNumber < 1024; pageNumber++ {
		if cursor.page == nil {
			if progress.TransferredBytes+maxHistoryListBytes+1 > transferBudget {
				progress.BudgetExhausted = true
				return nil
			}
			cursor.request.RequestID = fmt.Sprintf("keeper-%d-page-%d", keeperIndex, pageNumber)
			listed, err := RequestHistoryPage(ctx, h, keeper, cursor.request)
			if err != nil {
				if listed.Error == SyncSnapshotExpired {
					// An expired token does not discard the tuple cursor. If the
					// store changed, a fresh-token request reports that separately.
					cursor.request.SnapshotToken = ""
					if listed.SnapshotChanged {
						cursor.request.Generation = 0
						cursor.request.AfterTimestampMS = 0
						cursor.request.AfterAuthorMemberID = entmoot.MemberID{}
						cursor.request.AfterID = nil
						cursor.missingBodies = 0
						cursor.unknownHeads = 0
					}
				}
				return err
			}
			progress.TransferredBytes += encodedJSONSize(listed) + 1
			progress.Listed += len(listed.IDs)
			cursor.page = &listed
			cursor.offset = 0
		}
		listed := cursor.page
		progress.CoverageFloorMS = listed.CoverageFloorMS
		for cursor.offset < len(listed.IDs) {
			if progress.TransferredBytes+maxHistoryBodyBytes+1 > transferBudget {
				progress.BudgetExhausted = true
				return nil
			}
			if cursor.batch <= 0 || cursor.batch > maxHistoryBodyItems {
				cursor.batch = maxHistoryBodyItems
			}
			end := min(cursor.offset+cursor.batch, len(listed.IDs))
			missing := make([]entmoot.MessageID, 0, end-cursor.offset)
			for _, id := range listed.IDs[cursor.offset:end] {
				has, err := destination.Has(ctx, groupID, id)
				if err != nil {
					return err
				}
				if has {
					continue
				}
				// A message this node pruned on purpose is absent, not
				// missing. Without this check the keeper is asked for it every
				// round and the local store refuses it with ErrPruned, so one
				// expired message stalls the whole sync forever.
				tombstoned, err := store.HasTombstone(ctx, destination, groupID, id)
				if err != nil {
					return err
				}
				if tombstoned {
					progress.PrunedLocally++
					continue
				}
				missing = append(missing, id)
			}
			if len(missing) == 0 {
				cursor.offset = end
				continue
			}
			bodies, err := RequestHistoryPage(ctx, h, keeper, HistorySyncRequest{
				Version:   2,
				RequestID: fmt.Sprintf("keeper-%d-bodies-%d-%d", keeperIndex, pageNumber, cursor.offset),
				GroupID:   groupID, Mode: "bodies", IDs: missing,
			})
			if err != nil {
				// Shrink on a refused page and on a transport failure alike: a
				// relay circuit budget smaller than the page cap cuts the stream
				// mid-response, and retrying the same size would never finish.
				// Typed server denials keep their error.
				if (bodies.Error == SyncResourceExhausted || bodies.Error == "") && cursor.batch > 1 {
					cursor.batch /= 2
					continue
				}
				return err
			}
			progress.TransferredBytes += encodedJSONSize(bodies) + 1
			progress.MissingBodies += len(bodies.Missing)
			cursor.missingBodies += len(bodies.Missing)
			proofs := make(map[entmoot.MessageID]merkle.Proof, len(bodies.LegacyProofs))
			for _, item := range bodies.LegacyProofs {
				proofs[item.MessageID] = item.Proof
			}
			for _, message := range bodies.Messages {
				if message.GroupID != groupID {
					return errors.New("libp2p: keeper returned a message from another group")
				}
				if validate != nil {
					var proof *merkle.Proof
					if item, ok := proofs[message.ID]; ok {
						itemCopy := item
						proof = &itemCopy
					}
					if err := validate(message, proof); err != nil {
						// A head this node has not synchronized yet is a
						// synchronization gap, not a bad message: skip it,
						// report it, and let the next pass retry once roster
						// sync has caught up. Anything else is still fatal for
						// this keeper, because a keeper serving invalid
						// history is not a keeper.
						if errors.Is(err, entmoot.ErrRosterHeadUnknown) {
							progress.UnknownHeads++
							cursor.unknownHeads++
							continue
						}
						return fmt.Errorf("libp2p: invalid historical message: %w", err)
					}
				}
				inserted, err := destination.Put(ctx, groupID, message)
				if err != nil {
					// Retention may have tombstoned this id between listing and
					// insertion. Refusing it is correct; aborting the sync over
					// it is not.
					if errors.Is(err, store.ErrPruned) {
						progress.PrunedLocally++
						continue
					}
					return err
				}
				if inserted {
					progress.Inserted++
				}
			}
			// Advance only past the identifiers the keeper accounted for. A
			// truncated page leaves the rest for the next request.
			served := len(bodies.Messages) + len(bodies.Missing)
			if served >= len(missing) {
				cursor.offset = end
				continue
			}
			if served == 0 {
				return errors.New("libp2p: keeper served no body for a requested message")
			}
			remaining := served
			position := cursor.offset
			for position < end && remaining > 0 {
				if slices.ContainsFunc(missing, func(id entmoot.MessageID) bool { return id == listed.IDs[position] }) {
					remaining--
				}
				position++
			}
			cursor.offset = position
		}
		cursor.page = nil
		if !listed.HasMore {
			progress.ConvergedHint = cursor.missingBodies == 0 && cursor.unknownHeads == 0
			return nil
		}
		cursor.request.SnapshotToken = listed.SnapshotToken
		cursor.request.Generation = listed.Generation
		cursor.request.AfterTimestampMS = listed.NextTimestampMS
		cursor.request.AfterAuthorMemberID = listed.NextAuthorMemberID
		cursor.request.AfterID = listed.NextID
	}
	progress.BudgetExhausted = true
	return nil
}

func encodedJSONSize(value any) int {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return len(payload)
}
