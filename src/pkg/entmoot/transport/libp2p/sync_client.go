package libp2ptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

func RequestRosterPage(ctx context.Context, h host.Host, remote peer.AddrInfo, request RosterSyncRequest) (RosterSyncResponse, error) {
	var response RosterSyncResponse
	if err := requestResponse(ctx, h, remote, RosterProtocol, request, maxSyncRequestBytes, &response, maxRosterResponse); err != nil {
		return response, err
	}
	if response.RequestID != request.RequestID || response.GroupID != request.GroupID || response.Version != 2 {
		return response, errors.New("libp2p: roster response binding mismatch")
	}
	if response.Error != "" {
		return response, fmt.Errorf("libp2p: roster sync: %s", response.Error)
	}
	return response, nil
}

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

// ValidateRosterChain replays a fetched roster in temporary state. Nothing is
// installed unless the founder anchor, every signature and the advertised head
// all validate.
func ValidateRosterChain(groupID entmoot.GroupID, expectedFounder entmoot.NodeInfo, expectedHead entmoot.RosterEntryID, entries []entmoot.RosterEntry) (*roster.RosterLog, error) {
	if len(entries) == 0 {
		return nil, errors.New("libp2p: empty roster chain")
	}
	genesisFounder := entries[0].Subject
	genesisMemberID, genesisErr := entmoot.ResolvedMemberID(genesisFounder)
	expectedMemberID, expectedErr := entmoot.ResolvedMemberID(expectedFounder)
	if genesisErr != nil || expectedErr != nil ||
		!bytes.Equal(genesisFounder.EntmootPubKey, expectedFounder.EntmootPubKey) ||
		genesisMemberID != expectedMemberID {
		return nil, errors.New("libp2p: roster founder anchor mismatch")
	}
	temporary := roster.New(groupID)
	if err := temporary.AcceptGenesis(entries[0]); err != nil {
		return nil, fmt.Errorf("libp2p: invalid roster genesis: %w", err)
	}
	for _, entry := range entries[1:] {
		if err := temporary.Apply(entry); err != nil {
			return nil, fmt.Errorf("libp2p: invalid roster entry: %w", err)
		}
	}
	if temporary.Head() != expectedHead {
		return nil, errors.New("libp2p: roster head mismatch")
	}
	return temporary, nil
}

// FetchRosterUpdates downloads one committed roster snapshot and validates the
// complete chain before returning entries missing from the local prefix.
func FetchRosterUpdates(ctx context.Context, h host.Host, remote peer.AddrInfo, groupID entmoot.GroupID, local []entmoot.RosterEntry) ([]entmoot.RosterEntry, error) {
	if len(local) == 0 {
		return nil, errors.New("libp2p: local roster is empty")
	}
	all := append([]entmoot.RosterEntry(nil), local...)
	after := uint64(len(local))
	var token string
	var committedHead entmoot.RosterEntryID
	for page := 0; page < 64; page++ {
		request := RosterSyncRequest{
			Version:       2,
			RequestID:     fmt.Sprintf("roster-%d-%d", time.Now().UnixNano(), page),
			GroupID:       groupID,
			SnapshotToken: token,
			AfterSequence: after,
			Limit:         256,
		}
		response, err := RequestRosterPage(ctx, h, remote, request)
		if err != nil {
			return nil, err
		}
		if page == 0 {
			token = response.SnapshotToken
			committedHead = response.CommittedHead
		} else if response.SnapshotToken != token || response.CommittedHead != committedHead {
			return nil, errors.New("libp2p: roster snapshot changed")
		}
		if response.NextSequence != after+uint64(len(response.Entries)) {
			return nil, errors.New("libp2p: invalid roster continuation")
		}
		all = append(all, response.Entries...)
		after = response.NextSequence
		if response.Complete {
			if _, err := ValidateRosterChain(groupID, local[0].Subject, committedHead, all); err != nil {
				return nil, err
			}
			return append([]entmoot.RosterEntry(nil), all[len(local):]...), nil
		}
		if len(response.Entries) == 0 {
			return nil, errors.New("libp2p: empty roster continuation")
		}
	}
	return nil, errors.New("libp2p: roster page budget exhausted")
}

func equalMemberID(left, right *entmoot.MemberID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func requestResponse(ctx context.Context, h host.Host, remote peer.AddrInfo, protocolID protocol.ID, request any, requestLimit int, response any, responseLimit int) error {
	if h == nil || remote.ID == "" {
		return errors.New("libp2p: local host and remote peer are required")
	}
	if err := h.Connect(ctx, remote); err != nil {
		return err
	}
	stream, err := h.NewStream(network.WithAllowLimitedConn(ctx, "Entmoot synchronization"), remote.ID, protocolID)
	if err != nil {
		return err
	}
	defer stream.Close()
	deadline := time.Now().Add(30 * time.Second)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	_ = stream.SetDeadline(deadline)
	if err := encodeJSONLimit(stream, request, requestLimit); err != nil {
		return err
	}
	if err := stream.CloseWrite(); err != nil {
		return err
	}
	return decodeJSONLimit(stream, int64(responseLimit), response)
}

type KeeperProgress struct {
	PeerID           peer.ID
	Available        bool
	Listed           int
	Inserted         int
	MissingBodies    int
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
			end := min(cursor.offset+maxHistoryBodyItems, len(listed.IDs))
			missing := make([]entmoot.MessageID, 0, end-cursor.offset)
			for _, id := range listed.IDs[cursor.offset:end] {
				has, err := destination.Has(ctx, groupID, id)
				if err != nil {
					return err
				}
				if !has {
					missing = append(missing, id)
				}
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
						return fmt.Errorf("libp2p: invalid historical message: %w", err)
					}
				}
				inserted, err := destination.Put(ctx, groupID, message)
				if err != nil {
					return err
				}
				if inserted {
					progress.Inserted++
				}
			}
			cursor.offset = end
		}
		cursor.page = nil
		if !listed.HasMore {
			progress.ConvergedHint = cursor.missingBodies == 0
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
