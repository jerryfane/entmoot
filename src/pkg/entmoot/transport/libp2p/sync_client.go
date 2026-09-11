package libp2ptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot"
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
	if genesisFounder.PilotNodeID != expectedFounder.PilotNodeID ||
		!bytes.Equal(genesisFounder.EntmootPubKey, expectedFounder.EntmootPubKey) ||
		!equalMemberID(genesisFounder.MemberID, expectedFounder.MemberID) {
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
	stream, err := h.NewStream(ctx, remote.ID, protocolID)
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
	validate func(entmoot.Message) error,
) []KeeperProgress {
	progress := make([]KeeperProgress, 0, len(keepers))
	if destination == nil || validate == nil {
		for _, keeper := range keepers {
			progress = append(progress, KeeperProgress{PeerID: keeper.ID, Err: errors.New("libp2p: destination and historical validator are required")})
		}
		return progress
	}
	for keeperIndex, keeper := range keepers {
		item := KeeperProgress{PeerID: keeper.ID}
		keeperCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := syncFromKeeper(keeperCtx, h, groupID, keeper, destination, validate, keeperIndex, &item)
		cancel()
		if err != nil {
			item.Err = err
		} else {
			item.Available = true
		}
		progress = append(progress, item)
	}
	return progress
}

func syncFromKeeper(ctx context.Context, h host.Host, groupID entmoot.GroupID, keeper peer.AddrInfo, destination store.MessageStore, validate func(entmoot.Message) error, keeperIndex int, progress *KeeperProgress) error {
	var snapshotToken string
	var generation uint64
	var afterTimestamp int64
	var afterAuthor entmoot.NodeID
	var afterID *entmoot.MessageID
	for pageNumber := 0; pageNumber < 1024; pageNumber++ {
		requestID := fmt.Sprintf("keeper-%d-page-%d", keeperIndex, pageNumber)
		listed, err := RequestHistoryPage(ctx, h, keeper, HistorySyncRequest{
			Version:          2,
			RequestID:        requestID,
			GroupID:          groupID,
			Mode:             "list",
			SnapshotToken:    snapshotToken,
			Generation:       generation,
			AfterTimestampMS: afterTimestamp,
			AfterAuthor:      afterAuthor,
			AfterID:          afterID,
			Limit:            256,
		})
		if err != nil {
			return err
		}
		progress.TransferredBytes += encodedJSONSize(listed)
		progress.Listed += len(listed.IDs)
		progress.CoverageFloorMS = listed.CoverageFloorMS
		missing := make([]entmoot.MessageID, 0, len(listed.IDs))
		for _, id := range listed.IDs {
			has, err := destination.Has(ctx, groupID, id)
			if err != nil {
				return err
			}
			if !has {
				missing = append(missing, id)
			}
		}
		for start := 0; start < len(missing); start += maxHistoryBodyItems {
			end := start + maxHistoryBodyItems
			if end > len(missing) {
				end = len(missing)
			}
			bodies, err := RequestHistoryPage(ctx, h, keeper, HistorySyncRequest{
				Version:   2,
				RequestID: fmt.Sprintf("keeper-%d-bodies-%d-%d", keeperIndex, pageNumber, start),
				GroupID:   groupID,
				Mode:      "bodies",
				IDs:       missing[start:end],
			})
			if err != nil {
				return err
			}
			progress.TransferredBytes += encodedJSONSize(bodies)
			progress.MissingBodies += len(bodies.Missing)
			for _, message := range bodies.Messages {
				if message.GroupID != groupID {
					return errors.New("libp2p: keeper returned a message from another group")
				}
				if validate != nil {
					if err := validate(message); err != nil {
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
		}
		if !listed.HasMore {
			progress.ConvergedHint = progress.MissingBodies == 0
			return nil
		}
		if progress.TransferredBytes >= 16<<20 {
			progress.BudgetExhausted = true
			progress.Continuation = &HistorySyncRequest{
				Version:          2,
				GroupID:          groupID,
				Mode:             "list",
				SnapshotToken:    listed.SnapshotToken,
				Generation:       listed.Generation,
				AfterTimestampMS: listed.NextTimestampMS,
				AfterAuthor:      listed.NextAuthor,
				AfterID:          listed.NextID,
				Limit:            256,
			}
			return nil
		}
		snapshotToken = listed.SnapshotToken
		generation = listed.Generation
		afterTimestamp = listed.NextTimestampMS
		afterAuthor = listed.NextAuthor
		afterID = listed.NextID
	}
	return errors.New("libp2p: keeper page budget exhausted")
}

func encodedJSONSize(value any) int {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return len(payload)
}
