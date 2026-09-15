package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// adoptCheckpointZero brings a group that still holds only the linear roster
// chain onto its first checkpoint, by taking the founder's from a peer.
//
// Only the founder can mint checkpoint 0 — its signature is what anchors a
// group — so every other member has to receive it. Without this a follower is
// stuck for good: it cannot be served without a checkpoint, and it cannot
// synchronise one without being served.
//
// Nothing is taken on trust. The checkpoint must be signed by the founder the
// local chain records, must name that chain's head, and must carry exactly the
// membership the chain projects; membership.Adopt re-checks the signature and
// the chain binding. A peer that offers anything else is ignored, and the group
// stays where it is.
func (r *groupRuntime) adoptCheckpointZero(ctx context.Context, groupID entmoot.GroupID) (membership.Checkpoint, bool, error) {
	legacy, err := membership.LoadLegacyChain(r.dataDir, groupID)
	if err != nil {
		return membership.Checkpoint{}, false, err
	}
	expected := legacy.Founder()
	expectedID, err := entmoot.ResolvedMemberID(expected)
	if err != nil {
		return membership.Checkpoint{}, false, fmt.Errorf("legacy chain has no usable founder: %w", err)
	}
	head := legacy.Head()
	members := make(map[entmoot.MemberID]struct{}, 8)
	for _, member := range legacy.Members() {
		id, err := entmoot.ResolvedMemberID(member)
		if err != nil {
			return membership.Checkpoint{}, false, fmt.Errorf("legacy chain member is unresolvable: %w", err)
		}
		members[id] = struct{}{}
	}

	peers, err := loadGroupPeers(r.dataDir, groupID)
	if err != nil {
		return membership.Checkpoint{}, false, err
	}
	if len(peers) == 0 {
		return membership.Checkpoint{}, false, nil
	}
	var lastErr error
	for _, remote := range peers {
		requestCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		response, err := libp2ptransport.RequestMembership(requestCtx, r.host, remote, libp2ptransport.MembershipSyncRequest{
			Version:   1,
			RequestID: fmt.Sprintf("adopt-%d", time.Now().UnixNano()),
			GroupID:   groupID,
			Limit:     1,
		})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		for _, candidate := range response.Checkpoints {
			if err := r.checkpointZeroMatchesChain(candidate, expectedID, expected, head, members); err != nil {
				r.logger.Warn("membership adopt: refused a checkpoint from a peer",
					slog.String("group_id", groupID.String()),
					slog.String("peer_id", remote.ID.String()),
					slog.String("err", err.Error()))
				lastErr = err
				continue
			}
			group, err := membership.Adopt(r.dataDir, candidate)
			if err != nil {
				lastErr = err
				continue
			}
			if err := group.Close(); err != nil {
				return membership.Checkpoint{}, false, err
			}
			r.logger.Info("membership adopt: checkpoint 0 adopted",
				slog.String("group_id", groupID.String()),
				slog.String("checkpoint", candidate.ID.String()),
				slog.String("peer_id", remote.ID.String()),
				slog.Int("members", len(candidate.Members)))
			return candidate, true, nil
		}
	}
	return membership.Checkpoint{}, false, lastErr
}

// checkpointZeroMatchesChain refuses a checkpoint that does not describe the
// chain this node already holds. The chain is the node's own evidence, so a
// checkpoint that disagrees with it is either the wrong group or an attempt to
// rewrite membership during the one moment a node has no checkpoint to compare
// against.
func (r *groupRuntime) checkpointZeroMatchesChain(
	cp membership.Checkpoint,
	founderID entmoot.MemberID,
	founder entmoot.NodeInfo,
	head entmoot.RosterEntryID,
	members map[entmoot.MemberID]struct{},
) error {
	if cp.Sequence != 0 {
		return fmt.Errorf("checkpoint %d is not a starting point", cp.Sequence)
	}
	servedFounder, err := entmoot.ResolvedMemberID(cp.Founder)
	if err != nil || servedFounder != founderID {
		return errors.New("checkpoint names a different founder than the chain")
	}
	if !equalPubKey(cp.Founder.EntmootPubKey, founder.EntmootPubKey) {
		return errors.New("checkpoint founder key does not match the chain")
	}
	if cp.LegacyHead == nil || *cp.LegacyHead != head {
		return errors.New("checkpoint does not name this node's chain head")
	}
	if len(cp.Members) != len(members) {
		return fmt.Errorf("checkpoint carries %d members, the chain projects %d", len(cp.Members), len(members))
	}
	for _, member := range cp.Members {
		id, err := entmoot.ResolvedMemberID(member)
		if err != nil {
			return errors.New("checkpoint carries an unresolvable member")
		}
		if _, ok := members[id]; !ok {
			return fmt.Errorf("checkpoint adds %s, which the chain does not carry", id.String())
		}
	}
	return nil
}

// adoptPendingGroups tries once per group that is awaiting its first
// checkpoint. It is called by serve at startup and on a slow ticker, so an
// operator upgrading the founder does not have to restart every other node.
func (r *groupRuntime) adoptPendingGroups(ctx context.Context, groupIDs []entmoot.GroupID) []entmoot.GroupID {
	adopted := make([]entmoot.GroupID, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		if membership.Exists(r.dataDir, groupID) {
			continue
		}
		if !membership.LegacyExists(r.dataDir, groupID) {
			continue
		}
		if _, ok, err := r.adoptCheckpointZero(ctx, groupID); err != nil {
			r.logger.Warn("membership adopt: no checkpoint yet",
				slog.String("group_id", groupID.String()),
				slog.String("err", err.Error()))
		} else if ok {
			adopted = append(adopted, groupID)
		} else {
			r.logger.Warn("membership adopt: group awaits checkpoint 0 from its founder",
				slog.String("group_id", groupID.String()),
				slog.String("hint", "run `entmootd membership upgrade -group "+groupID.String()+"` on the founder"))
		}
	}
	return adopted
}

func equalPubKey(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// retryPendingAdoptions keeps asking for checkpoint 0 for groups that do not
// have one yet, and starts each group as soon as it arrives. It returns at
// once; the work happens until ctx ends or nothing is left to adopt.
func (r *groupRuntime) retryPendingAdoptions(ctx context.Context, groupIDs []entmoot.GroupID) {
	pending := make([]entmoot.GroupID, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		if !membership.Exists(r.dataDir, groupID) && membership.LegacyExists(r.dataDir, groupID) {
			pending = append(pending, groupID)
		}
	}
	if len(pending) == 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for len(pending) > 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			for _, groupID := range r.adoptPendingGroups(ctx, pending) {
				if _, _, err := r.AddLocalGroup(ctx, groupID); err != nil {
					r.logger.Warn("membership adopt: group adopted but would not start",
						slog.String("group_id", groupID.String()),
						slog.String("err", err.Error()))
				}
			}
			remaining := pending[:0]
			for _, groupID := range pending {
				if !membership.Exists(r.dataDir, groupID) {
					remaining = append(remaining, groupID)
				}
			}
			pending = remaining
		}
	}()
}
