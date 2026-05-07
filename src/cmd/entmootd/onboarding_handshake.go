package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
)

const (
	onboardingHandshakeTargetPeers = 6
	onboardingHandshakeMaxPeers    = 12
	onboardingHandshakeConcurrency = 2
	onboardingHandshakeTimeout     = 10 * time.Second
	onboardingTrustedPeersTimeout  = 5 * time.Second
	oneShotOnboardingTimeout       = 30 * time.Second
)

var (
	errPilotHandshakeUnsupported = errors.New("pilot handshake unsupported")
	onboardingHandshakeBackoffs  = []time.Duration{30 * time.Second, 2 * time.Minute, 5 * time.Minute, 15 * time.Minute}
)

type pilotHandshaker interface {
	Handshake(ctx context.Context, peer entmoot.NodeID, justification string) (map[string]interface{}, error)
}

func (r *groupRuntime) scheduleOnboardingHandshakes(sess *groupSession, invite entmoot.Invite) {
	if sess == nil || sess.ctx == nil {
		return
	}
	if _, ok := r.mux.base.(pilotHandshaker); !ok {
		r.logger.Debug("join: pilot onboarding handshakes skipped",
			slog.String("group_id", invite.GroupID.String()),
			slog.String("reason", "unsupported_transport"))
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.runOnboardingHandshakes(sess.ctx, sess, invite, r.mux)
	}()
}

func (r *groupRuntime) runOnboardingHandshakes(ctx context.Context, sess *groupSession, invite entmoot.Invite, h pilotHandshaker) {
	candidates := r.onboardingHandshakeCandidates(ctx, sess, invite)
	if len(candidates) == 0 {
		r.logger.Debug("join: no pilot onboarding handshakes needed",
			slog.String("group_id", invite.GroupID.String()))
		return
	}

	r.logger.Info("join: scheduling pilot onboarding handshakes",
		slog.String("group_id", invite.GroupID.String()),
		slog.Int("peers", len(candidates)))

	justification := "Entmoot onboarding for group " + invite.GroupID.String()
	r.runOnboardingHandshakeRounds(ctx, h, invite.GroupID, candidates, justification)
}

func (r *groupRuntime) runOneShotOnboardingHandshakes(ctx context.Context, invites map[entmoot.GroupID]entmoot.Invite) {
	if len(invites) == 0 || r == nil || r.mux == nil {
		return
	}
	h, ok := r.mux.base.(pilotHandshaker)
	if !ok {
		return
	}
	runCtx, cancel := context.WithTimeout(ctx, oneShotOnboardingTimeout)
	defer cancel()
	for _, gid := range r.ActiveGroupIDs() {
		invite, ok := invites[gid]
		if !ok {
			continue
		}
		sess, ok := r.Get(gid)
		if !ok {
			continue
		}
		r.runOneShotOnboardingHandshakeRound(runCtx, sess, invite, h)
		if runCtx.Err() != nil {
			return
		}
	}
}

func (r *groupRuntime) runOneShotOnboardingHandshakeRound(ctx context.Context, sess *groupSession, invite entmoot.Invite, h pilotHandshaker) {
	candidates := r.onboardingHandshakeCandidates(ctx, sess, invite)
	if len(candidates) == 0 {
		return
	}
	justification := "Entmoot onboarding for group " + invite.GroupID.String()
	r.runOnboardingHandshakeRound(ctx, h, invite.GroupID, candidates, justification, 0)
}

func (r *groupRuntime) onboardingHandshakeCandidates(ctx context.Context, sess *groupSession, invite entmoot.Invite) []entmoot.NodeID {
	if sess == nil || sess.roster == nil {
		return nil
	}
	trusted := r.onboardingTrustedPeers(ctx, invite.GroupID)
	return selectOnboardingHandshakePeers(r.nodeID, invite, sess.roster.Members(), trusted)
}

func (r *groupRuntime) runOnboardingHandshakeRounds(ctx context.Context, h pilotHandshaker, groupID entmoot.GroupID, peers []entmoot.NodeID, justification string) {
	pending := append([]entmoot.NodeID(nil), peers...)
	for attempt := 0; len(pending) > 0 && attempt <= len(onboardingHandshakeBackoffs); attempt++ {
		if attempt > 0 {
			backoff := onboardingHandshakeBackoffs[attempt-1] + onboardingHandshakeJitter(groupID, attempt)
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
		pending = r.runOnboardingHandshakeRound(ctx, h, groupID, pending, justification, attempt)
		if ctx.Err() != nil {
			return
		}
	}
}

func (r *groupRuntime) runOnboardingHandshakeRound(ctx context.Context, h pilotHandshaker, groupID entmoot.GroupID, peers []entmoot.NodeID, justification string, attempt int) []entmoot.NodeID {
	jobs := make(chan entmoot.NodeID)
	var wg sync.WaitGroup
	var failedMu sync.Mutex
	failedSet := make(map[entmoot.NodeID]struct{})
	workers := onboardingHandshakeConcurrency
	if len(peers) < workers {
		workers = len(peers)
	}
	finalAttempt := attempt == len(onboardingHandshakeBackoffs)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for peer := range jobs {
				if !r.sendOnboardingHandshakeAttempt(ctx, h, groupID, peer, justification, attempt, finalAttempt) {
					failedMu.Lock()
					failedSet[peer] = struct{}{}
					failedMu.Unlock()
				}
			}
		}()
	}
	for _, peer := range peers {
		select {
		case jobs <- peer:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil
		}
	}
	close(jobs)
	wg.Wait()
	failed := make([]entmoot.NodeID, 0, len(failedSet))
	for _, peer := range peers {
		if _, ok := failedSet[peer]; ok {
			failed = append(failed, peer)
		}
	}
	return failed
}

func (r *groupRuntime) onboardingTrustedPeers(ctx context.Context, groupID entmoot.GroupID) []entmoot.NodeID {
	qctx, cancel := context.WithTimeout(ctx, onboardingTrustedPeersTimeout)
	defer cancel()
	trusted, err := r.mux.TrustedPeers(qctx)
	if err != nil {
		r.logger.Debug("join: pilot trusted peer lookup failed before onboarding handshakes",
			slog.String("group_id", groupID.String()),
			slog.String("err", err.Error()))
		return nil
	}
	return trusted
}

func (r *groupRuntime) sendOnboardingHandshakeAttempt(ctx context.Context, h pilotHandshaker, groupID entmoot.GroupID, peer entmoot.NodeID, justification string, attempt int, finalAttempt bool) bool {
	hctx, cancel := context.WithTimeout(ctx, onboardingHandshakeTimeout)
	_, err := h.Handshake(hctx, peer, justification)
	cancel()
	if err == nil {
		r.logger.Info("join: pilot onboarding handshake sent",
			slog.String("group_id", groupID.String()),
			slog.Uint64("peer", uint64(peer)),
			slog.Int("attempt", attempt+1))
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if ctx.Err() != nil {
			return false
		}
	}
	if finalAttempt {
		r.logger.Debug("join: pilot onboarding handshake failed",
			slog.String("group_id", groupID.String()),
			slog.Uint64("peer", uint64(peer)),
			slog.Int("attempts", attempt+1),
			slog.String("err", err.Error()))
		return false
	}
	r.logger.Debug("join: pilot onboarding handshake retrying",
		slog.String("group_id", groupID.String()),
		slog.Uint64("peer", uint64(peer)),
		slog.Int("attempt", attempt+1),
		slog.String("err", err.Error()))
	return false
}

func onboardingHandshakeJitter(groupID entmoot.GroupID, attempt int) time.Duration {
	var seed uint64
	for _, b := range groupID {
		seed = seed*131 + uint64(b)
	}
	return time.Duration((seed+uint64(attempt)*997)%1000) * time.Millisecond
}

func selectOnboardingHandshakePeers(local entmoot.NodeID, invite entmoot.Invite, rosterMembers []entmoot.NodeID, trusted []entmoot.NodeID) []entmoot.NodeID {
	memberSet := make(map[entmoot.NodeID]struct{}, len(rosterMembers))
	for _, peer := range rosterMembers {
		if peer == 0 {
			continue
		}
		memberSet[peer] = struct{}{}
	}

	trustedSet := make(map[entmoot.NodeID]struct{}, len(trusted))
	trustedCount := 0
	for _, peer := range trusted {
		if peer == 0 || peer == local {
			continue
		}
		if _, ok := memberSet[peer]; !ok {
			continue
		}
		if _, exists := trustedSet[peer]; exists {
			continue
		}
		trustedSet[peer] = struct{}{}
		trustedCount++
	}

	out := make([]entmoot.NodeID, 0, onboardingHandshakeTargetPeers)
	added := make(map[entmoot.NodeID]struct{})
	add := func(peer entmoot.NodeID) bool {
		if peer == 0 || peer == local {
			return true
		}
		if _, ok := memberSet[peer]; !ok {
			return true
		}
		if _, ok := trustedSet[peer]; ok {
			return true
		}
		if _, ok := added[peer]; ok {
			return true
		}
		if len(out) >= onboardingHandshakeMaxPeers {
			return false
		}
		added[peer] = struct{}{}
		out = append(out, peer)
		return true
	}

	for _, peer := range invite.BootstrapPeers {
		if !add(peer.NodeID) {
			return out
		}
	}
	if !add(invite.Founder.PilotNodeID) {
		return out
	}
	if trustedCount+len(out) >= onboardingHandshakeTargetPeers {
		return out
	}
	for _, peer := range rosterMembers {
		if trustedCount+len(out) >= onboardingHandshakeTargetPeers {
			break
		}
		if !add(peer) {
			break
		}
	}
	return out
}
