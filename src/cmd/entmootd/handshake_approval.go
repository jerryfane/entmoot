package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

const (
	pilotCapabilityHandshakePendingNotifications = "handshake_pending_notifications"
	handshakePendingTopic                        = "handshake_pending"
	handshakeApprovalSubscribeTimeout            = 10 * time.Second
	handshakeApprovalIPCOperationTimeout         = 5 * time.Second
	handshakeApprovalPollInterval                = 2 * time.Minute
	handshakeApprovalNegativeCacheTTL            = 10 * time.Minute
	handshakeApprovalMaxUnrelatedPerSweep        = 32
)

type handshakeApprovalCacheEntry struct {
	decision string
	expires  time.Time
}

type handshakeApprovalRosterState struct {
	rosterKey string
	members   map[entmoot.NodeID][]entmoot.GroupID
}

type handshakeApprovalPilot interface {
	InfoStruct(context.Context) (ipcclient.Info, error)
	Subscribe(context.Context, string) ([]byte, *ipcclient.Subscription, error)
	PendingHandshakes(context.Context) ([]ipcclient.PendingHandshake, error)
	LookupNode(context.Context, uint32) (ipcclient.NodeIdentity, error)
	ApproveHandshake(context.Context, uint32) (map[string]interface{}, error)
}

func (r *groupRuntime) wakeHandshakeApproval() {
	if r.handshakeApprovalWake == nil {
		return
	}
	select {
	case r.handshakeApprovalWake <- struct{}{}:
	default:
	}
}

func (r *groupRuntime) watchRosterForHandshakeApproval(ctx context.Context, sess *groupSession) func() {
	if r.pilotDriver == nil || sess == nil || sess.roster == nil {
		return nil
	}
	events, cancel := sess.roster.Subscribe()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-events:
				if !ok {
					return
				}
				r.wakeHandshakeApproval()
			}
		}
	}()
	return cancel
}

func (r *groupRuntime) runHandshakeApprovalLoop(ctx context.Context) {
	cache := make(map[string]handshakeApprovalCacheEntry)
	if r.runHandshakeApprovalSubscribe(ctx, cache) {
		return
	}
	r.runHandshakeApprovalPolling(ctx, cache)
}

func (r *groupRuntime) runHandshakeApprovalSubscribe(ctx context.Context, cache map[string]handshakeApprovalCacheEntry) bool {
	if !r.pilotDriverHasHandshakeApprovalNotifications(ctx) {
		return false
	}
	subCtx, cancel := context.WithTimeout(ctx, handshakeApprovalSubscribeTimeout)
	initial, sub, err := r.pilotDriver.Subscribe(subCtx, handshakePendingTopic)
	cancel()
	if err != nil {
		if errors.Is(err, ipcclient.ErrSubscribeUnsupported) {
			r.logger.Info("handshake approval: pilot subscribe unsupported; falling back to polling")
			return false
		}
		r.logger.Warn("handshake approval: subscribe failed; falling back to polling",
			slog.String("err", err.Error()))
		return false
	}
	defer sub.Close()

	r.logger.Info("handshake approval: subscribed to pilot pending handshakes")
	r.evaluatePendingHandshakePayload(ctx, cache, initial, "subscribe_initial")
	for {
		select {
		case <-ctx.Done():
			return true
		case <-r.handshakeApprovalWake:
			r.evaluatePendingHandshakesFromPilot(ctx, cache, "wake")
		case n, ok := <-sub.Events():
			if !ok {
				return true
			}
			r.evaluatePendingHandshakePayload(ctx, cache, n.Payload, "notify")
		}
	}
}

func (r *groupRuntime) runHandshakeApprovalPolling(ctx context.Context, cache map[string]handshakeApprovalCacheEntry) {
	r.evaluatePendingHandshakesFromPilot(ctx, cache, "poll_initial")
	ticker := time.NewTicker(handshakeApprovalPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.handshakeApprovalWake:
			r.evaluatePendingHandshakesFromPilot(ctx, cache, "wake")
		case <-ticker.C:
			r.evaluatePendingHandshakesFromPilot(ctx, cache, "poll")
		}
	}
}

func (r *groupRuntime) evaluatePendingHandshakesFromPilot(ctx context.Context, cache map[string]handshakeApprovalCacheEntry, source string) {
	qctx, cancel := context.WithTimeout(ctx, handshakeApprovalIPCOperationTimeout)
	pending, err := r.pilotDriver.PendingHandshakes(qctx)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Debug("handshake approval: pending lookup failed",
				slog.String("source", source),
				slog.String("err", err.Error()))
		}
		return
	}
	r.evaluatePendingHandshakes(ctx, cache, pending, source)
}

func (r *groupRuntime) evaluatePendingHandshakePayload(ctx context.Context, cache map[string]handshakeApprovalCacheEntry, payload []byte, source string) {
	pending, err := ipcclient.DecodePendingHandshakes(payload)
	if err != nil {
		r.logger.Warn("handshake approval: decode pending snapshot",
			slog.String("source", source),
			slog.String("err", err.Error()))
		return
	}
	r.evaluatePendingHandshakes(ctx, cache, pending, source)
}

func (r *groupRuntime) evaluatePendingHandshakes(ctx context.Context, cache map[string]handshakeApprovalCacheEntry, pending []ipcclient.PendingHandshake, source string) {
	if len(pending) == 0 {
		return
	}
	state := r.handshakeApprovalRosterState()
	if len(state.members) == 0 {
		return
	}
	now := time.Now()
	sort.SliceStable(pending, func(i, j int) bool {
		_, inI := state.members[entmoot.NodeID(pending[i].NodeID)]
		_, inJ := state.members[entmoot.NodeID(pending[j].NodeID)]
		if inI != inJ {
			return inI
		}
		return pending[i].NodeID < pending[j].NodeID
	})
	unrelated := 0
	for _, p := range pending {
		if ctx.Err() != nil {
			return
		}
		peer := entmoot.NodeID(p.NodeID)
		cacheKey := handshakeApprovalCacheKey(p, state.rosterKey)
		if entry, ok := cache[cacheKey]; ok && entry.expires.After(now) {
			continue
		}
		groups := state.members[peer]
		if len(groups) == 0 {
			if unrelated >= handshakeApprovalMaxUnrelatedPerSweep {
				continue
			}
			unrelated++
			r.cacheHandshakeApprovalDecision(cache, p, state.rosterKey, "not_in_roster", now)
			continue
		}
		if peer == 0 || peer == r.nodeID || p.PublicKey == "" {
			continue
		}

		lookupCtx, cancel := context.WithTimeout(ctx, handshakeApprovalIPCOperationTimeout)
		identity, err := r.pilotDriver.LookupNode(lookupCtx, p.NodeID)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				r.logger.Debug("handshake approval: pilot lookup failed",
					slog.Uint64("peer", uint64(peer)),
					slog.String("source", source),
					slog.String("err", err.Error()))
			}
			continue
		}
		if identity.PublicKey == "" || identity.PublicKey != p.PublicKey {
			r.cacheHandshakeApprovalDecision(cache, p, state.rosterKey, "key_mismatch", now)
			r.logger.Warn("handshake approval: pending pilot key mismatch; leaving pending",
				slog.Uint64("peer", uint64(peer)),
				slog.String("lookup_source", identity.Source))
			continue
		}

		approveCtx, cancel := context.WithTimeout(ctx, handshakeApprovalIPCOperationTimeout)
		_, err = r.pilotDriver.ApproveHandshake(approveCtx, p.NodeID)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				r.logger.Warn("handshake approval: approve failed",
					slog.Uint64("peer", uint64(peer)),
					slog.String("err", err.Error()))
			}
			continue
		}
		delete(cache, cacheKey)
		r.logger.Info("handshake approval: approved roster member",
			slog.Uint64("peer", uint64(peer)),
			slog.String("groups", groupIDsLogValue(groups)))
	}
}

func (r *groupRuntime) pilotDriverHasHandshakeApprovalNotifications(ctx context.Context) bool {
	if r.pilotDriver == nil {
		return false
	}
	infoCtx, cancel := context.WithTimeout(ctx, handshakeApprovalIPCOperationTimeout)
	defer cancel()
	info, err := r.pilotDriver.InfoStruct(infoCtx)
	if err != nil {
		r.logger.Debug("handshake approval: pilot capability lookup failed",
			slog.String("capability", pilotCapabilityHandshakePendingNotifications),
			slog.String("err", err.Error()))
		return false
	}
	for _, cap := range info.Capabilities {
		if cap == pilotCapabilityHandshakePendingNotifications {
			return true
		}
	}
	return false
}

func (r *groupRuntime) cacheHandshakeApprovalDecision(cache map[string]handshakeApprovalCacheEntry, p ipcclient.PendingHandshake, rosterKey string, decision string, now time.Time) {
	cache[handshakeApprovalCacheKey(p, rosterKey)] = handshakeApprovalCacheEntry{
		decision: decision,
		expires:  now.Add(handshakeApprovalNegativeCacheTTL),
	}
}

func handshakeApprovalCacheKey(p ipcclient.PendingHandshake, rosterKey string) string {
	return fmt.Sprintf("%d|%s|%s", p.NodeID, p.PublicKey, rosterKey)
}

func (r *groupRuntime) handshakeApprovalRosterState() handshakeApprovalRosterState {
	r.mu.RLock()
	sessions := make([]*groupSession, 0, len(r.sessions))
	for _, sess := range r.sessions {
		sessions = append(sessions, sess)
	}
	r.mu.RUnlock()
	sort.Slice(sessions, func(i, j int) bool {
		return bytes.Compare(sessions[i].groupID[:], sessions[j].groupID[:]) < 0
	})

	state := handshakeApprovalRosterState{
		members: make(map[entmoot.NodeID][]entmoot.GroupID),
	}
	var key strings.Builder
	for _, sess := range sessions {
		if sess == nil || sess.roster == nil {
			continue
		}
		key.WriteString(sess.groupID.String())
		key.WriteByte('=')
		key.WriteString(sess.roster.Head().String())
		key.WriteByte(';')
		for _, member := range sess.roster.Members() {
			state.members[member] = append(state.members[member], sess.groupID)
		}
	}
	state.rosterKey = key.String()
	return state
}

func groupIDsLogValue(groups []entmoot.GroupID) string {
	if len(groups) == 0 {
		return ""
	}
	out := make([]string, 0, len(groups))
	for _, gid := range groups {
		out = append(out, gid.String())
	}
	return strings.Join(out, ",")
}
