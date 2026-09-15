package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/conversion"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/merkle"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/profile"
	"entmoot/pkg/entmoot/ratelimit"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// maxProfileLifetimeMS bounds how long one published name stays current
// without being republished. The author's own expiry is honoured when it is
// shorter; a longer or missing one is clamped to this, so a single message
// cannot keep a name alive indefinitely.
const maxProfileLifetime = 90 * 24 * time.Hour

// maxProfileLifetimeMS is the same bound in the unit the records carry.
const maxProfileLifetimeMS = int64(maxProfileLifetime / time.Millisecond)

var (
	errLocalGroupNotMember        = errors.New("local identity is not a current group member")
	errLocalGroupIdentityMismatch = errors.New("local identity does not match group roster member")
)

type groupRuntimeConfig struct {
	Identity *keystore.Identity
	DataDir  string
	Store    *store.SQLite
	Notify   *notifyingStore
	Host     host.Host
	Binding  libp2ptransport.Binding
	Logger   *slog.Logger
	// Profiles records member display names observed from the group. Optional:
	// a runtime without it simply does not learn names.
	Profiles esphttp.StateStore
	// Mode and ControlledRelays mirror the host's connectivity profile so
	// forwarded member addresses are filtered exactly as dial hints are.
	Mode             libp2ptransport.ConnectivityMode
	ControlledRelays []peer.AddrInfo
}

type groupRuntime struct {
	identity         *keystore.Identity
	dataDir          string
	store            *store.SQLite
	notify           *notifyingStore
	host             host.Host
	binding          libp2ptransport.Binding
	logger           *slog.Logger
	mode             libp2ptransport.ConnectivityMode
	controlledRelays []peer.AddrInfo
	policyStore      *entpolicy.FileStore
	invites          *libp2ptransport.InviteLedger
	liveRouter       *libp2ptransport.LiveRouter
	policyEnforcers  map[entmoot.GroupID]*groupPolicyEnforcer
	profiles         esphttp.StateStore

	mu       sync.RWMutex
	sessions map[entmoot.GroupID]*groupSession
	joining  map[entmoot.GroupID]chan struct{}
	closed   bool
}

type groupSession struct {
	groupID       entmoot.GroupID
	group         *membership.Group
	live          *libp2ptransport.LiveGroup
	legacyHistory *merkle.Tree
	cancel        context.CancelFunc
	catchup       sync.Mutex
	history       libp2ptransport.HistorySyncState
	// unknownHeads is the count the most recent history catch-up skipped for
	// want of a roster checkpoint, kept so status output can show the gap.
	unknownHeads atomic.Int64
	peerRecords  *libp2ptransport.PeerRecordCache
}

type groupPolicyEnforcer struct {
	mu          sync.Mutex
	policy      entpolicy.Policy
	initialized bool
	limiter     *ratelimit.Limiter
}

func newGroupRuntime(cfg groupRuntimeConfig) (*groupRuntime, error) {
	if cfg.Identity == nil || cfg.Store == nil || cfg.Notify == nil || cfg.Host == nil {
		return nil, errors.New("group runtime: identity, store, notifying store, and libp2p host are required")
	}
	if cfg.Binding.PeerID != cfg.Host.ID() {
		return nil, errors.New("group runtime: host identity binding mismatch")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	invites, err := libp2ptransport.OpenInviteLedger(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	policyStore, err := entpolicy.OpenFileStore(cfg.DataDir)
	if err != nil {
		_ = invites.Close()
		return nil, err
	}
	liveRouter, err := libp2ptransport.NewLiveRouter(context.Background(), cfg.Host)
	if err != nil {
		_ = invites.Close()
		return nil, err
	}
	r := &groupRuntime{
		identity:         cfg.Identity,
		dataDir:          cfg.DataDir,
		store:            cfg.Store,
		notify:           cfg.Notify,
		host:             cfg.Host,
		binding:          cfg.Binding,
		logger:           cfg.Logger,
		mode:             cfg.Mode,
		controlledRelays: cfg.ControlledRelays,
		policyStore:      policyStore,
		profiles:         cfg.Profiles,
		invites:          invites,
		liveRouter:       liveRouter,
		sessions:         make(map[entmoot.GroupID]*groupSession),
		joining:          make(map[entmoot.GroupID]chan struct{}),
		policyEnforcers:  make(map[entmoot.GroupID]*groupPolicyEnforcer),
	}
	syncServer := &libp2ptransport.SyncServer{
		Host:             cfg.Host,
		Group:            r.groupFor,
		Store:            cfg.Notify,
		LegacyHistory:    r.legacyHistoryForGroup,
		PeerRecords:      r.peerRecordsForGroup,
		MembershipGossip: r.gossipMembershipRecord,
	}
	if err := syncServer.Install(); err != nil {
		_ = invites.Close()
		_ = liveRouter.Close()
		return nil, err
	}
	return r, nil
}

func (r *groupRuntime) Start(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (r *groupRuntime) groupFor(groupID entmoot.GroupID) (*membership.Group, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[groupID]
	if !ok {
		return nil, false
	}
	return session.group, true
}

// gossipMembershipRecord forwards a record this node accepted from a peer to
// the group's other reachable members. One hop each is enough: every receiver
// forwards in turn, so a join reaches members the joiner never contacted,
// without the joiner holding addresses for any of them.
func (r *groupRuntime) gossipMembershipRecord(groupID entmoot.GroupID, record membership.Record) {
	session, ok := r.Get(groupID)
	if !ok {
		return
	}
	peers := r.membershipPeers(session)
	if len(peers) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for _, remote := range peers {
			if err := libp2ptransport.PushMembershipRecord(ctx, r.host, remote, groupID, record, nil); err != nil {
				r.logger.Debug("membership gossip",
					slog.String("group_id", groupID.String()),
					slog.String("peer_id", remote.ID.String()),
					slog.String("err", err.Error()))
			}
		}
	}()
}

func (r *groupRuntime) peerRecordsForGroup(groupID entmoot.GroupID) (*libp2ptransport.PeerRecordCache, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[groupID]
	if !ok {
		return nil, false
	}
	return session.peerRecords, true
}

// relayHints renders this node's controlled relays as full multiaddrs, so an
// invite can hand them to a joiner that has no other way to find a relay.
func (r *groupRuntime) relayHints() []string {
	out := make([]string, 0, len(r.controlledRelays))
	for _, relay := range r.controlledRelays {
		suffix := multiaddr.StringCast("/p2p/" + relay.ID.String())
		for _, address := range relay.Addrs {
			out = append(out, address.Encapsulate(suffix).String())
			if len(out) == libp2ptransport.MaxCapabilityRelays {
				return out
			}
		}
	}
	return out
}

func (r *groupRuntime) AddLocalGroup(ctx context.Context, groupID entmoot.GroupID) (*groupSession, bool, error) {
	r.mu.Lock()
	if session, ok := r.sessions[groupID]; ok {
		r.mu.Unlock()
		return session, false, nil
	}
	if wait, ok := r.joining[groupID]; ok {
		r.mu.Unlock()
		select {
		case <-wait:
			return r.AddLocalGroup(ctx, groupID)
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	wait := make(chan struct{})
	r.joining[groupID] = wait
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.joining, groupID)
		close(wait)
		r.mu.Unlock()
	}()

	group, err := membership.Open(r.dataDir, groupID)
	if err != nil {
		return nil, false, err
	}
	if err := group.ClaimWriter(); err != nil {
		_ = group.Close()
		return nil, false, err
	}
	group.SetLogger(r.logger)
	if err := r.validateLocalMembership(group); err != nil {
		_ = group.Close()
		return nil, false, err
	}
	var legacyEntries []entmoot.RosterEntry
	if legacy := group.Legacy(); legacy != nil {
		legacyEntries = legacy.Entries()
	}
	legacyHistory, err := conversion.LoadLegacyHistoryTree(filepath.Join(r.dataDir, "groups", base64.RawURLEncoding.EncodeToString(groupID[:])), groupID, legacyEntries)
	if err != nil {
		_ = group.Close()
		return nil, false, fmt.Errorf("load legacy history commitment: %w", err)
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	live, err := r.liveRouter.AddGroup(sessionCtx, libp2ptransport.LiveConfig{
		Host: r.host, GroupID: groupID, Group: group, Store: r.notify,
		Authorize: func(message entmoot.Message) error {
			return r.enforceGroupPolicy(context.Background(), groupID, message)
		},
		// A profile arrives as an ordinary message, so validation has already
		// established the author is a current member and the signature holds.
		// OnIngest fires for locally published messages too, so a node records
		// its own name by the same path as everyone else's.
		OnIngest: func(message entmoot.Message) {
			r.observeMemberProfile(sessionCtx, groupID, message)
		},
	})
	if err != nil {
		cancel()
		_ = group.Close()
		return nil, false, err
	}
	session := &groupSession{
		groupID: groupID, group: group, live: live, legacyHistory: legacyHistory, cancel: cancel,
		peerRecords: libp2ptransport.NewPeerRecordCache(),
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = live.Close()
		cancel()
		_ = group.Close()
		return nil, false, errors.New("group runtime is closed")
	}
	r.sessions[groupID] = session
	r.mu.Unlock()
	go r.maintainGroup(sessionCtx, session)
	return session, true, nil
}

func (r *groupRuntime) AddCapability(ctx context.Context, capability entmoot.BootstrapCapability) (*groupSession, bool, error) {
	if err := libp2ptransport.VerifyBootstrapCapability(capability, r.host.ID(), time.Now()); err != nil {
		return nil, false, err
	}
	if !capability.IsOpenInvite() &&
		(capability.TargetMemberID != r.binding.MemberID || capability.TargetPeerID != r.binding.PeerID.String() || !bytes.Equal(capability.TargetPublicKey, r.identity.PublicKey)) {
		return nil, false, errors.New("bootstrap capability is for a different local identity")
	}
	var lastErr error
	for _, raw := range capability.AllowedMultiaddrs {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			lastErr = err
			continue
		}
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil {
			lastErr = err
			continue
		}
		if !stringMember(capability.AllowedPeerIDs, info.ID.String()) {
			lastErr = errors.New("bootstrap address peer is not authorized by capability")
			continue
		}
		applicant := entmoot.NodeInfo{
			EntmootPubKey: append([]byte(nil), r.identity.PublicKey...),
			MemberID:      &r.binding.MemberID,
			PeerID:        r.binding.PeerID.String(),
		}
		// The join is this node's own signed record: the peer applies it under
		// the same rules, so nothing here depends on the peer being willing to
		// write on our behalf.
		group, err := libp2ptransport.JoinGroup(ctx, r.host, *info, r.dataDir, r.identity, capability, applicant)
		if err != nil {
			lastErr = err
			continue
		}
		if err := group.Close(); err != nil {
			return nil, false, err
		}
		if err := persistGroupPeer(r.dataDir, capability.GroupID, *info); err != nil {
			return nil, false, err
		}
		session, added, err := r.AddLocalGroup(ctx, capability.GroupID)
		if err != nil {
			return nil, false, err
		}
		// The join happens before the joining host owns its GossipSub topic.
		// Reconnect after topic setup so both peers exchange subscriptions
		// against the membership each now holds.
		_ = r.host.Network().ClosePeer(info.ID)
		if err := r.host.Connect(ctx, *info); err != nil {
			return nil, false, fmt.Errorf("reconnect enrolled group peer: %w", err)
		}
		return session, added, nil
	}
	if lastErr == nil {
		lastErr = errors.New("bootstrap capability contains no usable address")
	}
	return nil, false, lastErr
}

func stringMember(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (r *groupRuntime) validateLocalMembership(group *membership.Group) error {
	if group.IsMemberID(r.binding.MemberID) {
		info, ok := group.MemberInfoByID(r.binding.MemberID)
		if ok && bytes.Equal(info.EntmootPubKey, r.identity.PublicKey) {
			return nil
		}
		return errLocalGroupIdentityMismatch
	}
	return errLocalGroupNotMember
}

func (r *groupRuntime) groupPolicy(ctx context.Context, groupID entmoot.GroupID) (*entpolicy.Policy, error) {
	if r.policyStore == nil {
		return nil, nil
	}
	policy, ok, err := r.policyStore.Get(ctx, groupID)
	if err != nil || !ok {
		return nil, err
	}
	return &policy, nil
}
func (r *groupRuntime) enforceGroupPolicy(ctx context.Context, groupID entmoot.GroupID, message entmoot.Message) error {
	policy, err := r.groupPolicy(ctx, groupID)
	if err != nil {
		return fmt.Errorf("group policy: %w", err)
	}
	if policy == nil {
		return nil
	}
	if int64(len(message.Content)) > policy.MaxMessageBytes {
		return fmt.Errorf("group policy: content is %d bytes, maximum is %d", len(message.Content), policy.MaxMessageBytes)
	}
	limits, err := entpolicy.ContentLimits(*policy)
	if err != nil {
		return fmt.Errorf("group policy: %w", err)
	}
	memberID, err := entmoot.ResolvedMemberID(message.Author)
	if err != nil {
		return err
	}
	r.mu.Lock()
	enforcer := r.policyEnforcers[groupID]
	if enforcer == nil {
		enforcer = &groupPolicyEnforcer{}
		r.policyEnforcers[groupID] = enforcer
	}
	r.mu.Unlock()
	enforcer.mu.Lock()
	defer enforcer.mu.Unlock()
	if !enforcer.initialized || enforcer.policy != *policy {
		enforcer.policy = *policy
		enforcer.limiter = ratelimit.New(limits, nil)
		enforcer.initialized = true
	}
	return enforcer.limiter.Allow(memberID, len(message.Content))
}

func (r *groupRuntime) Get(groupID entmoot.GroupID) (*groupSession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[groupID]
	return session, ok
}

func (r *groupRuntime) RemoveGroup(groupID entmoot.GroupID) bool {
	r.mu.Lock()
	session, ok := r.sessions[groupID]
	if ok {
		delete(r.sessions, groupID)
	}
	r.mu.Unlock()
	if ok {
		session.cancel()
		_ = session.live.Close()
		_ = session.group.Close()
	}
	return ok
}

func (r *groupRuntime) ActiveGroupIDs() []entmoot.GroupID {
	r.mu.RLock()
	out := make([]entmoot.GroupID, 0, len(r.sessions))
	for groupID := range r.sessions {
		out = append(out, groupID)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

func (r *groupRuntime) SingleGroup() (entmoot.GroupID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sessions) != 1 {
		return entmoot.GroupID{}, false
	}
	for groupID := range r.sessions {
		return groupID, true
	}

	return entmoot.GroupID{}, false
}
func (r *groupRuntime) maintainGroup(ctx context.Context, session *groupSession) {
	r.syncMembership(ctx, session)
	go r.catchUp(ctx, session)
	r.pruneGroup(ctx, session)
	membershipTicker := time.NewTicker(membershipSyncInterval)
	historyTicker := time.NewTicker(time.Minute)
	retentionTicker := time.NewTicker(time.Hour)
	defer membershipTicker.Stop()
	defer historyTicker.Stop()
	defer retentionTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-membershipTicker.C:
			r.syncMembership(ctx, session)
			// A node that never pulls, such as the founder, still learns heads
			// by writing them, so drain on the tick as well. It is a no-op
			// when nothing is held.
			r.drainHeldMessages(ctx, session)
		case <-historyTicker.C:
			go r.catchUp(ctx, session)
		case <-retentionTicker.C:
			r.pruneGroup(ctx, session)
		}
	}
}

func (r *groupRuntime) pruneGroup(ctx context.Context, session *groupSession) {
	groupPolicy, err := r.groupPolicy(ctx, session.groupID)
	if err != nil || groupPolicy == nil {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -groupPolicy.RetentionDays).UnixMilli()
	pruned, err := store.PruneBeforeExceptTopics(ctx, r.store, session.groupID, cutoff, []string{entpolicy.UpdateTopic})
	if err != nil {
		r.logger.Warn("group retention prune", slog.String("group_id", session.groupID.String()), slog.String("err", err.Error()))
		return
	}
	if pruned > 0 {
		r.logger.Info("group retention pruned messages", slog.String("group_id", session.groupID.String()), slog.Int64("messages", pruned))
	}
}

const (
	// maxMembershipSyncPeers bounds how many members one membership round
	// pulls from. Membership changes are rare and a pull is idempotent, so a
	// handful of peers converges without turning every tick into a fan-out.
	maxMembershipSyncPeers = 8
	// membershipSyncInterval is how often a session pulls membership. It is
	// slower than the old roster tick because a pull now carries a set
	// difference rather than a chain, and because a pushed record arrives
	// immediately instead of waiting for the next round.
	membershipSyncInterval = 15 * time.Second
)

// syncMembership pulls membership from reachable members. There is no backoff,
// no fork detection and no repair: a peer that serves a different set simply
// contributes what it holds, and the projection of the union is the same on
// every node that ends up holding the same records.
//
// Pulling from a peer whose state is behind ours costs one request and
// changes nothing. That is the whole reason this is short: with a set there is
// no "wrong chain" to detect, adopt or roll back.
func (r *groupRuntime) syncMembership(ctx context.Context, session *groupSession) {
	peers := r.membershipPeers(session)
	if len(peers) == 0 {
		// Nobody to pull from, which is not a reason to skip the cadence: a
		// group whose only online node is the founder still accumulates the
		// records the founder signs, and still has to retire them.
		r.signCheckpointIfDue(session)
		return
	}
	progressed := false
	for _, remote := range peers {
		syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		checkpoints, records, complete, err := libp2ptransport.FetchMembership(syncCtx, r.host, remote, session.group, r.binding.MemberID)
		cancel()
		if err != nil {
			if errors.Is(err, libp2ptransport.ErrRemoved) {
				// The peer served the signed record that removed us, and it
				// has been applied. Say so once, loudly: an operator whose
				// node has been evicted needs to read that, not a debug line
				// about a failed pull.
				r.logger.Warn("libp2p membership: this node was removed from the group",
					slog.String("group_id", session.groupID.String()),
					slog.String("peer_id", remote.ID.String()))
				r.drainHeldMessages(ctx, session)
				return
			}
			// Any other refusal is information about that peer, not about the
			// group: log it and ask somebody else.
			r.logger.Debug("libp2p membership pull",
				slog.String("group_id", session.groupID.String()),
				slog.String("peer_id", remote.ID.String()),
				slog.String("err", err.Error()))
		}
		if checkpoints > 0 || records > 0 {
			progressed = true
			r.logger.Info("libp2p membership synchronized",
				slog.String("group_id", session.groupID.String()),
				slog.String("peer_id", remote.ID.String()),
				slog.Int("checkpoints", checkpoints),
				slog.Int("records", records),
				slog.Bool("complete", complete))
		}
		if !complete {
			// The pull paged as far as one exchange is allowed to and the
			// peer still had more. Nothing is lost: every record it did apply
			// is durable, so the next round resumes rather than restarting.
			r.logger.Info("libp2p membership pull incomplete",
				slog.String("group_id", session.groupID.String()),
				slog.String("peer_id", remote.ID.String()))
		}
	}
	if progressed {
		// Messages held for a checkpoint we have now learned can be accepted.
		r.drainHeldMessages(ctx, session)
	}
	// Every round, not only when a pull brought something back: records this
	// node signed itself count towards the cadence too, so a founder admitting
	// members while nothing arrives from anybody else must still checkpoint.
	r.signCheckpointIfDue(session)
}

// signCheckpointIfDue folds pending records into a checkpoint once the group's
// cadence is reached, if this node may sign one. Any admin may: a group whose
// founder is offline still retires history.
func (r *groupRuntime) signCheckpointIfDue(session *groupSession) {
	if !session.group.CanAdminister(r.binding.MemberID) {
		return
	}
	checkpoint, signed, err := session.group.SignCheckpoint(r.identity, false)
	if err != nil {
		r.logger.Warn("membership checkpoint",
			slog.String("group_id", session.groupID.String()),
			slog.String("err", err.Error()))
		return
	}
	if signed {
		r.logger.Info("membership checkpoint signed",
			slog.String("group_id", session.groupID.String()),
			slog.Uint64("sequence", checkpoint.Sequence),
			slog.Uint64("covered", checkpoint.Covered),
			slog.Int("members", len(checkpoint.Members)))
	}
}

// membershipPeers lists dialable members to exchange membership with: every
// current member except this node, founder first because it is the most likely
// to be reachable.
func (r *groupRuntime) membershipPeers(session *groupSession) []peer.AddrInfo {
	cached, _ := loadGroupPeers(r.dataDir, session.groupID)
	addrsFor := func(id peer.ID) []multiaddr.Multiaddr {
		if addrs := r.host.Peerstore().Addrs(id); len(addrs) > 0 {
			return addrs
		}
		for _, candidate := range cached {
			if candidate.ID == id {
				return candidate.Addrs
			}
		}
		return nil
	}
	memberIDs := session.group.MemberIDs()
	ordered := make([]entmoot.MemberID, 0, len(memberIDs)+1)
	if founder := session.group.Founder(); founder.MemberID != nil {
		ordered = append(ordered, *founder.MemberID)
	}
	for _, memberID := range memberIDs {
		if len(ordered) > 0 && memberID == ordered[0] {
			continue
		}
		ordered = append(ordered, memberID)
	}
	out := make([]peer.AddrInfo, 0, len(ordered))
	for _, memberID := range ordered {
		info, found := session.group.MemberInfoByID(memberID)
		if !found {
			continue
		}
		binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
		if err != nil || binding.PeerID == r.host.ID() {
			continue
		}
		addrs := addrsFor(binding.PeerID)
		if len(addrs) == 0 {
			continue
		}
		out = append(out, peer.AddrInfo{ID: binding.PeerID, Addrs: addrs})
		if len(out) == maxMembershipSyncPeers {
			break
		}
	}
	return out
}

// drainHeldMessages releases messages that were held for a checkpoint this node
// has now learned. Every path that advances a session's membership calls it: a
// checkpoint learned through a join or an ESP change releases held messages
// exactly as a pull does. A message the updated membership still refuses is
// dropped and counted: silence there would hide a message that never arrives.
func (r *groupRuntime) drainHeldMessages(ctx context.Context, session *groupSession) {
	if session == nil || session.live == nil {
		return
	}
	ingested, dropped := session.live.DrainQuarantine(ctx)
	if ingested > 0 {
		r.logger.Info("libp2p membership-ahead messages ingested",
			slog.String("group_id", session.groupID.String()),
			slog.Int("messages", ingested))
	}
	if dropped > 0 {
		r.logger.Warn("libp2p membership-ahead messages dropped",
			slog.String("group_id", session.groupID.String()),
			slog.Int("messages", dropped))
	}
}

func (r *groupRuntime) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}

func (r *groupRuntime) catchUp(ctx context.Context, session *groupSession) {
	if !session.catchup.TryLock() {
		return
	}
	defer session.catchup.Unlock()
	keepers, err := r.keepersFor(session)
	if err != nil {
		r.logger.Warn("libp2p history catch-up: load peers", slog.String("group_id", session.groupID.String()), slog.String("err", err.Error()))
		return
	}
	// Members behind NAT are only dialable at a circuit address they cannot
	// publish themselves, so collect signed records from every keeper first and
	// rebuild the set: a member whose address just arrived is synced in this
	// same pass.
	r.exchangePeerRecords(ctx, session, keepers)
	keepers, err = r.keepersFor(session)
	if err != nil {
		r.logger.Warn("libp2p history catch-up: load peers", slog.String("group_id", session.groupID.String()), slog.String("err", err.Error()))
		return
	}
	if len(keepers) == 0 {
		return
	}
	var summary libp2ptransport.SyncSummary
	var lastErr string
retry:
	for attempt := 0; attempt < 30; attempt++ {
		progress := libp2ptransport.SyncFromKeepers(ctx, r.host, session.groupID, keepers, r.notify, func(message entmoot.Message, proof *merkle.Proof) error {
			if err := libp2ptransport.VerifyHistoricalMessageWithProof(session.group, message, time.Now(), proof); err != nil {
				return err
			}
			return r.enforceGroupPolicy(ctx, session.groupID, message)
		}, &session.history)
		summary = libp2ptransport.SummarizeKeeperProgress(progress)
		session.unknownHeads.Store(int64(summary.UnknownHeads))
		if len(progress) > 0 && progress[0].Err != nil {
			lastErr = progress[0].Err.Error()
		}
		if summary.Available > 0 || ctx.Err() != nil {
			break
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			break retry
		case <-timer.C:
		}
	}
	r.logger.Info("libp2p history catch-up",
		slog.String("group_id", session.groupID.String()),
		slog.Int("keepers", summary.Eligible),
		slog.Int("available", summary.Available),
		slog.Int("inserted", summary.Inserted),
		slog.Int("missing_bodies", summary.MissingBodies),
		slog.Int("pruned_locally", summary.PrunedLocally),
		slog.Int("unknown_heads", summary.UnknownHeads),
		slog.Int("converged_hints", summary.ConvergedHints),
		slog.String("last_error", lastErr))
	// History insertion writes straight to the store, so it never passes
	// through the live OnIngest hook: a name whose only copy arrived by
	// catch-up would otherwise never be learned. Re-observing from the store
	// is safe to repeat, because ordering is by the author's issue time.
	if summary.Inserted > 0 {
		r.reconcileProfilesFromHistory(ctx, session)
	}
}

// keepersFor lists members with at least one known address, combining
// the persisted cache with whatever the peerstore currently holds.
func (r *groupRuntime) keepersFor(session *groupSession) ([]peer.AddrInfo, error) {
	cached, err := loadGroupPeers(r.dataDir, session.groupID)
	if err != nil {
		return nil, err
	}
	byID := make(map[peer.ID]peer.AddrInfo, len(cached))
	for _, keeper := range cached {
		byID[keeper.ID] = keeper
	}
	for _, memberID := range session.group.MemberIDs() {
		info, ok := session.group.MemberInfoByID(memberID)
		if !ok {
			continue
		}
		binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
		if err != nil || binding.PeerID == r.host.ID() {
			continue
		}
		known := byID[binding.PeerID]
		known.ID = binding.PeerID
		known.Addrs = append(known.Addrs, r.host.Peerstore().Addrs(binding.PeerID)...)
		byID[binding.PeerID] = known
	}
	keepers := make([]peer.AddrInfo, 0, len(byID))
	for _, keeper := range byID {
		if len(keeper.Addrs) > 0 {
			keepers = append(keepers, keeper)
		}
	}
	sort.Slice(keepers, func(i, j int) bool { return keepers[i].ID.String() < keepers[j].ID.String() })
	return keepers, nil
}

// exchangePeerRecords pulls the signed peer records each reachable member holds
// and installs the verified addresses, which is how a member learns the circuit
// address of a peer it has never been able to dial.
func (r *groupRuntime) exchangePeerRecords(ctx context.Context, session *groupSession, keepers []peer.AddrInfo) {
	installed := 0
	for _, keeper := range keepers {
		if ctx.Err() != nil {
			return
		}
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		records, err := libp2ptransport.RequestPeerRecords(attempt, r.host, keeper, session.groupID)
		cancel()
		if err != nil {
			// An unreachable or older keeper is expected; history sync reports
			// reachability for the same peer set.
			continue
		}
		installed += libp2ptransport.InstallPeerRecords(r.host, session.group, records,
			r.mode, r.controlledRelays, session.peerRecords)
	}
	if installed > 0 {
		r.logger.Info("libp2p peer records",
			slog.String("group_id", session.groupID.String()),
			slog.Int("installed", installed),
			slog.Int("forwardable", session.peerRecords.Len()))
	}
}

func (r *groupRuntime) persistKnownPeers(session *groupSession) {
	for _, memberID := range session.group.MemberIDs() {
		info, ok := session.group.MemberInfoByID(memberID)
		if !ok {
			continue
		}
		binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
		if err != nil || binding.PeerID == r.host.ID() {
			continue
		}
		_ = persistGroupPeer(r.dataDir, session.groupID, peer.AddrInfo{
			ID: binding.PeerID, Addrs: r.host.Peerstore().Addrs(binding.PeerID),
		})
	}
}
func (r *groupRuntime) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	sessions := make([]*groupSession, 0, len(r.sessions))
	for _, session := range r.sessions {
		sessions = append(sessions, session)
	}
	r.sessions = make(map[entmoot.GroupID]*groupSession)
	r.mu.Unlock()
	for _, session := range sessions {
		r.persistKnownPeers(session)
	}
	for _, session := range sessions {
		session.cancel()
		_ = session.live.Close()
		_ = session.group.Close()
	}
	_ = r.liveRouter.Close()
	_ = r.invites.Close()
	_ = r.host.Close()
}

func (r *groupRuntime) legacyHistoryForGroup(groupID entmoot.GroupID) (*merkle.Tree, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[groupID]
	return session.legacyHistory, ok && session.legacyHistory != nil
}

func groupHasLocalMemberIdentity(group *membership.Group, memberID entmoot.MemberID, publicKey []byte) bool {
	info, ok := group.MemberInfoByID(memberID)
	return ok && bytes.Equal(info.EntmootPubKey, publicKey)
}

// openExistingGroup opens a group's membership if this node holds one. A group
// that exists only as a pre-checkpoint chain is reported as absent: it cannot
// be served or published to until `membership upgrade` mints checkpoint 0.
func openExistingGroup(dataDir string, groupID entmoot.GroupID) (*membership.Group, bool, error) {
	if !membership.Exists(dataDir, groupID) {
		return nil, false, nil
	}
	group, err := membership.Open(dataDir, groupID)
	if err != nil {
		return nil, false, err
	}
	return group, true, nil
}

func groupHasLocalIdentityPubKey(group *membership.Group, publicKey []byte) bool {
	memberID, err := entmoot.MemberIDFromPublicKey(publicKey)
	if err != nil {
		return false
	}
	member, ok := group.MemberInfoByID(memberID)
	return ok && bytes.Equal(member.EntmootPubKey, publicKey)
}

// memberProfileRecordFor turns a message on the reserved profile topic into
// the record it would be stored as, or reports false when the message carries
// no usable claim.
//
// It is called for every ingested message, so it must be cheap for the common
// case: the topic check runs before anything is decoded. A malformed payload
// on the reserved topic is logged once and dropped — a name is a display hint,
// so a bad one must never affect delivery of the message that carried it.
func (r *groupRuntime) memberProfileRecordFor(groupID entmoot.GroupID, message entmoot.Message) (esphttp.NodeProfileRecord, bool) {
	if !profile.HasTopic(message.Topics) || message.Author.MemberID == nil {
		return esphttp.NodeProfileRecord{}, false
	}
	parsed, err := profile.Decode(message.Content)
	if err != nil {
		if !errors.Is(err, profile.ErrNotProfile) {
			r.logger.Warn("member profile ignored",
				slog.String("group_id", groupID.String()),
				slog.String("member_id", message.Author.MemberID.String()),
				slog.String("err", err.Error()))
		}
		return esphttp.NodeProfileRecord{}, false
	}
	// Order by the author's own issue time, and refuse a profile dated further
	// ahead than the clock bound. Receipt time was the wrong answer: it does
	// stop a future-dated message pinning a name, but it makes an old message
	// arriving late — from history catch-up, or a peer re-gossiping — beat the
	// newer profile already recorded, so two nodes end up disagreeing about a
	// member's name depending on what arrived when. The author's clock ordered
	// and bounded gives both: a crafted future date is rejected outright, and
	// a replay of an old profile loses to the newer one on every node.
	if err := profile.CheckClock(parsed, time.Now()); err != nil {
		r.logger.Warn("member profile refused",
			slog.String("group_id", groupID.String()),
			slog.String("member_id", message.Author.MemberID.String()),
			slog.String("err", err.Error()))
		return esphttp.NodeProfileRecord{}, false
	}
	observedAt := parsed.IssuedAtMS
	expiresAt := parsed.ExpiresAtMS
	if parsed.DisplayName != "" {
		// Bound how long one message can keep a name alive: the expiry is the
		// author's claim too.
		maxExpiry := observedAt + maxProfileLifetimeMS
		if expiresAt <= 0 || expiresAt > maxExpiry {
			expiresAt = maxExpiry
		}
	}
	// An empty name withdraws the published one; MemberProfileRecord turns it
	// into a tombstone at the same issue time, so a profile issued earlier
	// cannot undo it.
	rec := esphttp.MemberProfileRecord(groupID, *message.Author.MemberID,
		encodeBase64(message.Author.EntmootPubKey), parsed.DisplayName, observedAt, expiresAt)
	if _, ok := esphttp.NormalizeNodeProfileHostname(rec.Hostname); !ok {
		return esphttp.NodeProfileRecord{}, false
	}
	return rec, true
}

// observeMemberProfile records a member's self-chosen display name.
func (r *groupRuntime) observeMemberProfile(ctx context.Context, groupID entmoot.GroupID, message entmoot.Message) {
	if r.profiles == nil {
		return
	}
	rec, ok := r.memberProfileRecordFor(groupID, message)
	if !ok {
		return
	}
	r.storeMemberProfile(ctx, groupID, rec)
}

// storeMemberProfile writes one claim. The store decides whether it wins.
func (r *groupRuntime) storeMemberProfile(ctx context.Context, groupID entmoot.GroupID, rec esphttp.NodeProfileRecord) {
	if r.profiles == nil {
		return
	}
	if _, _, err := r.profiles.UpsertNodeProfile(ctx, rec); err != nil {
		r.logger.Warn("member profile not recorded",
			slog.String("group_id", groupID.String()),
			slog.String("member_id", rec.MemberID.String()),
			slog.String("err", err.Error()))
	}
}

// profileReconcilePageSize and maxProfileReconcilePages bound the window one
// catch-up considers: the newest 4096 messages on the profile topic by the
// store's paging key.
//
// The bound is on messages, so a member CAN be crowded out: 4096 profile
// messages outranking another member's newest claim leave that member at the
// member-id fallback until it republishes. Earlier shapes were far worse — 256
// messages, then 271 — but the limit is a window, not an absence of one. What
// the window does guarantee is that every claim inside it is ranked, so volume
// cannot make a member adopt a superseded name, only miss one entirely.
const (
	profileReconcilePageSize = 256
	maxProfileReconcilePages = 16
)

// reconcileProfilesFromHistory records the names of members whose profiles
// arrived by history sync, which writes straight to the store and never runs
// the live ingest hook.
//
// It ranks every claim in the window with the store's own comparison and
// writes the winner once per member. Two earlier shapes were wrong for the
// same underlying reason — the walk cannot use the message key to decide
// anything about profiles, because the store orders records by the author's
// issue time and the two only coincide while a member's message timestamps
// track its payload. Selecting "the newest message per member" adopted
// superseded names; stopping a member as soon as any of its messages appeared
// in a page skipped a retraction that sat one page deeper with a newer issue
// time. The message key is used for exactly one thing here: paging.
func (r *groupRuntime) reconcileProfilesFromHistory(ctx context.Context, session *groupSession) {
	if r.profiles == nil {
		return
	}
	wanted := make(map[entmoot.MemberID]struct{})
	for _, memberID := range session.group.MemberIDs() {
		wanted[memberID] = struct{}{}
	}
	if len(wanted) == 0 {
		return
	}
	best := make(map[entmoot.MemberID]esphttp.NodeProfileRecord, len(wanted))
	var boundary *store.PageBoundary
	for page := 0; page < maxProfileReconcilePages; page++ {
		messages, err := r.store.LatestByTopicBefore(ctx, session.groupID, profile.Topic, profileReconcilePageSize, boundary)
		if err != nil {
			r.logger.Warn("member profiles not reconciled from history",
				slog.String("group_id", session.groupID.String()),
				slog.String("err", err.Error()))
			return
		}
		if len(messages) == 0 {
			break
		}
		oldest := messages[0]
		for _, message := range messages {
			if profileMessageNewer(oldest, message) {
				oldest = message
			}
			if message.Author.MemberID == nil {
				continue
			}
			if _, ok := wanted[*message.Author.MemberID]; !ok {
				continue
			}
			rec, ok := r.memberProfileRecordFor(session.groupID, message)
			if !ok {
				continue
			}
			held, seen := best[rec.MemberID]
			if !seen || esphttp.BetterMemberProfileRecord(rec, held) {
				best[rec.MemberID] = rec
			}
		}
		if len(messages) < profileReconcilePageSize {
			// The topic is exhausted; there is nothing older to page to.
			break
		}
		boundary = &store.PageBoundary{
			TimestampMS:    oldest.Timestamp,
			AuthorMemberID: profileMessageAuthor(oldest),
			MessageID:      oldest.ID,
		}
	}
	for _, rec := range best {
		// The store still arbitrates against whatever it already holds; this
		// only avoids one write per message in the window.
		r.storeMemberProfile(ctx, session.groupID, rec)
	}
	if len(best) < len(wanted) {
		// Not an error: the remaining members may simply never have published
		// a name. It is logged because the alternative reading — a topic so
		// busy that the window ran out before reaching them — is worth seeing.
		r.logger.Debug("member profile reconciliation found no claim for some members",
			slog.String("group_id", session.groupID.String()),
			slog.Int("members_without_profile", len(wanted)-len(best)))
	}
}

// profileMessageNewer reports whether a outranks b under the store's paging
// key: timestamp, then author member id, then message id. It is the same
// comparison LatestByTopicBefore pages on, so the boundary it produces cannot
// skip or repeat a row.
func profileMessageNewer(a, b entmoot.Message) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp > b.Timestamp
	}
	left, right := profileMessageAuthor(a), profileMessageAuthor(b)
	if left != right {
		return bytes.Compare(left[:], right[:]) > 0
	}
	return bytes.Compare(a.ID[:], b.ID[:]) > 0
}

// profileMessageAuthor is the member id a page boundary needs, zero when the
// message predates member ids.
func profileMessageAuthor(message entmoot.Message) entmoot.MemberID {
	if message.Author.MemberID == nil {
		return entmoot.MemberID{}
	}
	return *message.Author.MemberID
}
