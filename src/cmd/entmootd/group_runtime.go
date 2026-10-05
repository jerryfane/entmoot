package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
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
	// pullTimeout bounds one member's pull and roundTimeout a membership
	// round's pulls together (membershipPullTimeout and
	// membershipRoundTimeout; shortened by tests).
	pullTimeout  time.Duration
	roundTimeout time.Duration

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
	seal         sealState
	// pullOffset is where the next membership round starts in its list of
	// members: the first member the previous round ran out of time before
	// asking, so every member is asked within a few rounds however many hang.
	pullOffset atomic.Int64
	// reconciler wakes reconcileInvites, the session's worker that keeps the
	// invites this node issued in line with the group's membership.
	reconciler *inviteReconciler
}

// sealState is the sealer's progress on sealing authority changes (see
// sealAuthorityChanges). Maintenance rounds can overlap, so it is locked.
type sealState struct {
	mu sync.Mutex
	// waiting lists the authority changes this node has seen and not sealed,
	// oldest first. Each entry is the newest change due when a round first
	// found it, with its own wait: a change that arrives while an older one
	// is held back waits its full time, not what is left of the older one's.
	waiting []waitingSeal
	// last is when this node last sealed, for minSealInterval.
	last time.Time
}

// waitingSeal is one authority change waiting to be sealed.
type waitingSeal struct {
	// through is the change's timestamp; a seal through it covers every
	// change dated no later.
	through int64
	// since is when a round first found it, and rounds counts the rounds
	// since then that reached another member: past sealDeadline and
	// sealDeadlineRounds it is sealed whether or not a round synced, once
	// every member this node can address is in pulled.
	since  time.Time
	rounds int
	// pulled holds the members asked since then, whatever they answered: a
	// seal forced on a view that never asked a member could leave out the
	// records only that member holds.
	pulled map[peer.ID]struct{}
}

func (s *sealState) isArmed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiting) > 0
}

// sealRound is what one membership round tells the sealer.
type sealRound struct {
	// reached is true when the round got through to at least one other
	// member - an answer of any kind, or a connection it then failed on - or
	// when there is no other member. Only such rounds count towards a seal's
	// deadline: a founder that cannot reach anybody may be the one that is
	// cut off, and must not seal its own view over records the others hold.
	reached bool
	// synced is true when the round pulled everything from every member it
	// got through to.
	synced bool
	// lagging names the members the round could not pull everything from.
	lagging []string
	// addressable lists every member the round could have asked, and pulled
	// the ones it did ask, whatever they answered.
	addressable []peer.ID
	pulled      []peer.ID
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
		pullTimeout:      membershipPullTimeout,
		roundTimeout:     membershipRoundTimeout,
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
// relayHints returns the relay set an invite may carry, already bounded in
// count and bytes. The bound lives HERE, at the producer, because it used to
// live at each mint site and one of them could then be reverted without any
// test noticing — the capability-size estimate depends on this bound holding,
// so the bound must not be something a call site can forget.
func (r *groupRuntime) relayHints() []string {
	out := make([]string, 0, len(r.controlledRelays))
	for _, relay := range r.controlledRelays {
		suffix := multiaddr.StringCast("/p2p/" + relay.ID.String())
		for _, address := range relay.Addrs {
			out = append(out, address.Encapsulate(suffix).String())
			if len(out) == libp2ptransport.MaxCapabilityRelays {
				return boundInviteRelays(out)
			}
		}
	}
	return boundInviteRelays(out)
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
	reconciler := newInviteReconciler()
	group.SetChangeHook(reconciler.signal)
	if err := r.validateLocalMembership(group); err != nil {
		_ = group.Close()
		return nil, false, err
	}
	var legacyEntries []entmoot.RosterEntry
	if legacy := group.Legacy(); legacy != nil {
		legacyEntries = legacy.Entries()
	}
	legacyHistory, err := conversion.LoadLegacyHistoryTree(groupDirPath(r.dataDir, groupID), groupID, legacyEntries)
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
		peerRecords: libp2ptransport.NewPeerRecordCache(), reconciler: reconciler,
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
	go r.reconcileInvites(session)
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
	var candidates []peer.AddrInfo
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
		candidates = append(candidates, *info)
	}
	if len(candidates) == 0 {
		if lastErr == nil {
			lastErr = errors.New("bootstrap capability contains no usable address")
		}
		return nil, false, lastErr
	}
	applicant := entmoot.NodeInfo{
		EntmootPubKey: append([]byte(nil), r.identity.PublicKey...),
		MemberID:      &r.binding.MemberID,
		PeerID:        r.binding.PeerID.String(),
	}
	// The join is this node's own signed record: the peer applies it under
	// the same rules, so nothing here depends on the peer being willing to
	// write on our behalf. Every address gets its turn before a stalled one is
	// dialled again, so one stalled address cannot spend the whole budget.
	group, info, err := libp2ptransport.JoinGroupVia(ctx, r.host, candidates, r.dataDir, r.identity, capability, applicant)
	if err != nil {
		return nil, false, err
	}
	if err := group.Close(); err != nil {
		return nil, false, err
	}
	if err := persistGroupPeer(r.dataDir, capability.GroupID, info); err != nil {
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
	if err := r.host.Connect(ctx, info); err != nil {
		return nil, false, fmt.Errorf("reconnect joined group peer: %w", err)
	}
	return session, added, nil
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
		return fmt.Errorf("group policy: content is %d bytes, maximum is %d: %w", len(message.Content), policy.MaxMessageBytes, entmoot.ErrOversized)
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
		session.reconciler.close()
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
	// maxRewoundSyncPeers reserves part of that fan-out for the members a
	// rewind dropped. Without a reservation a group with eight addressable
	// members never reaches them, because current members come first; with
	// the whole budget they could crowd out ordinary sync.
	maxRewoundSyncPeers = 3
	// membershipSyncInterval is how often a session pulls membership. It is
	// slower than the old roster tick because a pull now carries a set
	// difference rather than a chain, and because a pushed record arrives
	// immediately instead of waiting for the next round.
	membershipSyncInterval = 15 * time.Second
	// membershipPullTimeout bounds one member's pull, and
	// membershipRoundTimeout how long a round keeps starting pulls (see
	// pullMembers). Members are pulled maxMembershipSyncPeers at a time, so a
	// member that hangs frees its slot after membershipPullTimeout, a round
	// lasts at most the two together however many members hang, and every
	// member is asked within a few rounds (see pullOffset). A round that ran
	// out of time before asking a member is not a synchronized round.
	membershipPullTimeout  = 10 * time.Second
	membershipRoundTimeout = 30 * time.Second
	// minSealInterval bounds how often the sealer signs a checkpoint for
	// authority changes alone. Every one is founder-signed and so retained for
	// good (it is what a joiner holding no group state can anchor on), so an
	// admin revoking in a loop must not be able to make the founder sign one
	// per change.
	minSealInterval = time.Minute
	// sealDeadline and sealDeadlineRounds bound how long a seal waits for a
	// round that pulled everything from every member it reached; once a
	// change has waited both - counting only rounds that reached another
	// member - it is sealed on what this node holds. Without a bound a single
	// member could keep the seal off for ever by answering every pull as
	// incomplete or not at all, and the member with most reason to is an
	// admin keeping its own demotion unsealed while it signs backdated joins.
	sealDeadline       = 2 * time.Minute
	sealDeadlineRounds = 3
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
	// The backstop for anything the change hook could not wake: every
	// round, the invites this node issued are reconciled once more.
	session.reconciler.signal()
	// A round that is to seal an authority change pulls from every member it
	// can address, not the usual handful: the seal makes everything dated
	// before the change stale, so it may only vouch for what all the members
	// it can reach hold.
	limit := maxMembershipSyncPeers
	if session.seal.isArmed() {
		limit = math.MaxInt
	}
	peers := r.membershipPeersUpTo(session, limit)
	if len(peers) == 0 {
		// Nobody to pull from, which is not a reason to skip the cadence: a
		// group whose only online node is the founder still accumulates the
		// records the founder signs, and still has to retire them. A seal is
		// different: it would vouch for records other members may hold, so
		// this round counts for nothing unless there is nobody else.
		alone := r.onlyLocalMember(session)
		r.sealAuthorityChanges(session, sealRound{reached: alone, synced: alone})
		r.signCheckpointIfDue(session)
		return
	}
	// Start where the previous round ran out of time, so members that hang
	// cannot keep the ones after them from ever being asked.
	start := int(session.pullOffset.Load() % int64(len(peers)))
	order := append(append(make([]peer.AddrInfo, 0, len(peers)), peers[start:]...), peers[:start]...)
	pulls, removed := r.pullMembers(ctx, session, order)
	next := 0
	for i, pull := range pulls {
		if pull.skipped {
			next = (start + i) % len(peers)
			break
		}
	}
	session.pullOffset.Store(int64(next))
	if removed != nil {
		// The peer served the signed record that removed us, and it has been
		// applied. Say so once, loudly: an operator whose node has been
		// evicted needs to read that, not a debug line about a failed pull.
		r.logger.Warn("libp2p membership: this node was removed from the group",
			slog.String("group_id", session.groupID.String()),
			slog.String("peer_id", removed.ID.String()))
		r.drainHeldMessages(ctx, session)
		return
	}
	progressed := false
	// full counts the members this round pulled everything from. A member
	// that answered with only part of what it holds, or that was connected
	// but failed or hung, or that the round ran out of time before asking,
	// leaves the round short. A member that could not be connected to at all
	// is treated as unreachable rather than as holding the round up, or a
	// single offline member would delay every seal; but it does not count as
	// reached either. lagging names every member that was not pulled in
	// full, for the log of a forced seal (see sealAuthorityChanges).
	full, reached, short := 0, false, false
	var lagging []string
	addressable := make([]peer.ID, 0, len(pulls))
	pulled := make([]peer.ID, 0, len(pulls))
	for _, pull := range pulls {
		addressable = append(addressable, pull.remote.ID)
		if !pull.skipped {
			pulled = append(pulled, pull.remote.ID)
		}
	}
	for _, pull := range pulls {
		remote := pull.remote
		switch {
		case pull.skipped:
			short = true
			lagging = append(lagging, remote.ID.String())
			continue
		case pull.err == nil && pull.complete:
			full++
			reached = true
		case pull.err == nil || pull.connected:
			reached, short = true, true
			lagging = append(lagging, remote.ID.String())
		default:
			lagging = append(lagging, remote.ID.String())
		}
		if pull.err != nil {
			// Any refusal is information about that peer, not about the
			// group: log it and ask somebody else.
			r.logger.Debug("libp2p membership pull",
				slog.String("group_id", session.groupID.String()),
				slog.String("peer_id", remote.ID.String()),
				slog.String("err", pull.err.Error()))
		}
		if pull.checkpoints > 0 || pull.records > 0 {
			progressed = true
			r.logger.Info("libp2p membership synchronized",
				slog.String("group_id", session.groupID.String()),
				slog.String("peer_id", remote.ID.String()),
				slog.Int("checkpoints", pull.checkpoints),
				slog.Int("records", pull.records),
				slog.Bool("complete", pull.complete))
		}
		if !pull.complete {
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
	r.sealAuthorityChanges(session, sealRound{
		reached: reached, synced: full > 0 && !short, lagging: lagging,
		addressable: addressable, pulled: pulled,
	})
	// Every round, not only when a pull brought something back: records this
	// node signed itself count towards the cadence too, so a founder admitting
	// members while nothing arrives from anybody else must still checkpoint.
	r.signCheckpointIfDue(session)
}

// inviteReconciler wakes a session's invite worker, reconcileInvites,
// whenever the group may have changed: a record or checkpoint was applied, or
// a maintenance round came round. Signals coalesce - the worker looks at the
// group as it is, not at what changed - so signal never blocks, a burst of
// records costs one pass, and the order records arrived in cannot matter.
// Nothing is done on the signalling goroutine, which may be a pull, a push,
// or a caller holding lockESPInviteRoster while it signs a revocation.
type inviteReconciler struct {
	mu        sync.Mutex
	settled   *sync.Cond
	requested uint64
	completed uint64
	closed    bool
	wake      chan struct{}
	done      chan struct{}
}

func newInviteReconciler() *inviteReconciler {
	q := &inviteReconciler{wake: make(chan struct{}, 1), done: make(chan struct{})}
	q.settled = sync.NewCond(&q.mu)
	return q
}

// signal asks for a pass. It is a no-op on a nil or closed reconciler.
func (q *inviteReconciler) signal() {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.requested++
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// wait blocks until a pass has started after every signal so far, and ended.
func (q *inviteReconciler) wait() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.completed < q.requested {
		q.settled.Wait()
	}
}

// close stops accepting signals and waits for the worker to finish the pass
// still owed, so the group is not closed under it.
func (q *inviteReconciler) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	<-q.done
}

// reconcileInvites is the session's invite worker. It runs a pass whenever
// one is owed, until the reconciler is closed and nothing is owed.
func (r *groupRuntime) reconcileInvites(session *groupSession) {
	q := session.reconciler
	defer close(q.done)
	for {
		q.mu.Lock()
		if q.completed < q.requested {
			target := q.requested
			q.mu.Unlock()
			r.reconcileIssuedInvites(session)
			q.mu.Lock()
			q.completed = target
			q.settled.Broadcast()
			q.mu.Unlock()
			continue
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return
		}
		<-q.wake
	}
}

// reconcileIssuedInvites revokes every capability this node issued that the
// group's current membership says should no longer admit anybody, and does
// nothing else: a pass over a group with nothing to revoke signs nothing, so
// passes may repeat freely. It looks only at the group as it stands, never at
// what just changed, so every order the same records arrive in ends with the
// same capabilities revoked.
//
//   - A removed member: for each member this node issued live capabilities to
//     that is not a member now and whose latest membership a removal or ban
//     ended (Group.RemovedAt), every such capability minted no later than that
//     removal. Re-invites issued after it are never touched, a readmitted
//     member is a member, and a member that left or rekeyed is not removed.
//     A removal signed here already revoked them first (revokeInvitesForRemoval);
//     one signed elsewhere is caught here once this node holds it - but these
//     revocations carry this node's later timestamps, so a join with such a
//     capability dated between the removal and them is accepted, as for any
//     revoked invite, until the founder seals them.
//   - A used replacement chain: every capability still unrevoked in an ESP
//     replacement chain one of whose capabilities has been used to join.
//     Every capability in a chain is made out to the same target, so once one
//     has admitted it the others are only a way back in after a removal. The
//     capability a refresh replaces is revoked when the replacement is handed
//     out, but a join with it dated before that revocation is accepted until
//     the founder seals it.
//
// It runs only where this node may revoke, and holds lockESPInviteRoster only
// while it signs.
func (r *groupRuntime) reconcileIssuedInvites(session *groupSession) {
	group := session.group
	if r.invites == nil || r.identity == nil || !group.CanAdminister(r.binding.MemberID) {
		return
	}
	// Working out what to revoke reads the group and the ledger and holds
	// neither lock; only signing the revocations does.
	removed := r.invitesOfRemovedMembers(group)
	chains := r.unusedRestOfUsedChains(group)
	if len(removed) == 0 && len(chains) == 0 {
		return
	}
	gid := session.groupID
	unlock := lockESPInviteRoster(gid)
	defer unlock()
	var nonces [][32]byte
	for _, candidate := range removed {
		// Readmitted while the pass was working: its invites stand.
		if !group.IsMemberID(candidate.target) {
			nonces = append(nonces, candidate.nonce)
		}
	}
	nonces = append(nonces, chains...)
	revoked, err := revokeIssuedInvites(r.identity, group, r.invites, nonces)
	if err != nil {
		r.logger.Warn("revoke issued invites", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
	}
	if len(revoked) > 0 {
		r.logger.Info("revoked issued invites that no longer admit anybody",
			slog.String("group_id", gid.String()), slog.Int("revoked", len(revoked)))
	}
}

type removedMemberInvite struct {
	target entmoot.MemberID
	nonce  [32]byte
}

// invitesOfRemovedMembers lists the live invites this node minted to members
// that a removal has since taken out (see reconcileIssuedInvites).
//
// The cutoff is the later of the removal's own timestamp, by the remover's
// clock, and when this node learned of that removal, by its own: so an invite
// minted here before this node knew of the removal is caught even when the
// remover's clock runs behind, and a re-invite minted after it never is. The
// group notes that time as the record or checkpoint that makes the removal
// take effect is applied (membership.Removal.SeenAt), not when this worker
// gets round to it, so a busy worker cannot move it past a re-invite; the
// ledger keeps the earliest time each removal was ever given, which is what
// covers a restart. A removal the group has no time for - one it loaded when
// it opened - is given the time of this pass. A ledger row from before
// minted_at_ms existed has only its issue date, set minutes early, and is
// compared with the removal's timestamp alone.
//
// Only the targets of live invites are asked about, with the members the group
// noticed going out or being named by a removal since the last pass (see
// membership.Group.TakeNoticed): those are recorded in the ledger whether or
// not this node has invites for them yet, so a re-invite minted later is
// judged by when the removal arrived even across a restart. Every member is
// asked about only after the group was opened or its canonical checkpoint
// moved. A node with no live invite in the group asks nothing and keeps the
// notices for the first pass that has one: minting an invite starts a pass.
func (r *groupRuntime) invitesOfRemovedMembers(group *membership.Group) []removedMemberInvite {
	gid := group.GroupID()
	live, err := r.invites.LiveTargetedInvites(gid)
	if err != nil {
		r.logger.Warn("list issued invites", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
		return nil
	}
	targets := make([]entmoot.MemberID, 0, len(live))
	for _, record := range live {
		if !group.IsInviteRevoked(record.Nonce) &&
			(record.MaxUses == 0 || group.InviteUses(record.Nonce) < record.MaxUses) {
			targets = append(targets, *record.TargetMemberID)
		}
	}
	if len(targets) == 0 {
		return nil
	}
	noticed, all := group.TakeNoticed()
	var removals map[entmoot.MemberID]membership.Removal
	if all {
		removals = group.RemovedAt(nil, r.binding.MemberID)
	} else {
		removals = group.RemovedAt(append(noticed, targets...), r.binding.MemberID)
	}
	if len(removals) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	learned := make(map[entmoot.RosterEntryID]int64, len(removals))
	for _, removal := range removals {
		at := removal.SeenAt
		if at == 0 {
			at = now
		}
		if earlier, ok := learned[removal.Entry]; !ok || at < earlier {
			learned[removal.Entry] = at
		}
	}
	seen, err := r.invites.RemovalsSeenAt(gid, learned)
	if err != nil {
		group.Renotice(noticed, all)
		r.logger.Warn("record removals", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
		return nil
	}
	var out []removedMemberInvite
	for _, record := range live {
		removal, removed := removals[*record.TargetMemberID]
		if !removed || group.IsInviteRevoked(record.Nonce) ||
			(record.MaxUses > 0 && group.InviteUses(record.Nonce) >= record.MaxUses) {
			continue
		}
		cutoff, minted := removal.At, record.MintedAtMS
		if minted == 0 {
			minted = record.IssuedAtMS
		} else {
			cutoff = max(cutoff, seen[removal.Entry])
		}
		if minted <= cutoff {
			out = append(out, removedMemberInvite{target: *record.TargetMemberID, nonce: record.Nonce})
		}
	}
	return out
}

// unusedRestOfUsedChains lists the unused capabilities of every replacement
// chain one of whose capabilities has been used (see reconcileIssuedInvites).
func (r *groupRuntime) unusedRestOfUsedChains(group *membership.Group) [][32]byte {
	gid := group.GroupID()
	chains, err := r.invites.ReplacementChains(gid)
	if err != nil {
		r.logger.Warn("invite replacement chains", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
		return nil
	}
	var out [][32]byte
	for _, chain := range chains {
		used := false
		var unused [][32]byte
		for _, nonce := range chain {
			if group.InviteUses(nonce) > 0 {
				used = true
			} else if !group.IsInviteRevoked(nonce) {
				unused = append(unused, nonce)
			}
		}
		if used {
			out = append(out, unused...)
		}
	}
	return out
}

// memberPull is the outcome of pulling membership from one member.
type memberPull struct {
	remote      peer.AddrInfo
	checkpoints int
	records     int
	complete    bool
	err         error
	// connected reports a connection to the member after a failed pull: it
	// was reachable, and failed or hung rather than being offline.
	connected bool
	// skipped marks a member the round ran out of time before asking.
	skipped bool
}

// pullMembers pulls from peers maxMembershipSyncPeers at a time, in the order
// given. Each pull gets pullTimeout, so a member that hangs frees its slot for
// the next; roundTimeout stops the round starting more pulls, and the members
// it did not start are marked skipped. A pull that started always runs to its
// own answer or timeout, so a member counts as asked only when it was given
// its full time, and a round lasts at most roundTimeout plus pullTimeout
// however many members hang. It stops asking once a peer shows this node was
// removed, and returns that peer.
func (r *groupRuntime) pullMembers(ctx context.Context, session *groupSession, peers []peer.AddrInfo) ([]memberPull, *peer.AddrInfo) {
	pullsCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	roundCtx, roundCancel := context.WithTimeout(pullsCtx, r.roundTimeout)
	defer roundCancel()
	pulls := make([]memberPull, len(peers))
	var removedMu sync.Mutex
	var removed *peer.AddrInfo
	slots := make(chan struct{}, maxMembershipSyncPeers)
	var wg sync.WaitGroup
	for i, remote := range peers {
		pulls[i].remote = remote
		select {
		case slots <- struct{}{}:
		case <-roundCtx.Done():
		}
		if roundCtx.Err() != nil {
			pulls[i].skipped = true
			continue
		}
		wg.Add(1)
		go func(pull *memberPull) {
			defer wg.Done()
			defer func() { <-slots }()
			pullCtx, pullCancel := context.WithTimeout(pullsCtx, r.pullTimeout)
			defer pullCancel()
			pull.checkpoints, pull.records, pull.complete, pull.err = libp2ptransport.FetchMembership(pullCtx, r.host, pull.remote, session.group, r.binding.MemberID)
			if errors.Is(pull.err, libp2ptransport.ErrRemoved) {
				removedMu.Lock()
				if removed == nil {
					removed = &pull.remote
				}
				removedMu.Unlock()
				cancel()
				return
			}
			if pull.err != nil {
				pull.connected = r.host.Network().Connectedness(pull.remote.ID) == network.Connected
			}
		}(&pulls[i])
	}
	wg.Wait()
	return pulls, removed
}

// onlyLocalMember reports whether this node is the group's only member, so
// no other node can hold a record it lacks.
func (r *groupRuntime) onlyLocalMember(session *groupSession) bool {
	for _, id := range session.group.MemberIDs() {
		if id != r.binding.MemberID {
			return false
		}
	}
	return true
}

// sealAuthorityChanges makes a revoked invite, a demoted or removed admin, or
// a closed group final against joins dated before the change (see
// membership.Group.SealDue), by signing a checkpoint dated at the change.
//
// Signing one is safe only on a view the other members agree with: the
// checkpoint makes every record dated before it stale, so a record a member
// holds and this node does not is lost for good, and that member refuses the
// checkpoint and every one after it. So:
//   - only the founder's daemon seals. Every checkpoint lists the founder and
//     the founder may always sign one, so it is always able to, and with one
//     signer no two nodes cut sibling checkpoints over different records.
//     Admins, including one that signed the change or received it first,
//     never seal; while the founder's daemon is down a change waits for it,
//     or for the cadence;
//   - it seals only changes it already held at the end of an earlier round,
//     and only once a later round has pulled everything from every member
//     it got through to (synced), which gives records dated before a change
//     a full round to reach somebody and be pulled;
//   - but a change never waits past sealDeadline and sealDeadlineRounds of
//     its own: a member that answers every pull as incomplete, or hangs,
//     cannot hold the seal off. Such a seal goes only through the newest
//     change that is overdue, so a newer one still gets its full wait, and
//     the members that lagged are logged. It also needs every member this
//     node can address to have been asked since the change was found -
//     whatever it answered - so members that hang cannot crowd out an
//     honest one and have the seal leave its records out (rounds start
//     where the last one ran out of time, so each is asked within a few);
//   - only rounds that got through to another member count towards that
//     deadline, so a founder cut off from every member - partitioned, or
//     holding no address for any of them - never seals its own view over
//     theirs, unless it is the group's only member;
//   - the checkpoint folds only what is dated up to the change, so records
//     signed since stay out of what it vouches for;
//   - one seal per minSealInterval, however many changes arrive.
//
// The cadence checkpoint is separate and unchanged.
func (r *groupRuntime) sealAuthorityChanges(session *groupSession, round sealRound) {
	seal := &session.seal
	seal.mu.Lock()
	defer seal.mu.Unlock()
	founder := session.group.Founder()
	if founder.MemberID == nil || *founder.MemberID != r.binding.MemberID ||
		!bytes.Equal(founder.EntmootPubKey, r.identity.PublicKey) {
		return
	}
	now := time.Now()
	if round.reached {
		for i := range seal.waiting {
			seal.waiting[i].rounds++
		}
	}
	for i := range seal.waiting {
		for _, id := range round.pulled {
			seal.waiting[i].pulled[id] = struct{}{}
		}
	}
	if n := len(seal.waiting); n > 0 && now.Sub(seal.last) >= minSealInterval {
		var target *waitingSeal
		if round.synced {
			target = &seal.waiting[n-1]
		} else {
			for i := range seal.waiting {
				waiting := &seal.waiting[i]
				if waiting.rounds >= sealDeadlineRounds && now.Sub(waiting.since) >= sealDeadline &&
					askedAll(waiting.pulled, round.addressable) {
					target = waiting
				}
			}
		}
		if target != nil {
			checkpoint, signed, err := session.group.SealThrough(r.identity, target.through)
			switch {
			case err != nil:
				r.logger.Warn("membership seal",
					slog.String("group_id", session.groupID.String()),
					slog.String("err", err.Error()))
			case signed:
				seal.last = now
				attrs := []any{
					slog.String("group_id", session.groupID.String()),
					slog.Uint64("sequence", checkpoint.Sequence),
					slog.Int64("through", checkpoint.Timestamp),
					slog.Uint64("covered", checkpoint.Covered),
					slog.Int("members", len(checkpoint.Members)),
				}
				if round.synced {
					r.logger.Info("membership authority change sealed", attrs...)
				} else {
					// A member that lagged may hold a record dated before
					// the change that this seal now leaves out; it will
					// refuse the seal until repaired. Name it.
					r.logger.Warn("membership authority change sealed without a fully synchronized round",
						append(attrs,
							slog.Duration("waited", now.Sub(target.since)),
							slog.Any("lagging_peers", round.lagging))...)
				}
			}
		}
	}
	// Drop what a checkpoint now covers - this seal, or any other - and add
	// the newest change due if no entry reaches it yet.
	due, ok := session.group.SealDue()
	if !ok {
		seal.waiting = nil
		return
	}
	covered := session.group.Canonical().Timestamp
	kept := seal.waiting[:0]
	for _, waiting := range seal.waiting {
		if waiting.through > covered {
			kept = append(kept, waiting)
		}
	}
	seal.waiting = kept
	if n := len(seal.waiting); n == 0 || due > seal.waiting[n-1].through {
		seal.waiting = append(seal.waiting, waitingSeal{through: due, since: now, pulled: make(map[peer.ID]struct{})})
	}
}

// askedAll reports whether every member in addressable is in pulled.
func askedAll(pulled map[peer.ID]struct{}, addressable []peer.ID) bool {
	for _, id := range addressable {
		if _, ok := pulled[id]; !ok {
			return false
		}
	}
	return true
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
	return r.membershipPeersUpTo(session, maxMembershipSyncPeers)
}

// membershipPeersUpTo is membershipPeers with the fan-out given by limit.
func (r *groupRuntime) membershipPeersUpTo(session *groupSession, limit int) []peer.AddrInfo {
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
	out := make([]peer.AddrInfo, 0, min(limit, maxMembershipSyncPeers))
	seen := make(map[peer.ID]struct{}, min(limit, maxMembershipSyncPeers))
	take := func(infos []entmoot.NodeInfo, room int) {
		for _, info := range infos {
			if room == 0 || len(out) == limit {
				return
			}
			binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
			if err != nil || binding.PeerID == r.host.ID() {
				continue
			}
			if _, already := seen[binding.PeerID]; already {
				continue
			}
			addrs := addrsFor(binding.PeerID)
			if len(addrs) == 0 {
				continue
			}
			seen[binding.PeerID] = struct{}{}
			out = append(out, peer.AddrInfo{ID: binding.PeerID, Addrs: addrs})
			room--
		}
	}
	// The members a rewind dropped go first, and they are the reason this is
	// not simply the current projection: they hold the records that restore
	// what this node lost, and asking only the survivors can never get them
	// back. There are none in the ordinary case. They are bounded so a long
	// rewind cannot crowd out ordinary sync, and they go first so a group
	// with more addressable members than this fan-out still reaches them.
	take(session.group.RewoundMemberInfos(), maxRewoundSyncPeers)
	take(session.group.ReachableMemberInfos(), limit)
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
		slog.Int("unauthorized_authors", summary.UnauthorizedAuthors),
		slog.Int("rate_limited", summary.RateLimited),
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
		if len(keeper.Addrs) == 0 {
			// Outbound-only members can serve history over an existing
			// connection without advertising an address we could dial.
			state := r.host.Network().Connectedness(keeper.ID)
			if state != network.Connected {
				continue
			}
		}
		keepers = append(keepers, keeper)
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
		session.reconciler.close()
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
// messages outranking another member's newest claim leave that member
// unreconciled until it republishes. Earlier shapes were far worse — 256
// messages, then 271 — but the limit is a window, not an absence of one.
//
// Nor is being crowded out a clean miss. Every claim INSIDE the window is
// ranked correctly, but the walk only writes for members it finds, so a member
// pushed entirely out of the window keeps whatever the store already holds —
// a name from live ingest that its own newer claim was meant to replace. And
// when a member's paging keys do not track its issue times — a clock step, or
// a crafted profile, which this walk cannot rule out — the boundary can fall
// between two of its claims, and the older one is then the only one read: a
// node that never saw the newer records a superseded name, or a name whose
// retraction sits just outside the window.
// Widening the window shifts that boundary without removing it; removing it
// needs an index on issue time, which the message store does not have.
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
		for _, message := range messages {
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
		boundary = nextProfileBoundary(messages)
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

// nextProfileBoundary turns a page into the cursor for the page after it. The
// cursor must name the page's OLDEST row in the store's own order, or the next
// query skips rows (a member loses its published name) or repeats them. It is
// a named function so a test can exercise the rule the runtime actually uses
// rather than re-implement it - a test that rebuilt this literal could not see
// a wrong field in it.
func nextProfileBoundary(messages []entmoot.Message) *store.PageBoundary {
	if len(messages) == 0 {
		return nil
	}
	oldest := messages[0]
	for _, message := range messages {
		if profileMessageNewer(oldest, message) {
			oldest = message
		}
	}
	return &store.PageBoundary{
		TimestampMS:    oldest.Timestamp,
		AuthorMemberID: profileMessageAuthor(oldest),
		MessageID:      oldest.ID,
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
