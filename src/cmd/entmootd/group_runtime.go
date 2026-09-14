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
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/conversion"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/merkle"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/ratelimit"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

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
	admission        *libp2ptransport.PersistentBootstrapAdmission
	liveRouter       *libp2ptransport.LiveRouter
	policyEnforcers  map[entmoot.GroupID]*groupPolicyEnforcer

	mu       sync.RWMutex
	sessions map[entmoot.GroupID]*groupSession
	joining  map[entmoot.GroupID]chan struct{}
	closed   bool
}

type groupSession struct {
	groupID       entmoot.GroupID
	roster        *roster.RosterLog
	live          *libp2ptransport.LiveGroup
	legacyHistory *merkle.Tree
	cancel        context.CancelFunc
	catchup       sync.Mutex
	history       libp2ptransport.HistorySyncState
	peerRecords   *libp2ptransport.PeerRecordCache
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
	admission, err := libp2ptransport.OpenPersistentBootstrapAdmission(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	policyStore, err := entpolicy.OpenFileStore(cfg.DataDir)
	if err != nil {
		_ = admission.Close()
		return nil, err
	}
	liveRouter, err := libp2ptransport.NewLiveRouter(context.Background(), cfg.Host)
	if err != nil {
		_ = admission.Close()
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
		admission:        admission,
		liveRouter:       liveRouter,
		sessions:         make(map[entmoot.GroupID]*groupSession),
		joining:          make(map[entmoot.GroupID]chan struct{}),
		policyEnforcers:  make(map[entmoot.GroupID]*groupPolicyEnforcer),
	}
	syncServer := &libp2ptransport.SyncServer{
		Host:          cfg.Host,
		Admission:     admission.BootstrapAdmission,
		Roster:        r.rosterForGroup,
		Store:         cfg.Notify,
		LegacyHistory: r.legacyHistoryForGroup,
		PeerRecords:   r.peerRecordsForGroup,
	}
	if err := syncServer.Install(); err != nil {
		_ = admission.Close()
		_ = liveRouter.Close()
		return nil, err
	}
	enrollment := &libp2ptransport.EnrollmentServer{
		Admission: admission.BootstrapAdmission,
		Enroll:    r.enroll,
	}
	if err := enrollment.Install(cfg.Host); err != nil {
		_ = liveRouter.Close()
		_ = admission.Close()
		return nil, err
	}
	return r, nil
}

func (r *groupRuntime) Start(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (r *groupRuntime) rosterForGroup(groupID entmoot.GroupID) (*roster.RosterLog, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[groupID]
	if !ok {
		return nil, false
	}
	return session.roster, true
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

// enroll applies the roster add for an applicant that presented a valid
// invite. The invite's checkpoint only has to be somewhere on this group's
// chain: the first joiner advances the head, and every other outstanding
// invite must still work.
func (r *groupRuntime) enroll(_ context.Context, capability entmoot.BootstrapCapability, applicant entmoot.NodeInfo) (libp2ptransport.EnrollmentResponse, error) {
	r.mu.RLock()
	session, ok := r.sessions[capability.GroupID]
	r.mu.RUnlock()
	if !ok {
		return libp2ptransport.EnrollmentResponse{}, libp2ptransport.RejectEnrollment(
			libp2ptransport.EnrollRejectUnknownGroup, "group %s is not served here", capability.GroupID.String())
	}
	founder, ok := session.roster.Founder()
	if !ok || !bytes.Equal(founder.EntmootPubKey, r.identity.PublicKey) || founder.MemberID == nil || *founder.MemberID != r.binding.MemberID {
		return libp2ptransport.EnrollmentResponse{}, libp2ptransport.RejectEnrollment(
			libp2ptransport.EnrollRejectNotIssuer, "local identity cannot sign roster changes for this group")
	}
	if capability.Founder.MemberID == nil || *capability.Founder.MemberID != *founder.MemberID || !bytes.Equal(capability.Founder.EntmootPubKey, founder.EntmootPubKey) {
		return libp2ptransport.EnrollmentResponse{}, libp2ptransport.RejectEnrollment(
			libp2ptransport.EnrollRejectIssuerMismatch, "invite issuer is not this group's founder")
	}
	if applicant.MemberID == nil {
		return libp2ptransport.EnrollmentResponse{}, libp2ptransport.RejectEnrollment(
			libp2ptransport.EnrollRejectApplicant, "applicant identity is incomplete")
	}
	if session.roster.IsMemberID(*applicant.MemberID) {
		existing, found := session.roster.MemberInfoByID(*applicant.MemberID)
		if !found || existing.PeerID != applicant.PeerID || !bytes.Equal(existing.EntmootPubKey, applicant.EntmootPubKey) {
			return libp2ptransport.EnrollmentResponse{}, libp2ptransport.RejectEnrollment(
				libp2ptransport.EnrollRejectIdentityConflict, "member id is already bound to another key")
		}
		return libp2ptransport.EnrollmentResponse{RosterHead: session.roster.Head(), Entries: session.roster.Entries()}, nil
	}
	if !session.roster.HasEntry(capability.RosterHead) {
		return libp2ptransport.EnrollmentResponse{}, libp2ptransport.RejectEnrollment(
			libp2ptransport.EnrollRejectUnknownCheckpoint, "invite checkpoint %s is not on this group's roster chain", capability.RosterHead.String())
	}
	target := entmoot.NodeInfo{EntmootPubKey: append([]byte(nil), applicant.EntmootPubKey...), MemberID: applicant.MemberID, PeerID: applicant.PeerID}

	// Roster timestamps must grow strictly. Two people redeeming a multi-use
	// invite in the same millisecond would otherwise make the second add
	// unappliable.
	timestamp := time.Now().UnixMilli()
	if head := session.roster.HeadTimestamp(); timestamp <= head {
		timestamp = head + 1
	}
	entry, err := session.roster.SignEntry(r.identity, "add", target, nil, timestamp)
	if err != nil {
		return libp2ptransport.EnrollmentResponse{}, err
	}
	if err := session.roster.Apply(entry); err != nil {
		return libp2ptransport.EnrollmentResponse{}, err
	}
	return libp2ptransport.EnrollmentResponse{RosterHead: session.roster.Head(), Entries: session.roster.Entries()}, nil
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

	rlog, err := roster.OpenJSONL(r.dataDir, groupID)
	if err != nil {
		return nil, false, err
	}
	if err := rlog.ClaimWriter(); err != nil {
		_ = rlog.Close()
		return nil, false, err
	}
	if err := r.validateLocalMembership(rlog); err != nil {
		_ = rlog.Close()
		return nil, false, err
	}
	legacyHistory, err := conversion.LoadLegacyHistoryTree(filepath.Join(r.dataDir, "groups", base64.RawURLEncoding.EncodeToString(groupID[:])), groupID, rlog.Entries())
	if err != nil {
		_ = rlog.Close()
		return nil, false, fmt.Errorf("load legacy history commitment: %w", err)
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	live, err := r.liveRouter.AddGroup(sessionCtx, libp2ptransport.LiveConfig{
		Host: r.host, GroupID: groupID, Roster: rlog, Store: r.notify,
		Authorize: func(message entmoot.Message) error {
			return r.enforceGroupPolicy(context.Background(), groupID, message)
		},
	})
	if err != nil {
		cancel()
		_ = rlog.Close()
		return nil, false, err
	}
	session := &groupSession{
		groupID: groupID, roster: rlog, live: live, legacyHistory: legacyHistory, cancel: cancel,
		peerRecords: libp2ptransport.NewPeerRecordCache(),
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = live.Close()
		cancel()
		_ = rlog.Close()
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
		response, err := libp2ptransport.Enroll(ctx, r.host, *info, capability, r.identity.PublicKey)
		if err != nil {
			lastErr = err
			continue
		}
		if err := validateEnrollmentResponse(capability, r.binding.MemberID, response); err != nil {
			return nil, false, err
		}
		if err := persistEnrollment(r.dataDir, capability.GroupID, response.Entries); err != nil {
			return nil, false, err
		}
		if err := persistGroupPeer(r.dataDir, capability.GroupID, *info); err != nil {
			return nil, false, err
		}
		session, added, err := r.AddLocalGroup(ctx, capability.GroupID)
		if err != nil {
			return nil, false, err
		}
		// Enrollment starts before the joining host owns its GossipSub topic.
		// Reconnect after topic setup so both peers exchange subscriptions
		// against the newly committed roster.
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

// validateEnrollmentResponse replays the served roster locally and requires
// that it admits this node under the invite's issuer.
func validateEnrollmentResponse(capability entmoot.BootstrapCapability, localMemberID entmoot.MemberID, response libp2ptransport.EnrollmentResponse) error {
	if len(response.Entries) == 0 {
		return errors.New("enrollment response has no roster")
	}
	candidate := roster.New(capability.GroupID)
	if err := candidate.AcceptGenesis(response.Entries[0]); err != nil {
		return fmt.Errorf("validate enrollment genesis: %w", err)
	}
	for _, entry := range response.Entries[1:] {
		if err := candidate.Apply(entry); err != nil {
			return fmt.Errorf("validate enrollment roster: %w", err)
		}
	}
	if candidate.Head() != response.RosterHead || !candidate.IsMemberID(localMemberID) {
		return errors.New("enrollment response checkpoint or membership mismatch")
	}
	founder, ok := candidate.Founder()
	if !ok || founder.MemberID == nil || capability.Founder.MemberID == nil || *founder.MemberID != *capability.Founder.MemberID || !bytes.Equal(founder.EntmootPubKey, capability.Founder.EntmootPubKey) {
		return errors.New("enrollment response founder mismatch")
	}
	return nil
}

func persistEnrollment(dataDir string, groupID entmoot.GroupID, entries []entmoot.RosterEntry) error {
	rlog, err := roster.OpenJSONL(dataDir, groupID)
	if err != nil {
		return err
	}
	defer rlog.Close()
	if len(rlog.Entries()) != 0 {
		return errors.New("refusing to replace an existing local roster")
	}
	if err := rlog.ClaimWriter(); err != nil {
		return err
	}
	if err := rlog.AcceptGenesis(entries[0]); err != nil {
		return err
	}
	for _, entry := range entries[1:] {
		if err := rlog.Apply(entry); err != nil {
			return err
		}
	}
	return nil
}

func stringMember(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (r *groupRuntime) validateLocalMembership(rlog *roster.RosterLog) error {
	if rlog.IsMemberID(r.binding.MemberID) {
		info, ok := rlog.MemberInfoByID(r.binding.MemberID)
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
		_ = session.roster.Close()
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
	r.syncRoster(ctx, session)
	go r.catchUp(ctx, session)
	r.pruneGroup(ctx, session)
	rosterTicker := time.NewTicker(2 * time.Second)
	historyTicker := time.NewTicker(time.Minute)
	retentionTicker := time.NewTicker(time.Hour)
	defer rosterTicker.Stop()
	defer historyTicker.Stop()
	defer retentionTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-rosterTicker.C:
			r.syncRoster(ctx, session)
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

func (r *groupRuntime) syncRoster(ctx context.Context, session *groupSession) {
	founder, ok := session.roster.Founder()
	if !ok {
		return
	}
	binding, err := libp2ptransport.BindingFromPublicKey(founder.EntmootPubKey)
	if err != nil || binding.PeerID == r.host.ID() {
		return
	}
	remote := peer.AddrInfo{ID: binding.PeerID, Addrs: r.host.Peerstore().Addrs(binding.PeerID)}
	if len(remote.Addrs) == 0 {
		peers, loadErr := loadGroupPeers(r.dataDir, session.groupID)
		if loadErr != nil {
			return
		}
		for _, candidate := range peers {
			if candidate.ID == remote.ID {
				remote.Addrs = candidate.Addrs
				break
			}
		}
	}
	syncCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	updates, err := libp2ptransport.FetchRosterUpdates(syncCtx, r.host, remote, session.groupID, session.roster.Entries())
	if err != nil {
		r.logger.Debug("libp2p roster sync", slog.String("group_id", session.groupID.String()), slog.String("err", err.Error()))
		return
	}
	for _, entry := range updates {
		if err := session.roster.Apply(entry); err != nil {
			r.logger.Warn("libp2p roster apply", slog.String("group_id", session.groupID.String()), slog.String("err", err.Error()))
			return
		}
	}
	if len(updates) > 0 {
		r.logger.Info("libp2p roster synchronized", slog.String("group_id", session.groupID.String()), slog.Int("entries", len(updates)))
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
			if err := libp2ptransport.VerifyHistoricalMessageWithProof(session.roster, message, time.Now(), proof); err != nil {
				return err
			}
			return r.enforceGroupPolicy(ctx, session.groupID, message)
		}, &session.history)
		summary = libp2ptransport.SummarizeKeeperProgress(progress)
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
		slog.Int("converged_hints", summary.ConvergedHints),
		slog.String("last_error", lastErr))
}

// keepersFor lists roster members with at least one known address, combining
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
	for _, memberID := range session.roster.MemberIDs() {
		info, ok := session.roster.MemberInfoByID(memberID)
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
		installed += libp2ptransport.InstallPeerRecords(r.host, session.roster, records,
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
	for _, memberID := range session.roster.MemberIDs() {
		info, ok := session.roster.MemberInfoByID(memberID)
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
		_ = session.roster.Close()
	}
	_ = r.liveRouter.Close()
	_ = r.admission.Close()
	_ = r.host.Close()
}

func (r *groupRuntime) legacyHistoryForGroup(groupID entmoot.GroupID) (*merkle.Tree, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[groupID]
	return session.legacyHistory, ok && session.legacyHistory != nil
}

func rosterHasLocalMemberIdentity(rlog *roster.RosterLog, memberID entmoot.MemberID, publicKey []byte) bool {
	info, ok := rlog.MemberInfoByID(memberID)
	return ok && bytes.Equal(info.EntmootPubKey, publicKey)
}

func openExistingRosterLog(dataDir string, groupID entmoot.GroupID) (*roster.RosterLog, bool, error) {
	if !roster.Exists(dataDir, groupID) {
		return nil, false, nil
	}
	rlog, err := roster.OpenJSONL(dataDir, groupID)
	if err != nil {
		return nil, false, err
	}
	return rlog, true, nil
}

func rosterHasLocalIdentityPubKey(rlog *roster.RosterLog, publicKey []byte) bool {
	memberID, err := entmoot.MemberIDFromPublicKey(publicKey)
	if err != nil {
		return false
	}
	member, ok := rlog.MemberInfoByID(memberID)
	return ok && bytes.Equal(member.EntmootPubKey, publicKey)
}
