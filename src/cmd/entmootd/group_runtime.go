package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/ratelimit"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"
)

type groupRuntimeConfig struct {
	NodeID           entmoot.NodeID
	Identity         *keystore.Identity
	DataDir          string
	Store            *store.SQLite
	Notify           *notifyingStore
	Transport        gossip.Transport
	LocalEndpoints   func() []entmoot.NodeEndpoint
	EndpointsChanged <-chan struct{}
	HideIP           bool
	TraceReconcile   bool
	Logger           *slog.Logger
}

type groupRuntime struct {
	nodeID         entmoot.NodeID
	identity       *keystore.Identity
	dataDir        string
	store          *store.SQLite
	notify         *notifyingStore
	localEndpoints func() []entmoot.NodeEndpoint
	endpointFanout *endpointChangeFanout
	hideIP         bool
	traceReconcile bool
	logger         *slog.Logger

	mux *groupMuxTransport

	mu       sync.RWMutex
	sessions map[entmoot.GroupID]*groupSession
	joining  map[entmoot.GroupID]chan struct{}
	wg       sync.WaitGroup
}

type groupSession struct {
	groupID              entmoot.GroupID
	roster               *roster.RosterLog
	gossip               *gossip.Gossiper
	cancel               context.CancelFunc
	unsubscribeEndpoints func()
}

type endpointChangeFanout struct {
	upstream <-chan struct{}

	mu     sync.Mutex
	subs   map[chan struct{}]struct{}
	closed bool
}

func newEndpointChangeFanout(upstream <-chan struct{}) *endpointChangeFanout {
	return &endpointChangeFanout{
		upstream: upstream,
		subs:     make(map[chan struct{}]struct{}),
	}
}

func (f *endpointChangeFanout) Run(ctx context.Context) {
	defer f.closeAll()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-f.upstream:
			if !ok {
				return
			}
			f.broadcast()
		}
	}
}

func (f *endpointChangeFanout) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	f.mu.Lock()
	if f.closed {
		close(ch)
		f.mu.Unlock()
		return ch, func() {}
	}
	f.subs[ch] = struct{}{}
	f.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			f.mu.Lock()
			if _, ok := f.subs[ch]; ok {
				delete(f.subs, ch)
				close(ch)
			}
			f.mu.Unlock()
		})
	}
	return ch, unsubscribe
}

func (f *endpointChangeFanout) broadcast() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	for ch := range f.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (f *endpointChangeFanout) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	for ch := range f.subs {
		close(ch)
		delete(f.subs, ch)
	}
}

func newGroupRuntime(cfg groupRuntimeConfig) (*groupRuntime, error) {
	if cfg.NodeID == 0 {
		return nil, errors.New("group runtime: node id is required")
	}
	if cfg.Identity == nil {
		return nil, errors.New("group runtime: identity is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("group runtime: store is required")
	}
	if cfg.Notify == nil {
		return nil, errors.New("group runtime: notifying store is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("group runtime: transport is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	var endpointFanout *endpointChangeFanout
	if cfg.EndpointsChanged != nil {
		endpointFanout = newEndpointChangeFanout(cfg.EndpointsChanged)
	}
	return &groupRuntime{
		nodeID:         cfg.NodeID,
		identity:       cfg.Identity,
		dataDir:        cfg.DataDir,
		store:          cfg.Store,
		notify:         cfg.Notify,
		localEndpoints: cfg.LocalEndpoints,
		endpointFanout: endpointFanout,
		hideIP:         cfg.HideIP,
		traceReconcile: cfg.TraceReconcile,
		logger:         logger,
		mux:            newGroupMuxTransport(cfg.Transport, logger),
		sessions:       make(map[entmoot.GroupID]*groupSession),
		joining:        make(map[entmoot.GroupID]chan struct{}),
	}, nil
}

func (r *groupRuntime) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	if r.endpointFanout != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.endpointFanout.Run(runCtx)
		}()
	}
	err := r.mux.AcceptLoop(ctx)
	cancel()
	wg.Wait()
	return err
}

func (r *groupRuntime) AddInvite(ctx context.Context, invite entmoot.Invite) (*groupSession, bool, error) {
	groupID := invite.GroupID
	var joinDone chan struct{}
	for {
		r.mu.Lock()
		if sess, ok := r.sessions[groupID]; ok {
			r.mu.Unlock()
			return sess, false, nil
		}
		if ch, ok := r.joining[groupID]; ok {
			r.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}
		joinDone = make(chan struct{})
		r.joining[groupID] = joinDone
		r.mu.Unlock()
		break
	}
	defer func() {
		r.mu.Lock()
		if r.joining[groupID] == joinDone {
			delete(r.joining, groupID)
			close(joinDone)
		}
		r.mu.Unlock()
	}()

	groupTransport, createdGroup := r.mux.Group(groupID)
	endpointsChanged, unsubscribeEndpoints := r.subscribeEndpointChanges()
	cleanupGroup := func() {
		unsubscribeEndpoints()
		if createdGroup {
			r.mux.RemoveGroup(groupID)
		}
	}
	rlog, err := roster.OpenJSONL(r.dataDir, groupID)
	if err != nil {
		cleanupGroup()
		return nil, false, fmt.Errorf("open roster: %w", err)
	}
	g, err := gossip.New(gossip.Config{
		LocalNode:        r.nodeID,
		Identity:         r.identity,
		Roster:           rlog,
		Store:            r.notify,
		Transport:        groupTransport,
		GroupID:          invite.GroupID,
		Logger:           r.logger,
		TransportAdStore: r.store,
		RateLimiter:      ratelimit.New(ratelimit.DefaultLimits(), nil),
		LocalEndpoints:   r.localEndpoints,
		EndpointsChanged: endpointsChanged,
		HideIP:           r.hideIP,
		TraceReconcile:   r.traceReconcile,
	})
	if err != nil {
		_ = rlog.Close()
		cleanupGroup()
		return nil, false, fmt.Errorf("new gossiper: %w", err)
	}

	joinCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = g.Join(joinCtx, &invite)
	cancel()
	if err != nil {
		_ = rlog.Close()
		cleanupGroup()
		return nil, false, err
	}
	r.replayTransportAds(ctx, groupID)

	sessCtx, sessCancel := context.WithCancel(ctx)
	sess := &groupSession{
		groupID:              groupID,
		roster:               rlog,
		gossip:               g,
		cancel:               sessCancel,
		unsubscribeEndpoints: unsubscribeEndpoints,
	}

	r.mu.Lock()
	if existing, ok := r.sessions[groupID]; ok {
		r.mu.Unlock()
		sessCancel()
		unsubscribeEndpoints()
		_ = rlog.Close()
		return existing, false, nil
	}
	r.sessions[groupID] = sess
	r.mu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := g.Start(sessCtx); err != nil {
			r.logger.Warn("join: group gossiper stopped",
				slog.String("group_id", invite.GroupID.String()),
				slog.String("err", err.Error()))
		}
	}()
	return sess, true, nil
}

func (r *groupRuntime) subscribeEndpointChanges() (<-chan struct{}, func()) {
	if r.endpointFanout == nil {
		return nil, func() {}
	}
	return r.endpointFanout.Subscribe()
}

func (r *groupRuntime) replayTransportAds(ctx context.Context, groupID entmoot.GroupID) {
	replayCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ads, err := r.store.GetAllTransportAds(replayCtx, groupID, time.Now(), false)
	if err != nil {
		r.logger.Warn("join: replay transport ads",
			slog.String("group_id", groupID.String()),
			slog.String("err", err.Error()))
		return
	}
	for _, ad := range ads {
		if ad.Author.PilotNodeID == r.nodeID {
			continue
		}
		if err := r.mux.SetPeerEndpoints(replayCtx, ad.Author.PilotNodeID, ad.Endpoints); err != nil {
			r.logger.Debug("join: replay endpoint install failed",
				slog.String("group_id", groupID.String()),
				slog.Uint64("peer", uint64(ad.Author.PilotNodeID)),
				slog.String("err", err.Error()))
		}
	}
}

func (r *groupRuntime) Get(groupID entmoot.GroupID) (*groupSession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sess, ok := r.sessions[groupID]
	return sess, ok
}

func (r *groupRuntime) ActiveGroupIDs() []entmoot.GroupID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]entmoot.GroupID, 0, len(r.sessions))
	for gid := range r.sessions {
		out = append(out, gid)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out
}

func (r *groupRuntime) SingleGroup() (entmoot.GroupID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sessions) != 1 {
		return entmoot.GroupID{}, false
	}
	for gid := range r.sessions {
		return gid, true
	}
	return entmoot.GroupID{}, false
}

func (r *groupRuntime) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}

func (r *groupRuntime) Close() {
	r.mu.RLock()
	sessions := make([]*groupSession, 0, len(r.sessions))
	for _, sess := range r.sessions {
		sessions = append(sessions, sess)
	}
	r.mu.RUnlock()
	for _, sess := range sessions {
		sess.cancel()
		if sess.unsubscribeEndpoints != nil {
			sess.unsubscribeEndpoints()
		}
	}
	r.wg.Wait()
	for _, sess := range sessions {
		_ = sess.roster.Close()
	}
}

type groupMuxTransport struct {
	base   gossip.Transport
	logger *slog.Logger

	mu     sync.RWMutex
	groups map[entmoot.GroupID]*groupTransport
	cbs    map[entmoot.GroupID]func(entmoot.NodeID)
}

func newGroupMuxTransport(base gossip.Transport, logger *slog.Logger) *groupMuxTransport {
	if logger == nil {
		logger = slog.Default()
	}
	m := &groupMuxTransport{
		base:   base,
		logger: logger,
		groups: make(map[entmoot.GroupID]*groupTransport),
		cbs:    make(map[entmoot.GroupID]func(entmoot.NodeID)),
	}
	base.SetOnTunnelUp(m.fireTunnelUp)
	return m
}

func (m *groupMuxTransport) Group(groupID entmoot.GroupID) (*groupTransport, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if tr, ok := m.groups[groupID]; ok {
		return tr, false
	}
	tr := &groupTransport{
		parent:   m,
		groupID:  groupID,
		acceptCh: make(chan muxAccept, 64),
	}
	m.groups[groupID] = tr
	return tr, true
}

func (m *groupMuxTransport) RemoveGroup(groupID entmoot.GroupID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.groups, groupID)
	delete(m.cbs, groupID)
}

func (m *groupMuxTransport) AcceptLoop(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, remote, err := m.base.Accept(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("group mux accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.routeConn(ctx, conn, remote)
		}()
	}
}

func (m *groupMuxTransport) routeConn(ctx context.Context, conn net.Conn, remote entmoot.NodeID) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	t, body, err := wire.ReadFrame(conn)
	if err != nil {
		m.logger.Warn("group mux: read first frame",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	payload, err := wire.Decode(t, body)
	if err != nil {
		m.logger.Warn("group mux: decode first frame",
			slog.Uint64("remote", uint64(remote)),
			slog.String("type", t.String()),
			slog.String("err", err.Error()))
		_ = conn.Close()
		return
	}
	groupID, ok := m.groupForPayload(payload)
	if !ok {
		m.logger.Warn("group mux: first frame has no routable group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("type", t.String()))
		_ = conn.Close()
		return
	}
	tr, ok := m.lookupGroup(groupID)
	if !ok {
		m.logger.Warn("group mux: no active group for inbound frame",
			slog.Uint64("remote", uint64(remote)),
			slog.String("group_id", groupID.String()),
			slog.String("type", t.String()))
		_ = conn.Close()
		return
	}
	wrapped := &prefixedConn{
		Conn:   conn,
		prefix: bytes.NewReader(rawWireFrame(t, body)),
	}
	select {
	case tr.acceptCh <- muxAccept{conn: wrapped, remote: remote}:
	case <-ctx.Done():
		_ = wrapped.Close()
	}
}

func (m *groupMuxTransport) lookupGroup(groupID entmoot.GroupID) (*groupTransport, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tr, ok := m.groups[groupID]
	return tr, ok
}

func (m *groupMuxTransport) groupForPayload(payload any) (entmoot.GroupID, bool) {
	switch v := payload.(type) {
	case *wire.Hello:
		m.mu.RLock()
		defer m.mu.RUnlock()
		for _, gid := range v.Groups {
			if _, ok := m.groups[gid]; ok {
				return gid, true
			}
		}
	case *wire.RosterReq:
		return v.GroupID, true
	case *wire.RosterResp:
		return v.GroupID, true
	case *wire.Gossip:
		return v.GroupID, true
	case *wire.FetchReq:
		return v.GroupID, true
	case *wire.FetchResp:
		return v.GroupID, true
	case *wire.MerkleReq:
		return v.GroupID, true
	case *wire.MerkleResp:
		return v.GroupID, true
	case *wire.RangeReq:
		return v.GroupID, true
	case *wire.RangeResp:
		return v.GroupID, true
	case *wire.IHave:
		return v.GroupID, true
	case *wire.Graft:
		return v.GroupID, true
	case *wire.Prune:
		return v.GroupID, true
	case *wire.TransportAd:
		return v.GroupID, true
	case *wire.TransportSnapshotReq:
		return v.GroupID, true
	case *wire.TransportSnapshotResp:
		return v.GroupID, true
	case *wire.Reconcile:
		return v.GroupID, true
	}
	return entmoot.GroupID{}, false
}

func (m *groupMuxTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	return m.base.Dial(ctx, peer)
}

func (m *groupMuxTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return m.base.TrustedPeers(ctx)
}

func (m *groupMuxTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	return m.base.SetPeerEndpoints(ctx, peer, endpoints)
}

func (m *groupMuxTransport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {
	_ = cb
}

func (m *groupMuxTransport) Close() error {
	return nil
}

func (m *groupMuxTransport) setGroupCallback(groupID entmoot.GroupID, cb func(entmoot.NodeID)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cb == nil {
		delete(m.cbs, groupID)
		return
	}
	m.cbs[groupID] = cb
}

func (m *groupMuxTransport) fireTunnelUp(peer entmoot.NodeID) {
	m.mu.RLock()
	callbacks := make([]func(entmoot.NodeID), 0, len(m.cbs))
	for _, cb := range m.cbs {
		callbacks = append(callbacks, cb)
	}
	m.mu.RUnlock()
	for _, cb := range callbacks {
		cb(peer)
	}
}

type groupTransport struct {
	parent   *groupMuxTransport
	groupID  entmoot.GroupID
	acceptCh chan muxAccept
}

type muxAccept struct {
	conn   net.Conn
	remote entmoot.NodeID
}

func (t *groupTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	return t.parent.Dial(ctx, peer)
}

func (t *groupTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	select {
	case item := <-t.acceptCh:
		return item.conn, item.remote, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func (t *groupTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	return t.parent.TrustedPeers(ctx)
}

func (t *groupTransport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	return t.parent.SetPeerEndpoints(ctx, peer, endpoints)
}

func (t *groupTransport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {
	t.parent.setGroupCallback(t.groupID, cb)
}

func (t *groupTransport) Close() error {
	t.parent.RemoveGroup(t.groupID)
	return nil
}

type prefixedConn struct {
	net.Conn
	prefix *bytes.Reader
}

func (c *prefixedConn) Read(p []byte) (int, error) {
	if c.prefix != nil && c.prefix.Len() > 0 {
		return c.prefix.Read(p)
	}
	return c.Conn.Read(p)
}

func rawWireFrame(t wire.MsgType, body []byte) []byte {
	payloadLen := 1 + len(body)
	buf := make([]byte, 4+payloadLen)
	binary.BigEndian.PutUint32(buf[:4], uint32(payloadLen))
	buf[4] = byte(t)
	copy(buf[5:], body)
	return buf
}
