package pilot

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/yamux"

	"entmoot/pkg/entmoot"
)

const pilotSessionDrainTimeout = 15 * time.Second

type sessionRole string

const (
	sessionRoleOutbound sessionRole = "outbound"
	sessionRoleInbound  sessionRole = "inbound"
)

type sessionState string

const (
	sessionStateActive   sessionState = "active"
	sessionStateDraining sessionState = "draining"
	sessionStateClosed   sessionState = "closed"
)

var errSessionNotActive = errors.New("pilot: yamux session not active")

type sessionManager struct {
	mu         sync.Mutex
	nextGen    uint64
	outbound   map[entmoot.NodeID]*managedSession
	inbound    []*managedSession
	logger     *slog.Logger
	trace      bool
	drainAfter time.Duration
}

func newSessionManager(logger *slog.Logger, trace bool) *sessionManager {
	return &sessionManager{
		outbound:   make(map[entmoot.NodeID]*managedSession),
		logger:     logger,
		trace:      trace,
		drainAfter: pilotSessionDrainTimeout,
	}
}

func (m *sessionManager) getOutbound(peer entmoot.NodeID) (*managedSession, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.outbound[peer]
	if !ok || !entry.isActive() {
		if ok {
			delete(m.outbound, peer)
		}
		return nil, false
	}
	return entry, true
}

func (m *sessionManager) installOutbound(peer entmoot.NodeID, sess *yamux.Session) (*managedSession, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.outbound[peer]; ok && existing.isActive() {
		return existing, false
	}
	m.nextGen++
	entry := newManagedSession(peer, m.nextGen, sessionRoleOutbound, sess, m.logger, m.trace, m.drainAfter)
	m.outbound[peer] = entry
	entry.trace("installed")
	return entry, true
}

func (m *sessionManager) addInbound(peer entmoot.NodeID, sess *yamux.Session) *managedSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextGen++
	entry := newManagedSession(peer, m.nextGen, sessionRoleInbound, sess, m.logger, m.trace, m.drainAfter)
	m.inbound = append(m.inbound, entry)
	entry.trace("installed")
	return entry
}

func (m *sessionManager) drainOutbound(peer entmoot.NodeID, match *managedSession, reason string) bool {
	m.mu.Lock()
	entry, ok := m.outbound[peer]
	if !ok || (match != nil && entry != match) {
		m.mu.Unlock()
		return false
	}
	delete(m.outbound, peer)
	m.mu.Unlock()
	entry.drain(reason)
	return true
}

func (m *sessionManager) closeAll(reason string) {
	m.mu.Lock()
	outbound := make([]*managedSession, 0, len(m.outbound))
	for peer, entry := range m.outbound {
		outbound = append(outbound, entry)
		delete(m.outbound, peer)
	}
	inbound := m.inbound
	m.inbound = nil
	m.mu.Unlock()

	for _, entry := range outbound {
		entry.closeNow(reason)
	}
	for _, entry := range inbound {
		entry.closeNow(reason)
	}
}

type managedSession struct {
	peer       entmoot.NodeID
	generation uint64
	role       sessionRole
	sess       *yamux.Session
	logger     *slog.Logger
	traceOn    bool
	drainAfter time.Duration

	mu     sync.Mutex
	state  sessionState
	active int
	done   chan struct{}
	once   sync.Once
}

func newManagedSession(peer entmoot.NodeID, gen uint64, role sessionRole, sess *yamux.Session, logger *slog.Logger, trace bool, drainAfter time.Duration) *managedSession {
	return &managedSession{
		peer:       peer,
		generation: gen,
		role:       role,
		sess:       sess,
		logger:     logger,
		traceOn:    trace,
		drainAfter: drainAfter,
		state:      sessionStateActive,
		done:       make(chan struct{}),
	}
}

func (s *managedSession) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == sessionStateActive && !s.sess.IsClosed()
}

func (s *managedSession) openStream() (net.Conn, error) {
	if !s.acquire() {
		return nil, errSessionNotActive
	}
	stream, err := s.sess.OpenStream()
	if err != nil {
		s.release()
		return nil, err
	}
	s.trace("stream_opened")
	return &trackedSessionConn{Conn: stream, release: s.release}, nil
}

func (s *managedSession) wrapAcceptedStream(stream net.Conn) (net.Conn, bool) {
	if !s.acquire() {
		_ = stream.Close()
		return nil, false
	}
	s.trace("stream_accepted")
	return &trackedSessionConn{Conn: stream, release: s.release}, true
}

func (s *managedSession) acquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != sessionStateActive || s.sess.IsClosed() {
		return false
	}
	s.active++
	return true
}

func (s *managedSession) release() {
	shouldClose := false
	s.mu.Lock()
	if s.active > 0 {
		s.active--
	}
	if s.active == 0 && s.state == sessionStateDraining {
		shouldClose = true
	}
	active := s.active
	state := s.state
	s.mu.Unlock()
	s.trace("stream_closed", slog.Int("active_streams", active), slog.String("state", string(state)))
	if shouldClose {
		s.closeNow("drain complete")
	}
}

func (s *managedSession) drain(reason string) {
	s.mu.Lock()
	if s.state != sessionStateActive {
		state := s.state
		s.mu.Unlock()
		s.trace("drain_skipped", slog.String("state", string(state)), slog.String("reason", reason))
		return
	}
	s.state = sessionStateDraining
	active := s.active
	s.mu.Unlock()
	s.trace("draining", slog.Int("active_streams", active), slog.String("reason", reason))
	if err := s.sess.GoAway(); err != nil {
		s.trace("goaway_failed", slog.String("err", err.Error()), slog.String("reason", reason))
	}
	if active == 0 {
		s.closeNow(reason)
		return
	}
	go func() {
		select {
		case <-s.done:
		case <-time.After(s.drainAfter):
			s.closeNow("drain timeout: " + reason)
		}
	}()
}

func (s *managedSession) closeNow(reason string) {
	s.once.Do(func() {
		s.mu.Lock()
		s.state = sessionStateClosed
		active := s.active
		s.mu.Unlock()
		s.trace("closed", slog.Int("active_streams", active), slog.String("reason", reason))
		_ = s.sess.Close()
		close(s.done)
	})
}

func (s *managedSession) trace(event string, attrs ...any) {
	if !s.traceOn {
		return
	}
	base := []any{
		slog.String("event", event),
		slog.Uint64("peer", uint64(s.peer)),
		slog.Uint64("generation", s.generation),
		slog.String("role", string(s.role)),
	}
	s.logger.Info("pilot transport trace", append(base, attrs...)...)
}

type trackedSessionConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *trackedSessionConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
