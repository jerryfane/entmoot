// Package pilot implements the gossip.Transport interface over a live Pilot
// Protocol daemon.
//
// The adapter connects to a locally-running Pilot daemon over its Unix IPC
// socket using the in-tree ipcclient package (an independent implementation
// of Pilot's documented IPC wire protocol — see
// pkg/entmoot/transport/pilot/ipcclient and SPEC.md §6), binds a listener on a
// well-known port (typically :1004 for Entmoot), and wraps ipcclient.DialAddr /
// ipcclient.Listener / ipcclient.TrustedPeers into the small Transport surface
// the gossip package depends on.
//
// # Stream model
//
// The adapter opens one Pilot virtual stream per Entmoot interaction. Pilot
// owns the long-lived authenticated transport, NAT traversal, TURN routing,
// and stream lifecycle. Entmoot does not add a second persistent multiplexer
// above Pilot; this keeps stale Pilot conn_id state from being hidden behind
// an app-level session cache.
//
// Wire compatibility: Entmoot's frame format is unchanged. Peers running the
// older yamux-over-Pilot transport do NOT interoperate with this raw-stream
// transport. Upgrade all nodes together.
//
// # Why no unit tests
//
// The adapter is intentionally shipped without unit tests that exercise the
// Pilot driver itself:
//
//   - driver.Connect requires a running Pilot daemon on the other end of the
//     socket path. `go test ./...` runs in environments that have no such
//     daemon.
//   - Replacing the driver with a fake for these tests would exercise the
//     fake, not the adapter. The integration gate lives at Phase F's canary,
//     which stands up real Pilot daemons and runs entmootd against them.
package pilot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

const (
	pilotStreamDialTimeout       = 45 * time.Second
	pilotMaxConcurrentPeerDials  = 2
	pilotTraceTransportEventName = "pilot transport trace"
)

// Config parameterizes a Pilot-backed Transport.
type Config struct {
	// SocketPath is the Unix-domain IPC socket of the Pilot daemon. An empty
	// string selects ipcclient.DefaultSocketPath ("/tmp/pilot.sock").
	SocketPath string

	// ListenPort is the Pilot port Entmoot binds for inbound gossip
	// connections. v0 defaults to 1004 (see ARCHITECTURE §2).
	ListenPort uint16

	// Network is the Pilot network id to use when constructing outbound Addr
	// values. Zero (the default) is the global network.
	Network uint16

	// Logger receives slog.Warn / slog.Error surfaces for malformed driver
	// responses, close races, and similar non-fatal conditions. Nil selects
	// slog.Default().
	Logger *slog.Logger

	// TraceGossipTransport enables high-volume lifecycle logs for the Pilot
	// transport adapter. It is intended for operational debugging only.
	TraceGossipTransport bool
}

// inboundStream is one accepted Pilot stream plus the NodeID of the peer that
// opened it. acceptPilotConns sends these; Accept reads them.
type inboundStream struct {
	conn net.Conn
	peer entmoot.NodeID
}

// Transport implements gossip.Transport over a live Pilot daemon.
//
// Construction via Open is what actually talks to the daemon: it issues
// ipcclient.Connect, ipcclient.Driver.Info (to discover the local node id), and
// ipcclient.Driver.Listen. After Open returns, a background goroutine accepts
// inbound Pilot connections; outbound Dials create one Pilot stream per
// Entmoot interaction.
type Transport struct {
	cfg    Config
	logger *slog.Logger

	driver   *ipcclient.Driver
	listener *ipcclient.Listener
	nodeID   entmoot.NodeID
	dialAddr func(context.Context, Addr, uint16) (*ipcclient.Conn, error)

	limits *peerDialLimiter

	// inboundStreams fans accepted Pilot streams into one channel that Accept()
	// reads from. Buffered so bursty peers do not block the accept goroutine.
	inboundStreams chan inboundStream

	// acceptWG tracks the background acceptPilotConns loop so Close() waits for
	// clean shutdown.
	acceptWG sync.WaitGroup

	// closeOnce serializes driver/listener teardown. Close is idempotent.
	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error

	// onTunnelUp holds a func(entmoot.NodeID) — the callback installed by
	// SetOnTunnelUp. Stored via atomic.Value so the hot paths in Dial /
	// acceptPilotConns can load it without acquiring any per-transport mutex.
	// An empty Value (no Store yet) or a stored typed-nil both behave as "no
	// callback". (v1.2.1)
	onTunnelUp atomic.Value
}

// Open connects to the Pilot daemon at cfg.SocketPath, discovers the local
// node id via ipcclient.Driver.Info, and binds a listener on cfg.ListenPort.
// The returned Transport is ready to Dial / Accept immediately.
//
// On any failure mid-way (e.g. bind fails after connect), previously-acquired
// resources are released before returning.
func Open(cfg Config) (*Transport, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	d, err := ipcclient.Connect(cfg.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("pilot: connect %q: %w", cfg.SocketPath, err)
	}

	// Open is synchronous on callers' expectation; a background context
	// scoped to the brief Info/Listen exchanges is the right fit. Pilot's
	// IPC is local and responses arrive in milliseconds.
	openCtx := context.Background()

	info, err := d.Info(openCtx)
	if err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("pilot: info: %w", err)
	}
	nid, err := extractNodeID(info)
	if err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("pilot: info: %w", err)
	}

	ln, err := d.Listen(openCtx, cfg.ListenPort)
	if err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("pilot: listen :%d: %w", cfg.ListenPort, err)
	}

	t := &Transport{
		cfg:            cfg,
		logger:         logger,
		driver:         d,
		listener:       ln,
		nodeID:         nid,
		dialAddr:       d.DialAddr,
		closed:         make(chan struct{}),
		limits:         newPeerDialLimiter(pilotMaxConcurrentPeerDials),
		inboundStreams: make(chan inboundStream, 64),
	}
	t.acceptWG.Add(1)
	go t.acceptPilotConns()
	return t, nil
}

// NodeID returns the Pilot node id reported by the daemon at Open time.
func (t *Transport) NodeID() entmoot.NodeID {
	return t.nodeID
}

// Driver returns the underlying *ipcclient.Driver. Exposed so subcommands like
// `info` can query daemon state (hostname, address) without re-dialing the
// socket. Callers MUST NOT Close the returned driver; call Transport.Close
// instead.
func (t *Transport) Driver() *ipcclient.Driver {
	return t.driver
}

// Dial implements gossip.Transport. It returns a fresh Pilot stream for peer.
//
// Honoring ctx: callers may use short request deadlines, but the first-contact
// daemon DialAddr exchange can legitimately complete after that deadline. The
// adapter keeps the IPC waiter alive under an internal bounded context so a
// late DialOK is consumed and any late conn is closed instead of becoming an
// orphaned daemon-level connection that Entmoot cannot use.
func (t *Transport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	select {
	case <-t.closed:
		return nil, net.ErrClosed
	default:
	}

	slot, err := t.limits.acquireSlot(ctx, peer, t.closed)
	if err != nil {
		return nil, err
	}

	conn, releaseSlotOnError, err := t.dialPilotStream(ctx, peer, slot)
	if err != nil {
		if releaseSlotOnError {
			slot.Release()
		}
		return nil, err
	}
	t.trace("stream_opened", slog.Uint64("peer", uint64(peer)), slog.String("direction", "outbound"))
	t.fireOnTunnelUp(peer)
	return &limitedConn{
		Conn: wrapUnblockableConn(conn),
		release: func() {
			t.trace("stream_closed", slog.Uint64("peer", uint64(peer)), slog.String("direction", "outbound"))
			slot.Release()
		},
	}, nil
}

func (t *Transport) dialPilotStream(ctx context.Context, peer entmoot.NodeID, slot *peerDialSlot) (*ipcclient.Conn, bool, error) {
	addr := Addr{Network: t.cfg.Network, Node: uint32(peer)}

	type dialResult struct {
		conn *ipcclient.Conn
		err  error
	}
	dialCtx, dialCancel := context.WithTimeout(context.Background(), pilotStreamDialTimeout)
	doneWatchingDialCtx := make(chan struct{})
	go func() {
		defer close(doneWatchingDialCtx)
		select {
		case <-t.closed:
			dialCancel()
		case <-dialCtx.Done():
		}
	}()
	resultCh := make(chan dialResult, 1)
	go func() {
		dialAddr := t.dialAddr
		if dialAddr == nil {
			dialAddr = t.driver.DialAddr
		}
		c, err := dialAddr(dialCtx, addr, t.cfg.ListenPort)
		resultCh <- dialResult{conn: c, err: err}
	}()

	var pilotConn *ipcclient.Conn
	select {
	case r := <-resultCh:
		dialCancel()
		<-doneWatchingDialCtx
		if r.err != nil {
			return nil, true, fmt.Errorf("pilot: dial %s:%d: %w", addr, t.cfg.ListenPort, r.err)
		}
		pilotConn = r.conn
	case <-ctx.Done():
		go func() {
			r := <-resultCh
			dialCancel()
			if r.conn != nil {
				_ = r.conn.Close()
			}
			slot.Release()
		}()
		return nil, false, fmt.Errorf("pilot: dial %s:%d: %w", addr, t.cfg.ListenPort, ctx.Err())
	case <-t.closed:
		dialCancel()
		<-doneWatchingDialCtx
		go func() {
			r := <-resultCh
			dialCancel()
			if r.conn != nil {
				_ = r.conn.Close()
			}
			slot.Release()
		}()
		return nil, false, net.ErrClosed
	}

	return pilotConn, false, nil
}

// DropPeerSession implements gossip.PeerSessionDropper. Raw Pilot streams are
// not cached above Pilot, so there is no Entmoot-side peer session to drop.
func (t *Transport) DropPeerSession(peer entmoot.NodeID) bool {
	t.trace("drop_peer_session_noop", slog.Uint64("peer", uint64(peer)))
	return false
}

// acceptPilotConns runs for the lifetime of the Transport. It accepts raw
// inbound Pilot connections, extracts the remote NodeID from the driver's
// RemoteAddr, and fans them into inboundStreams.
//
// Exits on listener error; Close() closes the listener and cancels acceptCtx,
// which unblocks Accept via ctx.Done() regardless of whether a listener-level
// close propagated in time.
func (t *Transport) acceptPilotConns() {
	defer t.acceptWG.Done()
	// Derive a context that fires when t.closed closes so a blocked
	// listener.Accept unwinds deterministically on Transport.Close —
	// the ipcclient Accept honours context cancellation directly.
	acceptCtx, acceptCancel := context.WithCancel(context.Background())
	go func() {
		<-t.closed
		acceptCancel()
	}()
	defer acceptCancel()
	for {
		pilotConn, err := t.listener.Accept(acceptCtx)
		if err != nil {
			select {
			case <-t.closed:
				return
			default:
			}
			t.logger.Warn("pilot: accept pilot conn", slog.String("err", err.Error()))
			return
		}
		remote, err := remoteNodeID(pilotConn)
		if err != nil {
			t.logger.Warn("pilot: decode inbound remote", slog.String("err", err.Error()))
			_ = pilotConn.Close()
			continue
		}
		wrapped := &traceCloseConn{
			Conn: wrapUnblockableConn(pilotConn),
			onClose: func() {
				t.trace("stream_closed", slog.Uint64("peer", uint64(remote)), slog.String("direction", "inbound"))
			},
		}
		t.trace("stream_accepted", slog.Uint64("peer", uint64(remote)), slog.String("direction", "inbound"))
		t.fireOnTunnelUp(remote)
		select {
		case t.inboundStreams <- inboundStream{conn: wrapped, peer: remote}:
		case <-t.closed:
			_ = wrapped.Close()
			return
		}
	}
}

// Accept implements gossip.Transport. It reads one accepted Pilot stream off
// the fan-in channel populated by acceptPilotConns and returns it together
// with the NodeID of the peer that opened it.
//
// Returns net.ErrClosed after Close and ctx.Err() on cancellation.
func (t *Transport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	select {
	case <-t.closed:
		return nil, 0, net.ErrClosed
	case in := <-t.inboundStreams:
		return in.conn, in.peer, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

// TrustedPeers implements gossip.Transport. It calls driver.TrustedPeers and
// pulls NodeIDs out of the "trusted" array.
//
// Pilot's IPC returns a map[string]interface{}; JSON numbers come back as
// float64. Malformed entries are skipped with slog.Warn rather than failing
// the whole call so one corrupted peer record does not block join.
func (t *Transport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	select {
	case <-t.closed:
		return nil, net.ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	resp, err := t.driver.TrustedPeers(ctx)
	if err != nil {
		return nil, fmt.Errorf("pilot: trusted peers: %w", err)
	}
	list, ok := resp["trusted"].([]interface{})
	if !ok {
		// Empty/malformed response — treat as "no trusted peers" rather than
		// surfacing a typed error; the upstream bootstrap path handles an
		// empty list gracefully.
		t.logger.Warn("pilot: trusted_peers response has no trusted array")
		return nil, nil
	}
	out := make([]entmoot.NodeID, 0, len(list))
	for i, item := range list {
		entry, ok := item.(map[string]interface{})
		if !ok {
			t.logger.Warn("pilot: trusted peer entry is not an object",
				slog.Int("index", i))
			continue
		}
		idf, ok := entry["node_id"].(float64)
		if !ok {
			t.logger.Warn("pilot: trusted peer entry missing node_id",
				slog.Int("index", i))
			continue
		}
		if idf < 0 || idf > float64(^uint32(0)) {
			t.logger.Warn("pilot: trusted peer node_id out of uint32 range",
				slog.Int("index", i),
				slog.Float64("node_id", idf))
			continue
		}
		out = append(out, entmoot.NodeID(uint32(idf)))
	}
	return out, nil
}

// SetPeerEndpoints implements gossip.Transport. Forwards the endpoints
// straight to the Pilot daemon via driver.SetPeerEndpoints so the
// daemon's peerTCP map gets populated with externally-sourced endpoint
// hints (e.g. from an accepted Entmoot TransportAd gossip or an
// invite-embedded list at Join time). Registry-sourced endpoints still
// take precedence on the Pilot side if both exist. (v1.2.0)
func (t *Transport) SetPeerEndpoints(ctx context.Context, peer entmoot.NodeID, endpoints []entmoot.NodeEndpoint) error {
	select {
	case <-t.closed:
		return net.ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if peer == t.nodeID {
		return nil
	}
	ipcEps := make([]ipcclient.Endpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		ipcEps = append(ipcEps, ipcclient.Endpoint{Network: ep.Network, Addr: ep.Addr})
	}
	if err := t.driver.SetPeerEndpoints(ctx, uint32(peer), ipcEps); err != nil {
		return fmt.Errorf("pilot: set peer endpoints for %d: %w", peer, err)
	}
	return nil
}

// SetOnTunnelUp implements gossip.Transport. The callback is stored via
// atomic.Value for lock-free reads on the hot path (Dial / acceptPilotConns).
// Passing nil clears the callback (stored as a typed-nil so Load never races
// with Store). Subsequent calls replace any previously-installed callback.
// (v1.2.1)
func (t *Transport) SetOnTunnelUp(cb func(peer entmoot.NodeID)) {
	if cb == nil {
		t.onTunnelUp.Store((func(entmoot.NodeID))(nil))
		return
	}
	t.onTunnelUp.Store(cb)
}

// fireOnTunnelUp loads the currently-installed OnTunnelUp callback (if
// any) and invokes it in a fresh goroutine with a panic recover so a
// misbehaving callback cannot poison the session-wiring path. Safe to
// call with no callback installed. (v1.2.1)
func (t *Transport) fireOnTunnelUp(peer entmoot.NodeID) {
	raw := t.onTunnelUp.Load()
	if raw == nil {
		return
	}
	cb, ok := raw.(func(entmoot.NodeID))
	if !ok || cb == nil {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.logger.Warn("pilot transport: OnTunnelUp panic",
					slog.Any("panic", r),
					slog.Uint64("peer", uint64(peer)))
			}
		}()
		cb(peer)
	}()
}

// Close releases the listener and driver connection. Safe to call multiple
// times; the first call does the work, subsequent calls return nil.
//
// A pending Accept returns net.ErrClosed once t.closed fires. Pending Dials
// see net.ErrClosed on the fast path; an in-flight DialAddr that has not
// yet surfaced will have its eventual Conn closed by the background drain
// started in dialPilotStream.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
		if t.listener != nil {
			if err := t.listener.Close(); err != nil {
				t.closeErr = err
			}
		}
		// Wait for acceptPilotConns to return.
		t.acceptWG.Wait()
		if t.driver != nil {
			if err := t.driver.Close(); err != nil && t.closeErr == nil {
				t.closeErr = err
			}
		}
	})
	return t.closeErr
}

// extractNodeID reads "node_id" (float64 per Pilot's IPC JSON) from info and
// narrows it to uint32. Returns a typed error so Open can surface a readable
// message.
func extractNodeID(info map[string]interface{}) (entmoot.NodeID, error) {
	raw, ok := info["node_id"]
	if !ok {
		return 0, errors.New("missing node_id")
	}
	f, ok := raw.(float64)
	if !ok {
		return 0, fmt.Errorf("node_id is %T, want float64", raw)
	}
	if f < 0 || f > float64(^uint32(0)) {
		return 0, fmt.Errorf("node_id %v out of uint32 range", f)
	}
	return entmoot.NodeID(uint32(f)), nil
}

// remoteNodeID pulls the Pilot node id off an accepted conn.
// ipcclient.Conn's RemoteAddr().String() renders as "N:NNNN.HHHH.LLLL:PORT";
// we parse that with ParseSocketAddr (from addr.go in this package)
// rather than reflecting on the ipcclient struct.
func remoteNodeID(c net.Conn) (entmoot.NodeID, error) {
	addr := c.RemoteAddr()
	if addr == nil {
		return 0, errors.New("nil RemoteAddr")
	}
	sa, err := ParseSocketAddr(addr.String())
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", addr.String(), err)
	}
	return entmoot.NodeID(sa.Addr.Node), nil
}

// unblockableConn wraps an ipcclient.Conn so that Close() propagates to any
// blocked Read() call. The in-tree ipcclient already closes the recv channel
// on Close so a blocked Read returns io.EOF; the past-time read deadline is
// cheap defence-in-depth for edge races around channel close and deadline
// observation.
type unblockableConn struct {
	net.Conn
}

func wrapUnblockableConn(c net.Conn) net.Conn {
	return &unblockableConn{Conn: c}
}

func (u *unblockableConn) Close() error {
	// A past-time deadline unblocks any Read stuck on driver.Conn's recv
	// channel. Errors are ignored — the subsequent Close is the real work.
	_ = u.Conn.SetReadDeadline(time.Unix(1, 0))
	return u.Conn.Close()
}

type limitedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type traceCloseConn struct {
	net.Conn
	once    sync.Once
	onClose func()
}

func (c *traceCloseConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.onClose)
	return err
}

func (t *Transport) trace(event string, attrs ...any) {
	if !t.cfg.TraceGossipTransport {
		return
	}
	base := []any{slog.String("event", event)}
	t.logger.Info(pilotTraceTransportEventName, append(base, attrs...)...)
}
