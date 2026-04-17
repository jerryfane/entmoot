// Package pilot implements the gossip.Transport interface over a live Pilot
// Protocol daemon.
//
// The adapter connects to a locally-running Pilot daemon over its Unix IPC
// socket (see github.com/TeoSlayer/pilotprotocol/pkg/driver), binds a listener
// on a well-known port (typically :1004 for Entmoot), and wraps driver.DialAddr
// / driver.Listener / driver.TrustedPeers into the small Transport surface the
// gossip package depends on.
//
// # Why no unit tests
//
// The adapter is intentionally shipped without unit tests in this phase:
//
//   - driver.Connect requires a running Pilot daemon on the other end of the
//     socket path. `go test ./...` runs in environments that have no such
//     daemon.
//   - Replacing the driver with a fake for these tests would exercise the
//     fake, not the adapter. The integration gate lives at Phase F's canary,
//     which stands up real Pilot daemons and runs entmootd against them.
//
// The in-memory gossip.Transport mock (NewMemTransports) remains the
// contract-level test target for the Transport interface itself.
package pilot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/TeoSlayer/pilotprotocol/pkg/driver"
	"github.com/TeoSlayer/pilotprotocol/pkg/protocol"

	"entmoot/pkg/entmoot"
)

// Config parameterizes a Pilot-backed Transport.
type Config struct {
	// SocketPath is the Unix-domain IPC socket of the Pilot daemon. An empty
	// string selects driver.DefaultSocketPath ("/tmp/pilot.sock").
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
}

// Transport implements gossip.Transport over a live Pilot daemon.
//
// Construction via Open is what actually talks to the daemon: it issues
// driver.Connect, driver.Info (to discover the local node id), and
// driver.Listen. Dials and accepts thereafter are single-shot against the
// already-open listener and driver.
type Transport struct {
	cfg    Config
	logger *slog.Logger

	driver   *driver.Driver
	listener *driver.Listener
	nodeID   entmoot.NodeID

	// closeOnce serializes driver/listener teardown. Close is idempotent.
	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

// Open connects to the Pilot daemon at cfg.SocketPath, discovers the local
// node id via driver.Info, and binds a listener on cfg.ListenPort. The
// returned Transport is ready to Dial / Accept immediately.
//
// On any failure mid-way (e.g. bind fails after connect), previously-acquired
// resources are released before returning.
func Open(cfg Config) (*Transport, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	d, err := driver.Connect(cfg.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("pilot: connect %q: %w", cfg.SocketPath, err)
	}

	info, err := d.Info()
	if err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("pilot: info: %w", err)
	}
	nid, err := extractNodeID(info)
	if err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("pilot: info: %w", err)
	}

	ln, err := d.Listen(cfg.ListenPort)
	if err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("pilot: listen :%d: %w", cfg.ListenPort, err)
	}

	return &Transport{
		cfg:      cfg,
		logger:   logger,
		driver:   d,
		listener: ln,
		nodeID:   nid,
		closed:   make(chan struct{}),
	}, nil
}

// NodeID returns the Pilot node id reported by the daemon at Open time.
func (t *Transport) NodeID() entmoot.NodeID {
	return t.nodeID
}

// Driver returns the underlying *driver.Driver. Exposed so subcommands like
// `info` can query daemon state (hostname, address) without re-dialing the
// socket. Callers MUST NOT Close the returned driver; call Transport.Close
// instead.
func (t *Transport) Driver() *driver.Driver {
	return t.driver
}

// Dial implements gossip.Transport. It builds a protocol.Addr from
// cfg.Network + peer and issues driver.DialAddr on cfg.ListenPort.
//
// Honoring ctx: DialAddr blocks until the Pilot daemon responds with DialOK.
// If ctx fires before that happens, a watcher goroutine closes the returned
// Conn (if any) so the caller's Read/Write see net.ErrClosed and abort. The
// dial goroutine itself runs to completion — Pilot's IPC is request/response
// and cannot be externally cancelled — and any Conn returned after ctx fires
// is immediately closed so resources do not leak.
func (t *Transport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	select {
	case <-t.closed:
		return nil, net.ErrClosed
	default:
	}

	addr := protocol.Addr{Network: t.cfg.Network, Node: uint32(peer)}

	type dialResult struct {
		conn *driver.Conn
		err  error
	}
	resultCh := make(chan dialResult, 1)
	go func() {
		c, err := t.driver.DialAddr(addr, t.cfg.ListenPort)
		resultCh <- dialResult{conn: c, err: err}
	}()

	select {
	case r := <-resultCh:
		if r.err != nil {
			return nil, fmt.Errorf("pilot: dial %s:%d: %w", addr, t.cfg.ListenPort, r.err)
		}
		// Handle race: if the transport was closed while DialAddr was in
		// flight, return the just-opened conn anyway (best effort) — the
		// caller's Read/Write will fail on next use. We do close it on ctx
		// cancellation though (next case).
		return r.conn, nil
	case <-ctx.Done():
		// Wait for DialAddr to finish in the background; close whatever it
		// produces so resources do not leak. We do not block the caller on
		// that cleanup.
		go func() {
			r := <-resultCh
			if r.conn != nil {
				_ = r.conn.Close()
			}
		}()
		return nil, fmt.Errorf("pilot: dial %s:%d: %w", addr, t.cfg.ListenPort, ctx.Err())
	case <-t.closed:
		go func() {
			r := <-resultCh
			if r.conn != nil {
				_ = r.conn.Close()
			}
		}()
		return nil, net.ErrClosed
	}
}

// Accept implements gossip.Transport. It wraps listener.Accept with ctx
// cancellation: on ctx.Done, a watcher closes the listener, which unblocks
// the in-flight Accept with a closed-listener error that we normalize to
// net.ErrClosed.
//
// The remote NodeID is extracted from conn.RemoteAddr(), which is the
// *driver-wrapped* pilotAddr (unexported) whose String() renders the
// protocol.SocketAddr. We parse that string with protocol.ParseSocketAddr
// rather than type-asserting the unexported wrapper type.
func (t *Transport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	select {
	case <-t.closed:
		return nil, 0, net.ErrClosed
	default:
	}

	// Watcher: on ctx cancellation, close the listener to unblock Accept.
	// A sentinel channel lets us stop the watcher once Accept returns so we
	// do not leak a goroutine per successful accept.
	stop := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = t.listener.Close()
		case <-t.closed:
			// Close() itself closed the listener.
		case <-stop:
		}
	}()

	conn, err := t.listener.Accept()
	close(stop)
	<-watcherDone

	if err != nil {
		// Distinguish ctx cancellation from transport-close. In either case
		// the listener is closed by the watcher; return the most informative
		// error the caller can act on.
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-t.closed:
			return nil, 0, net.ErrClosed
		default:
			return nil, 0, fmt.Errorf("pilot: accept: %w", err)
		}
	}

	remote, err := remoteNodeID(conn)
	if err != nil {
		// Malformed remote addr from driver is unrecoverable for a single
		// connection; close and log.
		_ = conn.Close()
		return nil, 0, fmt.Errorf("pilot: decode remote addr: %w", err)
	}
	return conn, remote, nil
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
	// driver.TrustedPeers has no context parameter; ctx is respected only as
	// a pre-call fast path. Pilot IPC is synchronous and short.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	resp, err := t.driver.TrustedPeers()
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

// Close releases the listener and closes the driver connection. Safe to call
// multiple times; the first call does the work, subsequent calls return nil.
//
// A pending Accept returns net.ErrClosed once the listener closes. A pending
// Dial returns net.ErrClosed on the fast path; an in-flight DialAddr that has
// not yet surfaced will have its eventual Conn closed by the background
// drain started in Dial.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
		if t.listener != nil {
			if err := t.listener.Close(); err != nil {
				t.closeErr = err
			}
		}
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

// remoteNodeID pulls the Pilot node id off an accepted conn. driver.Conn
// renders RemoteAddr() as a pilotAddr whose String() is
// "N:NNNN.HHHH.LLLL:PORT"; we parse that with protocol.ParseSocketAddr
// rather than reflecting on an unexported type.
func remoteNodeID(c net.Conn) (entmoot.NodeID, error) {
	addr := c.RemoteAddr()
	if addr == nil {
		return 0, errors.New("nil RemoteAddr")
	}
	sa, err := protocol.ParseSocketAddr(addr.String())
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", addr.String(), err)
	}
	return entmoot.NodeID(sa.Addr.Node), nil
}
