package ipcclient

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

// Listener is a bound Pilot port. Each Accept pulls the next incoming
// virtual connection from a channel filled by the Driver's demuxer
// when it sees an AcceptedConn (0x05) frame whose bound-port prefix
// matches this Listener.
//
// The demuxer parses the AcceptedConn payload eagerly — it constructs
// the *Conn, registers it in the driver's conn-routing map, and then
// enqueues the ready-to-use *Conn on this Listener's acceptCh. This
// closes the race where a Recv frame for a freshly-accepted conn
// arrives before the application calls Accept: with eager registration
// the conn is routable the moment its AcceptedConn frame is consumed
// from the socket.
//
// Listener implements a net.Listener-flavoured surface, but the Accept
// variant that takes a context is the primary entry point for Entmoot's
// adapter so cancellation semantics are well-defined.
type Listener struct {
	port uint16
	drv  *Driver

	// acceptCh receives fully-wired *Conn values ready for the
	// application to use. The demuxer is the sender; the channel is
	// buffered so a burst of inbound connections doesn't stall demux.
	acceptCh chan *Conn

	closeOnce sync.Once
	closed    chan struct{}
}

func newListener(drv *Driver, port uint16) *Listener {
	return &Listener{
		port:     port,
		drv:      drv,
		acceptCh: make(chan *Conn, 64),
		closed:   make(chan struct{}),
	}
}

// pushAccept is invoked by the demuxer with an AcceptedConn frame
// payload (after the 2-byte bound-port prefix has been peeled off).
// The conn is parsed, constructed, and registered with the driver
// before it's handed to the acceptCh so any Recv frame in the same
// socket burst can be routed correctly. Returns false if the listener
// is closed or the payload is malformed.
func (l *Listener) pushAccept(payload []byte) bool {
	select {
	case <-l.closed:
		return false
	default:
	}
	c, err := l.buildConnFromPayload(payload)
	if err != nil {
		// Malformed frame — drop rather than poison the listener.
		return false
	}
	l.drv.registerConn(c.id, c)
	select {
	case l.acceptCh <- c:
		return true
	case <-l.closed:
		l.drv.unregisterConn(c.id)
		c.closeRecv()
		return false
	case <-l.drv.closedCh:
		l.drv.unregisterConn(c.id)
		c.closeRecv()
		return false
	}
}

// Accept blocks until the next inbound connection arrives, the
// listener is closed, or ctx is cancelled. The returned Conn is
// already registered with the driver's demuxer.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	select {
	case <-l.closed:
		return nil, ErrClosed
	case <-l.drv.closedCh:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case c, ok := <-l.acceptCh:
		if !ok {
			return nil, ErrClosed
		}
		return c, nil
	}
}

// Port returns the port the listener is bound to, which may differ
// from the port requested in Listen if the daemon chose the ephemeral
// range (requested port = 0).
func (l *Listener) Port() uint16 { return l.port }

// Addr returns a net.Addr describing the bound port on the zero
// virtual address (networkless form). Callers generally use Port()
// directly; Addr exists for net.Listener conformance when plumbed
// through adapter layers that expect it.
func (l *Listener) Addr() net.Addr {
	return netAddr{sa: SocketAddr{Port: l.port}, net: "pilot"}
}

// Close marks the listener closed and unregisters it from the driver's
// demuxer map. Any Accept blocked at the moment of Close returns
// ErrClosed.
func (l *Listener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		close(l.closed)
		l.drv.unregisterListener(l.port)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if unbindErr := l.drv.Unbind(ctx, l.port); unbindErr != nil {
			err = unbindErr
		}
	})
	return err
}

// buildConnFromPayload consumes an AcceptedConn frame payload (minus
// the 2-byte bound-port prefix) and returns a ready-to-use Conn. The
// caller (pushAccept) is responsible for registering the returned
// conn with the driver before handing it to user code.
//
// Wire layout (see doc.go "Spec clarifications"):
//
//	[4B conn_id][6B remote_addr][2B remote_port]
func (l *Listener) buildConnFromPayload(p []byte) (*Conn, error) {
	const need = 4 + addrSize + 2
	if len(p) < need {
		return nil, fmt.Errorf("ipcclient: accept payload %d bytes, want >= %d", len(p), need)
	}
	connID := binary.BigEndian.Uint32(p[0:4])
	remoteNet := binary.BigEndian.Uint16(p[4:6])
	remoteNode := binary.BigEndian.Uint32(p[6:10])
	remotePort := binary.BigEndian.Uint16(p[10:12])
	remote := SocketAddr{
		Addr: Addr{Network: remoteNet, Node: remoteNode},
		Port: remotePort,
	}
	local := SocketAddr{Port: l.port}

	return newConn(l.drv, connID, local, remote), nil
}
