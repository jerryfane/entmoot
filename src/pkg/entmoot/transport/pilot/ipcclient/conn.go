package ipcclient

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// Conn is one application-level virtual connection multiplexed on the
// IPC socket, identified by a 32-bit conn_id assigned by the daemon.
// Conn implements net.Conn so it drops into callers (yamux, the
// gossip transport adapter) that expect that interface.
//
// Reads pull bytes from an internal channel that the Driver's demuxer
// goroutine fills with the bodies of Recv (0x07) frames for this
// conn_id. Writes serialize a Send (0x06) frame onto the IPC socket
// through the Driver's write mutex. Close issues a Close (0x08) frame
// and unregisters the conn from the demuxer's routing table; any
// subsequent Recv frames the daemon might emit before its own teardown
// are silently dropped.
type pilotConn struct {
	id       uint32
	local    SocketAddr
	remote   SocketAddr
	drv      *Driver
	network  string // "pilot", the net.Addr network name

	// recvCh receives payloads from Recv frames for this conn_id. The
	// demuxer owns the send side; Close closes it (exactly once) to
	// unblock any goroutine sitting in Read.
	recvCh chan []byte
	// recvClose guards the channel-close so Close is idempotent even
	// if the daemon independently sends a peer-close indication.
	recvClose sync.Once
	// recvBuf holds leftover bytes from the previous Recv frame when
	// the caller's Read buffer was too small to consume it all.
	recvBuf []byte

	// readMu serializes concurrent Read calls; like net.TCPConn,
	// Conn.Read is not safe for concurrent use but we want a
	// single caller pattern to behave deterministically.
	readMu sync.Mutex

	// stateMu guards closed + deadline fields.
	stateMu      sync.Mutex
	closed       bool
	readDeadline time.Time
	// deadlineCh is closed when readDeadline is updated, so a Read
	// already blocked on the previous deadline can re-evaluate.
	deadlineCh chan struct{}
}

// newConn builds a Conn in the not-yet-closed state with a fresh recv
// channel. Callers (Driver.DialAddr, Listener.Accept) are expected to
// register the channel with the demuxer before returning the Conn to
// the user so no Recv frame is missed.
func newConn(drv *Driver, id uint32, local, remote SocketAddr) *pilotConn {
	return &pilotConn{
		id:         id,
		local:      local,
		remote:     remote,
		drv:        drv,
		network:    "pilot",
		recvCh:     make(chan []byte, 256),
		deadlineCh: make(chan struct{}),
	}
}

// pushRecv is called by the demuxer to deliver a Recv frame's payload
// to this conn. Blocks only if the recvCh is full, back-pressuring the
// demuxer — which is the correct behavior: if the app is too slow to
// read, we stop reading from the socket rather than buffer unboundedly.
//
// Returns false if the conn is already closed, so the demuxer can drop
// the data without wedging.
func (c *pilotConn) pushRecv(data []byte) bool {
	c.stateMu.Lock()
	closed := c.closed
	c.stateMu.Unlock()
	if closed {
		return false
	}
	// Copy: we do not own the buffer the demuxer passed in.
	buf := make([]byte, len(data))
	copy(buf, data)
	select {
	case c.recvCh <- buf:
		return true
	case <-c.drv.closedCh:
		return false
	}
}

// closeRecv is called by the demuxer when the daemon confirms this
// conn is gone (CloseOK for this conn_id) or when the driver shuts
// down. Closing recvCh lets any blocked Read observe io.EOF.
func (c *pilotConn) closeRecv() {
	c.recvClose.Do(func() {
		close(c.recvCh)
	})
}

// Read satisfies net.Conn.Read. Returns io.EOF after Close or a
// daemon-side close, and os.ErrDeadlineExceeded if a read deadline has
// been set and fires before data arrives.
func (c *pilotConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()

	// Serve leftover from a previously-partially-consumed frame.
	if len(c.recvBuf) > 0 {
		n := copy(p, c.recvBuf)
		c.recvBuf = c.recvBuf[n:]
		return n, nil
	}

	c.stateMu.Lock()
	dl := c.readDeadline
	dch := c.deadlineCh
	c.stateMu.Unlock()

	if !dl.IsZero() && !time.Now().Before(dl) {
		return 0, os.ErrDeadlineExceeded
	}

	var timer <-chan time.Time
	if !dl.IsZero() {
		t := time.NewTimer(time.Until(dl))
		defer t.Stop()
		timer = t.C
	}

	select {
	case data, ok := <-c.recvCh:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, data)
		if n < len(data) {
			c.recvBuf = data[n:]
		}
		return n, nil
	case <-timer:
		return 0, os.ErrDeadlineExceeded
	case <-dch:
		return 0, os.ErrDeadlineExceeded
	case <-c.drv.closedCh:
		return 0, ErrClosed
	}
}

// Write satisfies net.Conn.Write. Serializes a Send frame onto the IPC
// socket. If the caller's buffer exceeds the IPC frame cap (1 MiB
// minus the 5-byte header), the write is split across multiple Send
// frames — the daemon reassembles them by conn_id in arrival order.
func (c *pilotConn) Write(p []byte) (int, error) {
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		return 0, ErrClosed
	}
	c.stateMu.Unlock()

	// Per-Send frame cap: maxFrameSize minus the 5-byte
	// [opcode][conn_id] header. Leave a small safety margin so a caller
	// that happens to hand us an exactly-at-the-cap buffer still
	// succeeds rather than hitting an off-by-one rejection.
	const perFrame = maxFrameSize - 1024
	total := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > perFrame {
			chunk = chunk[:perFrame]
		}
		frame := make([]byte, 1+4+len(chunk))
		frame[0] = byte(opSend)
		binary.BigEndian.PutUint32(frame[1:5], c.id)
		copy(frame[5:], chunk)
		if err := c.drv.writeFrame(frame); err != nil {
			if total > 0 {
				return total, err
			}
			return 0, err
		}
		total += len(chunk)
		p = p[len(chunk):]
	}
	return total, nil
}

// Close satisfies net.Conn.Close. Sends a Close frame to the daemon
// (best-effort; a concurrent socket teardown turns this into a no-op),
// unregisters this conn from the demuxer, and closes the recv channel
// so any blocked Read returns io.EOF. Idempotent.
func (c *pilotConn) Close() error {
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		return nil
	}
	c.closed = true
	c.stateMu.Unlock()

	// Unregister first so any Recv frame still in flight after the
	// daemon receives our Close is dropped rather than queued on a
	// now-unowned channel.
	c.drv.unregisterConn(c.id)

	frame := make([]byte, 5)
	frame[0] = byte(opClose)
	binary.BigEndian.PutUint32(frame[1:5], c.id)
	writeErr := c.drv.writeFrame(frame)

	c.closeRecv()

	if writeErr != nil && !errors.Is(writeErr, ErrClosed) {
		// Emit the error only if it is not "driver already closed" —
		// that is a graceful race, not a failure.
		return writeErr
	}
	return nil
}

// LocalAddr satisfies net.Conn.LocalAddr.
func (c *pilotConn) LocalAddr() net.Addr { return netAddr{sa: c.local, net: c.network} }

// RemoteAddr satisfies net.Conn.RemoteAddr.
func (c *pilotConn) RemoteAddr() net.Addr { return netAddr{sa: c.remote, net: c.network} }

// SetDeadline sets only the read deadline; writes are issued through
// the shared IPC socket and cannot honor a per-conn deadline without
// affecting every other multiplexed conn. This matches Pilot's
// upstream observable contract.
func (c *pilotConn) SetDeadline(t time.Time) error {
	return c.SetReadDeadline(t)
}

// SetReadDeadline sets a future deadline for Read. A past-time
// deadline unblocks any in-progress Read with os.ErrDeadlineExceeded.
func (c *pilotConn) SetReadDeadline(t time.Time) error {
	c.stateMu.Lock()
	c.readDeadline = t
	prev := c.deadlineCh
	c.deadlineCh = make(chan struct{})
	c.stateMu.Unlock()
	close(prev) // wake any Read blocked on the old deadline ch
	return nil
}

// SetWriteDeadline is a no-op. See SetDeadline comment.
func (c *pilotConn) SetWriteDeadline(time.Time) error { return nil }

// netAddr is a net.Addr implementation backed by a Pilot SocketAddr.
type netAddr struct {
	sa  SocketAddr
	net string
}

func (a netAddr) Network() string { return a.net }
func (a netAddr) String() string  { return a.sa.String() }

// Conn is the exported alias for pilotConn. Kept as a distinct name
// only so the package has a `Conn` symbol in its public surface while
// the struct itself stays internal to this file.
type Conn = pilotConn
