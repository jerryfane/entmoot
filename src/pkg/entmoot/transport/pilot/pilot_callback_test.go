package pilot

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

// TestPilotTransport_SetOnTunnelUpStorageAndDispatch exercises the
// SetOnTunnelUp / fireOnTunnelUp plumbing on a Transport struct
// constructed by hand (without Open, which would require a running
// Pilot daemon). The integration-level "real tunnel brings the
// callback up" assertion lives behind the Phase F canary; here we
// only assert:
//
//   - A callback installed via SetOnTunnelUp is invoked when
//     fireOnTunnelUp is called (async via goroutine).
//   - The callback receives the right NodeID.
//   - A subsequent SetOnTunnelUp replaces the previous callback.
//   - Passing nil clears the callback (no dispatch afterward).
//   - A panicking callback does not crash the transport; the panic
//     is swallowed by the recover wrapper.
//
// These cover every contract of the Transport.SetOnTunnelUp surface
// that does not require a live Pilot driver.
func TestPilotTransport_SetOnTunnelUpStorageAndDispatch(t *testing.T) {
	t.Parallel()

	tr := &Transport{logger: slog.Default()}

	const peer entmoot.NodeID = 777

	// Initial state: no callback installed. fireOnTunnelUp must be a
	// no-op — not panic, not block.
	tr.fireOnTunnelUp(peer)

	var (
		firstCount atomic.Int32
		firstPeer  atomic.Uint32
	)
	tr.SetOnTunnelUp(func(p entmoot.NodeID) {
		firstCount.Add(1)
		firstPeer.Store(uint32(p))
	})
	tr.fireOnTunnelUp(peer)

	waitFor(t, 1*time.Second, func() bool { return firstCount.Load() == 1 },
		"first callback fired once")
	if got := entmoot.NodeID(firstPeer.Load()); got != peer {
		t.Fatalf("first callback peer = %d, want %d", got, peer)
	}

	// Install a replacement. Only the new one must fire.
	var secondCount atomic.Int32
	tr.SetOnTunnelUp(func(entmoot.NodeID) { secondCount.Add(1) })
	tr.fireOnTunnelUp(peer)

	waitFor(t, 1*time.Second, func() bool { return secondCount.Load() == 1 },
		"second callback fired once")
	// First callback must not have been invoked again.
	if got := firstCount.Load(); got != 1 {
		t.Fatalf("first callback fired after replacement: count = %d, want 1", got)
	}

	// Clear via nil. Subsequent fires must be silent.
	tr.SetOnTunnelUp(nil)
	tr.fireOnTunnelUp(peer)
	// Give any misdispatched goroutine a chance.
	time.Sleep(100 * time.Millisecond)
	if got := secondCount.Load(); got != 1 {
		t.Fatalf("second callback fired after nil-clear: count = %d, want 1", got)
	}

	// Panicking callback must not crash.
	panicked := make(chan struct{}, 1)
	tr.SetOnTunnelUp(func(entmoot.NodeID) {
		defer func() {
			// Allow the test to observe that the callback ran even if
			// it panics; the recover in fireOnTunnelUp will swallow
			// the panic after this defer.
			panicked <- struct{}{}
		}()
		panic("test panic from OnTunnelUp")
	})
	tr.fireOnTunnelUp(peer)
	select {
	case <-panicked:
	case <-time.After(1 * time.Second):
		t.Fatal("panicking callback never ran")
	}
	// If the process is still alive, the recover did its job. A short
	// sleep lets the goroutine finish its recover path before the test
	// scheduler tears down.
	time.Sleep(50 * time.Millisecond)
}

func TestPilotTransport_SetOnTunnelUpDoesNotFireUntilExplicitDispatch(t *testing.T) {
	t.Parallel()

	tr := &Transport{logger: slog.Default()}
	var count atomic.Int32
	tr.SetOnTunnelUp(func(entmoot.NodeID) { count.Add(1) })

	// Installing the callback must not invoke it by itself.
	time.Sleep(100 * time.Millisecond)
	if got := count.Load(); got != 0 {
		t.Fatalf("callback fired without fireOnTunnelUp: count = %d, want 0", got)
	}

	// Sanity: the callback IS wired — one explicit fire increments
	// count to 1.
	tr.fireOnTunnelUp(42)
	waitFor(t, 1*time.Second, func() bool { return count.Load() == 1 },
		"sanity fire after race-lost simulation")
}

func TestPilotTransport_DialFiresOnTunnelUp(t *testing.T) {
	t.Parallel()

	const peer entmoot.NodeID = 45981
	srv := newPilotCallbackServer(t)
	defer srv.Close()
	driver, err := ipcclient.Connect(srv.path)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer driver.Close()

	serverDone := make(chan error, 1)
	go func() {
		frame, err := srv.readFrame()
		if err != nil {
			serverDone <- err
			return
		}
		if len(frame) == 0 || frame[0] != 0x03 {
			serverDone <- io.ErrUnexpectedEOF
			return
		}
		var resp [5]byte
		resp[0] = 0x04
		binary.BigEndian.PutUint32(resp[1:5], 1)
		srv.writeFrame(resp[:])
		serverDone <- nil
	}()

	tp := &Transport{
		cfg:      Config{ListenPort: 1004},
		logger:   slog.Default(),
		driver:   driver,
		closed:   make(chan struct{}),
		limits:   newPeerDialLimiter(pilotMaxConcurrentPeerDials),
		dialAddr: driver.DialAddr,
	}
	var count atomic.Int32
	var gotPeer atomic.Uint32
	tp.SetOnTunnelUp(func(p entmoot.NodeID) {
		count.Add(1)
		gotPeer.Store(uint32(p))
	})

	conn, err := tp.Dial(context.Background(), peer)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	waitFor(t, time.Second, func() bool { return count.Load() == 1 }, "dial tunnel-up")
	if got := entmoot.NodeID(gotPeer.Load()); got != peer {
		t.Fatalf("callback peer = %d, want %d", got, peer)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish")
	}
}

func TestPilotTransport_InboundAcceptFiresOnTunnelUp(t *testing.T) {
	t.Parallel()

	const peer entmoot.NodeID = 45491
	srv := newPilotCallbackServer(t)
	defer srv.Close()

	releaseAccept := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		if frame, err := srv.readFrame(); err != nil {
			serverDone <- err
			return
		} else if len(frame) == 0 || frame[0] != 0x0D {
			serverDone <- io.ErrUnexpectedEOF
			return
		}
		srv.writeFrame(append([]byte{0x0E}, []byte(`{"node_id":777}`)...))

		if frame, err := srv.readFrame(); err != nil {
			serverDone <- err
			return
		} else if len(frame) == 0 || frame[0] != 0x01 {
			serverDone <- io.ErrUnexpectedEOF
			return
		}
		var bindOK [3]byte
		bindOK[0] = 0x02
		binary.BigEndian.PutUint16(bindOK[1:3], 1004)
		srv.writeFrame(bindOK[:])

		<-releaseAccept
		var accept [15]byte
		accept[0] = 0x05
		binary.BigEndian.PutUint16(accept[1:3], 1004)
		binary.BigEndian.PutUint32(accept[3:7], 99)
		binary.BigEndian.PutUint16(accept[7:9], 0)
		binary.BigEndian.PutUint32(accept[9:13], uint32(peer))
		binary.BigEndian.PutUint16(accept[13:15], 1004)
		srv.writeFrame(accept[:])
		serverDone <- nil
	}()

	tp, err := Open(Config{SocketPath: srv.path, ListenPort: 1004, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tp.Close()
	var count atomic.Int32
	var gotPeer atomic.Uint32
	tp.SetOnTunnelUp(func(p entmoot.NodeID) {
		count.Add(1)
		gotPeer.Store(uint32(p))
	})
	close(releaseAccept)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, remote, err := tp.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer conn.Close()
	if remote != peer {
		t.Fatalf("remote = %d, want %d", remote, peer)
	}

	waitFor(t, time.Second, func() bool { return count.Load() == 1 }, "inbound tunnel-up")
	if got := entmoot.NodeID(gotPeer.Load()); got != peer {
		t.Fatalf("callback peer = %d, want %d", got, peer)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish")
	}
}

type pilotCallbackServer struct {
	t        *testing.T
	path     string
	ln       net.Listener
	connMu   sync.Mutex
	conn     net.Conn
	connWait chan struct{}
}

func newPilotCallbackServer(t *testing.T) *pilotCallbackServer {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "entmoot-pilot-cb-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "p.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	s := &pilotCallbackServer{t: t, path: path, ln: ln, connWait: make(chan struct{})}
	go func() {
		c, err := ln.Accept()
		if err == nil {
			s.connMu.Lock()
			s.conn = c
			s.connMu.Unlock()
		}
		close(s.connWait)
	}()
	return s
}

func (s *pilotCallbackServer) Close() {
	_ = s.ln.Close()
	if c := s.getConn(); c != nil {
		_ = c.Close()
	}
}

func (s *pilotCallbackServer) getConn() net.Conn {
	select {
	case <-s.connWait:
	case <-time.After(time.Second):
		s.t.Fatal("server did not accept connection")
		return nil
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn
}

func (s *pilotCallbackServer) readFrame() ([]byte, error) {
	c := s.getConn()
	if c == nil {
		return nil, io.EOF
	}
	var hdr [4]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	buf := make([]byte, n)
	_, err := io.ReadFull(c, buf)
	return buf, err
}

func (s *pilotCallbackServer) writeFrame(payload []byte) {
	c := s.getConn()
	if c == nil {
		return
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	_, _ = c.Write(hdr[:])
	_, _ = c.Write(payload)
}

// waitFor polls cond every 10 ms until it returns true or the budget
// elapses. Fails the test with `what` as the failure message.
func waitFor(t *testing.T, budget time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
