package ipcclient

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newTestSocketPath returns a Unix socket path short enough for macOS.
// t.TempDir() lives under /var/folders/... on Darwin and can exceed the
// sockaddr_un limit once the randomized test directory suffix is added.
func newTestSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "entmoot-ipc-")
	if err != nil {
		t.Fatalf("mkdir temp socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "p.sock")
}

// mockServer is a tiny helper that listens on a Unix socket, accepts
// one connection (the client the Driver dials), and exposes readFrame
// / writeFrame so tests can act as a fake Pilot daemon.
//
// The helper is intentionally small — no state machine, no opcode
// handling. Each test wires its own script via goroutines that call
// srv.readFrame / srv.writeFrame in the right order. This keeps the
// helper's behaviour obvious and prevents "magic" mock responses from
// hiding a client-side bug.
type mockServer struct {
	t  *testing.T
	ln net.Listener

	// connMu guards conn. conn is written once by the accept
	// goroutine and read by test goroutines plus Close; all accesses
	// take the mutex to keep the race detector happy and to let
	// Close observe a possibly-still-nil conn without racing on the
	// write.
	connMu   sync.Mutex
	conn     net.Conn
	connWait chan struct{}
	writeMu  sync.Mutex

	closeOnce sync.Once
	closed    chan struct{}
}

func newMockServer(t *testing.T, socketPath string) *mockServer {
	t.Helper()
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("mock listen: %v", err)
	}
	s := &mockServer{
		t:        t,
		ln:       ln,
		connWait: make(chan struct{}),
		closed:   make(chan struct{}),
	}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(s.connWait)
			return
		}
		s.connMu.Lock()
		s.conn = c
		s.connMu.Unlock()
		close(s.connWait)
	}()
	return s
}

// waitConn blocks until the Driver's Connect has produced the accept
// on our side. Most tests can ignore this — readFrame calls waitConn
// internally.
func (s *mockServer) waitConn() {
	select {
	case <-s.connWait:
	case <-time.After(2 * time.Second):
		s.t.Fatal("mock server never accepted client")
	}
}

// getConn returns the accepted connection (or nil if accept failed).
func (s *mockServer) getConn() net.Conn {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn
}

// readFrame returns the next framed payload the client wrote on the
// socket, or an error if the socket has been closed or the payload is
// malformed.
func (s *mockServer) readFrame() ([]byte, error) {
	s.waitConn()
	c := s.getConn()
	if c == nil {
		return nil, io.EOF
	}
	return readFrame(c)
}

// writeFrame emits one framed payload to the client side.
func (s *mockServer) writeFrame(payload []byte) {
	s.waitConn()
	c := s.getConn()
	if c == nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := writeFrame(c, payload); err != nil && !errors.Is(err, net.ErrClosed) {
		s.t.Logf("mock writeFrame: %v", err)
	}
}

// Close tears down the mock server.
func (s *mockServer) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		_ = s.ln.Close()
		c := s.getConn()
		if c != nil {
			_ = c.Close()
		}
	})
}
