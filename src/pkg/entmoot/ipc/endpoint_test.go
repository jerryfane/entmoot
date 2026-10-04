package ipc

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// The application receives a request only after the production listener has
// authenticated the connection. The counter detects unauthorized dispatch.
func serveControlFixture(t *testing.T, l *Listener) *atomic.Int32 {
	t.Helper()
	return serveControlFixtureWithDeadline(t, l, time.Second)
}

// firstFrame is the application read deadline set before the first request,
// which also caps authentication (cmd/entmootd uses 10s).
func serveControlFixtureWithDeadline(t *testing.T, l *Listener, firstFrame time.Duration) *atomic.Int32 {
	t.Helper()
	var dispatched atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		defer wg.Wait()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				_ = c.SetReadDeadline(time.Now().Add(firstFrame))
				var request [1]byte
				if _, err := io.ReadFull(c, request[:]); err != nil {
					return
				}
				if request[0] != 7 {
					return
				}
				dispatched.Add(1)
				_, _ = c.Write([]byte{9})
			}()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("control server did not stop")
		}
	})
	return &dispatched
}
func requestControl(t *testing.T, path string) {
	t.Helper()
	c, err := DialTimeout(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte{7}); err != nil {
		t.Fatal(err)
	}
	var response [1]byte
	if _, err := io.ReadFull(c, response[:]); err != nil {
		t.Fatal(err)
	}
	if response[0] != 9 {
		t.Fatalf("wrong control response %v", response)
	}
}
func writeControlFixture(t *testing.T, path string, e controlEndpoint) {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestControlTransportAndRestart(t *testing.T) {
	for _, mode := range []string{"unix", "tcp"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "control.sock")
			l, err := Listen(path, mode)
			if err != nil {
				t.Fatal(err)
			}
			dispatch := serveControlFixture(t, l)
			requestControl(t, path)
			if dispatch.Load() != 1 {
				t.Fatal("authorized request did not reach application")
			}
			if duplicate, err := Listen(path, mode); !errors.Is(err, ErrControlActive) {
				if duplicate != nil {
					duplicate.Close()
				}
				t.Fatalf("duplicate daemon not excluded: %v", err)
			}
			if err := l.StopAccepting(); err != nil {
				t.Fatal(err)
			}
			if duplicate, err := Listen(path, mode); !errors.Is(err, ErrControlActive) {
				if duplicate != nil {
					duplicate.Close()
				}
				t.Fatalf("shutdown prematurely released ownership: %v", err)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("endpoint removed before final cleanup: %v", err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("endpoint not removed: %v", err)
			}
			next, err := Listen(path, mode)
			if err != nil {
				t.Fatal(err)
			}
			nextDispatch := serveControlFixture(t, next)
			requestControl(t, path)
			if nextDispatch.Load() != 1 {
				t.Fatal("restart did not accept authenticated request")
			}
		})
	}
}

func TestTCPControlRejectsUnauthorizedClients(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.sock")
	l, err := Listen(path, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	dispatch := serveControlFixture(t, l)
	original, err := readEndpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	// A raw client cannot bypass TLS/token authentication by sending IPC bytes.
	raw, err := net.DialTimeout("tcp", original.Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.SetDeadline(time.Now().Add(time.Second))
	_, _ = raw.Write([]byte("unauthenticated plaintext request"))
	var b [1]byte
	_, err = raw.Read(b[:])
	raw.Close()
	if err == nil {
		t.Fatal("plaintext client received an application reply")
	}
	for _, scenario := range []string{"wrong-token", "wrong-certificate", "readable-by-others", "symlink", "non-loopback"} {
		t.Run(scenario, func(t *testing.T) {
			changed := original
			copyPath := filepath.Join(dir, scenario)
			switch scenario {
			case "wrong-token":
				changed.Token = base64.StdEncoding.EncodeToString(make([]byte, 32))
			case "wrong-certificate":
				_, cert, err := newControlCertificate()
				if err != nil {
					t.Fatal(err)
				}
				changed.Certificate = string(cert)
			case "non-loopback":
				changed.Address = "192.0.2.1:443"
			}
			if scenario == "symlink" {
				if err := os.Symlink(path, copyPath); err != nil {
					t.Fatal(err)
				}
			} else {
				writeControlFixture(t, copyPath, changed)
			}
			if scenario == "readable-by-others" {
				if err := os.Chmod(copyPath, 0644); err != nil {
					t.Fatal(err)
				}
			}
			c, err := DialTimeout(copyPath, time.Second)
			if err == nil {
				c.Close()
				t.Fatal("unauthorized control endpoint accepted")
			}
		})
	}
	if dispatch.Load() != 0 {
		t.Fatal("unauthorized request reached application")
	}
	requestControl(t, path)
	if dispatch.Load() != 1 {
		t.Fatal("denials blocked the authorized client too")
	}
}

func TestTCPControlPreservesFirstFrameDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	l, err := Listen(path, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	result := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			result <- err
			return
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		var request [1]byte
		_, err = c.Read(request[:])
		result <- err
	}()
	c, err := DialTimeout(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case err := <-result:
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("expected application read deadline, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("authentication erased application deadline")
	}
}

func TestTCPControlRotatesCredentialsAndRejectsStaleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.sock")
	first, err := Listen(path, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	old, err := readEndpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash's leftover rendezvous file: lease, not a failed health
	// probe, decides ownership; startup may replace the now-unreachable endpoint.
	writeControlFixture(t, path, old)
	next, err := Listen(path, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	dispatch := serveControlFixture(t, next)
	current, err := readEndpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	if current.Token == old.Token || current.Certificate == old.Certificate {
		t.Fatal("credentials survived a daemon restart")
	}
	old.Address = current.Address
	stalePath := filepath.Join(dir, "stale")
	writeControlFixture(t, stalePath, old)
	if c, err := DialTimeout(stalePath, time.Second); err == nil {
		c.Close()
		t.Fatal("stale descriptor authenticated replacement daemon")
	}
	requestControl(t, path)
	if dispatch.Load() != 1 {
		t.Fatal("replacement daemon inaccessible")
	}
}

func TestTCPControlHandshakeHonorsCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	l, err := Listen(path, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	// Accepting alone must not authenticate or block the accept loop. The client
	// context must abort TLS even if its server never starts reading.
	accepted := make(chan net.Conn, 1)
	go func() { c, _ := l.Accept(); accepted <- c }()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	c, err := DialContext(ctx, path)
	if err == nil {
		c.Close()
		t.Fatal("stalled server authenticated")
	}
	if c := <-accepted; c != nil {
		c.Close()
	}
}

// Local users who cannot read the endpoint file can still connect to the
// loopback port. Stalled clients, whether silent or holding a finished TLS
// handshake without the credential, must not lock out the authorized owner:
// beyond the pending-authentication capacity, the oldest ones make way.
func TestTCPControlStalledClientsCannotExhaustAuthentication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	l, err := Listen(path, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	// As in cmd/entmootd, stalled authentication lasts the full authenticationTimeout.
	dispatch := serveControlFixtureWithDeadline(t, l, 10*time.Second)
	endpoint, err := readEndpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	var stalled []net.Conn
	defer func() {
		for _, c := range stalled {
			c.Close()
		}
	}()
	dial := func(handshake bool) {
		t.Helper()
		var c net.Conn
		var err error
		if handshake {
			// The attacker lacks the private certificate, so it skips verification.
			dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: time.Second}, Config: &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true}}
			c, err = dialer.Dial("tcp4", endpoint.Address)
		} else {
			c, err = net.DialTimeout("tcp4", endpoint.Address, time.Second)
		}
		if err != nil {
			t.Fatalf("stalled connection %d: %v", len(stalled), err)
		}
		stalled = append(stalled, c)
	}
	for i := range maxPendingAuthentications {
		dial(i%2 == 1)
	}
	const extra = 8
	for range extra {
		dial(false)
	}
	// Each connection beyond capacity closes the oldest pending one, well before
	// its authenticationTimeout would have.
	for i, c := range stalled[:extra] {
		_ = c.SetReadDeadline(time.Now().Add(authenticationTimeout / 2))
		var b [1]byte
		if _, err := c.Read(b[:]); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("oldest pending connection %d was not evicted: %v", i, err)
		}
	}
	// Eviction of stalled[extra-1] proves the newest was admitted, not refused.
	newest := stalled[len(stalled)-1]
	_ = newest.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var b [1]byte
	if _, err := newest.Read(b[:]); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("newest pending connection was closed instead of the oldest: %v", err)
	}
	requestControl(t, path)
	if dispatch.Load() != 1 {
		t.Fatalf("dispatched %d requests, want only the authorized one", dispatch.Load())
	}
}

func TestControlRejectsNamedPipeWithoutWaitingForWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	if err := unix.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		conn, err := DialTimeout(path, time.Second)
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("named pipe accepted as a control endpoint")
		}
	case <-time.After(3 * time.Second):
		// Release a regressed blocking open before failing the test.
		writer, err := os.OpenFile(path, os.O_RDWR|unix.O_NONBLOCK, 0)
		if err == nil {
			defer writer.Close()
		}
		t.Fatal("control dial waited for a named-pipe writer instead of rejecting it")
	}
}
