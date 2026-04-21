package pilot

import (
	"io"
	"net"
	"testing"

	"github.com/hashicorp/yamux"
)

// TestYamuxSessionReusedForMultipleStreams is the narrow yamux-wiring test
// described in the v1.1.0 plan: end-to-end sanity that a single yamux
// session multiplexes >=2 streams over a shared underlying transport
// (net.Pipe here, Pilot's driver.Conn in production). No fake driver — we
// never touch driver.DialAddr / driver.Listener here. The test asserts the
// library wiring we rely on actually works; the adapter-level behavior
// lives behind the Phase F canary.
func TestYamuxSessionReusedForMultipleStreams(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	server, err := yamux.Server(serverConn, nil)
	if err != nil {
		t.Fatalf("yamux.Server: %v", err)
	}
	client, err := yamux.Client(clientConn, nil)
	if err != nil {
		t.Fatalf("yamux.Client: %v", err)
	}

	// Accept in a goroutine so OpenStream + AcceptStream don't deadlock.
	accepted := make(chan net.Conn, 2)
	go func() {
		for i := 0; i < 2; i++ {
			s, err := server.AcceptStream()
			if err != nil {
				return
			}
			accepted <- s
		}
	}()

	s1, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream 1: %v", err)
	}
	s2, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream 2: %v", err)
	}
	if s1 == s2 {
		t.Fatal("OpenStream returned same stream twice")
	}
	// Drain accepts so the server-side pump goroutine exits cleanly.
	<-accepted
	<-accepted
}

// TestYamuxConfigLogOutputDiscardIsValid verifies that yamuxConfig's choice
// of LogOutput=io.Discard / Logger=nil passes yamux's "exactly one of
// Logger/LogOutput must be set" validation — otherwise yamux.Server /
// yamux.Client would fail at runtime with a confusing config error.
func TestYamuxConfigLogOutputDiscardIsValid(t *testing.T) {
	cfg := yamuxConfig(nil)
	if cfg.Logger != nil {
		t.Fatalf("Logger should be nil, got %T", cfg.Logger)
	}
	if cfg.LogOutput == nil {
		t.Fatal("LogOutput must be non-nil (yamux validates this)")
	}
	if cfg.LogOutput != io.Discard {
		t.Fatalf("LogOutput should be io.Discard to suppress noise, got %T", cfg.LogOutput)
	}

	// Actually instantiate a session with this config against net.Pipe —
	// proves the config survives yamux's internal validation.
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	done := make(chan error, 1)
	go func() {
		s, err := yamux.Server(a, cfg)
		if err != nil {
			done <- err
			return
		}
		_ = s.Close()
		done <- nil
	}()

	c, err := yamux.Client(b, yamuxConfig(nil))
	if err != nil {
		t.Fatalf("yamux.Client with yamuxConfig: %v", err)
	}
	_ = c.Close()
	if err := <-done; err != nil {
		t.Fatalf("yamux.Server with yamuxConfig: %v", err)
	}
}
