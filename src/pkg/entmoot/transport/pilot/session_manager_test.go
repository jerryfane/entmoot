package pilot

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

func TestSessionManagerDropDrainsActiveStreamBeforeClose(t *testing.T) {
	t.Parallel()

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	client, err := yamux.Client(left, yamuxConfig(slog.Default()))
	if err != nil {
		t.Fatalf("yamux.Client: %v", err)
	}
	server, err := yamux.Server(right, yamuxConfig(slog.Default()))
	if err != nil {
		t.Fatalf("yamux.Server: %v", err)
	}
	defer server.Close()

	mgr := newSessionManager(slog.Default(), false)
	mgr.drainAfter = time.Second
	entry, installed := mgr.installOutbound(45981, client)
	if !installed {
		t.Fatal("installOutbound did not install fresh session")
	}

	stream, err := entry.openStream()
	if err != nil {
		t.Fatalf("openStream: %v", err)
	}
	acceptedCh := make(chan net.Conn, 1)
	go func() {
		s, _ := server.AcceptStream()
		acceptedCh <- s
	}()
	var accepted net.Conn
	select {
	case accepted = <-acceptedCh:
		if accepted == nil {
			t.Fatal("server accepted nil stream")
		}
	case <-time.After(time.Second):
		t.Fatal("server did not accept stream")
	}
	defer accepted.Close()

	if !mgr.drainOutbound(45981, entry, "test") {
		t.Fatal("drainOutbound returned false")
	}
	if _, err := stream.Write([]byte("ok")); err != nil {
		t.Fatalf("active stream write after drain: %v", err)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(accepted, buf); err != nil {
		t.Fatalf("active stream read after drain: %v", err)
	}
	if string(buf) != "ok" {
		t.Fatalf("read %q, want ok", string(buf))
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("stream close: %v", err)
	}
	select {
	case <-entry.done:
	case <-time.After(time.Second):
		t.Fatal("drained session did not close after active stream closed")
	}
}
