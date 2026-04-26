package ipcclient

import (
	"io"
	"testing"
)

func TestRegisterConnAppliesPendingClose(t *testing.T) {
	d := newPendingCloseTestDriver()
	d.pendingClose[42] = struct{}{}

	c := newConn(d, 42, SocketAddr{}, SocketAddr{})
	d.registerConn(42, c)

	buf := make([]byte, 1)
	if _, err := c.Read(buf); err != io.EOF {
		t.Fatalf("Read err = %v, want EOF", err)
	}
	if _, ok := d.conns[42]; ok {
		t.Fatalf("closed conn registered in conns")
	}
}

func TestRegisterConnDrainsPendingRecvBeforePendingClose(t *testing.T) {
	d := newPendingCloseTestDriver()
	d.pendingRecv[42] = [][]byte{[]byte("hello")}
	d.pendingClose[42] = struct{}{}

	c := newConn(d, 42, SocketAddr{}, SocketAddr{})
	d.registerConn(42, c)

	buf := make([]byte, 16)
	n, err := c.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("first Read = %q, %v; want hello, nil", buf[:n], err)
	}
	if _, err := c.Read(buf); err != io.EOF {
		t.Fatalf("second Read err = %v, want EOF", err)
	}
	if _, ok := d.conns[42]; ok {
		t.Fatalf("closed conn registered in conns")
	}
}

func newPendingCloseTestDriver() *Driver {
	return &Driver{
		conns:        make(map[uint32]*pilotConn),
		pendingRecv:  make(map[uint32][][]byte),
		pendingClose: make(map[uint32]struct{}),
		closedCh:     make(chan struct{}),
	}
}
