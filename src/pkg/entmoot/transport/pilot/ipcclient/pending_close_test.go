package ipcclient

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
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
	if _, err := c.Write([]byte("stale")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write err = %v, want ErrClosed", err)
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
	if _, err := c.Write([]byte("stale")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write err = %v, want ErrClosed", err)
	}
	if _, ok := d.conns[42]; ok {
		t.Fatalf("closed conn registered in conns")
	}
}

func TestLocalConnCloseThenCloseOKDoesNotLeavePendingClose(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0xCAFE01
	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, c)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertServerSawClose(t, srv, connID)

	srv.writeFrame(closeOKFrame(connID))
	waitCloseBookkeeping(t, drv, connID, false, false)
}

func TestConnIDCanRegisterAfterLocalCloseOK(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0xCAFE02
	closedConn := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, closedConn)

	if err := closedConn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertServerSawClose(t, srv, connID)
	srv.writeFrame(closeOKFrame(connID))
	waitCloseBookkeeping(t, drv, connID, false, false)

	nextConn := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, nextConn)
	sendDone := make(chan struct{})
	go func() {
		frame, err := srv.readFrame()
		if err != nil {
			t.Errorf("server readFrame: %v", err)
			close(sendDone)
			return
		}
		if Opcode(frame[0]) != opSendTracked {
			t.Errorf("server opcode = %x, want opSend", frame[0])
		}
		gotID, sendID, body := trackedSendParts(frame)
		if gotID != connID {
			t.Errorf("server Send conn_id = %x, want %x", gotID, connID)
		}
		if got := string(body); got != "reused" {
			t.Errorf("server Send payload = %q, want reused", got)
		}
		srv.writeSendResult(connID, sendID, sendResultOK, "")
		close(sendDone)
	}()
	if n, err := nextConn.Write([]byte("reused")); err != nil || n != len("reused") {
		t.Fatalf("reused conn Write = %d, %v; want full write, nil", n, err)
	}
	select {
	case <-sendDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not consume reused send")
	}
}

func TestLocalCloseDropsLateRecvBeforeCloseOKForReusedConnID(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0xCAFE04
	closedConn := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, closedConn)

	if err := closedConn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertServerSawClose(t, srv, connID)

	srv.writeFrame(recvFrame(connID, []byte("late-stale")))
	srv.writeFrame(closeOKFrame(connID))
	waitConnBookkeepingAbsent(t, drv, connID)

	nextConn := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, nextConn)

	_ = nextConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	buf := make([]byte, len("late-stale"))
	if n, err := nextConn.Read(buf); err == nil || n != 0 {
		t.Fatalf("reused conn Read delivered %q; want no stale bytes", buf[:n])
	}

	waitConnBookkeepingAbsent(t, drv, connID)
}

func TestUnknownPreRegistrationCloseOKClosesOnRegisterConn(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0xCAFE03
	srv.writeFrame(closeOKFrame(connID))
	waitCloseBookkeeping(t, drv, connID, true, false)

	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, c)

	buf := make([]byte, 1)
	if _, err := c.Read(buf); err != io.EOF {
		t.Fatalf("Read err = %v, want EOF", err)
	}
	if _, err := c.Write([]byte("stale")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write err = %v, want ErrClosed", err)
	}
	if _, ok := drv.conns[connID]; ok {
		t.Fatalf("closed conn registered in conns")
	}
}

func newPendingCloseTestDriver() *Driver {
	return &Driver{
		conns:        make(map[uint32]*pilotConn),
		pendingRecv:  make(map[uint32][][]byte),
		pendingClose: make(map[uint32]struct{}),
		localClose:   make(map[uint32]struct{}),
		closedCh:     make(chan struct{}),
	}
}

func assertServerSawClose(t *testing.T, srv *mockServer, wantID uint32) {
	t.Helper()
	frame, err := srv.readFrame()
	if err != nil {
		t.Fatalf("server readFrame: %v", err)
	}
	if Opcode(frame[0]) != opClose {
		t.Fatalf("server opcode = %x, want opClose", frame[0])
	}
	if gotID := binary.BigEndian.Uint32(frame[1:5]); gotID != wantID {
		t.Fatalf("server Close conn_id = %x, want %x", gotID, wantID)
	}
}

func recvFrame(id uint32, data []byte) []byte {
	frame := make([]byte, 5+len(data))
	frame[0] = byte(opRecv)
	binary.BigEndian.PutUint32(frame[1:5], id)
	copy(frame[5:], data)
	return frame
}

func waitCloseBookkeeping(t *testing.T, d *Driver, id uint32, wantPending, wantLocal bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		d.connsMu.Lock()
		_, pending := d.pendingClose[id]
		_, local := d.localClose[id]
		d.connsMu.Unlock()
		if pending == wantPending && local == wantLocal {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("close bookkeeping for %x: pendingClose=%v localClose=%v, want pendingClose=%v localClose=%v",
				id, pending, local, wantPending, wantLocal)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitConnBookkeepingAbsent(t *testing.T, d *Driver, id uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		d.connsMu.Lock()
		_, pendingRecv := d.pendingRecv[id]
		_, pendingClose := d.pendingClose[id]
		_, localClose := d.localClose[id]
		d.connsMu.Unlock()
		if !pendingRecv && !pendingClose && !localClose {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("bookkeeping for %x: pendingRecv=%v pendingClose=%v localClose=%v, want all absent",
				id, pendingRecv, pendingClose, localClose)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
