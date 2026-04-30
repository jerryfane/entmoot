package ipcclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// newTestDriver pairs a Driver with a minimal mock daemon over a
// net.Pipe-style Unix socketpair. Returned helpers let tests inject
// framed bytes from the server side (as if the daemon sent them) and
// read the bytes the client emitted (as if the daemon received them).
//
// The caller MUST defer cleanup() which closes both ends deterministically.
func newTestDriver(t *testing.T) (drv *Driver, srv *mockServer, cleanup func()) {
	t.Helper()
	sock := newTestSocketPath(t)

	srv = newMockServer(t, sock)
	d, err := Connect(sock)
	if err != nil {
		srv.Close()
		t.Fatalf("Connect: %v", err)
	}
	drv = d
	cleanup = func() {
		_ = drv.Close()
		srv.Close()
	}
	return drv, srv, cleanup
}

// TestDriverInfoRoundTrip exercises the command-response plumbing
// end-to-end: the client sends an Info frame, the mock daemon replies
// with InfoOK carrying a JSON body, and Info returns the decoded map.
func TestDriverInfoRoundTrip(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Server-side handler: wait for an Info frame, reply with InfoOK.
	go func() {
		frame, err := srv.readFrame()
		if err != nil {
			t.Logf("server readFrame: %v", err)
			return
		}
		if len(frame) != 1 || Opcode(frame[0]) != opInfo {
			t.Errorf("server got frame %x, want single Info opcode", frame)
			return
		}
		body := []byte(`{"node_id": 42, "hostname": "alice"}`)
		srv.writeFrame(append([]byte{byte(opInfoOK)}, body...))
	}()

	info, err := drv.Info(context.Background())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if nid, _ := info["node_id"].(float64); nid != 42 {
		t.Fatalf("Info node_id = %v, want 42", info["node_id"])
	}
	if h, _ := info["hostname"].(string); h != "alice" {
		t.Fatalf("Info hostname = %v, want alice", info["hostname"])
	}
}

// TestDriverListenAndAccept drives a full Bind -> AcceptedConn -> Recv
// sequence through the demuxer and confirms the accepted Conn reads
// the data the mock daemon injects.
func TestDriverListenAndAccept(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Server: reply to Bind, then push an AcceptedConn + a Recv.
	const boundPort uint16 = 1004
	const connID uint32 = 0xDEADBEEF
	go func() {
		// Expect Bind.
		frame, err := srv.readFrame()
		if err != nil {
			return
		}
		if Opcode(frame[0]) != opBind {
			t.Errorf("server got %x, want Bind", frame[0])
			return
		}
		// BindOK reply with the bound port.
		var resp [3]byte
		resp[0] = byte(opBindOK)
		binary.BigEndian.PutUint16(resp[1:3], boundPort)
		srv.writeFrame(resp[:])

		// Push an AcceptedConn notification. Layout:
		// [opcode][2B local_port][4B conn_id][6B remote_addr][2B remote_port]
		var accept [1 + 2 + 4 + 6 + 2]byte
		accept[0] = byte(opAcceptedConn)
		binary.BigEndian.PutUint16(accept[1:3], boundPort)
		binary.BigEndian.PutUint32(accept[3:7], connID)
		// Remote addr: network=0, node=0x11223344, port=5555
		binary.BigEndian.PutUint16(accept[7:9], 0)
		binary.BigEndian.PutUint32(accept[9:13], 0x11223344)
		binary.BigEndian.PutUint16(accept[13:15], 5555)
		srv.writeFrame(accept[:])

		// Follow with a Recv for that conn_id.
		recvPayload := []byte("hello world")
		frame = make([]byte, 1+4+len(recvPayload))
		frame[0] = byte(opRecv)
		binary.BigEndian.PutUint32(frame[1:5], connID)
		copy(frame[5:], recvPayload)
		srv.writeFrame(frame)
	}()

	ln, err := drv.Listen(context.Background(), 1004)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()
	if ln.Port() != boundPort {
		t.Fatalf("Listener.Port() = %d, want %d", ln.Port(), boundPort)
	}

	acceptCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := ln.Accept(acceptCtx)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer c.Close()

	if got := c.RemoteAddr().String(); got != "0:0000.1122.3344:5555" {
		t.Fatalf("RemoteAddr = %q, want 0:0000.1122.3344:5555", got)
	}
	buf := make([]byte, 64)
	// Read may take a moment while demuxer delivers the Recv frame.
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := string(buf[:n]); got != "hello world" {
		t.Fatalf("Read = %q, want %q", got, "hello world")
	}
}

// TestDriverDialAddrAndWrite confirms the client emits a correctly-
// framed Dial, parses DialOK, and then emits Send frames with the
// right conn_id prefix.
func TestDriverDialAddrAndWrite(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0x01020304

	go func() {
		// Expect Dial.
		frame, err := srv.readFrame()
		if err != nil {
			return
		}
		if Opcode(frame[0]) != opDial {
			t.Errorf("server got %x, want Dial", frame[0])
			return
		}
		if len(frame) != 1+addrSize+2 {
			t.Errorf("Dial frame len %d, want %d", len(frame), 1+addrSize+2)
		}
		// Respond with DialOK + conn_id.
		resp := make([]byte, 5)
		resp[0] = byte(opDialOK)
		binary.BigEndian.PutUint32(resp[1:5], connID)
		srv.writeFrame(resp)

		// Expect a Send frame carrying our payload.
		frame, err = srv.readFrame()
		if err != nil {
			return
		}
		if Opcode(frame[0]) != opSendTracked {
			t.Errorf("server got %x, want Send", frame[0])
			return
		}
		gotID, sendID, body := trackedSendParts(frame)
		if gotID != connID {
			t.Errorf("Send conn_id = %x, want %x", gotID, connID)
		}
		if !bytes.Equal(body, []byte("ping")) {
			t.Errorf("Send payload = %q, want %q", body, "ping")
		}
		srv.writeSendResult(connID, sendID, sendResultOK, "")
	}()

	c, err := drv.DialAddr(context.Background(), Addr{Network: 0, Node: 42}, 1000)
	if err != nil {
		t.Fatalf("DialAddr: %v", err)
	}
	defer c.Close()

	n, err := c.Write([]byte("ping"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != 4 {
		t.Fatalf("Write n = %d, want 4", n)
	}
}

func TestConnWriteReturnsSendResultErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		code uint16
		want error
	}{
		{name: "missing", code: sendResultConnectionNotFound, want: ErrConnectionNotFound},
		{name: "not-established", code: sendResultConnectionNotEstablished, want: ErrConnectionNotEstablished},
		{name: "closing", code: sendResultConnectionClosing, want: ErrConnectionClosing},
		{name: "failed", code: sendResultFailed, want: ErrSendFailed},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			drv, srv, cleanup := newTestDriver(t)
			defer cleanup()
			const connID uint32 = 0x10203040
			c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
			drv.registerConn(connID, c)
			defer c.Close()

			done := make(chan error, 1)
			go func() {
				frame, err := srv.readFrame()
				if err != nil {
					done <- err
					return
				}
				if Opcode(frame[0]) != opSendTracked {
					done <- errors.New("unexpected send opcode")
					return
				}
				gotID, sendID, _ := trackedSendParts(frame)
				srv.writeSendResult(gotID, sendID, tt.code, "synthetic send result")
				done <- nil
			}()

			_, err := c.Write([]byte("payload"))
			if !errors.Is(err, tt.want) {
				t.Fatalf("Write err = %v, want %v", err, tt.want)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("server: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not finish")
			}
		})
	}
}

func TestConnWriteSerializesConcurrentSendResults(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0x10203040
	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, c)
	defer c.Close()

	firstDone := startConnWrite(t, c, []byte("first"))
	frame, err := srv.readFrame()
	if err != nil {
		t.Fatalf("server read first frame: %v", err)
	}
	if Opcode(frame[0]) != opSendTracked {
		t.Fatalf("first opcode = %x, want opSendTracked", frame[0])
	}
	gotID, firstSendID, body := trackedSendParts(frame)
	if gotID != connID || string(body) != "first" {
		t.Fatalf("first send = conn %x body %q; want conn %x body first", gotID, body, connID)
	}

	secondDone := startConnWrite(t, c, []byte("second"))
	requireWriteStillBlocked(t, secondDone, 30*time.Millisecond)

	serverConn := srv.getConn()
	if serverConn == nil {
		t.Fatal("mock server has no connection")
	}
	if err := serverConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline(server): %v", err)
	}
	if frame, err := srv.readFrame(); err == nil {
		t.Fatalf("second frame arrived before first send-result: %x", frame)
	}
	if err := serverConn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear server read deadline: %v", err)
	}

	srv.writeSendResult(connID, firstSendID, sendResultOK, "")
	requireSuccessfulWrite(t, firstDone, len("first"))

	frame, err = srv.readFrame()
	if err != nil {
		t.Fatalf("server read second frame: %v", err)
	}
	if Opcode(frame[0]) != opSendTracked {
		t.Fatalf("second opcode = %x, want opSendTracked", frame[0])
	}
	gotID, secondSendID, body := trackedSendParts(frame)
	if gotID != connID || string(body) != "second" {
		t.Fatalf("second send = conn %x body %q; want conn %x body second", gotID, body, connID)
	}
	srv.writeSendResult(connID, secondSendID, sendResultOK, "")
	requireSuccessfulWrite(t, secondDone, len("second"))
}

func TestConnSetWriteDeadlineWakesSendResultWait(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0x10203040
	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, c)
	defer c.Close()

	writeDone := startConnWrite(t, c, []byte("first"))
	frame, err := srv.readFrame()
	if err != nil {
		t.Fatalf("server read first frame: %v", err)
	}
	gotID, firstSendID, body := trackedSendParts(frame)
	if Opcode(frame[0]) != opSendTracked || gotID != connID || string(body) != "first" {
		t.Fatalf("first send frame = opcode %x conn %x body %q", frame[0], gotID, body)
	}
	requireWriteStillBlocked(t, writeDone, 30*time.Millisecond)

	if err := c.SetWriteDeadline(time.Now()); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	requireDeadlineWrite(t, writeDone)
	select {
	case <-drv.closedCh:
		t.Fatal("send-result wait timeout closed the driver")
	default:
	}

	// A late result for the timed-out send_id must be ignored, not delivered
	// to the next write on the same conn_id.
	srv.writeSendResult(connID, firstSendID, sendResultOK, "")
	if err := c.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear write deadline: %v", err)
	}
	nextDone := startConnWrite(t, c, []byte("second"))
	frame, err = srv.readFrame()
	if err != nil {
		t.Fatalf("server read second frame: %v", err)
	}
	gotID, secondSendID, body := trackedSendParts(frame)
	if Opcode(frame[0]) != opSendTracked || gotID != connID || string(body) != "second" {
		t.Fatalf("second send frame = opcode %x conn %x body %q", frame[0], gotID, body)
	}
	srv.writeSendResult(connID, secondSendID, sendResultOK, "")
	requireSuccessfulWrite(t, nextDone, len("second"))
}

func TestConnReusedConnIDDoesNotReuseSendID(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0x10203040
	oldConn := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, oldConn)

	oldDone := startConnWrite(t, oldConn, []byte("old"))
	frame, err := srv.readFrame()
	if err != nil {
		t.Fatalf("server read old frame: %v", err)
	}
	gotID, oldSendID, body := trackedSendParts(frame)
	if Opcode(frame[0]) != opSendTracked || gotID != connID || string(body) != "old" {
		t.Fatalf("old send frame = opcode %x conn %x body %q", frame[0], gotID, body)
	}
	if err := oldConn.SetWriteDeadline(time.Now()); err != nil {
		t.Fatalf("old SetWriteDeadline: %v", err)
	}
	requireDeadlineWrite(t, oldDone)
	drv.unregisterConn(connID)
	oldConn.closeLocal()

	freshConn := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, freshConn)
	defer freshConn.Close()

	newDone := startConnWrite(t, freshConn, []byte("new"))
	frame, err = srv.readFrame()
	if err != nil {
		t.Fatalf("server read new frame: %v", err)
	}
	gotID, newSendID, body := trackedSendParts(frame)
	if Opcode(frame[0]) != opSendTracked || gotID != connID || string(body) != "new" {
		t.Fatalf("new send frame = opcode %x conn %x body %q", frame[0], gotID, body)
	}
	if newSendID == oldSendID {
		t.Fatalf("reused conn_id reused send_id %d", newSendID)
	}

	srv.writeSendResult(connID, oldSendID, sendResultConnectionNotFound, "stale result")
	requireWriteStillBlocked(t, newDone, 30*time.Millisecond)
	srv.writeSendResult(connID, newSendID, sendResultOK, "")
	requireSuccessfulWrite(t, newDone, len("new"))
}

func TestConnWriteWakesWhenDemuxExitsBeforeSendResult(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0x10203040
	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, c)
	defer c.Close()

	writeDone := startConnWrite(t, c, []byte("first"))
	frame, err := srv.readFrame()
	if err != nil {
		t.Fatalf("server read first frame: %v", err)
	}
	gotID, _, body := trackedSendParts(frame)
	if Opcode(frame[0]) != opSendTracked || gotID != connID || string(body) != "first" {
		t.Fatalf("first send frame = opcode %x conn %x body %q", frame[0], gotID, body)
	}

	serverConn := srv.getConn()
	if serverConn == nil {
		t.Fatal("mock server has no connection")
	}
	if err := serverConn.Close(); err != nil {
		t.Fatalf("server close: %v", err)
	}
	requireClosedWrite(t, writeDone)

	secondDone := startConnWrite(t, c, []byte("second"))
	requireFailedWrite(t, secondDone)
}

func TestConnWriteAfterDemuxExitFailsBeforeRegisteringWaiter(t *testing.T) {
	t.Parallel()
	drv, cleanup := newPostDemuxTestDriver(t)
	defer cleanup()

	const connID uint32 = 0x10203040
	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})

	n, err := c.Write([]byte("post-demux"))
	if n != 0 {
		t.Fatalf("Write n = %d, want 0", n)
	}
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Write err = %v, want ErrClosed", err)
	}

	drv.sendResultsMu.Lock()
	waiters := len(drv.sendResults)
	drv.sendResultsMu.Unlock()
	if waiters != 0 {
		t.Fatalf("post-demux Write registered %d send-result waiters", waiters)
	}
}

func TestConnWaitSendResultPrefersAckOverDemuxDone(t *testing.T) {
	t.Parallel()
	drv, cleanup := newPostDemuxTestDriver(t)
	defer cleanup()

	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	ch := make(chan sendResult, 1)
	ch <- sendResult{code: sendResultOK}

	if err := c.waitSendResult(ch); err != nil {
		t.Fatalf("waitSendResult err = %v, want nil", err)
	}
}

func TestConnWritePrefersAckOverDaemonClose(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0x10203040
	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, c)
	defer c.Close()

	writeDone := startConnWrite(t, c, []byte("ack-then-close"))
	frame, err := srv.readFrame()
	if err != nil {
		t.Fatalf("server read send: %v", err)
	}
	gotID, sendID, body := trackedSendParts(frame)
	if Opcode(frame[0]) != opSendTracked || gotID != connID || string(body) != "ack-then-close" {
		t.Fatalf("send frame = opcode %x conn %x body %q", frame[0], gotID, body)
	}
	srv.writeSendResult(connID, sendID, sendResultOK, "")
	if serverConn := srv.getConn(); serverConn != nil {
		_ = serverConn.Close()
	}

	requireSuccessfulWrite(t, writeDone, len("ack-then-close"))
}

func TestConnSetDeadlineAffectsWrites(t *testing.T) {
	t.Parallel()
	drv, peer, cleanup := newPipeWriteDeadlineTestDriver(t)
	defer cleanup()

	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	if err := c.SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	start := time.Now()
	_, err := c.Write([]byte("blocked"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Write err = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("Write returned before the deadline could block: %v", elapsed)
	}

	_ = peer.Close()
}

func TestConnSetWriteDeadlineUnblocksBlockedWrite(t *testing.T) {
	t.Parallel()
	drv, peer, cleanup := newPipeWriteDeadlineTestDriver(t)
	defer cleanup()

	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	if err := c.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	start := time.Now()
	_, err := c.Write([]byte("blocked"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Write err = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("Write returned before the deadline could block: %v", elapsed)
	}

	_ = peer.Close()
}

func TestConnSetWriteDeadlineFromAnotherGoroutineUnblocksInFlightWrite(t *testing.T) {
	t.Parallel()
	drv, _, cleanup := newPipeWriteDeadlineTestDriver(t)
	defer cleanup()

	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	writeDone := startConnWrite(t, c, []byte("blocked"))
	requireWriteStillBlocked(t, writeDone, 50*time.Millisecond)

	setDone := make(chan error, 1)
	go func() {
		setDone <- c.SetWriteDeadline(time.Now())
	}()
	requireNoAsyncErr(t, "SetWriteDeadline", setDone)
	requireDeadlineWriteAndClosedDriver(t, drv, writeDone)
}

func TestConnSetDeadlineFromAnotherGoroutineUnblocksInFlightWrite(t *testing.T) {
	t.Parallel()
	drv, _, cleanup := newPipeWriteDeadlineTestDriver(t)
	defer cleanup()

	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	writeDone := startConnWrite(t, c, []byte("blocked"))
	requireWriteStillBlocked(t, writeDone, 50*time.Millisecond)

	setDone := make(chan error, 1)
	go func() {
		setDone <- c.SetDeadline(time.Now())
	}()
	requireNoAsyncErr(t, "SetDeadline", setDone)
	requireDeadlineWriteAndClosedDriver(t, drv, writeDone)
}

func TestConnSetWriteDeadlineOnDifferentConnDoesNotUnblockInFlightWrite(t *testing.T) {
	t.Parallel()
	drv, _, cleanup := newPipeWriteDeadlineTestDriver(t)
	defer cleanup()

	active := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	other := newConn(drv, 2, SocketAddr{}, SocketAddr{})
	writeDone := startConnWrite(t, active, []byte("blocked"))
	requireWriteStillBlocked(t, writeDone, 50*time.Millisecond)

	setDone := make(chan error, 1)
	go func() {
		setDone <- other.SetWriteDeadline(time.Now())
	}()
	requireNoAsyncErr(t, "SetWriteDeadline(other)", setDone)
	requireWriteStillBlocked(t, writeDone, 100*time.Millisecond)

	select {
	case <-drv.closedCh:
		t.Fatal("different conn's write deadline closed the driver")
	default:
	}

	setActiveDone := make(chan error, 1)
	go func() {
		setActiveDone <- active.SetWriteDeadline(time.Now())
	}()
	requireNoAsyncErr(t, "SetWriteDeadline(active)", setActiveDone)
	requireDeadlineWriteAndClosedDriver(t, drv, writeDone)
}

func TestConnWriteDeadlineClosesDriverAfterTimedOutWrite(t *testing.T) {
	t.Parallel()
	drv, _, cleanup := newPipeWriteDeadlineTestDriver(t)
	defer cleanup()

	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	if err := c.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if _, err := c.Write([]byte("blocked")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("timed Write err = %v, want deadline exceeded", err)
	}

	select {
	case <-drv.closedCh:
	case <-time.After(time.Second):
		t.Fatal("driver did not close after timed-out write")
	}

	if err := drv.writeFrame([]byte{byte(opInfo)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("driver command write after timed conn write = %v, want ErrClosed", err)
	}
}

func TestConnWriteDeadlineClearedAfterSuccessfulWrite(t *testing.T) {
	t.Parallel()
	drv, peer, cleanup := newPipeWriteDeadlineTestDriver(t)
	defer cleanup()

	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	if err := c.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	readSendDone := make(chan error, 1)
	go func() {
		frame, err := readFrame(peer)
		if err != nil {
			readSendDone <- err
			return
		}
		if Opcode(frame[0]) != opSendTracked {
			readSendDone <- errors.New("unexpected send frame")
			return
		}
		gotID, sendID, body := trackedSendParts(frame)
		if gotID != 1 {
			readSendDone <- errors.New("unexpected send conn id")
			return
		}
		if got := string(body); got != "ok" {
			readSendDone <- errors.New("unexpected send payload")
			return
		}
		drv.routeSendResult(sendResultPayload(1, sendID, sendResultOK, ""))
		readSendDone <- nil
	}()

	if n, err := c.Write([]byte("ok")); err != nil || n != 2 {
		t.Fatalf("Write = %d, %v; want 2, nil", n, err)
	}
	select {
	case err := <-readSendDone:
		if err != nil {
			t.Fatalf("read send frame: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive send frame")
	}

	readCommandDone := make(chan error, 1)
	go func() {
		frame, err := readFrame(peer)
		if err != nil {
			readCommandDone <- err
			return
		}
		if len(frame) != 1 || Opcode(frame[0]) != opInfo {
			readCommandDone <- errors.New("unexpected command frame")
			return
		}
		readCommandDone <- nil
	}()

	if err := drv.writeFrame([]byte{byte(opInfo)}); err != nil {
		t.Fatalf("driver command write after successful conn write: %v", err)
	}
	select {
	case err := <-readCommandDone:
		if err != nil {
			t.Fatalf("read command frame: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive command frame")
	}
}

func TestConnClearingReadDeadlineKeepsBlockedReadAlive(t *testing.T) {
	t.Parallel()
	drv := newPendingCloseTestDriver()
	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})

	if err := c.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	readDone := startConnRead(t, c)
	time.Sleep(10 * time.Millisecond)
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear SetReadDeadline: %v", err)
	}
	requireReadStillBlocked(t, readDone, 60*time.Millisecond)

	c.pushRecv([]byte("ok"))
	requireReadResult(t, readDone, "ok", nil)
}

func TestConnExtendingReadDeadlineKeepsBlockedReadAlive(t *testing.T) {
	t.Parallel()
	drv := newPendingCloseTestDriver()
	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})

	if err := c.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	readDone := startConnRead(t, c)
	time.Sleep(10 * time.Millisecond)
	if err := c.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("extend SetReadDeadline: %v", err)
	}
	requireReadStillBlocked(t, readDone, 60*time.Millisecond)

	c.pushRecv([]byte("ok"))
	requireReadResult(t, readDone, "ok", nil)
}

func TestConnPastReadDeadlineUnblocksBlockedRead(t *testing.T) {
	t.Parallel()
	drv := newPendingCloseTestDriver()
	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})

	readDone := startConnRead(t, c)
	requireReadStillBlocked(t, readDone, 20*time.Millisecond)
	if err := c.SetReadDeadline(time.Now()); err != nil {
		t.Fatalf("SetReadDeadline(now): %v", err)
	}
	requireReadResult(t, readDone, "", os.ErrDeadlineExceeded)
}

// TestDriverSetPeerEndpointsTLVShape checks the TLV payload the client
// emits — a wire-format regression here would silently desync the
// daemon's endpoint map.
func TestDriverSetPeerEndpointsTLVShape(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	go func() {
		frame, err := srv.readFrame()
		if err != nil {
			return
		}
		if Opcode(frame[0]) != opSetPeerEndpoints {
			t.Errorf("server got %x, want SetPeerEndpoints", frame[0])
			return
		}
		// Validate the TLV shape.
		if len(frame) < 1+4+1 {
			t.Errorf("SetPeerEndpoints frame too short: %d", len(frame))
			return
		}
		gotID := binary.BigEndian.Uint32(frame[1:5])
		if gotID != 7 {
			t.Errorf("nodeID = %d, want 7", gotID)
		}
		count := int(frame[5])
		if count != 2 {
			t.Errorf("count = %d, want 2", count)
		}
		off := 6
		for i := 0; i < count; i++ {
			if off >= len(frame) {
				t.Errorf("entry %d: truncated before netlen", i)
				return
			}
			nl := int(frame[off])
			off++
			off += nl
			if off >= len(frame) {
				t.Errorf("entry %d: truncated before addrlen", i)
				return
			}
			al := int(frame[off])
			off++
			off += al
			if off > len(frame) {
				t.Errorf("entry %d: truncated addr", i)
				return
			}
		}
		// Reply with SetPeerEndpointsOK (empty JSON body is fine).
		srv.writeFrame([]byte{byte(opSetPeerEndpointsOK)})
	}()

	eps := []Endpoint{
		{Network: "tcp", Addr: "1.2.3.4:5678"},
		{Network: "udp", Addr: "10.0.0.1:9000"},
	}
	if err := drv.SetPeerEndpoints(context.Background(), 7, eps); err != nil {
		t.Fatalf("SetPeerEndpoints: %v", err)
	}
}

func newPipeWriteDeadlineTestDriver(t *testing.T) (*Driver, net.Conn, func()) {
	t.Helper()
	client, peer := net.Pipe()
	demuxDone := make(chan struct{})
	drv := &Driver{
		conn:      client,
		closedCh:  make(chan struct{}),
		demuxDone: demuxDone,
	}
	go func() {
		<-drv.closedCh
		close(demuxDone)
	}()
	cleanup := func() {
		_ = drv.Close()
		_ = peer.Close()
	}
	return drv, peer, cleanup
}

func newPostDemuxTestDriver(t *testing.T) (*Driver, func()) {
	t.Helper()
	client, peer := net.Pipe()
	demuxDone := make(chan struct{})
	close(demuxDone)
	drv := &Driver{
		conn:        client,
		closedCh:    make(chan struct{}),
		demuxDone:   demuxDone,
		sendResults: make(map[sendResultKey]chan sendResult),
	}
	cleanup := func() {
		_ = client.Close()
		_ = peer.Close()
	}
	return drv, cleanup
}

type connWriteResult struct {
	n   int
	err error
}

type connReadResult struct {
	n    int
	data string
	err  error
}

func startConnWrite(t *testing.T, c *Conn, p []byte) <-chan connWriteResult {
	t.Helper()
	done := make(chan connWriteResult, 1)
	go func() {
		n, err := c.Write(p)
		done <- connWriteResult{n: n, err: err}
	}()
	return done
}

func requireWriteStillBlocked(t *testing.T, done <-chan connWriteResult, d time.Duration) {
	t.Helper()
	select {
	case got := <-done:
		t.Fatalf("Write returned before deadline update: n=%d err=%v", got.n, got.err)
	case <-time.After(d):
	}
}

func requireNoAsyncErr(t *testing.T, name string, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("%s did not return", name)
	}
}

func requireSuccessfulWrite(t *testing.T, done <-chan connWriteResult, wantN int) {
	t.Helper()
	select {
	case got := <-done:
		if got.n != wantN || got.err != nil {
			t.Fatalf("Write = %d, %v; want %d, nil", got.n, got.err, wantN)
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not complete")
	}
}

func requireDeadlineWrite(t *testing.T, done <-chan connWriteResult) {
	t.Helper()
	select {
	case got := <-done:
		if got.n != 0 {
			t.Fatalf("Write n = %d, want 0", got.n)
		}
		if !errors.Is(got.err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write err = %v, want deadline exceeded", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not unblock after deadline update")
	}
}

func requireClosedWrite(t *testing.T, done <-chan connWriteResult) {
	t.Helper()
	select {
	case got := <-done:
		if got.n != 0 {
			t.Fatalf("Write n = %d, want 0", got.n)
		}
		if !errors.Is(got.err, ErrClosed) {
			t.Fatalf("Write err = %v, want ErrClosed", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not unblock after demux exit")
	}
}

func requireFailedWrite(t *testing.T, done <-chan connWriteResult) {
	t.Helper()
	select {
	case got := <-done:
		if got.n != 0 {
			t.Fatalf("Write n = %d, want 0", got.n)
		}
		if got.err == nil {
			t.Fatal("Write err = nil, want failure")
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not fail")
	}
}

func requireDeadlineWriteAndClosedDriver(t *testing.T, drv *Driver, done <-chan connWriteResult) {
	t.Helper()
	requireDeadlineWrite(t, done)

	select {
	case <-drv.closedCh:
	case <-time.After(time.Second):
		t.Fatal("driver did not close after timed-out write")
	}
}

func startConnRead(t *testing.T, c *Conn) <-chan connReadResult {
	t.Helper()
	done := make(chan connReadResult, 1)
	go func() {
		buf := make([]byte, 16)
		n, err := c.Read(buf)
		done <- connReadResult{n: n, data: string(buf[:n]), err: err}
	}()
	return done
}

func requireReadStillBlocked(t *testing.T, done <-chan connReadResult, d time.Duration) {
	t.Helper()
	select {
	case got := <-done:
		t.Fatalf("Read returned early: n=%d data=%q err=%v", got.n, got.data, got.err)
	case <-time.After(d):
	}
}

func requireReadResult(t *testing.T, done <-chan connReadResult, wantData string, wantErr error) {
	t.Helper()
	select {
	case got := <-done:
		if wantErr != nil {
			if !errors.Is(got.err, wantErr) {
				t.Fatalf("Read err = %v, want %v", got.err, wantErr)
			}
			return
		}
		if got.err != nil || got.data != wantData {
			t.Fatalf("Read = %q, %v; want %q, nil", got.data, got.err, wantData)
		}
	case <-time.After(time.Second):
		t.Fatal("Read did not return")
	}
}

// TestDriverSetPeerEndpointsEnforcesLimits preflight-rejects invalid
// input without touching the socket. A daemon-side rejection would
// also work but client-side validation yields clearer errors.
func TestDriverSetPeerEndpointsEnforcesLimits(t *testing.T) {
	t.Parallel()
	drv, _, cleanup := newTestDriver(t)
	defer cleanup()

	// Too many endpoints.
	tooMany := make([]Endpoint, 9)
	if err := drv.SetPeerEndpoints(context.Background(), 1, tooMany); err == nil {
		t.Fatal("SetPeerEndpoints(9 endpoints) should error")
	}
	// Network too long.
	bigNet := []Endpoint{{Network: string(make([]byte, 17)), Addr: "x:1"}}
	if err := drv.SetPeerEndpoints(context.Background(), 1, bigNet); err == nil {
		t.Fatal("SetPeerEndpoints(17-byte network) should error")
	}
	// Addr too long.
	bigAddr := []Endpoint{{Network: "tcp", Addr: string(make([]byte, 256))}}
	if err := drv.SetPeerEndpoints(context.Background(), 1, bigAddr); err == nil {
		t.Fatal("SetPeerEndpoints(256-byte addr) should error")
	}
}

// TestDriverError surfaces a daemon-side Error frame as an IPCError.
func TestDriverError(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	go func() {
		_, _ = srv.readFrame() // ignore the Bind we receive
		errFrame := []byte{byte(opError), 0x01, 0x23}
		errFrame = append(errFrame, []byte("port in use")...)
		srv.writeFrame(errFrame)
	}()

	_, err := drv.Listen(context.Background(), 1004)
	if err == nil {
		t.Fatal("Listen should have errored")
	}
	var ipcErr *IPCError
	if !errors.As(err, &ipcErr) {
		t.Fatalf("Listen err type = %T, want *IPCError (chain: %v)", err, err)
	}
	if ipcErr.Code != 0x0123 {
		t.Errorf("IPCError code = %04X, want 0123", ipcErr.Code)
	}
	if ipcErr.Message != "port in use" {
		t.Errorf("IPCError msg = %q, want 'port in use'", ipcErr.Message)
	}
}

// TestDriverCloseIdempotent guarantees Close is safe to call multiple
// times, a contract the rest of Entmoot relies on (deferred Close in
// tests, cascading Transport teardown, etc.).
func TestDriverCloseIdempotent(t *testing.T) {
	t.Parallel()
	drv, _, cleanup := newTestDriver(t)
	defer cleanup()

	if err := drv.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second call must not panic or return an error.
	if err := drv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestDriverCloseUnblocksPendingCommand confirms a sendAndWait blocked
// on an unanswered response unwinds with ErrClosed when the driver
// closes, instead of hanging forever.
func TestDriverCloseUnblocksPendingCommand(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Drain the frame the server will receive so readLoop doesn't
	// block on buffer backpressure during Close.
	go func() {
		_, _ = srv.readFrame()
	}()

	done := make(chan error, 1)
	go func() {
		_, err := drv.Info(context.Background())
		done <- err
	}()

	// Give Info a moment to register its pending waiter.
	time.Sleep(50 * time.Millisecond)
	_ = drv.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, io.EOF) {
			// Close can race with the demuxer reading EOF on the
			// shutdown socket; accept any of the plausible
			// teardown errors.
			if err == nil {
				t.Fatalf("Info returned nil after Close, want error")
			}
			t.Logf("Info err after Close = %v (accepted)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Info still blocked after Close")
	}
}

func TestDriverInfoPrefersReplyOverDaemonClose(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	done := make(chan struct{}, 1)
	go func() {
		frame, err := srv.readFrame()
		if err != nil {
			t.Errorf("server read Info: %v", err)
			close(done)
			return
		}
		if len(frame) != 1 || Opcode(frame[0]) != opInfo {
			t.Errorf("server got frame %x, want Info", frame)
			close(done)
			return
		}
		body := []byte(`{"node_id": 42, "hostname": "alice"}`)
		srv.writeFrame(append([]byte{byte(opInfoOK)}, body...))
		if serverConn := srv.getConn(); serverConn != nil {
			_ = serverConn.Close()
		}
		close(done)
	}()

	info, err := drv.Info(context.Background())
	if err != nil {
		t.Fatalf("Info after reply-then-close: %v", err)
	}
	if nid, _ := info["node_id"].(float64); nid != 42 {
		t.Fatalf("Info node_id = %v, want 42", info["node_id"])
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server did not finish")
	}
}

// TestDriverCtxCancelUnblocksPendingCommand checks the symmetric case:
// a cancelled context on sendAndWait unwinds the call with ctx.Err(),
// not a hang.
func TestDriverCtxCancelUnblocksPendingCommand(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	go func() {
		_, _ = srv.readFrame() // swallow the Info we never answer
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := drv.Info(ctx)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Info err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Info still blocked after ctx cancel")
	}
}

// TestConnConcurrentWrites issues many concurrent Writes on the same
// Conn and asserts that the server observes exactly as many framed
// Send payloads as we sent, none garbled across frame boundaries.
//
// Because outbound frames are batched through a single writer mutex,
// the race we want to catch is interleaving: concurrent Write calls
// must not smear each other's bytes across the socket.
func TestConnConcurrentWrites(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0xCAFEBABE
	const writers = 8
	const iters = 16
	const expected = writers * iters

	// Pre-register a connection.
	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, c)
	defer c.Close()

	// Server-side: read exactly `expected` Send frames, then stop.
	frames := make(chan []byte, expected)
	readErr := make(chan error, 1)
	go func() {
		for i := 0; i < expected; i++ {
			frame, err := srv.readFrame()
			if err != nil {
				readErr <- err
				return
			}
			if len(frame) >= 13 && Opcode(frame[0]) == opSendTracked {
				connID, sendID, _ := trackedSendParts(frame)
				srv.writeSendResult(connID, sendID, sendResultOK, "")
			}
			frames <- frame
		}
		readErr <- nil
	}()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				msg := []byte(timeFmtPayload(id, i))
				if _, err := c.Write(msg); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Wait for the server-side reader to finish consuming all frames.
	select {
	case err := <-readErr:
		if err != nil {
			t.Fatalf("server reader: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("server reader did not consume %d frames in time", expected)
	}

	got := make(map[string]int)
	close(frames)
	for frame := range frames {
		if Opcode(frame[0]) != opSendTracked {
			t.Errorf("non-Send frame observed: opcode=%x", frame[0])
			continue
		}
		gotID, _, body := trackedSendParts(frame)
		if gotID != connID {
			t.Errorf("Send conn_id = %x, want %x", gotID, connID)
			continue
		}
		got[string(body)]++
	}
	observed := 0
	for payload, n := range got {
		observed += n
		if n != 1 {
			t.Errorf("payload %q observed %d times, want 1 (interleaving?)", payload, n)
		}
	}
	if observed != expected {
		t.Fatalf("observed %d Send payloads, want %d", observed, expected)
	}
}

// timeFmtPayload composes a tagged payload per writer iteration so we
// can detect cross-frame garbling.
func timeFmtPayload(w, i int) string {
	return "w" + itoa(w) + "-i" + itoa(i)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// TestConnCloseIdempotent covers the Conn-level Close contract: two
// calls must not panic or return distinct errors.
func TestConnCloseIdempotent(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Pre-register a conn so we don't need the Dial round-trip.
	c := newConn(drv, 1, SocketAddr{}, SocketAddr{})
	drv.registerConn(1, c)

	// Drain the two Close frames the client will emit.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err := srv.readFrame()
			if err != nil {
				return
			}
		}
	}()

	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Post-close reads must return io.EOF, not block.
	_ = c.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 8)
	_, err := c.Read(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Read after Close = %v, want io.EOF", err)
	}
}

func TestConnCloseOKActiveClosesReadAndWrite(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const connID uint32 = 0x12345678
	c := newConn(drv, connID, SocketAddr{}, SocketAddr{})
	drv.registerConn(connID, c)

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := c.Read(buf)
		readDone <- err
	}()

	srv.writeFrame(closeOKFrame(connID))

	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read err = %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not unblock after CloseOK")
	}

	if _, err := c.Write([]byte("stale")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write err = %v, want ErrClosed", err)
	}
}

func TestConnCloseOKOnlyClosesMatchingConn(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	const closedID uint32 = 0xA1
	const openID uint32 = 0xB2
	closedConn := newConn(drv, closedID, SocketAddr{}, SocketAddr{})
	openConn := newConn(drv, openID, SocketAddr{}, SocketAddr{})
	drv.registerConn(closedID, closedConn)
	drv.registerConn(openID, openConn)

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := closedConn.Read(buf)
		readDone <- err
	}()

	srv.writeFrame(closeOKFrame(closedID))

	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("closed conn Read err = %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closed conn Read did not unblock after CloseOK")
	}

	if _, err := closedConn.Write([]byte("stale")); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed conn Write err = %v, want ErrClosed", err)
	}
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
		if gotID != openID {
			t.Errorf("server Send conn_id = %x, want %x", gotID, openID)
		}
		if got := string(body); got != "still-open" {
			t.Errorf("server Send payload = %q, want still-open", got)
		}
		srv.writeSendResult(openID, sendID, sendResultOK, "")
		close(sendDone)
	}()
	if n, err := openConn.Write([]byte("still-open")); err != nil || n != len("still-open") {
		t.Fatalf("open conn Write = %d, %v; want full write, nil", n, err)
	}
	select {
	case <-sendDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not consume open send")
	}
}

func closeOKFrame(id uint32) []byte {
	frame := make([]byte, 5)
	frame[0] = byte(opCloseOK)
	binary.BigEndian.PutUint32(frame[1:5], id)
	return frame
}

// TestListenerCloseUnblocksAccept covers the symmetric contract on
// the listener side: a blocked Accept must unwind when Close is
// invoked.
func TestListenerCloseUnblocksAccept(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Reply to Bind so Listen returns.
	go func() {
		_, _ = srv.readFrame()
		resp := []byte{byte(opBindOK), 0, 1}
		srv.writeFrame(resp)
		_, _ = srv.readFrame()
		resp = []byte{byte(opUnbindOK), 0, 1}
		srv.writeFrame(resp)
	}()

	ln, err := drv.Listen(context.Background(), 1)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := ln.Accept(context.Background())
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Accept err after Close = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept still blocked after listener Close")
	}
}

// TestAddrStringAndParseRoundTrip exercises the fact-shape of the
// Pilot text address format. The round-trip is the load-bearing
// invariant; a regression here breaks peer decoding in remoteNodeID.
func TestAddrStringAndParseRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []SocketAddr{
		{Addr: Addr{Network: 0, Node: 1}, Port: 1000},
		{Addr: Addr{Network: 1, Node: 0xF2910004}, Port: 443},
		{Addr: Addr{Network: 0xABCD, Node: 0xFFFFFFFF}, Port: 0},
	}
	for _, want := range cases {
		s := want.String()
		got, err := ParseSocketAddr(s)
		if err != nil {
			t.Errorf("ParseSocketAddr(%q): %v", s, err)
			continue
		}
		if got != want {
			t.Errorf("round-trip %q: got %+v, want %+v", s, got, want)
		}
	}
}

// TestAddrParseRejectsMalformed guards the error branches in parseAddr.
func TestAddrParseRejectsMalformed(t *testing.T) {
	t.Parallel()
	bad := []string{
		"",
		"0",
		"0:1234",
		"0:1234.5678",
		"0:ZZZZ.1111.2222:10",
		"0:0001.1111.2222:10", // decimal 0 != hex 0001
		"notadecimal:0001.2222.3333:10",
		"0:0000.0000.0000:notaport",
	}
	for _, s := range bad {
		if _, err := ParseSocketAddr(s); err == nil {
			t.Errorf("ParseSocketAddr(%q) succeeded, want error", s)
		}
	}
}
