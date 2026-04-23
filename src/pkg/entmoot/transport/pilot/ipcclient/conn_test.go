package ipcclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
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
	dir := t.TempDir()
	sock := filepath.Join(dir, "pilot.sock")

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
		_ = os.Remove(sock)
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
		if Opcode(frame[0]) != opSend {
			t.Errorf("server got %x, want Send", frame[0])
			return
		}
		if gotID := binary.BigEndian.Uint32(frame[1:5]); gotID != connID {
			t.Errorf("Send conn_id = %x, want %x", gotID, connID)
		}
		if !bytes.Equal(frame[5:], []byte("ping")) {
			t.Errorf("Send payload = %q, want %q", frame[5:], "ping")
		}
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
		if Opcode(frame[0]) != opSend {
			t.Errorf("non-Send frame observed: opcode=%x", frame[0])
			continue
		}
		if gotID := binary.BigEndian.Uint32(frame[1:5]); gotID != connID {
			t.Errorf("Send conn_id = %x, want %x", gotID, connID)
			continue
		}
		got[string(frame[5:])]++
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
