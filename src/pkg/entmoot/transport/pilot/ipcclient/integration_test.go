package ipcclient

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDaemon is an in-process Pilot-daemon simulator. It speaks the
// IPC wire protocol this package describes in doc.go and responds to
// the handful of opcodes Entmoot's adapter uses. The daemon is
// intentionally minimal — it does NOT replicate Pilot's networking
// behaviour, just the IPC side of it — but it is enough to verify
// this package's client-side conformance end-to-end on a real Unix
// socket.
//
// Each fakeDaemon handles at most one client connection (matching
// Entmoot's one-daemon-per-process model). Multiple fakeDaemon
// instances are cheap to spin up in parallel tests.
type fakeDaemon struct {
	ln   net.Listener
	path string

	nodeID   uint32
	hostname string

	// counters for assertions across a test.
	dialCount  atomic.Int64
	sendCount  atomic.Int64
	closeCount atomic.Int64
	epCount    atomic.Int64

	// sent holds the last SetPeerEndpoints payload we decoded, for
	// test assertions on wire shape.
	sentMu   sync.Mutex
	lastPeer uint32
	lastEps  []Endpoint

	// trustedPeers is the static TrustedPeers response body.
	trustedPeers []map[string]interface{}

	// client tracks the most-recent accepted client connection so
	// the "inbound injection" helpers can write server-originated
	// frames to it.
	clientMu sync.Mutex
	client   net.Conn
	// injectMu serializes injectOn writes so they can't interleave
	// with response writes from the serve() loop on the same socket.
	injectMu sync.Mutex

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newFakeDaemon(t *testing.T, nodeID uint32, hostname string) *fakeDaemon {
	t.Helper()
	dir := t.TempDir()
	sock := filepath.Join(dir, "pilot.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("fake daemon listen: %v", err)
	}
	d := &fakeDaemon{
		ln:       ln,
		path:     sock,
		nodeID:   nodeID,
		hostname: hostname,
		stopCh:   make(chan struct{}),
	}
	d.wg.Add(1)
	go d.acceptLoop(t)
	return d
}

func (d *fakeDaemon) SocketPath() string { return d.path }

func (d *fakeDaemon) SetTrustedPeers(peers []map[string]interface{}) {
	d.trustedPeers = peers
}

func (d *fakeDaemon) Close() {
	select {
	case <-d.stopCh:
		return
	default:
		close(d.stopCh)
	}
	_ = d.ln.Close()
	d.wg.Wait()
	_ = os.Remove(d.path)
}

func (d *fakeDaemon) acceptLoop(t *testing.T) {
	defer d.wg.Done()
	for {
		conn, err := d.ln.Accept()
		if err != nil {
			return
		}
		d.wg.Add(1)
		go d.serve(t, conn)
	}
}

// nextConnID allocates conn_ids for virtual connections. The exact
// scheme is unimportant; monotonically increasing is easy to debug.
var fakeNextConnID atomic.Uint32

func (d *fakeDaemon) serve(t *testing.T, conn net.Conn) {
	defer d.wg.Done()
	defer conn.Close()

	d.clientMu.Lock()
	d.client = conn
	d.clientMu.Unlock()
	defer func() {
		d.clientMu.Lock()
		if d.client == conn {
			d.client = nil
		}
		d.clientMu.Unlock()
	}()

	// serve-originated writes share the daemon's injectMu so
	// inject-from-tests and response-writes from this loop cannot
	// interleave on the socket.
	sendFrame := func(op Opcode, body []byte) error {
		d.injectMu.Lock()
		defer d.injectMu.Unlock()
		frame := make([]byte, 1+len(body))
		frame[0] = byte(op)
		copy(frame[1:], body)
		return writeFrame(conn, frame)
	}

	for {
		select {
		case <-d.stopCh:
			return
		default:
		}
		frame, err := readFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				t.Logf("fake daemon read: %v", err)
			}
			return
		}
		if len(frame) < 1 {
			continue
		}
		op := Opcode(frame[0])
		payload := frame[1:]

		switch op {
		case opInfo:
			body, _ := json.Marshal(map[string]interface{}{
				"node_id":  d.nodeID,
				"hostname": d.hostname,
			})
			_ = sendFrame(opInfoOK, body)

		case opBind:
			if len(payload) < 2 {
				continue
			}
			port := binary.BigEndian.Uint16(payload[0:2])
			if port == 0 {
				port = 49152 // ephemeral
			}
			var resp [2]byte
			binary.BigEndian.PutUint16(resp[:], port)
			_ = sendFrame(opBindOK, resp[:])

		case opDial:
			d.dialCount.Add(1)
			id := fakeNextConnID.Add(1)
			var resp [4]byte
			binary.BigEndian.PutUint32(resp[:], id)
			_ = sendFrame(opDialOK, resp[:])

		case opSend:
			d.sendCount.Add(1)
			// Echo the bytes back as a Recv frame on the same conn.
			if len(payload) < 4 {
				continue
			}
			echo := make([]byte, len(payload))
			copy(echo, payload) // [4B conn_id][data]
			_ = sendFrame(opRecv, echo)

		case opClose:
			d.closeCount.Add(1)
			if len(payload) >= 4 {
				// Confirm with CloseOK.
				var resp [4]byte
				copy(resp[:], payload[0:4])
				_ = sendFrame(opCloseOK, resp[:])
			}

		case opHandshake:
			if len(payload) < 1 {
				continue
			}
			sub := payload[0]
			if sub == subHandshakeTrusted {
				body, _ := json.Marshal(map[string]interface{}{
					"trusted": d.trustedPeers,
				})
				_ = sendFrame(opHandshakeOK, body)
			}

		case opSetPeerEndpoints:
			d.epCount.Add(1)
			eps, peer, err := parseSetPeerEndpoints(payload)
			if err != nil {
				// Emit a wire Error so the client surfaces it.
				errBody := []byte{0x00, 0x01}
				errBody = append(errBody, []byte(err.Error())...)
				_ = sendFrame(opError, errBody)
				continue
			}
			d.sentMu.Lock()
			d.lastPeer = peer
			d.lastEps = eps
			d.sentMu.Unlock()
			body, _ := json.Marshal(map[string]interface{}{"ok": true})
			_ = sendFrame(opSetPeerEndpointsOK, body)

		default:
			// Respond with a generic Error for unknown opcodes —
			// surfaces bugs in the client that try to talk opcodes
			// this fake doesn't handle.
			msg := fmt.Sprintf("unhandled opcode 0x%02X", byte(op))
			errBody := append([]byte{0x01, 0x00}, []byte(msg)...)
			_ = sendFrame(opError, errBody)
		}
	}
}

// parseSetPeerEndpoints decodes the TLV payload the client emits.
// This is the symmetric half of driver.go's builder and doubles as
// a conformance check for its on-wire shape.
func parseSetPeerEndpoints(p []byte) ([]Endpoint, uint32, error) {
	if len(p) < 5 {
		return nil, 0, errors.New("short frame")
	}
	peer := binary.BigEndian.Uint32(p[0:4])
	count := int(p[4])
	off := 5
	eps := make([]Endpoint, 0, count)
	for i := 0; i < count; i++ {
		if off >= len(p) {
			return nil, peer, fmt.Errorf("entry %d: netlen missing", i)
		}
		nl := int(p[off])
		off++
		if off+nl > len(p) {
			return nil, peer, fmt.Errorf("entry %d: network truncated", i)
		}
		net := string(p[off : off+nl])
		off += nl
		if off >= len(p) {
			return nil, peer, fmt.Errorf("entry %d: addrlen missing", i)
		}
		al := int(p[off])
		off++
		if off+al > len(p) {
			return nil, peer, fmt.Errorf("entry %d: addr truncated", i)
		}
		addr := string(p[off : off+al])
		off += al
		eps = append(eps, Endpoint{Network: net, Addr: addr})
	}
	return eps, peer, nil
}

// TestIntegrationFullLifecycle walks through the client-side sequence
// Entmoot's adapter uses at startup: Connect → Info → Listen →
// DialAddr → Write/Read → SetPeerEndpoints → Close. Each step talks
// to an in-process fakeDaemon that speaks the documented wire
// protocol. The test is both a protocol-conformance spec AND a
// regression guard on the public surface.
func TestIntegrationFullLifecycle(t *testing.T) {
	t.Parallel()
	daemon := newFakeDaemon(t, 0x12345678, "alice.test")
	defer daemon.Close()
	daemon.SetTrustedPeers([]map[string]interface{}{
		{"node_id": float64(100), "hostname": "bob"},
		{"node_id": float64(200), "hostname": "carol"},
	})

	ctx := context.Background()
	drv, err := Connect(daemon.SocketPath())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer drv.Close()

	// --- Info ---
	info, err := drv.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if got := info["node_id"].(float64); uint32(got) != 0x12345678 {
		t.Fatalf("Info node_id = %v, want 0x12345678", got)
	}
	if got := info["hostname"].(string); got != "alice.test" {
		t.Fatalf("Info hostname = %q, want alice.test", got)
	}

	// --- Listen ---
	ln, err := drv.Listen(ctx, 1004)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if ln.Port() != 1004 {
		t.Fatalf("Listener.Port = %d, want 1004", ln.Port())
	}
	defer ln.Close()

	// --- DialAddr + Write/Read round trip ---
	c, err := drv.DialAddr(ctx, Addr{Network: 0, Node: 42}, 1000)
	if err != nil {
		t.Fatalf("DialAddr: %v", err)
	}
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read (echo): %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("Read echo = %q, want %q", buf[:n], "hello")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}

	// --- TrustedPeers ---
	tp, err := drv.TrustedPeers(ctx)
	if err != nil {
		t.Fatalf("TrustedPeers: %v", err)
	}
	trusted, ok := tp["trusted"].([]interface{})
	if !ok {
		t.Fatalf("TrustedPeers body missing 'trusted' array: %+v", tp)
	}
	if len(trusted) != 2 {
		t.Fatalf("TrustedPeers len = %d, want 2", len(trusted))
	}

	// --- SetPeerEndpoints ---
	eps := []Endpoint{
		{Network: "tcp", Addr: "203.0.113.4:1222"},
		{Network: "udp", Addr: "203.0.113.4:1222"},
	}
	if err := drv.SetPeerEndpoints(ctx, 100, eps); err != nil {
		t.Fatalf("SetPeerEndpoints: %v", err)
	}
	daemon.sentMu.Lock()
	peer, seenEps := daemon.lastPeer, daemon.lastEps
	daemon.sentMu.Unlock()
	if peer != 100 {
		t.Fatalf("daemon saw peer = %d, want 100", peer)
	}
	if len(seenEps) != 2 || seenEps[0] != eps[0] || seenEps[1] != eps[1] {
		t.Fatalf("daemon saw eps %+v, want %+v", seenEps, eps)
	}

	// --- Sanity counters ---
	if got := daemon.dialCount.Load(); got != 1 {
		t.Errorf("daemon dial count = %d, want 1", got)
	}
	if got := daemon.sendCount.Load(); got != 1 {
		t.Errorf("daemon send count = %d, want 1", got)
	}
	if got := daemon.closeCount.Load(); got != 1 {
		t.Errorf("daemon close count = %d, want 1", got)
	}
	if got := daemon.epCount.Load(); got != 1 {
		t.Errorf("daemon set-peer-endpoints count = %d, want 1", got)
	}
}

// TestIntegrationInboundAcceptAndEcho exercises the server-side
// listener path. The fakeDaemon here dials *into* the client's
// listener (by emitting an AcceptedConn frame out of band) to prove
// that the full receive chain — AcceptedConn → Conn registration →
// subsequent Recv delivery — behaves correctly against a real
// socket rather than the net.Pipe used in the unit tests.
func TestIntegrationInboundAcceptAndEcho(t *testing.T) {
	t.Parallel()
	daemon := newFakeDaemon(t, 1, "server")
	defer daemon.Close()

	// Extend the daemon with a custom hook: after Bind, emit an
	// AcceptedConn + a Recv so the client sees an inbound
	// connection carrying pre-delivered data.
	drv, err := Connect(daemon.SocketPath())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer drv.Close()

	ln, err := drv.Listen(context.Background(), 2000)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// Reach into the daemon and send an inbound notification
	// directly. The live-daemon equivalent is "some other peer
	// dialed us"; for the unit test we just simulate the
	// corresponding IPC frames.
	//
	// Layout: [opcode][2B local_port=2000][4B conn_id][6B remote_addr][2B remote_port]
	const inboundID uint32 = 0xABCDEF01
	var accept [1 + 2 + 4 + 6 + 2]byte
	accept[0] = byte(opAcceptedConn)
	binary.BigEndian.PutUint16(accept[1:3], 2000)
	binary.BigEndian.PutUint32(accept[3:7], inboundID)
	binary.BigEndian.PutUint16(accept[7:9], 0)
	binary.BigEndian.PutUint32(accept[9:13], 55)
	binary.BigEndian.PutUint16(accept[13:15], 3000)

	// Find the daemon's server-side conn for our client connection.
	// The fakeDaemon keeps this implicit, so we inject the frames
	// via a helper below that reuses the serve()-local writeMu
	// convention.
	daemonConn := daemon.injectTo(t, accept[:])

	// Also inject a Recv frame for that new conn_id.
	recvPayload := []byte("inbound hello")
	recv := make([]byte, 1+4+len(recvPayload))
	recv[0] = byte(opRecv)
	binary.BigEndian.PutUint32(recv[1:5], inboundID)
	copy(recv[5:], recvPayload)
	daemon.injectOn(t, daemonConn, recv)

	acceptCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := ln.Accept(acceptCtx)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 32)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "inbound hello" {
		t.Fatalf("Read = %q, want %q", buf[:n], "inbound hello")
	}
}

// injectTo sends one framed payload to whatever client is currently
// connected to the fakeDaemon. Returns the daemon-side conn handle so
// subsequent injectOn calls can write to the same socket.
//
// This exists because the normal serve() path is response-driven
// (the daemon only writes in reply to a client command). Our inbound
// test needs to originate a frame.
func (d *fakeDaemon) injectTo(t *testing.T, frame []byte) net.Conn {
	t.Helper()
	c := d.waitClient(t)
	d.injectOn(t, c, frame)
	return c
}

func (d *fakeDaemon) injectOn(t *testing.T, c net.Conn, frame []byte) {
	t.Helper()
	d.injectMu.Lock()
	defer d.injectMu.Unlock()
	if err := writeFrame(c, frame); err != nil {
		t.Fatalf("inject writeFrame: %v", err)
	}
}

func (d *fakeDaemon) waitClient(t *testing.T) net.Conn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		d.clientMu.Lock()
		c := d.client
		d.clientMu.Unlock()
		if c != nil {
			return c
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("fakeDaemon: no client connected")
	return nil
}
