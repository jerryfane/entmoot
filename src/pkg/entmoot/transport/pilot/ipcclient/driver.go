package ipcclient

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// DefaultSocketPath is the Unix-domain socket Pilot's daemon listens
// on by default. SPEC.md §6 does not mandate a path; this matches the
// daemon's documented out-of-the-box default.
const DefaultSocketPath = "/tmp/pilot.sock"

// Driver is a connected Pilot IPC client. It owns one Unix-domain
// socket to the daemon, a demuxer goroutine that reads frames off
// that socket, a writer mutex that serializes outbound frames, and
// routing state for open virtual connections + listeners + in-flight
// command replies.
//
// All methods are safe for concurrent use. Close is idempotent.
type Driver struct {
	socketPath string
	conn       net.Conn

	// writeMu serializes outbound frames across all goroutines (app
	// Write calls on Conns, plus the command-issuing paths below).
	// Without this, concurrent writes would interleave bytes on the
	// socket and the daemon would desync.
	writeMu sync.Mutex

	// connsMu guards conns, pendingRecv, and pendingClose; it is distinct from
	// writeMu because the demuxer has to route Recv frames while
	// Write goroutines are busy emitting Send frames.
	connsMu sync.Mutex
	conns   map[uint32]*pilotConn
	// pendingRecv buffers Recv payloads that arrive between the
	// daemon assigning a conn_id (in DialOK or AcceptedConn) and the
	// client's registerConn call. Without this buffer the small
	// window between "demuxer delivers DialOK to the DialAddr
	// goroutine" and "DialAddr calls registerConn" drops any Recv
	// frames the daemon happens to emit in the same socket burst.
	// Entries are drained into the conn's own channel when
	// registerConn fires.
	pendingRecv map[uint32][][]byte
	// pendingClose remembers CloseOK frames that arrive in the same
	// DialOK/AcceptedConn race window. Without it, a peer that accepts
	// and closes immediately can leave the caller with a registered conn
	// whose recv channel never closes.
	pendingClose map[uint32]struct{}

	// listenersMu guards listeners.
	listenersMu sync.RWMutex
	listeners   map[uint16]*Listener

	// subsMu guards topicSubs. topicSubs maps a topic name to the
	// Subscriptions registered for it. Notify frames pushed by
	// pilot are routed via topicSubs and bypass the request-reply
	// `pending` FIFO entirely (server-pushed, same model as
	// opAcceptedConn). A topic can have multiple Subscriptions if
	// the application Subscribed more than once to the same topic;
	// every Subscription receives every Notification. (v1.5.0)
	subsMu    sync.Mutex
	topicSubs map[string][]*Subscription

	// pendingMu guards pending and pendingSeq. pending is a SINGLE
	// global FIFO of in-flight commands ordered by issue time
	// (v1.4.6). Each entry carries its expected response opcode so
	// successful replies still match by opcode (deliverPending walks
	// the slice and picks the first entry whose op matches), while
	// untagged Error frames route to the oldest entry regardless of
	// opcode (deliverError pops index 0).
	//
	// Why a global FIFO instead of a per-opcode map: the daemon's
	// Error frame carries no correlation back to the command that
	// caused it (just code+message), so when multiple opcodes have
	// concurrent waiters — which v1.4.4's TURN-endpoint poller
	// introduced by running InfoStruct alongside gossip Dial/Send —
	// a per-opcode map can only "guess" which waiter to deliver the
	// error to, and Go map iteration is randomised, so the guess is
	// arbitrary. The result was Info calls receiving Pilot's
	// "dial timeout" errors and vice versa. Live evidence
	// 2026-04-25: every poll on phobos failed at 3 s with errors
	// like `ipcclient: info: ipcclient: daemon: dial timeout`.
	// FIFO ordering matches the daemon's actual reply order on a
	// single socket (replies come in command-issue order), so an
	// untagged Error always belongs to the head-of-queue.
	pendingMu  sync.Mutex
	pending    []pendingEntry
	pendingSeq uint64 // monotonic, debug only

	// closeOnce + closedCh serialize shutdown.
	closeOnce sync.Once
	closedCh  chan struct{}
	// demuxDone is closed when the demuxer goroutine exits so Close
	// can join it deterministically.
	demuxDone chan struct{}

	// demuxErr is set by the demuxer on exit to describe why it
	// stopped reading frames. Typically io.EOF on clean shutdown or
	// a net.OpError on forced socket close.
	demuxErrMu sync.Mutex
	demuxErr   error
}

// pendingReply carries the response payload or an IPC-level error
// (Error frame 0x0A) back to a sendAndWait caller.
type pendingReply struct {
	payload []byte
	err     error
}

// pendingEntry tracks one in-flight command in the Driver's global
// FIFO. The expected response opcode is recorded so deliverPending
// can match successful replies by opcode (via a linear scan — the
// queue is rarely deeper than a handful of entries), while the
// FIFO ordering itself lets deliverError route untagged Error
// frames to the oldest in-flight command regardless of opcode.
//
// seq is informational; ordering is implicit in slice index.
// (v1.4.6)
type pendingEntry struct {
	op  Opcode
	ch  chan pendingReply
	seq uint64
}

// Connect dials the Unix-domain socket at socketPath and starts the
// demuxer goroutine. If socketPath is empty, DefaultSocketPath is used.
func Connect(socketPath string) (*Driver, error) {
	if socketPath == "" {
		socketPath = DefaultSocketPath
	}
	c, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("ipcclient: dial %q: %w", socketPath, err)
	}
	d := &Driver{
		socketPath:   socketPath,
		conn:         c,
		conns:        make(map[uint32]*pilotConn),
		pendingRecv:  make(map[uint32][][]byte),
		pendingClose: make(map[uint32]struct{}),
		listeners:    make(map[uint16]*Listener),
		topicSubs:    make(map[string][]*Subscription),
		closedCh:     make(chan struct{}),
		demuxDone:    make(chan struct{}),
	}
	go d.demux()
	return d, nil
}

// SocketPath returns the Unix socket path the driver is connected to.
// Exposed mainly for diagnostics and test harnesses.
func (d *Driver) SocketPath() string { return d.socketPath }

// Close tears down the driver: closes the socket (which unblocks the
// demuxer and any blocked Read on a Conn), waits for the demuxer to
// exit, and fails any pending replies with ErrClosed. Safe to call
// multiple times; second and subsequent calls are no-ops that return
// nil.
func (d *Driver) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.closedCh)
		// Closing the socket is what actually wakes the demuxer's
		// blocked readFrame; do it before joining.
		if cerr := d.conn.Close(); cerr != nil {
			err = cerr
		}
		<-d.demuxDone

		// Fail every pending reply so command goroutines don't block
		// forever.
		d.pendingMu.Lock()
		for _, e := range d.pending {
			select {
			case e.ch <- pendingReply{err: ErrClosed}:
			default:
			}
		}
		d.pending = nil
		d.pendingMu.Unlock()

		// v1.5.0: close every Subscription so callers blocked on
		// Events() see the channel close (== EOF on the topic). We
		// don't try to send Unsubscribe here — pilot's connection
		// is also tearing down.
		d.subsMu.Lock()
		closing := make([]*Subscription, 0)
		for _, subs := range d.topicSubs {
			closing = append(closing, subs...)
		}
		d.topicSubs = make(map[string][]*Subscription)
		d.subsMu.Unlock()
		for _, sub := range closing {
			sub.closeMu.Lock()
			if !sub.closed {
				sub.closed = true
				close(sub.eventCh)
			}
			sub.closeMu.Unlock()
		}

		// Close every conn's recv channel so any blocked Read returns
		// io.EOF — it would otherwise block on the channel forever.
		d.connsMu.Lock()
		for id, c := range d.conns {
			c.closeRecv()
			delete(d.conns, id)
		}
		for id := range d.pendingRecv {
			delete(d.pendingRecv, id)
		}
		for id := range d.pendingClose {
			delete(d.pendingClose, id)
		}
		d.connsMu.Unlock()

		// Close each listener so any blocked Accept returns ErrClosed.
		d.listenersMu.Lock()
		for p, l := range d.listeners {
			l.closeOnce.Do(func() { close(l.closed) })
			delete(d.listeners, p)
		}
		d.listenersMu.Unlock()
	})
	return err
}

// writeFrame serializes a single outbound frame onto the socket. Used
// by Conn.Write, Conn.Close, and the command-issuing paths.
func (d *Driver) writeFrame(payload []byte) error {
	select {
	case <-d.closedCh:
		return ErrClosed
	default:
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	return writeFrame(d.conn, payload)
}

// registerConn adds a conn to the routing map so the demuxer delivers
// its Recv frames. Called by DialAddr (after DialOK) and Listener's
// accept path. If any Recv frames were buffered while the conn_id was
// still unregistered, they're delivered here in order before the
// method returns, so the caller can hand the Conn to the user without
// worrying about dropped early data.
func (d *Driver) registerConn(id uint32, c *pilotConn) {
	d.connsMu.Lock()
	pending := d.pendingRecv[id]
	delete(d.pendingRecv, id)
	_, closed := d.pendingClose[id]
	delete(d.pendingClose, id)
	if !closed {
		d.conns[id] = c
	}
	d.connsMu.Unlock()
	for _, data := range pending {
		c.pushRecv(data)
	}
	if closed {
		c.closeRecv()
	}
}

// unregisterConn removes a conn from the routing map. Called by
// Conn.Close; later Recv frames for that conn_id (shouldn't happen
// but defensively) are dropped by the demuxer.
func (d *Driver) unregisterConn(id uint32) {
	d.connsMu.Lock()
	delete(d.conns, id)
	delete(d.pendingRecv, id)
	delete(d.pendingClose, id)
	d.connsMu.Unlock()
}

// registerListener adds a listener to the port routing map. Called
// internally by Listen after BindOK; listeners receive the AcceptedConn
// frames whose bound-port prefix matches their port.
func (d *Driver) registerListener(port uint16, l *Listener) {
	d.listenersMu.Lock()
	d.listeners[port] = l
	d.listenersMu.Unlock()
}

// unregisterListener removes a listener from the port routing map.
// Called by Listener.Close.
func (d *Driver) unregisterListener(port uint16) {
	d.listenersMu.Lock()
	delete(d.listeners, port)
	d.listenersMu.Unlock()
}

// demux is the package's single reader goroutine. It pulls frames off
// the socket, switches on opcode, and routes each to the right
// destination (Conn, Listener, pending command reply). Exits when
// readFrame returns io.EOF (daemon closed) or any other error (socket
// broken or driver.Close closed the conn).
func (d *Driver) demux() {
	defer close(d.demuxDone)
	for {
		frame, err := readFrame(d.conn)
		if err != nil {
			d.demuxErrMu.Lock()
			d.demuxErr = err
			d.demuxErrMu.Unlock()
			return
		}
		if len(frame) < 1 {
			// Zero-length frame carries no opcode; skip rather than
			// desync the stream.
			continue
		}
		op := Opcode(frame[0])
		payload := frame[1:]

		switch op {
		case opRecv:
			// [4B conn_id][N data]
			if len(payload) < 4 {
				continue
			}
			id := binary.BigEndian.Uint32(payload[0:4])
			data := payload[4:]
			d.connsMu.Lock()
			c, ok := d.conns[id]
			if !ok {
				// Unknown conn_id — may be a conn we just dialed
				// whose registerConn hasn't run yet, or a conn we
				// already closed. Buffer the data so a pending
				// register call will drain it. If nothing ever
				// registers, the buffer will be freed when the
				// driver closes (which frees the whole map).
				// Copy the data since the demuxer reuses the
				// underlying slice.
				cp := make([]byte, len(data))
				copy(cp, data)
				d.pendingRecv[id] = append(d.pendingRecv[id], cp)
			}
			d.connsMu.Unlock()
			if ok {
				c.pushRecv(data)
			}
		case opAcceptedConn:
			// [2B local_port][4B conn_id][6B remote_addr][2B remote_port]
			if len(payload) < 2 {
				continue
			}
			port := binary.BigEndian.Uint16(payload[0:2])
			rest := payload[2:]
			d.listenersMu.RLock()
			l, ok := d.listeners[port]
			d.listenersMu.RUnlock()
			if ok {
				l.pushAccept(rest)
			}
		case opError:
			// Pair with the oldest pending command of any opcode: the
			// daemon replies in issue-order on a single socket, so the
			// error belongs to whichever command is at the head of
			// *some* queue. We can't know which opcode until we look;
			// route to the longest queue's head (which is the oldest
			// in-flight regardless of expected opcode) as a
			// best-effort. In practice Entmoot's usage serializes
			// commands anyway, so there is exactly one in-flight at a
			// time.
			code := uint16(0)
			msg := ""
			if len(payload) >= 2 {
				code = binary.BigEndian.Uint16(payload[0:2])
				msg = string(payload[2:])
			}
			d.deliverError(&IPCError{Code: code, Message: msg})
		case opNotify:
			// v1.5.0 / pilot jf.11b: server-pushed state-change
			// notification. Payload:
			//   [topic_len:uint16][topic][payload_len:uint32][payload]
			// Notify frames bypass the request-reply pending FIFO
			// entirely — they're routed via topicSubs to whichever
			// Subscriptions are registered for the topic.
			if topic, body, ok := parseNotifyPayload(payload); ok {
				d.routeNotify(topic, body)
			}
		case opCloseOK:
			// Daemon-initiated close confirmation. Payload: [4B conn_id].
			// We mirror the app-initiated-close bookkeeping: close the
			// recv chan so any blocked Read returns EOF.
			if len(payload) >= 4 {
				id := binary.BigEndian.Uint32(payload[0:4])
				d.connsMu.Lock()
				c, ok := d.conns[id]
				if ok {
					delete(d.conns, id)
				} else {
					d.pendingClose[id] = struct{}{}
				}
				d.connsMu.Unlock()
				if ok {
					c.closeRecv()
				}
			}
			// Also deliver to any pending command that waits on
			// opCloseOK (none in this package today, but spec
			// compliance).
			d.deliverPending(op, payload, nil)
		default:
			// Everything else is a command response (BindOK, DialOK,
			// InfoOK, HandshakeOK, SetPeerEndpointsOK, ...) — route
			// through the pending-reply FIFO.
			d.deliverPending(op, payload, nil)
		}
	}
}

// deliverPending hands a response payload (or an error) to the oldest
// waiter for the given opcode. A response with no waiter is dropped —
// that shouldn't happen in practice (we only wait for opcodes we know
// are replies to commands we issued) but we defend against it rather
// than leak a goroutine.
//
// v1.4.6: walks the global FIFO and picks the first entry whose op
// matches. The queue is rarely deeper than a handful, so the linear
// scan is cheap; the upside is that error-routing in deliverError
// can be FIFO-correct across all opcodes.
func (d *Driver) deliverPending(op Opcode, payload []byte, err error) {
	d.pendingMu.Lock()
	idx := -1
	for i, e := range d.pending {
		if e.op == op {
			idx = i
			break
		}
	}
	if idx < 0 {
		d.pendingMu.Unlock()
		return
	}
	ch := d.pending[idx].ch
	d.pending = append(d.pending[:idx], d.pending[idx+1:]...)
	d.pendingMu.Unlock()
	select {
	case ch <- pendingReply{payload: payload, err: err}:
	default:
		// The caller already gave up (context cancelled) — drop.
	}
}

// deliverError hands an IPCError to the oldest in-flight command,
// regardless of opcode. The daemon's Error frame (0x0A) is untagged —
// it carries no correlation ID back to the command that caused it —
// so we route by FIFO order, which matches the daemon's actual reply
// order on a single socket. Does nothing if no commands are pending.
//
// v1.4.6: pre-fix, this picked from a randomised map iteration over
// per-opcode queues, which mis-routed errors when concurrent
// opcodes had waiters (the bug v1.4.4's poller surfaced).
func (d *Driver) deliverError(ipcErr *IPCError) {
	d.pendingMu.Lock()
	if len(d.pending) == 0 {
		d.pendingMu.Unlock()
		return
	}
	ch := d.pending[0].ch
	d.pending = d.pending[1:]
	d.pendingMu.Unlock()
	select {
	case ch <- pendingReply{err: ipcErr}:
	default:
	}
}

// sendAndWait emits a command frame and blocks until the matching
// response opcode arrives, ctx is cancelled, or the driver closes.
// If the daemon sends an Error frame while this command is in flight,
// it is returned as an *IPCError.
func (d *Driver) sendAndWait(ctx context.Context, frame []byte, want Opcode) ([]byte, error) {
	select {
	case <-d.closedCh:
		return nil, ErrClosed
	default:
	}
	ch := make(chan pendingReply, 1)

	// Register before write, so if the daemon replies instantly the
	// demuxer finds us waiting. v1.4.6: enrolled in a single global
	// FIFO so error-routing is correct across concurrent opcodes.
	d.pendingMu.Lock()
	d.pendingSeq++
	d.pending = append(d.pending, pendingEntry{op: want, ch: ch, seq: d.pendingSeq})
	d.pendingMu.Unlock()

	if err := d.writeFrame(frame); err != nil {
		d.removePending(want, ch)
		return nil, err
	}

	select {
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return r.payload, nil
	case <-ctx.Done():
		d.removePending(want, ch)
		return nil, ctx.Err()
	case <-d.closedCh:
		d.removePending(want, ch)
		return nil, ErrClosed
	}
}

// removePending removes ch from the pending queue for op, if still
// present. Called by sendAndWait on cancellation/close to prevent a
// late reply from wedging the demuxer on a full channel.
//
// v1.4.6: walks the global FIFO and removes by (op, ch) match.
func (d *Driver) removePending(op Opcode, ch chan pendingReply) {
	d.pendingMu.Lock()
	defer d.pendingMu.Unlock()
	for i, e := range d.pending {
		if e.op == op && e.ch == ch {
			d.pending = append(d.pending[:i], d.pending[i+1:]...)
			return
		}
	}
}

// Info issues an Info (0x0D) command and returns the InfoOK JSON
// body parsed as a map. The caller can extract node_id, hostname, and
// any other daemon-reported fields.
func (d *Driver) Info(ctx context.Context) (map[string]interface{}, error) {
	frame := []byte{byte(opInfo)}
	resp, err := d.sendAndWait(ctx, frame, opInfoOK)
	if err != nil {
		return nil, fmt.Errorf("ipcclient: info: %w", err)
	}
	if len(resp) == 0 {
		return map[string]interface{}{}, nil
	}
	var out map[string]interface{}
	if err := json.Unmarshal(resp, &out); err != nil {
		return nil, fmt.Errorf("ipcclient: info: decode: %w", err)
	}
	return out, nil
}

// InfoStruct issues an Info (0x0D) command and returns the InfoOK
// JSON body decoded into the typed Info struct. Added in v1.4.0 so
// callers that need the TURNEndpoint field (or any other typed
// access) can avoid the string-keyed map shape. Forward-compat:
// unknown fields on the daemon side are silently dropped by
// encoding/json. Backwards-compat: a jf.7 daemon that omits
// turn_endpoint decodes into Info{TURNEndpoint: ""}.
func (d *Driver) InfoStruct(ctx context.Context) (Info, error) {
	frame := []byte{byte(opInfo)}
	resp, err := d.sendAndWait(ctx, frame, opInfoOK)
	if err != nil {
		return Info{}, fmt.Errorf("ipcclient: info: %w", err)
	}
	if len(resp) == 0 {
		return Info{}, nil
	}
	var out Info
	if err := json.Unmarshal(resp, &out); err != nil {
		return Info{}, fmt.Errorf("ipcclient: info: decode: %w", err)
	}
	return out, nil
}

// Subscribe registers a topic listener with pilot. Returns the
// initial-state snapshot delivered in opSubscribeOK plus a
// Subscription whose Events() channel receives subsequent opNotify
// frames. The Subscription must be closed when no longer needed.
//
// Backwards compat: pilot < v1.9.0-jf.11b doesn't know opSubscribe
// and replies with an opError carrying "unknown command: 0x30".
// Callers (e.g. the entmootd TURN-rotation poller) should detect
// this with errors.Is(err, ErrSubscribeUnsupported) and fall back
// to polling.
//
// (v1.5.0)
func (d *Driver) Subscribe(ctx context.Context, topic string) ([]byte, *Subscription, error) {
	if topic == "" {
		return nil, nil, fmt.Errorf("ipcclient: subscribe: empty topic")
	}
	if len(topic) > 0xFFFF {
		return nil, nil, fmt.Errorf("ipcclient: subscribe: topic too long (%d bytes; max 65535)", len(topic))
	}

	// Pre-register the Subscription BEFORE writing the frame, so a
	// Notify pushed by pilot between SubscribeOK delivery and our
	// returning the Subscription is still routed correctly. (Mirror
	// of pilot-side register-before-reply ordering.)
	sub := &Subscription{
		drv:     d,
		topic:   topic,
		eventCh: make(chan Notification, subscriptionBufferSize),
	}
	d.subsMu.Lock()
	d.topicSubs[topic] = append(d.topicSubs[topic], sub)
	d.subsMu.Unlock()

	frame := make([]byte, 1+2+len(topic))
	frame[0] = byte(opSubscribe)
	binary.BigEndian.PutUint16(frame[1:3], uint16(len(topic)))
	copy(frame[3:], topic)

	resp, err := d.sendAndWait(ctx, frame, opSubscribeOK)
	if err != nil {
		// Unwind the pre-registration on any error path.
		d.unregisterSubscription(sub)
		// Detect "unknown command" so callers can fall back. Pilot
		// sends an opError with code=1 message "unknown command: 0x30"
		// when the daemon predates jf.11b.
		if isUnknownCommandError(err, opSubscribe) {
			return nil, nil, fmt.Errorf("%w: %v", ErrSubscribeUnsupported, err)
		}
		return nil, nil, fmt.Errorf("ipcclient: subscribe: %w", err)
	}
	gotTopic, snapshot, ok := parseNotifyPayload(resp)
	if !ok || gotTopic != topic {
		d.unregisterSubscription(sub)
		return nil, nil, fmt.Errorf("ipcclient: subscribe: malformed SubscribeOK")
	}
	return snapshot, sub, nil
}

// sendUnsubscribe issues an opUnsubscribe + waits for opUnsubscribeOK.
// Best-effort; called from Subscription.Close. (v1.5.0)
func (d *Driver) sendUnsubscribe(topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	frame := make([]byte, 1+2+len(topic))
	frame[0] = byte(opUnsubscribe)
	binary.BigEndian.PutUint16(frame[1:3], uint16(len(topic)))
	copy(frame[3:], topic)
	_, err := d.sendAndWait(ctx, frame, opUnsubscribeOK)
	return err
}

// unregisterSubscription removes sub from topicSubs. Idempotent.
// (v1.5.0)
func (d *Driver) unregisterSubscription(sub *Subscription) {
	d.subsMu.Lock()
	defer d.subsMu.Unlock()
	subs := d.topicSubs[sub.topic]
	for i, s := range subs {
		if s == sub {
			d.topicSubs[sub.topic] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(d.topicSubs[sub.topic]) == 0 {
		delete(d.topicSubs, sub.topic)
	}
}

// routeNotify hands a parsed Notify frame to every Subscription
// registered for the topic. Each Subscription gets a defensively
// copied payload so the demuxer's frame buffer can be reused.
// (v1.5.0)
func (d *Driver) routeNotify(topic string, payload []byte) {
	d.subsMu.Lock()
	subs := append([]*Subscription(nil), d.topicSubs[topic]...)
	d.subsMu.Unlock()
	if len(subs) == 0 {
		return
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	n := Notification{Topic: topic, Payload: cp}
	for _, sub := range subs {
		sub.deliverNotification(n)
	}
}

// parseNotifyPayload decodes the wire format shared by opSubscribeOK
// and opNotify: [topic_len:uint16][topic][payload_len:uint32][payload].
// Returns ok=false on any structural mismatch. (v1.5.0)
func parseNotifyPayload(payload []byte) (topic string, body []byte, ok bool) {
	if len(payload) < 2+4 {
		return "", nil, false
	}
	tn := binary.BigEndian.Uint16(payload[0:2])
	if int(tn) > len(payload)-2-4 {
		return "", nil, false
	}
	topic = string(payload[2 : 2+tn])
	pn := binary.BigEndian.Uint32(payload[2+tn : 2+int(tn)+4])
	if int(pn) > len(payload)-int(2+tn)-4 {
		return "", nil, false
	}
	body = payload[2+int(tn)+4 : 2+int(tn)+4+int(pn)]
	return topic, body, true
}

// isUnknownCommandError returns true when err is an *IPCError whose
// message indicates the daemon doesn't recognise op. Used by
// Subscribe to surface ErrSubscribeUnsupported so callers can fall
// back to legacy polling. (v1.5.0)
func isUnknownCommandError(err error, op Opcode) bool {
	if err == nil {
		return false
	}
	var ipcErr *IPCError
	for e := err; e != nil; {
		if cast, ok := e.(*IPCError); ok {
			ipcErr = cast
			break
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := e.(unwrapper)
		if !ok {
			break
		}
		e = u.Unwrap()
	}
	if ipcErr == nil {
		return false
	}
	// Pilot's sendError emits "unknown command: 0x%02X" for unknown opcodes.
	want := fmt.Sprintf("unknown command: 0x%02X", byte(op))
	return strings.Contains(ipcErr.Message, want)
}

// Listen binds a virtual port. A requested port of 0 asks the daemon
// to pick one from the ephemeral range; Listener.Port reports the
// bound value. The returned Listener is registered with the driver's
// demuxer BEFORE the Bind command is sent so no AcceptedConn frame is
// missed — even if the daemon emits one in the same read burst as
// BindOK.
//
// If port is non-zero we know the daemon will bind exactly that port
// (or fail), so we can pre-register on it. If port is zero the daemon
// picks an ephemeral port; in that case we wait for BindOK, read the
// bound port, and then register — the daemon cannot emit an
// AcceptedConn for an ephemeral port before it tells us which one it
// chose, so there is no race window.
func (d *Driver) Listen(ctx context.Context, port uint16) (*Listener, error) {
	var preRegistered *Listener
	if port != 0 {
		preRegistered = newListener(d, port)
		d.registerListener(port, preRegistered)
	}
	frame := make([]byte, 3)
	frame[0] = byte(opBind)
	binary.BigEndian.PutUint16(frame[1:3], port)
	resp, err := d.sendAndWait(ctx, frame, opBindOK)
	if err != nil {
		if preRegistered != nil {
			d.unregisterListener(port)
		}
		return nil, fmt.Errorf("ipcclient: bind :%d: %w", port, err)
	}
	if len(resp) < 2 {
		if preRegistered != nil {
			d.unregisterListener(port)
		}
		return nil, fmt.Errorf("ipcclient: bind :%d: %w", port, ErrShortResponse)
	}
	bound := binary.BigEndian.Uint16(resp[0:2])
	if preRegistered != nil {
		if bound == port {
			return preRegistered, nil
		}
		// Daemon bound a different port than requested; surprising
		// but we handle it gracefully by relocating the registration.
		d.unregisterListener(port)
	}
	l := newListener(d, bound)
	d.registerListener(bound, l)
	return l, nil
}

// DialAddr opens a stream connection to dst:remotePort. On success the
// returned Conn is registered with the demuxer so incoming Recv frames
// are delivered immediately.
func (d *Driver) DialAddr(ctx context.Context, dst Addr, remotePort uint16) (*Conn, error) {
	frame := make([]byte, 1+addrSize+2)
	frame[0] = byte(opDial)
	binary.BigEndian.PutUint16(frame[1:3], dst.Network)
	binary.BigEndian.PutUint32(frame[3:7], dst.Node)
	binary.BigEndian.PutUint16(frame[7:9], remotePort)
	resp, err := d.sendAndWait(ctx, frame, opDialOK)
	if err != nil {
		return nil, fmt.Errorf("ipcclient: dial %s:%d: %w", dst, remotePort, err)
	}
	if len(resp) < 4 {
		return nil, fmt.Errorf("ipcclient: dial %s:%d: %w", dst, remotePort, ErrShortResponse)
	}
	id := binary.BigEndian.Uint32(resp[0:4])
	remote := SocketAddr{Addr: dst, Port: remotePort}
	c := newConn(d, id, SocketAddr{}, remote)
	d.registerConn(id, c)
	return c, nil
}

// TrustedPeers asks the daemon to enumerate the node IDs that have
// been accepted through the trust-handshake protocol. Returns the
// decoded JSON body of the Handshake(sub=trusted) response.
//
// The response shape is {"trusted": [{"node_id": <uint32>, ...}, ...]}.
// The surrounding object is returned verbatim so callers can pull any
// additional fields the daemon adds in future releases.
func (d *Driver) TrustedPeers(ctx context.Context) (map[string]interface{}, error) {
	frame := []byte{byte(opHandshake), subHandshakeTrusted}
	resp, err := d.sendAndWait(ctx, frame, opHandshakeOK)
	if err != nil {
		return nil, fmt.Errorf("ipcclient: trusted_peers: %w", err)
	}
	if len(resp) == 0 {
		return map[string]interface{}{}, nil
	}
	var out map[string]interface{}
	if err := json.Unmarshal(resp, &out); err != nil {
		return nil, fmt.Errorf("ipcclient: trusted_peers: decode: %w", err)
	}
	return out, nil
}

// SetPeerEndpoints installs externally-sourced transport endpoints for
// a peer into the daemon's peer-endpoint map. Called when the gossip
// layer accepts a transport-advertisement from another node.
//
// Per the daemon's envelope limits: at most 8 endpoints, each
// Network <= 16 bytes, each Addr <= 255 bytes. Frames are rejected
// client-side before hitting the socket so we return a clear error
// rather than a generic "daemon said no".
func (d *Driver) SetPeerEndpoints(ctx context.Context, nodeID uint32, endpoints []Endpoint) error {
	if len(endpoints) > 8 {
		return fmt.Errorf("ipcclient: set_peer_endpoints: %d endpoints exceeds daemon limit of 8", len(endpoints))
	}
	for i, ep := range endpoints {
		if len(ep.Network) > 16 {
			return fmt.Errorf("ipcclient: set_peer_endpoints: endpoint %d network %q exceeds 16 bytes", i, ep.Network)
		}
		if len(ep.Addr) > 255 {
			return fmt.Errorf("ipcclient: set_peer_endpoints: endpoint %d addr exceeds 255 bytes", i)
		}
	}
	// Size: opcode + nodeID + count + per-ep (1+netlen + 1+addrlen).
	size := 1 + 4 + 1
	for _, ep := range endpoints {
		size += 1 + len(ep.Network) + 1 + len(ep.Addr)
	}
	frame := make([]byte, size)
	frame[0] = byte(opSetPeerEndpoints)
	binary.BigEndian.PutUint32(frame[1:5], nodeID)
	frame[5] = byte(len(endpoints))
	off := 6
	for _, ep := range endpoints {
		frame[off] = byte(len(ep.Network))
		off++
		off += copy(frame[off:], ep.Network)
		frame[off] = byte(len(ep.Addr))
		off++
		off += copy(frame[off:], ep.Addr)
	}
	if _, err := d.sendAndWait(ctx, frame, opSetPeerEndpointsOK); err != nil {
		return fmt.Errorf("ipcclient: set_peer_endpoints: %w", err)
	}
	return nil
}

// DemuxError returns the reason the demuxer stopped reading, if it
// has stopped. Returns nil while the driver is operational. Useful in
// tests and diagnostic surfaces; production callers typically just
// observe ErrClosed on their next operation.
func (d *Driver) DemuxError() error {
	d.demuxErrMu.Lock()
	defer d.demuxErrMu.Unlock()
	if d.demuxErr == nil {
		return nil
	}
	if errors.Is(d.demuxErr, io.EOF) {
		return nil // clean shutdown, not a failure
	}
	var opErr *net.OpError
	if errors.As(d.demuxErr, &opErr) && errors.Is(opErr.Err, net.ErrClosed) {
		return nil // Close() closed the socket; expected
	}
	return d.demuxErr
}
