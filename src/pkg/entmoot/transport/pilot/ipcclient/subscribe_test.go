package ipcclient

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"
)

// encodeNotifyFrame is a test helper that builds a wire-format
// SubscribeOK / Notify payload (sans leading opcode):
//
//	[topic_len:uint16][topic][payload_len:uint32][payload]
//
// Tests use it to drive the mock server side of the pipe.
func encodeNotifyFrame(opcode Opcode, topic string, payload []byte) []byte {
	out := make([]byte, 1+2+len(topic)+4+len(payload))
	out[0] = byte(opcode)
	binary.BigEndian.PutUint16(out[1:3], uint16(len(topic)))
	copy(out[3:3+len(topic)], topic)
	binary.BigEndian.PutUint32(out[3+len(topic):7+len(topic)], uint32(len(payload)))
	copy(out[7+len(topic):], payload)
	return out
}

// TestSubscribe_RoundTripDeliversInitialAndNotify validates the
// happy path: Subscribe receives initial-state in SubscribeOK; the
// returned Subscription's Events channel receives subsequent Notify
// frames; closing the Subscription stops delivery.
func TestSubscribe_RoundTripDeliversInitialAndNotify(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Mock daemon: read Subscribe frame, reply SubscribeOK with
	// initial snapshot, then push two Notify frames.
	go func() {
		frame, err := srv.readFrame()
		if err != nil {
			t.Logf("server read: %v", err)
			return
		}
		if Opcode(frame[0]) != opSubscribe {
			t.Errorf("opcode = 0x%02x, want opSubscribe 0x30", frame[0])
			return
		}
		// Verify wire shape: [opcode][topic_len:uint16][topic]
		tn := binary.BigEndian.Uint16(frame[1:3])
		topic := string(frame[3 : 3+tn])
		if topic != "turn_endpoint" {
			t.Errorf("server saw topic %q, want turn_endpoint", topic)
		}
		// Reply SubscribeOK with initial value.
		srv.writeFrame(encodeNotifyFrame(opSubscribeOK, "turn_endpoint", []byte("104.30.x.x:INITIAL")))
		// Two Notify pushes.
		srv.writeFrame(encodeNotifyFrame(opNotify, "turn_endpoint", []byte("104.30.x.x:ROTATED1")))
		srv.writeFrame(encodeNotifyFrame(opNotify, "turn_endpoint", []byte("104.30.x.x:ROTATED2")))
	}()

	initial, sub, err := drv.Subscribe(context.Background(), "turn_endpoint")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	if string(initial) != "104.30.x.x:INITIAL" {
		t.Fatalf("initial snapshot = %q, want 104.30.x.x:INITIAL", string(initial))
	}

	for i, want := range []string{"104.30.x.x:ROTATED1", "104.30.x.x:ROTATED2"} {
		select {
		case n := <-sub.Events():
			if n.Topic != "turn_endpoint" {
				t.Fatalf("notification %d topic = %q", i, n.Topic)
			}
			if string(n.Payload) != want {
				t.Fatalf("notification %d payload = %q, want %q", i, string(n.Payload), want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("notification %d never received", i)
		}
	}
}

// TestSubscribe_FallsBackOnUnknownCommand: when the daemon replies
// CmdError "unknown command: 0x30" (pre-jf.11b pilot), Subscribe
// returns ErrSubscribeUnsupported so the caller can branch into a
// legacy polling fallback path.
func TestSubscribe_FallsBackOnUnknownCommand(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	go func() {
		frame, err := srv.readFrame()
		if err != nil || Opcode(frame[0]) != opSubscribe {
			return
		}
		// Mock pilot's sendError shape:
		//   [opError(1)][code(2)][message...]
		errMsg := "unknown command: 0x30"
		errFrame := make([]byte, 1+2+len(errMsg))
		errFrame[0] = byte(opError)
		binary.BigEndian.PutUint16(errFrame[1:3], 1)
		copy(errFrame[3:], errMsg)
		srv.writeFrame(errFrame)
	}()

	_, _, err := drv.Subscribe(context.Background(), "turn_endpoint")
	if err == nil {
		t.Fatalf("Subscribe succeeded; want ErrSubscribeUnsupported")
	}
	if !errors.Is(err, ErrSubscribeUnsupported) {
		t.Fatalf("error %q is not ErrSubscribeUnsupported (caller's "+
			"errors.Is branch can't fall back to legacy polling)", err)
	}

	// The Subscription should NOT be left registered after the fall-back.
	drv.subsMu.Lock()
	n := len(drv.topicSubs["turn_endpoint"])
	drv.subsMu.Unlock()
	if n != 0 {
		t.Fatalf("topicSubs[turn_endpoint] has %d entries after failed Subscribe; want 0", n)
	}
}

// TestSubscribe_BufferOverflowDropsOldest exercises the
// subscriptionBufferSize guard. Push more notifications than the
// buffer can hold without reading; verify the oldest are silently
// dropped (not blocking the demuxer) and the most recent are
// preserved. Live impact: if entmoot's TURN-rotation handler is
// slow / wedged, pilot's CmdNotify writes can't deadlock the
// demuxer.
func TestSubscribe_BufferOverflowDropsOldest(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	go func() {
		// Read Subscribe.
		_, err := srv.readFrame()
		if err != nil {
			return
		}
		// Reply SubscribeOK.
		srv.writeFrame(encodeNotifyFrame(opSubscribeOK, "t", []byte("init")))
		// Push 3× the buffer size of Notify frames without giving
		// the test a chance to drain. Drop-oldest should keep the
		// latest subscriptionBufferSize values.
		for i := 0; i < 3*subscriptionBufferSize; i++ {
			payload := []byte{byte(i)}
			srv.writeFrame(encodeNotifyFrame(opNotify, "t", payload))
		}
	}()

	_, sub, err := drv.Subscribe(context.Background(), "t")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	// Give the demuxer a moment to enqueue all pushes.
	time.Sleep(150 * time.Millisecond)

	// Drain whatever's in the channel. With drop-oldest semantics
	// we expect exactly subscriptionBufferSize entries, all from
	// the upper end of the producer's range.
	got := []byte{}
	deadline := time.After(500 * time.Millisecond)
drain:
	for {
		select {
		case n, ok := <-sub.Events():
			if !ok {
				break drain
			}
			if len(n.Payload) != 1 {
				t.Fatalf("payload len = %d, want 1", len(n.Payload))
			}
			got = append(got, n.Payload[0])
		case <-deadline:
			break drain
		case <-time.After(50 * time.Millisecond):
			break drain
		}
	}

	if len(got) > subscriptionBufferSize {
		t.Fatalf("drained %d notifications; want at most %d (buffer cap)",
			len(got), subscriptionBufferSize)
	}
	if len(got) == 0 {
		t.Fatalf("drained 0 notifications; expected some after %d producer pushes",
			3*subscriptionBufferSize)
	}
	// The latest pushes should win — first byte we drained should
	// be at least subscriptionBufferSize (i.e. earlier ones got
	// dropped). Concrete bound: the first kept value is at least
	// 3*sz - sz = 2*sz, but races with the drop-during-enqueue
	// path can shift this. We assert the looser invariant: NO
	// values from the very first batch (0..sz-1) survive.
	for _, v := range got {
		if int(v) < subscriptionBufferSize {
			t.Fatalf("kept value %d is from the first batch (< %d); drop-oldest didn't fire",
				v, subscriptionBufferSize)
		}
	}
}

// TestSubscribe_CloseStopsDelivery: after Subscription.Close, no
// further Notifications appear on Events. Idempotent.
func TestSubscribe_CloseStopsDelivery(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Synchronisation: server waits for a signal before pushing
	// the second Notify, so we can Close in between.
	push2 := make(chan struct{})

	go func() {
		_, err := srv.readFrame()
		if err != nil {
			return
		}
		srv.writeFrame(encodeNotifyFrame(opSubscribeOK, "t", nil))
		srv.writeFrame(encodeNotifyFrame(opNotify, "t", []byte("first")))

		// Wait for the test to close the subscription, then push.
		<-push2
		// Read the Unsubscribe frame from Close (best-effort RPC)
		// and reply UnsubscribeOK so Subscription.Close doesn't
		// block on the timeout.
		go func() {
			_, _ = srv.readFrame()
			srv.writeFrame([]byte{byte(opUnsubscribeOK)})
		}()
		// Even if pilot still sends a Notify here (race in real
		// life), the Subscription must have a closed channel so
		// the receive ends cleanly.
		srv.writeFrame(encodeNotifyFrame(opNotify, "t", []byte("after-close")))
	}()

	_, sub, err := drv.Subscribe(context.Background(), "t")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Drain "first".
	select {
	case n := <-sub.Events():
		if string(n.Payload) != "first" {
			t.Fatalf("first payload = %q, want first", string(n.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("never received first notification")
	}

	// Close. Trigger the second push afterwards.
	if err := sub.Close(); err != nil {
		t.Fatalf("sub.Close: %v", err)
	}
	close(push2)

	// Drain whatever remains. The channel must close (Events read
	// returns ok=false) and the late "after-close" payload must
	// not be observed via the subscription handle. It's fine if
	// the demuxer drops it silently.
	deadline := time.After(500 * time.Millisecond)
drain:
	for {
		select {
		case _, ok := <-sub.Events():
			if !ok {
				break drain
			}
			t.Fatalf("received notification after Close; want channel closed or silent drop")
		case <-deadline:
			break drain
		}
	}

	// Idempotent re-close.
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

// TestParseNotifyPayload covers the wire-shape decoder directly.
// Cheap regression guard for off-by-one bugs in the variable-length
// envelope.
func TestParseNotifyPayload(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		input    []byte
		ok       bool
		wantTop  string
		wantBody string
	}{
		{
			name:     "well-formed",
			input:    encodeNotifyFrame(opNotify, "t", []byte("payload"))[1:], // strip opcode
			ok:       true,
			wantTop:  "t",
			wantBody: "payload",
		},
		{
			name:  "truncated-header",
			input: []byte{0x00, 0x05, 'a'}, // claims topic_len=5 but only 1 byte
			ok:    false,
		},
		{
			name:  "missing-payload-len",
			input: []byte{0x00, 0x01, 't'}, // topic ok, no payload_len bytes
			ok:    false,
		},
		{
			name:  "truncated-payload",
			input: []byte{0x00, 0x01, 't', 0x00, 0x00, 0x00, 0x05, 'a'}, // claims 5, has 1
			ok:    false,
		},
		{
			name: "empty-payload",
			input: append(
				[]byte{0x00, 0x01, 't'},
				[]byte{0x00, 0x00, 0x00, 0x00}...,
			),
			ok:       true,
			wantTop:  "t",
			wantBody: "",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			topic, body, ok := parseNotifyPayload(tc.input)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !tc.ok {
				return
			}
			if topic != tc.wantTop {
				t.Fatalf("topic = %q, want %q", topic, tc.wantTop)
			}
			if string(body) != tc.wantBody {
				t.Fatalf("body = %q, want %q", string(body), tc.wantBody)
			}
		})
	}
}

// TestRouteNotify_FansOutToMultipleSubscriptions: routeNotify
// delivers to every subscription registered for a topic. Live
// impact: a single entmoot process could (in theory) Subscribe
// twice to the same topic if two separate subsystems both need
// rotation events; both must receive the push.
func TestRouteNotify_FansOutToMultipleSubscriptions(t *testing.T) {
	t.Parallel()
	drv, _, cleanup := newTestDriver(t)
	defer cleanup()

	sub1 := &Subscription{drv: drv, topic: "t", eventCh: make(chan Notification, subscriptionBufferSize)}
	sub2 := &Subscription{drv: drv, topic: "t", eventCh: make(chan Notification, subscriptionBufferSize)}
	drv.subsMu.Lock()
	drv.topicSubs["t"] = []*Subscription{sub1, sub2}
	drv.subsMu.Unlock()

	drv.routeNotify("t", []byte("hello"))

	for i, sub := range []*Subscription{sub1, sub2} {
		select {
		case n := <-sub.Events():
			if string(n.Payload) != "hello" {
				t.Fatalf("sub %d payload = %q", i+1, string(n.Payload))
			}
		case <-time.After(time.Second):
			t.Fatalf("sub %d never received notification", i+1)
		}
	}
}

// TestSubscribe_ConcurrentSafe: concurrent Subscribes and Closes on
// the same Driver under -race shouldn't trip on topicSubs map
// access or subscription channel sends.
func TestSubscribe_ConcurrentSafe(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Mock server: reply SubscribeOK to every Subscribe; reply
	// UnsubscribeOK to every Unsubscribe.
	go func() {
		for {
			frame, err := srv.readFrame()
			if err != nil {
				return
			}
			switch Opcode(frame[0]) {
			case opSubscribe:
				tn := binary.BigEndian.Uint16(frame[1:3])
				topic := string(frame[3 : 3+tn])
				srv.writeFrame(encodeNotifyFrame(opSubscribeOK, topic, nil))
			case opUnsubscribe:
				srv.writeFrame([]byte{byte(opUnsubscribeOK)})
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, sub, err := drv.Subscribe(ctx, "topic")
			if err != nil {
				t.Errorf("Subscribe %d: %v", i, err)
				return
			}
			// Push a notify and close.
			drv.routeNotify("topic", []byte("x"))
			_ = sub.Close()
		}(i)
	}
	wg.Wait()
}
