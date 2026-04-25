package ipcclient

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

// TestErrorRouting_FIFOAcrossOpcodes pins the v1.4.6 fix for the
// cross-opcode error misrouting bug. Pre-fix, Driver.deliverError
// picked from a randomised Go map iteration over per-opcode pending
// queues, so an Error frame from the daemon would land on a random
// waiter — for example, an Info call could receive an error meant
// for a Dial. v1.4.4's TURN-endpoint poller surfaced this in
// production: every poll on phobos failed at 3 s with
// `ipcclient: info: ipcclient: daemon: dial timeout` because the
// gossip layer's stuck Dial was returning an error frame that the
// random map iteration handed to the Info waiter.
//
// Post-v1.4.6, the pending queue is a single global FIFO and
// deliverError pops the head — which always corresponds to the
// command the daemon is actually replying to first (single-socket
// command-issue order is preserved by the daemon's own reply
// pipeline).
func TestErrorRouting_FIFOAcrossOpcodes(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	// Mock daemon: read both frames the client emits, then reply
	// with one Error and one InfoOK (matched in issue order).
	// Critically, both pending waiters are concurrent at the time
	// the Error is delivered — pre-fix this is where map iteration
	// randomisation could mis-route.
	serverReady := make(chan struct{})
	go func() {
		// First frame: Bind for port 9999.
		first, err := srv.readFrame()
		if err != nil {
			t.Logf("first read: %v", err)
			return
		}
		if Opcode(first[0]) != opBind {
			t.Errorf("first frame opcode = 0x%02x, want opBind 0x01", first[0])
		}
		// Second frame: Info.
		second, err := srv.readFrame()
		if err != nil {
			t.Logf("second read: %v", err)
			return
		}
		if Opcode(second[0]) != opInfo {
			t.Errorf("second frame opcode = 0x%02x, want opInfo 0x0D", second[0])
		}

		// Both waiters are now enrolled in the driver's pending
		// FIFO. Reply with: Error (belongs to Bind, the FIRST
		// caller) then InfoOK (belongs to Info).
		close(serverReady)
		errFrame := make([]byte, 1+2+len("bind failed: port in use"))
		errFrame[0] = byte(opError)
		binary.BigEndian.PutUint16(errFrame[1:3], 1)
		copy(errFrame[3:], "bind failed: port in use")
		srv.writeFrame(errFrame)

		infoBody := []byte(`{"node_id":42,"hostname":"test"}`)
		infoFrame := make([]byte, 1+len(infoBody))
		infoFrame[0] = byte(opInfoOK)
		copy(infoFrame[1:], infoBody)
		srv.writeFrame(infoFrame)
	}()

	// Issue Bind FIRST (this should receive the Error reply).
	bindResult := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := drv.Listen(context.Background(), 9999)
		bindResult <- err
	}()

	// Wait briefly so Bind is enrolled in the FIFO before Info is.
	// We can't observe enrollment directly, but the driver registers
	// pending under the lock before writing the frame, and the mock
	// server only reads the second frame after the first — so by the
	// time the first frame is read on the server, Bind is enrolled.
	// We add a tiny sleep purely as defence against goroutine
	// scheduling on slow machines; the assertion below is the
	// real test.
	time.Sleep(20 * time.Millisecond)

	// Issue Info SECOND.
	infoResult := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := drv.InfoStruct(context.Background())
		infoResult <- err
	}()

	// Bind must receive the Error.
	select {
	case err := <-bindResult:
		if err == nil {
			t.Fatalf("Bind succeeded; want error (the daemon's "+
				"Error frame was the FIRST reply and Bind was the "+
				"FIRST issuer — FIFO routing must hand the error to Bind)")
		}
		var ipcErr *IPCError
		if !asIPCError(err, &ipcErr) {
			t.Fatalf("Bind error type = %T (%v); want *IPCError", err, err)
		}
		if ipcErr.Message != "bind failed: port in use" {
			t.Fatalf("Bind error message = %q; want \"bind failed: port in use\"",
				ipcErr.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Bind never returned; pre-fix it could be wedged "+
			"if deliverError sent the IPCError to the Info waiter instead")
	}

	// Info must succeed (InfoOK was the second reply, it should
	// pair with the second issuer).
	select {
	case err := <-infoResult:
		if err != nil {
			t.Fatalf("Info failed: %v "+
				"(pre-fix it could receive bind's error if map "+
				"iteration picked the InfoOK queue first)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Info never returned")
	}

	wg.Wait()
}

// asIPCError unwraps to *IPCError if possible.
func asIPCError(err error, out **IPCError) bool {
	for err != nil {
		if e, ok := err.(*IPCError); ok {
			*out = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// TestErrorRouting_OnlyOnePending preserves the legacy behaviour:
// when only one command is in flight, deliverError still routes
// correctly. Guards against a regression where the FIFO refactor
// might break the simpler case.
func TestErrorRouting_OnlyOnePending(t *testing.T) {
	t.Parallel()
	drv, srv, cleanup := newTestDriver(t)
	defer cleanup()

	go func() {
		_, err := srv.readFrame()
		if err != nil {
			return
		}
		errFrame := make([]byte, 1+2+len("not allowed"))
		errFrame[0] = byte(opError)
		binary.BigEndian.PutUint16(errFrame[1:3], 7)
		copy(errFrame[3:], "not allowed")
		srv.writeFrame(errFrame)
	}()

	_, err := drv.InfoStruct(context.Background())
	if err == nil {
		t.Fatalf("InfoStruct succeeded; want error from the mock daemon")
	}
	var ipcErr *IPCError
	if !asIPCError(err, &ipcErr) {
		t.Fatalf("error type = %T (%v); want *IPCError wrapping", err, err)
	}
	if ipcErr.Code != 7 || ipcErr.Message != "not allowed" {
		t.Fatalf("error = code=%d msg=%q; want code=7 msg=\"not allowed\"",
			ipcErr.Code, ipcErr.Message)
	}
}
