package ipcclient

import (
	"log/slog"
	"sync"
)

// subscriptionBufferSize is the per-Subscription channel capacity.
// Sized for "many bursts of state changes can be in flight without
// dropping any". TURN-rotation is the only producer today and it
// fires at most a handful of times per day, so 16 is safe headroom.
//
// On overflow the OLDEST entry is dropped and a Warn is logged so a
// slow consumer cannot wedge the demuxer goroutine. The next genuine
// change re-syncs the consumer to the latest state, so a dropped
// notification only delays detection — never desynchronises it.
// (v1.5.0)
const subscriptionBufferSize = 16

// Notification is one server-pushed event delivered to a Subscription.
// Topic identifies the producer (e.g. "turn_endpoint"); Payload is
// opaque bytes whose encoding is the topic's responsibility (TURN
// uses UTF-8 host:port).
type Notification struct {
	Topic   string
	Payload []byte
}

// Subscription is a client-side handle for a topic the driver has
// successfully subscribed to via Driver.Subscribe. Notifications are
// delivered on Events() until the Subscription is Closed or the
// underlying Driver shuts down. Safe for concurrent use; close is
// idempotent. (v1.5.0)
type Subscription struct {
	drv   *Driver
	topic string

	closeMu sync.Mutex
	closed  bool

	eventCh chan Notification
}

// Topic returns the topic this Subscription was registered for.
func (s *Subscription) Topic() string { return s.topic }

// Events returns the receive-only channel of Notifications. Buffered
// at subscriptionBufferSize. On overflow the oldest queued event is
// dropped (Warn-logged); the channel itself never closes for that
// reason. The channel does close when Subscription.Close runs OR
// when the underlying Driver shuts down.
func (s *Subscription) Events() <-chan Notification { return s.eventCh }

// Close removes the Subscription from the driver's topic registry,
// sends an Unsubscribe to pilot (best-effort, errors swallowed —
// pilot will clean up on disconnect anyway), and closes the events
// channel. Idempotent.
//
// Lock ordering: closeMu serialises against deliverNotification so
// the close(eventCh) is never racing with a chansend. We do NOT
// hold closeMu while taking drv.subsMu (in unregisterSubscription)
// or sending the Unsubscribe RPC — those would invert the
// driver-side ordering (routeNotify takes subsMu first, then
// deliverNotification wants closeMu) and risk deadlock.
func (s *Subscription) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	close(s.eventCh)
	s.closeMu.Unlock()

	s.drv.unregisterSubscription(s)

	// Best-effort unsubscribe RPC. Pilot's handleClient defer also
	// removes us from the topic set on disconnect, so a failed
	// Unsubscribe just means pilot may push a few stale Notify
	// frames during the gap — they're routed to zero subscribers
	// (we already unregistered) and dropped silently.
	if err := s.drv.sendUnsubscribe(s.topic); err != nil {
		slog.Debug("ipcclient: unsubscribe failed (best-effort)",
			slog.String("topic", s.topic),
			slog.String("err", err.Error()))
	}
	return nil
}

// deliverNotification pushes n onto the events channel with
// drop-oldest overflow semantics. Called by the demuxer's routeNotify.
// Holds closeMu for the entire chansend sequence so a concurrent
// Close (which also holds closeMu while close()ing the channel)
// cannot race the channel ops here. All sends/receives below are
// non-blocking (default branch), so the lock is held only
// briefly.
func (s *Subscription) deliverNotification(n Notification) {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return
	}

	select {
	case s.eventCh <- n:
		return
	default:
	}
	// Buffer full — drop oldest, then enqueue.
	select {
	case <-s.eventCh:
		slog.Warn("ipcclient: subscription buffer full; dropping oldest notification",
			slog.String("topic", s.topic),
			slog.Int("buffer", subscriptionBufferSize))
	default:
		// Concurrent reader drained the channel between our
		// non-blocking try and our drop attempt — fine, fall
		// through.
	}
	select {
	case s.eventCh <- n:
	default:
		// Could not enqueue even after the drop (consumer
		// re-filled or other concurrent producer beat us).
		// Drop silently.
	}
}
