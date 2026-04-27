package events

import (
	"testing"
	"time"
)

func TestBusDeliversEvent(t *testing.T) {
	bus := NewBus()
	ch := make(chan Event, 1)
	cancel := bus.Subscribe(ch)
	defer cancel()

	bus.Emit(Event{Type: TypeMessageIngested})
	select {
	case ev := <-ch:
		if ev.Type != TypeMessageIngested {
			t.Fatalf("event type = %s", ev.Type)
		}
		if ev.At.IsZero() {
			t.Fatalf("event timestamp was not populated")
		}
	case <-time.After(time.Second):
		t.Fatalf("event not delivered")
	}
}

func TestBusDoesNotBlockOnFullSubscriber(t *testing.T) {
	bus := NewBus()
	ch := make(chan Event)
	cancel := bus.Subscribe(ch)
	defer cancel()

	done := make(chan struct{})
	go func() {
		bus.Emit(Event{Type: TypeMessageIngested})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Emit blocked on slow subscriber")
	}
}
