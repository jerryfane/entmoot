package libp2ptransport

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/prometheus/client_golang/prometheus"
)

type reservationObservation struct {
	at      time.Time
	renewal bool
}
type reservationObserver struct {
	relay.MetricsTracer
	events chan reservationObservation
}

func (m *reservationObserver) ReservationAllowed(renewal bool) {
	m.MetricsTracer.ReservationAllowed(renewal)
	select {
	case m.events <- reservationObservation{time.Now(), renewal}:
	default:
	}
}

func TestConfiguredHostAutomaticallyRenewsBeforeShortReservationExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	relayHost, _, err := NewHost(ctx, mustIdentity(t), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.ForceReachabilityPublic())
	if err != nil {
		t.Fatal(err)
	}
	defer relayHost.Close()
	observed := &reservationObserver{MetricsTracer: relay.NewMetricsTracer(relay.WithRegisterer(prometheus.NewRegistry())), events: make(chan reservationObservation, 8)}
	resources := relay.DefaultResources()
	resources.ReservationTTL = 4 * time.Second
	service, err := relay.New(relayHost, relay.WithResources(resources), relay.WithMetricsTracer(observed))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	private, _, err := NewConfiguredHost(ctx, mustIdentity(t), HostConfig{Mode: RelayOnlyConnectivity, ControlledRelays: []peer.AddrInfo{{ID: relayHost.ID(), Addrs: relayHost.Addrs()}}})
	if err != nil {
		t.Fatal(err)
	}
	defer private.Close()
	var first reservationObservation
	select {
	case first = <-observed.events:
	case <-ctx.Done():
		t.Fatal("configured host did not reserve automatically")
	}
	if first.renewal {
		t.Fatal("first reservation was unexpectedly a renewal")
	}
	deadline, done := context.WithDeadline(ctx, first.at.Add(3*time.Second))
	defer done()
	select {
	case next := <-observed.events:
		if !next.renewal {
			t.Fatal("host replaced an expired reservation instead of renewing")
		}
	case <-deadline.Done():
		t.Fatal("host did not renew the real four-second reservation near half-life")
	}
	changes, err := private.EventBus().Subscribe(new(event.EvtLocalAddressesUpdated))
	if err != nil {
		t.Fatal(err)
	}
	defer changes.Close()
	for len(private.Addrs()) == 0 {
		select {
		case <-changes.Out():
		case <-ctx.Done():
			t.Fatal("automatic reservation was not advertised")
		}
	}
	for _, address := range private.Addrs() {
		if !isCircuitAddress(address) {
			t.Fatalf("private host advertised direct address %s", address)
		}
	}
}
