package libp2ptransport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	relayv2client "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
)

// An invalid relay service must be refused before the host exists, so a caller
// gets an error rather than a daemon quietly running with no relay.
func TestConfiguredHostRefusesAnInvalidRelayService(t *testing.T) {
	allowed := []peer.ID{"12D3KooWPXb5rMPAHKYBc5Cwx9dbDhjm2Dsqwt8gFkGe6kGNCDSp"}
	for name, service := range map[string]RelayServerConfig{
		"no allowed peers":   {ReservationTTL: time.Hour, CircuitDuration: time.Minute, CircuitBytes: 1 << 20, MaxReservations: 8, MaxCircuitsPerPeer: 2, MaxReservationsPerIP: 2, MaxReservationsPerASN: 2},
		"unbounded circuits": {AllowedPeers: allowed, ReservationTTL: time.Hour, CircuitDuration: time.Minute, CircuitBytes: 1 << 20, MaxReservations: 8, MaxCircuitsPerPeer: 0, MaxReservationsPerIP: 2, MaxReservationsPerASN: 2},
	} {
		h, _, err := NewConfiguredHost(context.Background(), mustIdentity(t), HostConfig{
			ListenAddrs:  []string{"/ip4/127.0.0.1/tcp/0"},
			RelayService: &service,
		})
		if err == nil {
			_ = h.Close()
			t.Fatalf("%s: a host was built for an invalid relay service", name)
		}
	}
}

// One process that both talks to a group and relays for its members: a
// relay-only peer takes a reservation on a daemon host that was started with
// a relay service, which is the whole point of the flag.
func TestDaemonHostCanRelayForAnAllowedPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := mustIdentity(t)
	clientBinding, err := BindingFromPublicKey(client.PublicKey)
	if err != nil {
		t.Fatal(err)
	}

	daemon := mustIdentity(t)
	host, _, err := NewConfiguredHost(ctx, daemon, HostConfig{
		Mode:        DirectConnectivity,
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
		RelayService: &RelayServerConfig{
			AllowedPeers:          []peer.ID{clientBinding.PeerID},
			ReservationTTL:        time.Hour,
			CircuitDuration:       15 * time.Minute,
			CircuitBytes:          64 << 20,
			MaxReservations:       8,
			MaxCircuitsPerPeer:    4,
			MaxReservationsPerIP:  8,
			MaxReservationsPerASN: 8,
		},
	})
	if err != nil {
		t.Fatalf("daemon host with relay service: %v", err)
	}
	defer host.Close()

	guest, _, err := NewHost(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()

	relay := peer.AddrInfo{ID: host.ID(), Addrs: host.Addrs()}
	if err := guest.Connect(ctx, relay); err != nil {
		t.Fatalf("connect to the daemon: %v", err)
	}
	reservation, err := relayv2client.Reserve(ctx, guest, relay)
	if err != nil {
		t.Fatalf("reserve on the daemon's relay service: %v", err)
	}
	if reservation == nil {
		t.Fatal("no reservation")
	}
	if !reservation.Expiration.After(time.Now()) {
		t.Fatalf("the reservation is already expired: %s", reservation.Expiration)
	}
	// Circuit addresses are only advertised for a publicly routable relay, so
	// a loopback fixture yields none; the reservation itself is the capability
	// under test.

	// And a peer it does not name is refused, because the flag relays for the
	// operator's own peers rather than for strangers.
	stranger := mustIdentity(t)
	other, _, err := NewHost(ctx, stranger)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Connect(ctx, relay); err != nil {
		t.Fatalf("connect as the stranger: %v", err)
	}
	if _, err := relayv2client.Reserve(ctx, other, relay); err == nil {
		t.Fatal("a peer outside the allowlist reserved on the relay service")
	}
}

// The daemon keeps announcing its own addresses. The standalone relay replaces
// the announced list with its -announce value; doing that here would put a
// relay address into the invites this daemon mints in place of its own.
func TestRelayServiceDoesNotReplaceAnnouncedAddresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	guest := mustIdentity(t)
	guestBinding, err := BindingFromPublicKey(guest.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	host, _, err := NewConfiguredHost(ctx, mustIdentity(t), HostConfig{
		Mode:        DirectConnectivity,
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
		RelayService: &RelayServerConfig{
			AllowedPeers:          []peer.ID{guestBinding.PeerID},
			ReservationTTL:        time.Hour,
			CircuitDuration:       time.Minute,
			CircuitBytes:          1 << 20,
			MaxReservations:       4,
			MaxCircuitsPerPeer:    2,
			MaxReservationsPerIP:  4,
			MaxReservationsPerASN: 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	announced := make(map[string]struct{}, len(host.Addrs()))
	for _, address := range host.Addrs() {
		announced[address.String()] = struct{}{}
	}
	for _, listen := range host.Network().ListenAddresses() {
		if listen.String() == "/p2p-circuit" {
			// Enabling relay adds a circuit listener, which is not an address
			// anybody dials this host on.
			continue
		}
		if _, ok := announced[listen.String()]; !ok {
			t.Fatalf("listen address %s is not announced; announced=%v", listen, host.Addrs())
		}
	}
	if len(announced) == 0 {
		t.Fatal("the daemon announces nothing")
	}
}

// Relay-only is the one profile where this is refused: it has no public
// listener to relay through, and it exists to keep a peer's address private.
func TestRelayOnlyCannotRunARelayService(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	relay := mustIdentity(t)
	relayBinding, err := BindingFromPublicKey(relay.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = NewConfiguredHost(ctx, mustIdentity(t), HostConfig{
		Mode:             RelayOnlyConnectivity,
		ControlledRelays: []peer.AddrInfo{{ID: relayBinding.PeerID}},
		RelayService: &RelayServerConfig{
			AllowedPeers:          []peer.ID{relayBinding.PeerID},
			ReservationTTL:        time.Hour,
			CircuitDuration:       time.Minute,
			CircuitBytes:          1 << 20,
			MaxReservations:       4,
			MaxCircuitsPerPeer:    2,
			MaxReservationsPerIP:  4,
			MaxReservationsPerASN: 4,
		},
	})
	if err == nil {
		t.Fatal("relay-only accepted a relay service")
	}
}
