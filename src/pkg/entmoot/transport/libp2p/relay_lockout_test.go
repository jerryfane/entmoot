package libp2ptransport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot/keystore"
)

// The harm the budget covers: a full relay allowlist must not stop the
// daemon's own group peers from connecting. Group members are not Protect()ed,
// so a shared budget denies them deterministically once the relay fills it.
func TestAFullRelayAllowlistStillLetsAGroupPeerConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("starts one host per allowlisted relay client")
	}
	ctx := context.Background()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}

	allowed := make([]peer.ID, 0, 70)
	hosts := make([]host.Host, 0, 70)
	for range 70 {
		h, hostErr := libp2p.New(libp2p.NoListenAddrs)
		if hostErr != nil {
			t.Fatal(hostErr)
		}
		t.Cleanup(func() { _ = h.Close() })
		allowed = append(allowed, h.ID())
		hosts = append(hosts, h)
	}

	relaying, _, err := NewConfiguredHost(ctx, identity, HostConfig{
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
		RelayService: &RelayServerConfig{
			AllowedPeers: allowed, ReservationTTL: time.Hour, CircuitDuration: time.Minute,
			CircuitBytes: 1 << 20, MaxReservations: 128, MaxCircuitsPerPeer: 16,
			MaxReservationsPerIP: 128, MaxReservationsPerASN: 128,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relaying.Close()
	target := peer.AddrInfo{ID: relaying.ID(), Addrs: relaying.Addrs()}

	connected := 0
	for _, h := range hosts {
		if err := h.Connect(ctx, target); err == nil {
			connected++
		}
	}
	if connected != len(hosts) {
		t.Fatalf("%d of %d allowlisted relay clients connected", connected, len(hosts))
	}

	// A group peer is not on the relay allowlist and gets no protection.
	member, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	if err := member.Connect(ctx, target); err != nil {
		t.Fatalf("a group peer was locked out by relay load: %v", err)
	}
}
