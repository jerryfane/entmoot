package libp2ptransport

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"

	"entmoot/pkg/entmoot/keystore"
)

func systemConnLimit(t *testing.T, h interface {
	Network() network.Network
}) int {
	t.Helper()
	var limit int
	err := h.Network().ResourceManager().ViewSystem(func(scope network.ResourceScope) error {
		limiter, ok := scope.(rcmgr.ResourceScopeLimiter)
		if !ok {
			t.Fatalf("the system scope does not expose its limit: %T", scope)
		}
		limit = limiter.Limit().GetConnTotalLimit()
		return nil
	})
	if err != nil {
		t.Fatalf("view system scope: %v", err)
	}
	return limit
}

// A relay client holds a connection for as long as its reservation lives, so a
// relay sharing the group's connection budget can lock the daemon's own group
// members out - and makes the advertised reservation cap unreachable. The
// relay's share has to be added on top of the group's.
func TestRelayServiceDoesNotSpendTheGroupConnectionBudget(t *testing.T) {
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	allowed := make([]peer.ID, 0, 80)
	for range 80 {
		clientIdentity, genErr := keystore.Generate()
		if genErr != nil {
			t.Fatal(genErr)
		}
		binding, bindErr := BindingFromPublicKey(clientIdentity.PublicKey)
		if bindErr != nil {
			t.Fatal(bindErr)
		}
		allowed = append(allowed, binding.PeerID)
	}

	plain, _, err := NewConfiguredHost(context.Background(), identity, HostConfig{
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	group := systemConnLimit(t, plain)
	if group != maxHostConnections {
		t.Fatalf("a daemon with no relay admits %d connections, want %d", group, maxHostConnections)
	}

	relaying, _, err := NewConfiguredHost(context.Background(), identity, HostConfig{
		ListenAddrs:  []string{"/ip4/127.0.0.1/tcp/0"},
		RelayService: &RelayServerConfig{AllowedPeers: allowed, ReservationTTL: time.Hour, CircuitDuration: time.Minute, CircuitBytes: 1 << 20, MaxReservations: 128, MaxCircuitsPerPeer: 16, MaxReservationsPerIP: 8, MaxReservationsPerASN: 32},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relaying.Close()

	if got := systemConnLimit(t, relaying); got != group+len(allowed) {
		t.Fatalf("a relaying daemon admits %d connections, want %d (%d for the group plus %d allowlisted relay clients)",
			got, group+len(allowed), group, len(allowed))
	}
}
