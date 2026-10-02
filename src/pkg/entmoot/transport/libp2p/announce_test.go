package libp2ptransport

import (
	"context"
	"testing"

	ma "github.com/multiformats/go-multiaddr"
)

func TestReverseProxyAnnouncementDoesNotExposePrivateListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	identity := mustIdentity(t)
	public := "/ip4/192.0.2.1/tcp/443/tls/sni/peer.example.org/ws"
	host, _, err := NewConfiguredHost(ctx, identity, HostConfig{ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0/ws"}, AnnounceAddrs: []string{public}})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	addresses := host.Addrs()
	if len(addresses) != 1 || addresses[0].String() != public {
		t.Fatalf("private or wrong endpoints advertised: %v", addresses)
	}
	listener := false
	for _, addr := range host.Network().ListenAddresses() {
		if ip, err := addr.ValueForProtocol(ma.P_IP4); err == nil && ip == "127.0.0.1" {
			listener = true
		}
	}
	if !listener {
		t.Fatal("external announcement replaced the private listener itself")
	}
	// Callers may mutate an address list; it must not corrupt future announcements.
	addresses[0] = ma.StringCast("/ip4/127.0.0.1/tcp/1")
	if got := host.Addrs(); len(got) != 1 || got[0].String() != public {
		t.Fatalf("announcement state was aliased: %v", got)
	}
}

func TestAnnouncementCannotOverrideRelayOnlyOrPeerIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	identity := mustIdentity(t)
	binding, err := BindingFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		config HostConfig
	}{
		{"relay-only-direct-leak", HostConfig{Mode: RelayOnlyConnectivity, AnnounceAddrs: []string{"/ip4/192.0.2.1/tcp/443/tls/ws"}}},
		{"embedded-peer-id", HostConfig{AnnounceAddrs: []string{"/ip4/192.0.2.1/tcp/443/tls/ws/p2p/" + binding.PeerID.String()}}},
		{"malformed", HostConfig{AnnounceAddrs: []string{"not-a-multiaddr"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, err := NewConfiguredHost(ctx, identity, tc.config)
			if err == nil {
				h.Close()
				t.Fatal("unsafe announce configuration accepted")
			}
		})
	}
}
