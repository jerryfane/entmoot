package libp2ptransport

import (
	"context"
	"slices"
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

// A browser can dial webrtc-direct only with the listener's /certhash, which
// libp2p derives from the host key, so an operator cannot write it into
// -p2p-announce. The announced webrtc-direct address must carry it; other
// announcements, and a certhash the operator did write, stay as given.
func TestWebRTCDirectAnnouncementCarriesListenerCertHash(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	identity := mustIdentity(t)
	webrtc := "/ip4/192.0.2.1/udp/47200/webrtc-direct"
	explicit := "/ip4/192.0.2.2/udp/47201/webrtc-direct/certhash/uEiDDq4_xNyDorZBH3TlGazyJdOWSwvo4PUo5YHFMrvDE8g"
	tcp := "/ip4/192.0.2.1/tcp/4001"
	host, _, err := NewConfiguredHost(ctx, identity, HostConfig{
		ListenAddrs:   []string{"/ip4/0.0.0.0/udp/0/webrtc-direct", "/ip4/127.0.0.1/tcp/0"},
		AnnounceAddrs: []string{webrtc, explicit, tcp},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	var listenerHash string
	for _, addr := range host.Network().ListenAddresses() {
		if hash, err := addr.ValueForProtocol(ma.P_CERTHASH); err == nil {
			listenerHash = hash
		}
	}
	if listenerHash == "" {
		t.Fatalf("webrtc-direct listener has no certhash: %v", host.Network().ListenAddresses())
	}
	var got []string
	for _, addr := range host.Addrs() {
		got = append(got, addr.String())
	}
	want := []string{webrtc + "/certhash/" + listenerHash, explicit, tcp}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("announced %v, want %v", got, want)
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
