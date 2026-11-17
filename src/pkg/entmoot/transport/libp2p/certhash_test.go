package libp2ptransport

import (
	"context"
	"testing"

	ma "github.com/multiformats/go-multiaddr"
)

// An invite minted from a typed -bootstrap address must carry the same
// /certhash the running node's webrtc-direct listener uses, or a browser
// cannot dial it.
func TestCompleteOwnCertHashesMatchesListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	identity := mustIdentity(t)
	other := mustIdentity(t)
	host, binding, err := NewConfiguredHost(ctx, identity, HostConfig{ListenAddrs: []string{"/ip4/127.0.0.1/udp/0/webrtc-direct"}})
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
	otherBinding, err := BindingFromPublicKey(other.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	self := "/p2p/" + binding.PeerID.String()
	explicit := "/ip4/192.0.2.1/udp/47200/webrtc-direct/certhash/uEiDDq4_xNyDorZBH3TlGazyJdOWSwvo4PUo5YHFMrvDE8g" + self
	in := []string{
		"/ip4/192.0.2.1/udp/47200/webrtc-direct" + self,
		"/ip4/192.0.2.1/udp/47200/webrtc-direct/p2p/" + otherBinding.PeerID.String(),
		explicit,
		"/ip4/192.0.2.1/tcp/4001" + self,
	}
	got, err := CompleteOwnCertHashes(identity, in)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/ip4/192.0.2.1/udp/47200/webrtc-direct/certhash/" + listenerHash + self,
		in[1], // another peer's hash is unknown here
		explicit,
		in[3],
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("address %d: got %s, want %s", i, got[i], want[i])
		}
	}
}
