package libp2ptransport

import (
	"fmt"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	webrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	"github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot/keystore"
)

// CompleteOwnCertHashes appends this identity's webrtc-direct /certhash to
// every address that names the identity's own peer over webrtc-direct and
// lacks one. Other addresses are returned unchanged.
//
// A browser cannot dial webrtc-direct without the hash of the listener's DTLS
// certificate. libp2p derives that certificate from the host key, so the hash
// is fixed per identity but not something an operator can type, and an invite
// minted from an address written without it is unusable from a browser. The
// running host adds the hash to its own advertised addresses; this does the
// same for addresses given explicitly, e.g. `invite create -bootstrap`.
// Addresses for other peers cannot be completed: their key is not known here.
func CompleteOwnCertHashes(identity *keystore.Identity, addresses []string) ([]string, error) {
	own, err := BindingFromPublicKey(identity.PublicKey)
	if err != nil {
		return nil, err
	}
	var certhash multiaddr.Multiaddr
	out := make([]string, len(addresses))
	for i, raw := range addresses {
		out[i] = raw
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			continue // callers validate addresses and report errors themselves
		}
		transport, id := peer.SplitAddr(address)
		if id != own.PeerID || transport == nil {
			continue
		}
		if _, err := transport.ValueForProtocol(multiaddr.P_WEBRTC_DIRECT); err != nil {
			continue
		}
		if _, err := transport.ValueForProtocol(multiaddr.P_CERTHASH); err == nil {
			continue
		}
		if certhash == nil {
			if certhash, err = ownCertHash(identity); err != nil {
				return nil, err
			}
		}
		p2p, err := multiaddr.NewMultiaddr("/p2p/" + id.String())
		if err != nil {
			return nil, err
		}
		out[i] = transport.Encapsulate(certhash).Encapsulate(p2p).String()
	}
	return out, nil
}

// ownCertHash returns the /certhash component of this identity's
// webrtc-direct listener, computed the way the libp2p transport computes it.
func ownCertHash(identity *keystore.Identity) (multiaddr.Multiaddr, error) {
	key, err := libp2pcrypto.UnmarshalEd25519PrivateKey(identity.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("libp2p: unmarshal Entmoot private key: %w", err)
	}
	transport, err := webrtc.New(key, nil, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("libp2p: webrtc-direct certificate: %w", err)
	}
	base := multiaddr.StringCast("/ip4/127.0.0.1/udp/1/webrtc-direct")
	withHash, ok := transport.AddCertHashes(base)
	if !ok {
		return nil, fmt.Errorf("libp2p: webrtc-direct certificate hash unavailable")
	}
	hash, err := withHash.ValueForProtocol(multiaddr.P_CERTHASH)
	if err != nil {
		return nil, err
	}
	return multiaddr.NewMultiaddr("/certhash/" + hash)
}
