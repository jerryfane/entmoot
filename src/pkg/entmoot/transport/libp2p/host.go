// Package libp2ptransport constructs libp2p hosts from the Entmoot Ed25519
// identity.
package libp2ptransport

import (
	"context"
	"crypto/ed25519"
	"fmt"

	libp2p "github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// Binding is the directly-derived application and secure-transport identity.
type Binding struct {
	MemberID entmoot.MemberID
	PeerID   peer.ID
}

// BindingFromPublicKey derives and cross-checks both identifiers from one key.
func BindingFromPublicKey(publicKey []byte) (Binding, error) {
	memberID, err := entmoot.MemberIDFromPublicKey(publicKey)
	if err != nil {
		return Binding{}, err
	}
	key, err := libp2pcrypto.UnmarshalEd25519PublicKey(publicKey)
	if err != nil {
		return Binding{}, fmt.Errorf("libp2p: unmarshal Entmoot public key: %w", err)
	}
	peerID, err := peer.IDFromPublicKey(key)
	if err != nil {
		return Binding{}, fmt.Errorf("libp2p: derive peer id: %w", err)
	}
	return Binding{MemberID: memberID, PeerID: peerID}, nil
}

// VerifyBinding rejects a claimed MemberID or PeerID that is not derived from
// the supplied roster public key.
func VerifyBinding(publicKey []byte, memberID entmoot.MemberID, peerID peer.ID) error {
	want, err := BindingFromPublicKey(publicKey)
	if err != nil {
		return err
	}
	if want.MemberID != memberID {
		return fmt.Errorf("libp2p: member id does not match roster key")
	}
	if want.PeerID != peerID {
		return fmt.Errorf("libp2p: peer id does not match roster key")
	}
	return nil
}

// NewHost reuses the persisted Entmoot private key as the libp2p secure-host
// identity. Application authorization remains a separate roster/invite layer.
func NewHost(ctx context.Context, identity *keystore.Identity, options ...libp2p.Option) (host.Host, Binding, error) {
	if identity == nil || len(identity.PrivateKey) != ed25519.PrivateKeySize {
		return nil, Binding{}, fmt.Errorf("libp2p: valid Entmoot identity is required")
	}
	privateKey, err := libp2pcrypto.UnmarshalEd25519PrivateKey(identity.PrivateKey)
	if err != nil {
		return nil, Binding{}, fmt.Errorf("libp2p: unmarshal Entmoot private key: %w", err)
	}
	binding, err := BindingFromPublicKey(identity.PublicKey)
	if err != nil {
		return nil, Binding{}, err
	}
	options = append([]libp2p.Option{libp2p.Identity(privateKey)}, options...)
	h, err := libp2p.New(options...)
	if err != nil {
		return nil, Binding{}, fmt.Errorf("libp2p: create host: %w", err)
	}
	if h.ID() != binding.PeerID {
		_ = h.Close()
		return nil, Binding{}, fmt.Errorf("libp2p: host peer id does not match Entmoot key")
	}
	go func() {
		<-ctx.Done()
		_ = h.Close()
	}()
	return h, binding, nil
}
