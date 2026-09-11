package libp2ptransport

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

const (
	EnrollmentProtocol protocol.ID = "/entmoot/enrollment/2"
	RosterProtocol     protocol.ID = "/entmoot/roster/2"
	HistoryProtocol    protocol.ID = "/entmoot/history/2"

	bootstrapCapabilityDomain = "entmoot/bootstrap-capability/v1\x00"
)

var ErrBootstrapDenied = errors.New("libp2p: bootstrap capability denied")

// BootstrapCapability is a founder-signed, expiring, single-use grant for a
// specific fresh identity. It grants only the bounded pre-membership protocols.
type BootstrapCapability struct {
	GroupID           entmoot.GroupID       `json:"group_id"`
	TargetPublicKey   []byte                `json:"target_public_key"`
	TargetMemberID    entmoot.MemberID      `json:"target_member_id"`
	TargetPeerID      string                `json:"target_peer_id"`
	Founder           entmoot.NodeInfo      `json:"founder"`
	RosterHead        entmoot.RosterEntryID `json:"roster_head"`
	AllowedPeerIDs    []string              `json:"allowed_peer_ids,omitempty"`
	AllowedMultiaddrs []string              `json:"allowed_multiaddrs,omitempty"`
	Nonce             [32]byte              `json:"nonce"`
	IssuedAtMS        int64                 `json:"issued_at_ms"`
	ExpiresAtMS       int64                 `json:"expires_at_ms"`
	Signature         []byte                `json:"signature,omitempty"`
}

func bootstrapSigningBytes(capability BootstrapCapability) ([]byte, error) {
	capability.Signature = nil
	payload, err := json.Marshal(capability)
	if err != nil {
		return nil, err
	}
	return append([]byte(bootstrapCapabilityDomain), payload...), nil
}

// SignBootstrapCapability binds the grant to the founder identity.
func SignBootstrapCapability(founder *keystore.Identity, capability *BootstrapCapability) error {
	if founder == nil || capability == nil {
		return errors.New("libp2p: founder and capability are required")
	}
	if !bytes.Equal(founder.PublicKey, capability.Founder.EntmootPubKey) {
		return errors.New("libp2p: capability founder does not match signing key")
	}
	payload, err := bootstrapSigningBytes(*capability)
	if err != nil {
		return err
	}
	capability.Signature = founder.Sign(payload)
	return nil
}

// VerifyBootstrapCapability validates every identity and authority binding but
// does not consume the nonce.
func VerifyBootstrapCapability(capability BootstrapCapability, remotePeer peer.ID, now time.Time) error {
	if capability.ExpiresAtMS <= capability.IssuedAtMS || now.UnixMilli() < capability.IssuedAtMS || now.UnixMilli() > capability.ExpiresAtMS {
		return fmt.Errorf("%w: capability is outside its validity window", ErrBootstrapDenied)
	}
	if capability.Nonce == ([32]byte{}) {
		return fmt.Errorf("%w: zero nonce", ErrBootstrapDenied)
	}
	if err := entmoot.ValidateMemberInfo(capability.Founder); err != nil {
		return fmt.Errorf("%w: invalid founder: %v", ErrBootstrapDenied, err)
	}
	target, err := BindingFromPublicKey(capability.TargetPublicKey)
	if err != nil {
		return fmt.Errorf("%w: invalid target key: %v", ErrBootstrapDenied, err)
	}
	if target.MemberID != capability.TargetMemberID || target.PeerID.String() != capability.TargetPeerID || target.PeerID != remotePeer {
		return fmt.Errorf("%w: target identity binding mismatch", ErrBootstrapDenied)
	}
	allowed := false
	for _, value := range capability.AllowedPeerIDs {
		if value == remotePeer.String() {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Errorf("%w: remote peer is not allowed", ErrBootstrapDenied)
	}
	payload, err := bootstrapSigningBytes(capability)
	if err != nil {
		return fmt.Errorf("%w: encode capability: %v", ErrBootstrapDenied, err)
	}
	if len(capability.Founder.EntmootPubKey) != ed25519.PublicKeySize || !keystore.Verify(capability.Founder.EntmootPubKey, payload, capability.Signature) {
		return fmt.Errorf("%w: invalid founder signature", ErrBootstrapDenied)
	}
	return nil
}

type capabilityKey struct {
	GroupID entmoot.GroupID
	Nonce   [32]byte
}

// BootstrapAdmission atomically consumes valid capabilities. A process restart
// must replace the memory store with a persisted implementation before serving
// remote enrollment; this type is for bounded tests and ephemeral callers.
type BootstrapAdmission struct {
	mu   sync.Mutex
	used map[capabilityKey]struct{}
}

func NewBootstrapAdmission() *BootstrapAdmission {
	return &BootstrapAdmission{used: make(map[capabilityKey]struct{})}
}

// Authorize permits only enrollment/roster/history and consumes the capability
// once. Normal gossip and publication remain unavailable before membership.
func (a *BootstrapAdmission) Authorize(capability BootstrapCapability, remotePeer peer.ID, requested protocol.ID, now time.Time) error {
	switch requested {
	case EnrollmentProtocol, RosterProtocol, HistoryProtocol:
	default:
		return fmt.Errorf("%w: protocol %q is not available before membership", ErrBootstrapDenied, requested)
	}
	if err := VerifyBootstrapCapability(capability, remotePeer, now); err != nil {
		return err
	}
	key := capabilityKey{GroupID: capability.GroupID, Nonce: capability.Nonce}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.used[key]; exists {
		return fmt.Errorf("%w: capability already used", ErrBootstrapDenied)
	}
	a.used[key] = struct{}{}
	return nil
}
