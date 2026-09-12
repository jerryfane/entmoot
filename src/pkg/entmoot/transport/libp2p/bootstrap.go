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

type BootstrapCapability = entmoot.BootstrapCapability

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
	if err := entmoot.ValidateOperationalMemberInfo(capability.Founder); err != nil {
		return fmt.Errorf("libp2p: invalid capability founder: %w", err)
	}
	targetBinding, err := BindingFromPublicKey(capability.TargetPublicKey)
	if err != nil {
		return fmt.Errorf("libp2p: derive target identity: %w", err)
	}
	if capability.TargetMemberID != targetBinding.MemberID || capability.TargetPeerID != targetBinding.PeerID.String() {
		return errors.New("libp2p: capability target identity binding mismatch")
	}
	payload, err := bootstrapSigningBytes(*capability)
	if err != nil {
		return err
	}
	capability.Signature = founder.Sign(payload)
	return nil
}

// VerifyBootstrapCapability validates the signature and target identity without
// anchoring the signer to a group's roster or checking enrollment state.
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

// BootstrapAdmission reserves then commits capabilities. Enrollment releases a
// reservation on failure; direct Authorize callers reserve and commit in one
// operation.
type BootstrapAdmission struct {
	mu          sync.Mutex
	used        map[capabilityKey]struct{}
	reserved    map[capabilityKey]struct{}
	reserve     func(capabilityKey) (bool, error)
	release     func(capabilityKey) error
	commit      func(capabilityKey) error
	unavailable func(capabilityKey) (bool, error)
}

func NewBootstrapAdmission() *BootstrapAdmission {
	return &BootstrapAdmission{
		used:     make(map[capabilityKey]struct{}),
		reserved: make(map[capabilityKey]struct{}),
	}
}

func validateBootstrapRequest(capability BootstrapCapability, remotePeer peer.ID, requested protocol.ID, now time.Time) error {
	switch requested {
	case EnrollmentProtocol, RosterProtocol, HistoryProtocol:
	default:
		return fmt.Errorf("%w: protocol %q is not available before membership", ErrBootstrapDenied, requested)
	}
	return VerifyBootstrapCapability(capability, remotePeer, now)
}

// Verify checks an unused bootstrap grant without consuming it. Callers must
// also bind its founder and checkpoint to the group's authoritative roster.
func (a *BootstrapAdmission) Verify(capability BootstrapCapability, remotePeer peer.ID, requested protocol.ID, now time.Time) error {
	if a == nil {
		return fmt.Errorf("%w: missing admission controller", ErrBootstrapDenied)
	}
	if err := validateBootstrapRequest(capability, remotePeer, requested, now); err != nil {
		return err
	}
	key := capabilityKey{GroupID: capability.GroupID, Nonce: capability.Nonce}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.used[key]; exists {
		return fmt.Errorf("%w: capability already used", ErrBootstrapDenied)
	}
	if _, exists := a.reserved[key]; exists {
		return fmt.Errorf("%w: capability already reserved", ErrBootstrapDenied)
	}
	if a.unavailable != nil {
		unavailable, err := a.unavailable(key)
		if err != nil {
			return fmt.Errorf("%w: read nonce state: %v", ErrBootstrapDenied, err)
		}
		if unavailable {
			return fmt.Errorf("%w: capability already used or reserved", ErrBootstrapDenied)
		}
	}
	return nil
}

func (a *BootstrapAdmission) Reserve(capability BootstrapCapability, remotePeer peer.ID, requested protocol.ID, now time.Time) error {
	if err := validateBootstrapRequest(capability, remotePeer, requested, now); err != nil {
		return err
	}
	key := capabilityKey{GroupID: capability.GroupID, Nonce: capability.Nonce}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.used[key]; exists {
		return fmt.Errorf("%w: capability already used", ErrBootstrapDenied)
	}
	if _, exists := a.reserved[key]; exists {
		return fmt.Errorf("%w: capability already reserved", ErrBootstrapDenied)
	}
	if a.reserve != nil {
		reserved, err := a.reserve(key)
		if err != nil {
			return fmt.Errorf("%w: persist nonce reservation: %v", ErrBootstrapDenied, err)
		}
		if !reserved {
			return fmt.Errorf("%w: capability already used or reserved", ErrBootstrapDenied)
		}
	}
	a.reserved[key] = struct{}{}
	return nil
}

func (a *BootstrapAdmission) Release(capability BootstrapCapability) error {
	key := capabilityKey{GroupID: capability.GroupID, Nonce: capability.Nonce}
	a.mu.Lock()
	defer a.mu.Unlock()
	var err error
	if a.release != nil {
		err = a.release(key)
	}
	delete(a.reserved, key)
	return err
}

func (a *BootstrapAdmission) Commit(capability BootstrapCapability) error {
	key := capabilityKey{GroupID: capability.GroupID, Nonce: capability.Nonce}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.reserved[key]; !exists {
		return fmt.Errorf("%w: capability is not reserved", ErrBootstrapDenied)
	}
	if a.commit != nil {
		if err := a.commit(key); err != nil {
			return err
		}
	}
	delete(a.reserved, key)
	a.used[key] = struct{}{}
	return nil
}

// Authorize preserves one-shot semantics for bootstrap roster/history calls.
func (a *BootstrapAdmission) Authorize(capability BootstrapCapability, remotePeer peer.ID, requested protocol.ID, now time.Time) error {
	if err := a.Reserve(capability, remotePeer, requested, now); err != nil {
		return err
	}
	if err := a.Commit(capability); err != nil {
		_ = a.Release(capability)
		return err
	}
	return nil
}
