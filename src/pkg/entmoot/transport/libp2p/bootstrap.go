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
	EnrollmentProtocol protocol.ID = "/entmoot/enrollment/3"
	RosterProtocol     protocol.ID = "/entmoot/roster/2"
	HistoryProtocol    protocol.ID = "/entmoot/history/2"
	// PeerRecordProtocol serves members the signed peer records this node holds
	// for other members. It is members-only and never a bootstrap target.
	PeerRecordProtocol protocol.ID = "/entmoot/peer-records/1"

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

// SignBootstrapCapability binds the grant to the issuing identity. An open
// invite carries no target identity and is redeemable by any holder while uses
// remain.
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
	if capability.MaxUses < 0 {
		return errors.New("libp2p: capability max uses cannot be negative")
	}
	if capability.IsOpenInvite() {
		if capability.TargetMemberID != (entmoot.MemberID{}) || capability.TargetPeerID != "" {
			return errors.New("libp2p: open invite must not carry a target identity")
		}
	} else {
		targetBinding, err := BindingFromPublicKey(capability.TargetPublicKey)
		if err != nil {
			return fmt.Errorf("libp2p: derive target identity: %w", err)
		}
		if capability.TargetMemberID != targetBinding.MemberID || capability.TargetPeerID != targetBinding.PeerID.String() {
			return errors.New("libp2p: capability target identity binding mismatch")
		}
	}
	payload, err := bootstrapSigningBytes(*capability)
	if err != nil {
		return err
	}
	capability.Signature = founder.Sign(payload)
	return nil
}

// VerifyBootstrapCapability validates the signature, the use limit and, for a
// target-bound capability, that the redeeming peer is its target. It does not
// anchor the signer to a group's roster or check enrollment state.
func VerifyBootstrapCapability(capability BootstrapCapability, remotePeer peer.ID, now time.Time) error {
	if capability.ExpiresAtMS <= capability.IssuedAtMS || now.UnixMilli() < capability.IssuedAtMS || now.UnixMilli() > capability.ExpiresAtMS {
		return fmt.Errorf("%w: capability is outside its validity window", ErrBootstrapDenied)
	}
	if capability.Nonce == ([32]byte{}) {
		return fmt.Errorf("%w: zero nonce", ErrBootstrapDenied)
	}
	if capability.MaxUses < 0 {
		return fmt.Errorf("%w: negative max uses", ErrBootstrapDenied)
	}
	if err := entmoot.ValidateMemberInfo(capability.Founder); err != nil {
		return fmt.Errorf("%w: invalid founder: %v", ErrBootstrapDenied, err)
	}
	if capability.IsOpenInvite() {
		if capability.TargetMemberID != (entmoot.MemberID{}) || capability.TargetPeerID != "" {
			return fmt.Errorf("%w: open invite carries a partial target identity", ErrBootstrapDenied)
		}
	} else {
		target, err := BindingFromPublicKey(capability.TargetPublicKey)
		if err != nil {
			return fmt.Errorf("%w: invalid target key: %v", ErrBootstrapDenied, err)
		}
		if target.MemberID != capability.TargetMemberID || target.PeerID.String() != capability.TargetPeerID || target.PeerID != remotePeer {
			return fmt.Errorf("%w: target identity binding mismatch", ErrBootstrapDenied)
		}
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

// capabilityKey identifies one issued invite.
type capabilityKey struct {
	GroupID entmoot.GroupID
	Nonce   [32]byte
}

// redemptionKey identifies one invite redeemed by one applicant peer. Uses are
// counted per invite, so a multi-use invite admits distinct peers up to its
// limit while a repeat from the same peer is still a replay.
type redemptionKey struct {
	capabilityKey
	Peer string
}

// BootstrapAdmission reserves then commits invite redemptions. Uses are
// counted per invite and keyed per applicant peer, so a multi-use invite
// admits distinct peers up to its limit while a repeat from the same peer is
// refused. Enrollment releases a reservation on failure, which leaves the use
// available for a retry; direct Authorize callers reserve and commit in one
// operation.
type BootstrapAdmission struct {
	mu          sync.Mutex
	used        map[redemptionKey]struct{}
	reserved    map[redemptionKey]struct{}
	reserve     func(key redemptionKey, maxUses int) (bool, error)
	release     func(redemptionKey) error
	commit      func(redemptionKey) error
	unavailable func(key redemptionKey, maxUses int) (bool, error)
	revoked     func(capabilityKey) (bool, error)
}

func NewBootstrapAdmission() *BootstrapAdmission {
	return &BootstrapAdmission{
		used:     make(map[redemptionKey]struct{}),
		reserved: make(map[redemptionKey]struct{}),
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

func redemption(capability BootstrapCapability, remotePeer peer.ID) redemptionKey {
	return redemptionKey{
		capabilityKey: capabilityKey{GroupID: capability.GroupID, Nonce: capability.Nonce},
		Peer:          remotePeer.String(),
	}
}

// localUses counts reservations and commitments this process knows about for
// one invite. a.mu must be held.
func (a *BootstrapAdmission) localUses(invite capabilityKey) int {
	count := 0
	for key := range a.used {
		if key.capabilityKey == invite {
			count++
		}
	}
	for key := range a.reserved {
		if key.capabilityKey == invite {
			count++
		}
	}
	return count
}

// checkRevoked refuses an invite the issuer has withdrawn. a.mu must be held.
func (a *BootstrapAdmission) checkRevoked(invite capabilityKey) error {
	if a.revoked == nil {
		return nil
	}
	revoked, err := a.revoked(invite)
	if err != nil {
		return fmt.Errorf("%w: read revocation state: %v", ErrBootstrapDenied, err)
	}
	if revoked {
		return fmt.Errorf("%w: capability is revoked", ErrBootstrapDenied)
	}
	return nil
}

// Verify checks a redeemable bootstrap grant without consuming it. Callers
// must also bind its founder and checkpoint to the group's authoritative
// roster.
func (a *BootstrapAdmission) Verify(capability BootstrapCapability, remotePeer peer.ID, requested protocol.ID, now time.Time) error {
	if a == nil {
		return fmt.Errorf("%w: missing admission controller", ErrBootstrapDenied)
	}
	if err := validateBootstrapRequest(capability, remotePeer, requested, now); err != nil {
		return err
	}
	key := redemption(capability, remotePeer)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkRevoked(key.capabilityKey); err != nil {
		return err
	}
	if _, exists := a.used[key]; exists {
		return fmt.Errorf("%w: capability already used", ErrBootstrapDenied)
	}
	if _, exists := a.reserved[key]; exists {
		return fmt.Errorf("%w: capability already reserved", ErrBootstrapDenied)
	}
	if a.localUses(key.capabilityKey) >= capability.Uses() {
		return fmt.Errorf("%w: capability use limit reached", ErrBootstrapDenied)
	}
	if a.unavailable != nil {
		unavailable, err := a.unavailable(key, capability.Uses())
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
	key := redemption(capability, remotePeer)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkRevoked(key.capabilityKey); err != nil {
		return err
	}
	if _, exists := a.used[key]; exists {
		return fmt.Errorf("%w: capability already used", ErrBootstrapDenied)
	}
	if _, exists := a.reserved[key]; exists {
		return fmt.Errorf("%w: capability already reserved", ErrBootstrapDenied)
	}
	if a.localUses(key.capabilityKey) >= capability.Uses() {
		return fmt.Errorf("%w: capability use limit reached", ErrBootstrapDenied)
	}
	if a.reserve != nil {
		reserved, err := a.reserve(key, capability.Uses())
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

func (a *BootstrapAdmission) Release(capability BootstrapCapability, remotePeer peer.ID) error {
	key := redemption(capability, remotePeer)
	a.mu.Lock()
	defer a.mu.Unlock()
	var err error
	if a.release != nil {
		err = a.release(key)
	}
	delete(a.reserved, key)
	return err
}

func (a *BootstrapAdmission) Commit(capability BootstrapCapability, remotePeer peer.ID) error {
	key := redemption(capability, remotePeer)
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

// Authorize consumes one use for bootstrap roster/history calls.
func (a *BootstrapAdmission) Authorize(capability BootstrapCapability, remotePeer peer.ID, requested protocol.ID, now time.Time) error {
	if err := a.Reserve(capability, remotePeer, requested, now); err != nil {
		return err
	}
	if err := a.Commit(capability, remotePeer); err != nil {
		_ = a.Release(capability, remotePeer)
		return err
	}
	return nil
}
