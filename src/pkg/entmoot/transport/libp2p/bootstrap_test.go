package libp2ptransport

import (
	"context"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

func TestBootstrapCapabilityAdmitsFreshNonMemberOnce(t *testing.T) {
	founder := mustIdentity(t)
	target := mustIdentity(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targetHost, targetBinding, err := NewHost(ctx, target, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer targetHost.Close()
	now := time.UnixMilli(10_000)
	capability := BootstrapCapability{
		TargetPublicKey: target.PublicKey,
		TargetMemberID:  targetBinding.MemberID,
		TargetPeerID:    targetBinding.PeerID.String(),
		Founder:         mustNodeInfo(t, founder.PublicKey),
		AllowedPeerIDs:  []string{targetBinding.PeerID.String()},
		IssuedAtMS:      now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     now.Add(time.Minute).UnixMilli(),
	}
	capability.GroupID[0] = 1
	capability.RosterHead[0] = 2
	capability.Nonce[0] = 3
	if err := SignBootstrapCapability(founder, &capability); err != nil {
		t.Fatal(err)
	}
	admission := NewBootstrapAdmission()
	if err := admission.Authorize(capability, targetHost.ID(), protocol.ID("/entmoot/gossip/2"), now); err == nil {
		t.Fatal("pre-member gossip was authorized")
	}
	if err := admission.Authorize(capability, targetHost.ID(), EnrollmentProtocol, now); err != nil {
		t.Fatalf("fresh non-member enrollment denied: %v", err)
	}
	if err := admission.Authorize(capability, targetHost.ID(), EnrollmentProtocol, now); err == nil {
		t.Fatal("single-use capability was replayed")
	}
}

func TestBootstrapCapabilityRejectsInvalidPeerBinding(t *testing.T) {
	founder := mustIdentity(t)
	target := mustIdentity(t)
	attacker := mustIdentity(t)
	targetBinding, err := BindingFromPublicKey(target.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	attackerBinding, err := BindingFromPublicKey(attacker.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(20_000)
	capability := BootstrapCapability{
		TargetPublicKey: target.PublicKey,
		TargetMemberID:  targetBinding.MemberID,
		TargetPeerID:    targetBinding.PeerID.String(),
		Founder:         mustNodeInfo(t, founder.PublicKey),
		AllowedPeerIDs:  []string{targetBinding.PeerID.String()},
		IssuedAtMS:      now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     now.Add(time.Minute).UnixMilli(),
	}
	capability.GroupID[0] = 1
	capability.Nonce[0] = 1
	if err := SignBootstrapCapability(founder, &capability); err != nil {
		t.Fatal(err)
	}
	if err := NewBootstrapAdmission().Authorize(capability, attackerBinding.PeerID, EnrollmentProtocol, now); err == nil {
		t.Fatal("capability accepted from a different secure transport identity")
	}
}

func TestBootstrapCapabilityReplayRejectedAfterRestart(t *testing.T) {
	founder := mustIdentity(t)
	target := mustIdentity(t)
	targetBinding, err := BindingFromPublicKey(target.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(30_000)
	capability := BootstrapCapability{
		TargetPublicKey: target.PublicKey,
		TargetMemberID:  targetBinding.MemberID,
		TargetPeerID:    targetBinding.PeerID.String(),
		Founder:         mustNodeInfo(t, founder.PublicKey),
		AllowedPeerIDs:  []string{targetBinding.PeerID.String()},
		IssuedAtMS:      now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     now.Add(time.Minute).UnixMilli(),
	}
	capability.GroupID[0] = 4
	capability.Nonce[0] = 5
	if err := SignBootstrapCapability(founder, &capability); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	first, err := OpenPersistentBootstrapAdmission(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Authorize(capability, targetBinding.PeerID, EnrollmentProtocol, now); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenPersistentBootstrapAdmission(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Authorize(capability, targetBinding.PeerID, EnrollmentProtocol, now); err == nil {
		t.Fatal("capability replay succeeded after admission restart")
	}
}

func TestPersistentReservationExpiresAfterRestart(t *testing.T) {
	founder := mustIdentity(t)
	target := mustIdentity(t)
	targetBinding, err := BindingFromPublicKey(target.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	capability := BootstrapCapability{
		TargetPublicKey: target.PublicKey,
		TargetMemberID:  targetBinding.MemberID,
		TargetPeerID:    targetBinding.PeerID.String(),
		Founder:         mustNodeInfo(t, founder.PublicKey),
		AllowedPeerIDs:  []string{targetBinding.PeerID.String()},
		IssuedAtMS:      now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     now.Add(time.Hour).UnixMilli(),
	}
	capability.GroupID[0] = 6
	capability.Nonce[0] = 7
	if err := SignBootstrapCapability(founder, &capability); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	first, err := OpenPersistentBootstrapAdmission(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Reserve(capability, targetBinding.PeerID, EnrollmentProtocol, now); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenPersistentBootstrapAdmission(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Reserve(capability, targetBinding.PeerID, EnrollmentProtocol, now); err == nil {
		t.Fatal("fresh reservation was stolen after restart")
	}
	if _, err := second.db.Exec(`UPDATE used_bootstrap_capabilities SET reserved_at_ms=0 WHERE group_id=? AND nonce=?`, capability.GroupID[:], capability.Nonce[:]); err != nil {
		t.Fatal(err)
	}
	if err := second.Reserve(capability, targetBinding.PeerID, EnrollmentProtocol, now); err != nil {
		t.Fatalf("stale reservation did not recover: %v", err)
	}
}

func mustIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func mustNodeInfo(t *testing.T, publicKey []byte) entmoot.NodeInfo {
	t.Helper()
	memberID, err := entmoot.MemberIDFromPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := entmoot.PeerIDFromPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return entmoot.NodeInfo{EntmootPubKey: append([]byte(nil), publicKey...), MemberID: &memberID, PeerID: peerID}
}
