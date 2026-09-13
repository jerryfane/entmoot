package libp2ptransport

import (
	"context"
	"errors"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

func TestFreshMemberEnrollsAcrossLibp2pWithoutPilot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	targetIdentity := mustIdentity(t)
	founderHost, _, err := NewHost(ctx, founderIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()
	targetHost, targetBinding, err := NewHost(ctx, targetIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer targetHost.Close()

	var groupID entmoot.GroupID
	groupID[0] = 1
	founderInfo := mustNodeInfo(t, founderIdentity.PublicKey)
	rosterLog := roster.New(groupID)
	if err := rosterLog.Genesis(founderIdentity, founderInfo, 1_000); err != nil {
		t.Fatal(err)
	}
	founderStore := store.NewMemory()
	defer founderStore.Close()
	founderLive, err := NewLiveGroup(ctx, LiveConfig{Host: founderHost, GroupID: groupID, Roster: rosterLog, Store: founderStore})
	if err != nil {
		t.Fatal(err)
	}
	defer founderLive.Close()
	now := time.UnixMilli(10_000)
	capability := BootstrapCapability{
		GroupID:         groupID,
		TargetPublicKey: targetIdentity.PublicKey,
		TargetMemberID:  targetBinding.MemberID,
		TargetPeerID:    targetBinding.PeerID.String(),
		Founder:         founderInfo,
		RosterHead:      rosterLog.Head(),
		AllowedPeerIDs:  []string{founderHost.ID().String()},
		IssuedAtMS:      now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     now.Add(time.Minute).UnixMilli(),
	}
	capability.Nonce[0] = 1
	if err := SignBootstrapCapability(founderIdentity, &capability); err != nil {
		t.Fatal(err)
	}
	server := EnrollmentServer{
		Admission: NewBootstrapAdmission(),
		Now:       func() time.Time { return now },
		Enroll: func(_ context.Context, cap BootstrapCapability) (EnrollmentResponse, error) {
			if cap.GroupID != groupID || cap.RosterHead != rosterLog.Head() {
				return EnrollmentResponse{}, errors.New("stale bootstrap checkpoint")
			}
			subject := mustNodeInfo(t, cap.TargetPublicKey)
			entry, err := rosterLog.SignEntry(founderIdentity, "add", subject, nil, 2_000)
			if err != nil {
				return EnrollmentResponse{}, err
			}
			if err := rosterLog.Apply(entry); err != nil {
				return EnrollmentResponse{}, err
			}
			return EnrollmentResponse{RosterHead: rosterLog.Head(), Entries: rosterLog.Entries()}, nil
		},
	}
	if err := server.Install(founderHost); err != nil {
		t.Fatal(err)
	}
	response, err := Enroll(ctx, targetHost, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}, capability)
	if err != nil {
		t.Fatal(err)
	}
	if response.RosterHead != rosterLog.Head() || !rosterLog.IsMemberID(targetBinding.MemberID) {
		t.Fatalf("enrollment response=%+v members=%v", response, rosterLog.MemberIDs())
	}
	targetRoster := roster.New(groupID)
	for index, entry := range response.Entries {
		if index == 0 {
			if err := targetRoster.AcceptGenesis(entry); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := targetRoster.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	targetStore := store.NewMemory()
	defer targetStore.Close()
	targetLive, err := NewLiveGroup(ctx, LiveConfig{Host: targetHost, GroupID: groupID, Roster: targetRoster, Store: targetStore})
	if err != nil {
		t.Fatal(err)
	}
	defer targetLive.Close()
	_ = targetHost.Network().ClosePeer(founderHost.ID())
	if err := targetHost.Connect(ctx, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	message := signedAcceptedLiveMessage(t, founderIdentity, *founderInfo.MemberID, rosterLog, groupID, 10_001, "enrolled-live")
	if _, err := founderLive.Publish(ctx, message); err != nil {
		t.Fatal(err)
	}
	waitForStoredMessage(t, ctx, targetStore, groupID, message.ID)
	if _, err := Enroll(ctx, targetHost, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}, capability); err == nil {
		t.Fatal("replayed enrollment capability succeeded")
	}
}

func TestEnrollmentCapabilityRetriesAfterCallbackFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	targetIdentity := mustIdentity(t)
	founderHost, _, err := NewHost(ctx, founderIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()
	targetHost, targetBinding, err := NewHost(ctx, targetIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer targetHost.Close()
	groupID := entmoot.GroupID{2}
	founderInfo := mustNodeInfo(t, founderIdentity.PublicKey)
	now := time.Now()
	capability := BootstrapCapability{
		GroupID:         groupID,
		TargetPublicKey: targetIdentity.PublicKey,
		TargetMemberID:  targetBinding.MemberID,
		TargetPeerID:    targetBinding.PeerID.String(),
		Founder:         founderInfo,
		AllowedPeerIDs:  []string{founderHost.ID().String()},
		IssuedAtMS:      now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     now.Add(time.Minute).UnixMilli(),
	}
	capability.Nonce[0] = 2
	if err := SignBootstrapCapability(founderIdentity, &capability); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := EnrollmentServer{
		Admission: NewBootstrapAdmission(),
		Now:       func() time.Time { return now },
		Enroll: func(context.Context, BootstrapCapability) (EnrollmentResponse, error) {
			calls++
			if calls == 1 {
				return EnrollmentResponse{}, errors.New("temporary persistence failure")
			}
			return EnrollmentResponse{RosterHead: capability.RosterHead}, nil
		},
	}
	if err := server.Install(founderHost); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}
	if _, err := Enroll(ctx, targetHost, remote, capability); err == nil {
		t.Fatal("first enrollment unexpectedly succeeded")
	}
	if _, err := Enroll(ctx, targetHost, remote, capability); err != nil {
		t.Fatalf("retry after callback failure: %v", err)
	}
	if _, err := Enroll(ctx, targetHost, remote, capability); err == nil {
		t.Fatal("committed capability replay succeeded")
	}
	if calls != 2 {
		t.Fatalf("enrollment callback called %d times, want 2", calls)
	}
}

func TestEnrollmentCommitFailureReleasesReservationForIdempotentRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	targetIdentity := mustIdentity(t)
	founderHost, _, err := NewHost(ctx, founderIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()
	targetHost, targetBinding, err := NewHost(ctx, targetIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer targetHost.Close()
	now := time.Now()
	capability := BootstrapCapability{
		GroupID:         entmoot.GroupID{3},
		TargetPublicKey: targetIdentity.PublicKey,
		TargetMemberID:  targetBinding.MemberID,
		TargetPeerID:    targetBinding.PeerID.String(),
		Founder:         mustNodeInfo(t, founderIdentity.PublicKey),
		AllowedPeerIDs:  []string{founderHost.ID().String()},
		IssuedAtMS:      now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     now.Add(time.Minute).UnixMilli(),
	}
	capability.Nonce[0] = 3
	if err := SignBootstrapCapability(founderIdentity, &capability); err != nil {
		t.Fatal(err)
	}
	admission := NewBootstrapAdmission()
	reserved := false
	used := false
	commitCalls := 0
	releaseCalls := 0
	admission.reserve = func(capabilityKey) (bool, error) {
		if reserved || used {
			return false, nil
		}
		reserved = true
		return true, nil
	}
	admission.release = func(capabilityKey) error {
		releaseCalls++
		reserved = false
		return nil
	}
	admission.commit = func(capabilityKey) error {
		commitCalls++
		if commitCalls == 1 {
			return errors.New("temporary admission store failure")
		}
		reserved = false
		used = true
		return nil
	}
	enrollCalls := 0
	server := EnrollmentServer{
		Admission: admission,
		Now:       func() time.Time { return now },
		Enroll: func(context.Context, BootstrapCapability) (EnrollmentResponse, error) {
			enrollCalls++
			return EnrollmentResponse{}, nil
		},
	}
	if err := server.Install(founderHost); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}
	if _, err := Enroll(ctx, targetHost, remote, capability); err == nil {
		t.Fatal("enrollment unexpectedly succeeded when admission commit failed")
	}
	if _, err := Enroll(ctx, targetHost, remote, capability); err != nil {
		t.Fatalf("retry after admission commit failure: %v", err)
	}
	if _, err := Enroll(ctx, targetHost, remote, capability); err == nil {
		t.Fatal("committed capability replay succeeded")
	}
	if releaseCalls != 1 || enrollCalls != 2 {
		t.Fatalf("release calls = %d, enrollment calls = %d; want 1 and 2", releaseCalls, enrollCalls)
	}
}
