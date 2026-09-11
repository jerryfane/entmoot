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
)

func TestFreshMemberEnrollsAcrossLibp2pWithoutPilot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	targetIdentity := mustIdentity(t)
	founderHost, founderBinding, err := NewHost(ctx, founderIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
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
	founderInfo := entmoot.NodeInfo{EntmootPubKey: founderIdentity.PublicKey, MemberID: &founderBinding.MemberID}
	rosterLog := roster.New(groupID)
	if err := rosterLog.Genesis(founderIdentity, founderInfo, 1_000); err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(10_000)
	capability := BootstrapCapability{
		GroupID:         groupID,
		TargetPublicKey: targetIdentity.PublicKey,
		TargetMemberID:  targetBinding.MemberID,
		TargetPeerID:    targetBinding.PeerID.String(),
		Founder:         founderInfo,
		RosterHead:      rosterLog.Head(),
		AllowedPeerIDs:  []string{targetBinding.PeerID.String()},
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
		Enroll: func(_ context.Context, cap BootstrapCapability) (string, error) {
			if cap.GroupID != groupID || cap.RosterHead != rosterLog.Head() {
				return "", errors.New("stale bootstrap checkpoint")
			}
			subject := entmoot.NodeInfo{EntmootPubKey: cap.TargetPublicKey, MemberID: &cap.TargetMemberID}
			entry, err := rosterLog.SignEntry(founderIdentity, "add", subject, nil, 0, 2_000)
			if err != nil {
				return "", err
			}
			if err := rosterLog.Apply(entry); err != nil {
				return "", err
			}
			return rosterLog.Head().String(), nil
		},
	}
	if err := server.Install(founderHost); err != nil {
		t.Fatal(err)
	}
	response, err := Enroll(ctx, targetHost, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}, capability)
	if err != nil {
		t.Fatal(err)
	}
	if response.RosterHead != rosterLog.Head().String() || !rosterLog.IsMemberID(targetBinding.MemberID) {
		t.Fatalf("enrollment response=%+v members=%v", response, rosterLog.MemberIDs())
	}
	if _, err := Enroll(ctx, targetHost, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}, capability); err == nil {
		t.Fatal("replayed enrollment capability succeeded")
	}
}
