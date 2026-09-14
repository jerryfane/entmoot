package main

import (
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

func TestDaemonHostConfigSupportsRelayOnly(t *testing.T) {
	relayIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	relayPeerID, err := entmoot.PeerIDFromPublicKey(relayIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	relayAddress := "/ip4/127.0.0.1/tcp/41004/p2p/" + relayPeerID
	config, err := daemonHostConfig(&globalFlags{
		connectivity:     "relay-only",
		controlledRelays: stringListFlag{relayAddress},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.Mode != libp2ptransport.RelayOnlyConnectivity || len(config.ControlledRelays) != 1 || config.ControlledRelays[0].ID.String() != relayPeerID {
		t.Fatalf("relay-only host config = %+v", config)
	}
	if len(config.ListenAddrs) != 0 {
		t.Fatalf("relay-only host exposed direct listen addresses: %v", config.ListenAddrs)
	}
}

func TestDaemonHostConfigRejectsUncontrolledRelayOnly(t *testing.T) {
	if _, err := daemonHostConfig(&globalFlags{connectivity: "relay-only"}); err == nil {
		t.Fatal("relay-only daemon config accepted no controlled relay")
	}
}

func TestDaemonHostConfigKeepsDirectProfileHolePunchRelays(t *testing.T) {
	relayIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	relayPeerID, err := entmoot.PeerIDFromPublicKey(relayIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	config, err := daemonHostConfig(&globalFlags{
		connectivity:     "direct",
		listenPort:       41005,
		controlledRelays: stringListFlag{"/ip4/127.0.0.1/tcp/41004/p2p/" + relayPeerID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.Mode != libp2ptransport.DirectConnectivity || len(config.ListenAddrs) != 1 {
		t.Fatalf("direct host config = %+v", config)
	}
	if len(config.ControlledRelays) != 1 || config.ControlledRelays[0].ID.String() != relayPeerID {
		t.Fatalf("direct profile discarded its hole-punch relay: %+v", config.ControlledRelays)
	}
}

func TestDaemonHostConfigRejectsMalformedRelayInDirectProfile(t *testing.T) {
	if _, err := daemonHostConfig(&globalFlags{
		connectivity:     "direct",
		listenPort:       41006,
		controlledRelays: stringListFlag{"/ip4/127.0.0.1/tcp/41004"},
	}); err == nil {
		t.Fatal("direct profile accepted a relay address without a peer id")
	}
}

func TestEnrollmentRetryReturnsExistingMatchingMembership(t *testing.T) {
	founderIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founderIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	targetBinding, err := libp2ptransport.BindingFromPublicKey(targetIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	groupID := entmoot.GroupID{9}
	groupRoster := roster.New(groupID)
	founder := entmoot.NodeInfo{
		MemberID: &founderBinding.MemberID, PeerID: founderBinding.PeerID.String(),
		EntmootPubKey: founderIdentity.PublicKey,
	}
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	inviteHead := groupRoster.Head()
	target := entmoot.NodeInfo{
		MemberID: &targetBinding.MemberID, PeerID: targetBinding.PeerID.String(),
		EntmootPubKey: targetIdentity.PublicKey,
	}
	add, err := groupRoster.SignEntry(founderIdentity, "add", target, nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := groupRoster.Apply(add); err != nil {
		t.Fatal(err)
	}
	enrolledHead := groupRoster.Head()
	enrolledCount := len(groupRoster.Entries())
	runtime := &groupRuntime{
		identity: founderIdentity, binding: founderBinding,
		sessions: map[entmoot.GroupID]*groupSession{groupID: {groupID: groupID, roster: groupRoster}},
	}
	response, err := runtime.enroll(nil, entmoot.BootstrapCapability{
		GroupID: groupID, Founder: founder, RosterHead: inviteHead,
		TargetPublicKey: targetIdentity.PublicKey, TargetMemberID: targetBinding.MemberID,
		TargetPeerID: targetBinding.PeerID.String(),
	}, target)
	if err != nil {
		t.Fatalf("idempotent enrollment retry failed: %v", err)
	}
	if groupRoster.Head() != enrolledHead || len(groupRoster.Entries()) != enrolledCount {
		t.Fatalf("idempotent enrollment mutated roster: head=%s entries=%d", groupRoster.Head(), len(groupRoster.Entries()))
	}
	if response.RosterHead != enrolledHead || len(response.Entries) != enrolledCount {
		t.Fatalf("idempotent enrollment response = %+v", response)
	}
}
