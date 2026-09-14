package main

import (
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
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
