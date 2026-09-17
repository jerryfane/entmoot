package main

import "testing"

// The flag pair has to fail closed. An operator who types -relay-service and
// forgets the allowlist would otherwise get either an open relay for
// strangers or a silently disabled one.
func TestRelayServiceFlagsRequireEachOther(t *testing.T) {
	peerID := "12D3KooWPXb5rMPAHKYBc5Cwx9dbDhjm2Dsqwt8gFkGe6kGNCDSp"

	if _, err := daemonRelayService(&globalFlags{relayService: true}); err == nil {
		t.Fatal("-relay-service was accepted with no allowed peer")
	}
	if _, err := daemonRelayService(&globalFlags{relayAllowPeers: stringListFlag{peerID}}); err == nil {
		t.Fatal("-relay-allow-peer was accepted without -relay-service")
	}
	if _, err := daemonRelayService(&globalFlags{relayService: true, relayAllowPeers: stringListFlag{"not-a-peer-id"}}); err == nil {
		t.Fatal("a malformed peer id was accepted")
	}

	off, err := daemonRelayService(&globalFlags{})
	if err != nil || off != nil {
		t.Fatalf("the default is off: got %v err %v", off, err)
	}

	on, err := daemonRelayService(&globalFlags{relayService: true, relayAllowPeers: stringListFlag{peerID}})
	if err != nil {
		t.Fatalf("a well-formed pair was refused: %v", err)
	}
	if len(on.AllowedPeers) != 1 || on.AllowedPeers[0].String() != peerID {
		t.Fatalf("allowlist = %v", on.AllowedPeers)
	}
	// The caps are the ones `relay serve` uses, because it is the same service.
	if on.MaxReservations != 128 || on.MaxCircuitsPerPeer != 16 ||
		on.MaxReservationsPerIP != 8 || on.MaxReservationsPerASN != 32 || on.CircuitBytes != 64<<20 {
		t.Fatalf("caps = %+v", *on)
	}
}
