package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

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

// Flag misuse has to read as flag misuse. It is diagnosed before serve's group
// precondition, so a new node reports the typo rather than "no joined groups",
// and it never exits as a transport failure, which a supervisor would retry.
func TestRelayServiceFlagMisuseExitsInvalidArgument(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the daemon")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "entmootd")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	peerID := "12D3KooWPXb5rMPAHKYBc5Cwx9dbDhjm2Dsqwt8gFkGe6kGNCDSp"
	cases := map[string][]string{
		"allowlist without the flag": {"-relay-allow-peer", peerID},
		"flag without an allowlist":  {"-relay-service"},
		"malformed peer id":          {"-relay-service", "-relay-allow-peer", "not-a-peer-id"},
		"relay-only conflict":        {"-relay-service", "-relay-allow-peer", peerID, "-connectivity", "relay-only", "-controlled-relay", "/ip4/127.0.0.1/tcp/1/p2p/" + peerID},
	}
	for name, flags := range cases {
		data := filepath.Join(dir, "data-"+name)
		args := append([]string{"-identity", filepath.Join(data, "identity.json"), "-data", data, "-allow-new-identity"}, flags...)
		cmd := exec.Command(binary, append(args, "serve")...)
		cmd.Env = append(os.Environ(), "HERDR_SOCKET_PATH="+filepath.Join(dir, "no-herdr.sock"))
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if err == nil {
			t.Fatalf("%s: the daemon started\n%s", name, out)
		} else if !asExitError(err, &exit) {
			t.Fatalf("%s: %v", name, err)
		}
		if exit.ExitCode() != exitInvalidArgument {
			t.Fatalf("%s: exit %d, want %d (invalid argument)\n%s", name, exit.ExitCode(), exitInvalidArgument, out)
		}
	}
}

func asExitError(err error, target **exec.ExitError) bool {
	exit, ok := err.(*exec.ExitError)
	if ok {
		*target = exit
	}
	return ok
}
