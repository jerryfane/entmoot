package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// TestProbeReportsReachabilityAndItsAbsence pins the two answers an operator
// acts on, through the real handler and the real client over a unix socket:
// a member that answers a membership read, and a member that does not.
//
// Before -probe was wired, doctor reported membership only, so a group that
// would not converge looked identical to one that was healthy.
func TestProbeReportsReachabilityAndItsAbsence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, root, gid, founder, policy)

	runtime, session, host := startTestRuntime(t, ctx, root, founder, gid)
	defer host.Close()
	defer runtime.Close()

	// A second member with no address this host has ever seen: the honest
	// answer is "no address known", not "unreachable".
	absent, absentInfo := mustDaemonIdentity(t)
	if _, err := session.group.SignRecord(absent, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if !session.group.IsMemberID(*absentInfo.MemberID) {
		t.Fatal("the second member did not join, so there is nothing to probe")
	}

	results, incomplete, err := runtime.probePeers(ctx, gid, 2*time.Second)
	if err != nil {
		t.Fatalf("probePeers: %v", err)
	}
	if incomplete {
		t.Fatal("probe reported incomplete with one peer and a 2s budget")
	}

	for _, result := range results {
		if result.MemberID == *founderInfo.MemberID {
			t.Fatalf("the probe reported on this node, which answers nothing: %+v", result)
		}
	}
	if len(results) != 1 {
		t.Fatalf("probe returned %d rows, want one per other member: %+v", len(results), results)
	}
	other := &results[0]
	if other.Reachable {
		t.Fatalf("a member with no known address was reported reachable: %+v", other)
	}
	if other.Addresses != 0 {
		t.Fatalf("addresses = %d, want 0 for a member never seen", other.Addresses)
	}
	if !strings.Contains(other.Error, "no address known") {
		t.Fatalf("error = %q, want it to say no address is known rather than blame the peer", other.Error)
	}

	// Now a member this host does have addresses for, none of which answer.
	// A multi-homed peer's dial failure carries one line per address, so the
	// recorded error must be summarised where it is recorded, not only in the
	// helper that summarises it.
	unreachable, unreachableInfo := mustDaemonIdentity(t)
	if _, err := session.group.SignRecord(unreachable, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("join unreachable: %v", err)
	}
	binding, err := libp2ptransport.BindingFromPublicKey(unreachableInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	var addrs []multiaddr.Multiaddr
	for i := 0; i < 6; i++ {
		addrs = append(addrs, mustMultiaddr(t, fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 39500+i)))
	}
	if err := persistGroupPeer(root, gid, peer.AddrInfo{ID: binding.PeerID, Addrs: addrs}); err != nil {
		t.Fatalf("persistGroupPeer: %v", err)
	}

	results, _, err = runtime.probePeers(ctx, gid, 3*time.Second)
	if err != nil {
		t.Fatalf("probePeers: %v", err)
	}
	var dialed *ipc.PeerProbeResult
	for i := range results {
		if results[i].MemberID == *unreachableInfo.MemberID {
			dialed = &results[i]
		}
	}
	if dialed == nil {
		t.Fatalf("probe omitted the member with addresses: %+v", results)
	}
	if dialed.Reachable {
		t.Fatalf("nothing listens on those ports, yet the probe reported reachable: %+v", dialed)
	}
	if dialed.Addresses != len(addrs) {
		t.Fatalf("addresses = %d, want %d", dialed.Addresses, len(addrs))
	}
	if strings.Contains(dialed.Error, "\n") {
		t.Fatalf("recorded error spans lines, so a report pastes a wall of dial failures: %q", dialed.Error)
	}
	if !strings.Contains(dialed.Error, "addresses tried") {
		t.Fatalf("error = %q, want it to say how many addresses were tried", dialed.Error)
	}
}

// TestProbeErrorStaysReadable pins the summary. A failed dial to a multi-homed
// member carries one line per address; the raw error ran past 2 KB on a host
// with 28 interfaces, and that lands in JSON an operator greps.
func TestProbeErrorStaysReadable(t *testing.T) {
	var lines []string
	for i := 0; i < 28; i++ {
		lines = append(lines, fmt.Sprintf("  * [/ip4/10.0.%d.1/tcp/1004] dial tcp4 10.0.%d.1:1004: connect: connection refused", i, i))
	}
	raw := errors.New("libp2p: connect 12D3KooTest: failed to dial: all dials failed\n" + strings.Join(lines, "\n"))

	got := summarizeProbeError(raw, 28)
	if strings.Contains(got, "\n") {
		t.Fatalf("summary spans lines: %q", got)
	}
	if len(got) > maxProbeErrorBytes+40 {
		t.Fatalf("summary is %d bytes, want the first line plus the count", len(got))
	}
	if !strings.Contains(got, "all dials failed") {
		t.Fatalf("summary dropped the reason: %q", got)
	}
	if !strings.Contains(got, "28 addresses tried") {
		t.Fatalf("summary dropped how much was tried: %q", got)
	}
	if single := summarizeProbeError(errors.New("connection refused"), 1); single != "connection refused" {
		t.Fatalf("one address should not get a count: %q", single)
	}
	if summarizeProbeError(nil, 3) != "" {
		t.Fatal("no error must summarise to no text")
	}
}

// TestProbeStatusSaysWhyNoProbeRan pins the wiring that was missing for as
// long as the flag existed: without the daemon there is no host to dial with,
// and the report must say so rather than leave rows looking healthy.
func TestProbeStatusSaysWhyNoProbeRan(t *testing.T) {
	group := doctorGroupReport{
		Peers: []doctorPeerReport{
			{MemberID: entmoot.MemberID{1}},
			{MemberID: entmoot.MemberID{2}, Self: true},
		},
	}
	applyPeerProbe(context.Background(), &group, "/nonexistent/control.sock", false, time.Second)
	if !strings.Contains(group.ProbeStatus, "runtime_unavailable") {
		t.Fatalf("probe_status = %q, want it to name the missing daemon", group.ProbeStatus)
	}
	for _, peer := range group.Peers {
		if peer.Probe != nil {
			t.Fatalf("peer %s carries a probe result when no probe ran", peer.MemberID)
		}
	}

	// A daemon that is running but unreachable on its socket is a different
	// answer, and must not be reported as a peer failure either.
	group.ProbeStatus = ""
	applyPeerProbe(context.Background(), &group, "/nonexistent/control.sock", true, time.Second)
	if !strings.HasPrefix(group.ProbeStatus, "failed: ") {
		t.Fatalf("probe_status = %q, want the socket failure named", group.ProbeStatus)
	}
	for _, peer := range group.Peers {
		if peer.Probe != nil {
			t.Fatalf("peer %s carries a probe result when the probe never reached the daemon", peer.MemberID)
		}
	}
}

// TestDoctorProbeFlagIsWired pins the call site, not the probe. The -redact
// flag in this same command sat documented, tested and unwired for as long as
// it existed, because every test drove the helper instead of the flag. This
// one runs cmdDoctor and fails if the flag stops reaching applyPeerProbe.
func TestDoctorProbeFlagIsWired(t *testing.T) {
	root := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, root, gid, founder, policy)

	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(root, "identity.json")
	if err := identity.Save(identityPath); err != nil {
		t.Fatal(err)
	}
	gf := &globalFlags{data: root, identity: identityPath}

	code, plain, stderr := captureCommandOutput(t, func() int {
		return cmdDoctor(gf, []string{"-group", gid.String(), "-json"})
	})
	if code != exitOK {
		t.Fatalf("doctor -json exit %d, stderr %q", code, stderr)
	}
	if strings.Contains(plain, "probe_status") {
		t.Fatalf("doctor reported a probe status without -probe: %s", plain)
	}

	code, probed, stderr := captureCommandOutput(t, func() int {
		return cmdDoctor(gf, []string{"-group", gid.String(), "-json", "-probe"})
	})
	if code != exitOK {
		t.Fatalf("doctor -probe exit %d, stderr %q", code, stderr)
	}
	if !strings.Contains(probed, "probe_status") {
		t.Fatalf("doctor -probe reported no probe status, so the flag does not reach the probe: %s", probed)
	}
	// No daemon here, so the honest answer is why no probe ran - never silence.
	if !strings.Contains(probed, "runtime_unavailable") {
		t.Fatalf("doctor -probe without a daemon must say why: %s", probed)
	}
}
