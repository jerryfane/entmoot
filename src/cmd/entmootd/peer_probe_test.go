package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// TestProbeReportsReachabilityAndItsAbsence pins the two answers an operator
// acts on, at the runtime: a member with no address this host has ever seen,
// and a member with addresses that answer nothing. The IPC handler and client
// are covered separately by TestProbeOverTheControlSocket.
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

// TestProbeOverTheControlSocket covers the daemon half: the dispatch in
// handleConn, handlePeerProbe, and probePeersOverIPC talking to it over a real
// unix socket. Without this, deleting the dispatch case left the whole suite
// green - the CLI test only reaches applyPeerProbe, which fails the same way
// whether the daemon lacks the frame or the socket is absent.
func TestProbeOverTheControlSocket(t *testing.T) {
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

	absent, absentInfo := mustDaemonIdentity(t)
	if _, err := session.group.SignRecord(absent, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("join: %v", err)
	}

	binding, err := libp2ptransport.BindingFromPublicKey(founderInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	sockPath := filepath.Join(root, "probe.sock")
	server := &ipcServer{
		memberID:          binding.MemberID,
		peerID:            binding.PeerID.String(),
		identity:          founder,
		dataDir:           root,
		controlSocketPath: sockPath,
		runtime:           runtime,
	}
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go server.acceptLoop(ctx, listener)

	resp, err := probePeersOverIPC(ctx, sockPath, &ipc.PeerProbeReq{GroupID: gid, BudgetMS: 2000}, 2*time.Second)
	if err != nil {
		t.Fatalf("probePeersOverIPC: %v", err)
	}
	if resp.Status != "probed" {
		t.Fatalf("status = %q, want probed", resp.Status)
	}
	if resp.GroupID != gid {
		t.Fatalf("response names group %s, want %s", resp.GroupID, gid)
	}
	if len(resp.Peers) != 1 {
		t.Fatalf("got %d rows over the socket, want one per other member: %+v", len(resp.Peers), resp.Peers)
	}
	if resp.Peers[0].MemberID != *absentInfo.MemberID {
		t.Fatalf("row names %s, want the other member %s", resp.Peers[0].MemberID, absentInfo.MemberID)
	}
	if resp.Peers[0].Reachable || resp.Peers[0].Answered {
		t.Fatalf("a member with no known address was reported as answering: %+v", resp.Peers[0])
	}

	// An unknown group is an error frame, not an empty success: a caller must
	// not read "no peers" into "no such group".
	var other entmoot.GroupID
	other[0] = 9
	if _, err := probePeersOverIPC(ctx, sockPath, &ipc.PeerProbeReq{GroupID: other, BudgetMS: 500}, time.Second); err == nil {
		t.Fatal("probing a group this daemon has not joined succeeded")
	}
}

// TestPeersProbeSaysWhenNoProbeRan pins the peers path. It marshals only the
// peer array, so a probe that never ran left its output byte-identical to no
// flag at all - the operator saw membership rows and read them as health.
func TestPeersProbeSaysWhenNoProbeRan(t *testing.T) {
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

	code, plainOut, plainErr := captureCommandOutput(t, func() int {
		return cmdPeers(gf, []string{"-group", gid.String(), "-json"})
	})
	if code != exitOK {
		t.Fatalf("peers exit %d, stderr %q", code, plainErr)
	}
	code, probedOut, probedErr := captureCommandOutput(t, func() int {
		return cmdPeers(gf, []string{"-group", gid.String(), "-json", "-probe"})
	})
	if code != exitOK {
		t.Fatalf("peers -probe exit %d, stderr %q", code, probedErr)
	}
	if probedOut != plainOut {
		t.Fatalf("the documented JSON shape changed under -probe:\n without: %s\n with:    %s", plainOut, probedOut)
	}
	if probedErr == plainErr {
		t.Fatalf("peers -probe said nothing the plain form did not, so the flag is invisible: %q", probedErr)
	}
	if !strings.Contains(probedErr, "runtime_unavailable") {
		t.Fatalf("peers -probe without a daemon must say why: %q", probedErr)
	}
}

// TestProbeAgainstALiveServingPeer covers the answer path against a real
// second host, which the address-less fixtures above cannot: a member that
// serves the group answers, and one that serves a group this node is not in
// answers AND refuses. The difference matters because a removed node sees
// refusals from every peer, and calling that unreachable sends its operator
// hunting a firewall that is working.
func TestProbeAgainstALiveServingPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// One group, two members: A probes B, which serves it.
	rootA, rootB := t.TempDir(), t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		t.Fatal(err)
	}
	policy := membership.DefaultPolicy()
	policy.JoinRule = membership.JoinRuleOpen
	mustCreateGroup(t, rootA, gid, founder, policy)

	runtimeA, sessionA, hostA := startTestRuntime(t, ctx, rootA, founder, gid)
	defer hostA.Close()
	defer runtimeA.Close()
	joinRecord, err := sessionA.group.SignRecord(member, membership.Record{Kind: membership.KindJoin})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	// B adopts A's checkpoint 0, which is exactly how a joining node gets the
	// group, so B projects the same membership and serves A.
	adopted, err := membership.Adopt(rootB, sessionA.group.Canonical())
	if err != nil {
		t.Fatalf("Adopt on B: %v", err)
	}
	// B's own join is what makes it a member, and its runtime refuses to serve
	// a group it is not in, so the record has to land before the host starts.
	if _, err := adopted.Apply(joinRecord); err != nil {
		t.Fatalf("apply join on B: %v", err)
	}
	if err := adopted.Close(); err != nil {
		t.Fatalf("close adopted group: %v", err)
	}
	runtimeB, _, hostB := startTestRuntime(t, ctx, rootB, member, gid)
	defer hostB.Close()
	defer runtimeB.Close()

	if err := persistGroupPeer(rootA, gid, peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatalf("persistGroupPeer: %v", err)
	}

	// B admits a third member that A has never heard of, so B's answer
	// carries a record A lacks. The probe must report reachability and adopt
	// none of it: that is the invariant the whole design rests on, and only a
	// peer serving something new can test it.
	sessionB, ok := runtimeB.Get(gid)
	if !ok {
		t.Fatal("B has no session for the group it just adopted")
	}
	third, thirdInfo := mustDaemonIdentity(t)
	if _, err := sessionB.group.SignRecord(third, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("third join on B: %v", err)
	}
	if !sessionB.group.IsMemberID(*thirdInfo.MemberID) {
		t.Fatal("B did not admit the third member, so its answer carries nothing new")
	}
	membersBefore := len(sessionA.group.MemberIDs())
	pendingBefore := len(sessionA.group.Pending())
	checkpointBefore := sessionA.group.Canonical().ID

	results, _, err := runtimeA.probePeers(ctx, gid, 3*time.Second)
	if err != nil {
		t.Fatalf("probePeers: %v", err)
	}
	var served *ipc.PeerProbeResult
	for i := range results {
		if results[i].MemberID == *memberInfo.MemberID {
			served = &results[i]
		}
	}
	if served == nil {
		t.Fatalf("probe omitted the live member: %+v", results)
	}
	if !served.Reachable || !served.Answered {
		t.Fatalf("a live serving member was not reported reachable: %+v", served)
	}
	if served.Refusal != "" {
		t.Fatalf("a serving peer reported a refusal: %+v", served)
	}
	if sessionA.group.IsMemberID(*thirdInfo.MemberID) {
		t.Fatal("the probe adopted a member from the peer's answer: a probe must never be a second path into membership")
	}
	if got := len(sessionA.group.MemberIDs()); got != membersBefore {
		t.Fatalf("the probe changed A's member count from %d to %d", membersBefore, got)
	}
	if got := len(sessionA.group.Pending()); got != pendingBefore {
		t.Fatalf("the probe changed A's pending records from %d to %d", pendingBefore, got)
	}
	if got := sessionA.group.Canonical().ID; got != checkpointBefore {
		t.Fatalf("the probe advanced A's checkpoint from %s to %s", checkpointBefore, got)
	}

	// Now the refusal. A's own signed departure is applied on B and nowhere
	// else, which is the state a removed node is really in: its peers no
	// longer admit it while it still believes it is a member. B answers, and
	// the answer is a refusal.
	leave, err := membership.SignRecord(founder, membership.Record{
		Version:   membership.Version,
		GroupID:   gid,
		Kind:      membership.KindLeave,
		Actor:     founderInfo,
		Subject:   founderInfo,
		Timestamp: sessionA.group.Canonical().Timestamp + 10_000,
	})
	if err != nil {
		t.Fatalf("sign leave: %v", err)
	}
	if _, err := sessionB.group.Apply(leave); err != nil {
		t.Fatalf("apply leave on B: %v", err)
	}
	if sessionB.group.IsMemberID(*founderInfo.MemberID) {
		t.Fatal("B still holds A as a member, so it would not refuse")
	}
	if !sessionA.group.IsMemberID(*founderInfo.MemberID) {
		t.Fatal("A stopped believing it is a member, which is not the case under test")
	}

	results, _, err = runtimeA.probePeers(ctx, gid, 3*time.Second)
	if err != nil {
		t.Fatalf("probePeers after removal: %v", err)
	}
	var refused *ipc.PeerProbeResult
	for i := range results {
		if results[i].MemberID == *memberInfo.MemberID {
			refused = &results[i]
		}
	}
	if refused == nil {
		t.Fatalf("probe omitted the live member: %+v", results)
	}
	if refused.Reachable {
		t.Fatalf("a peer that refuses this node was reported reachable: %+v", refused)
	}
	if !refused.Answered {
		t.Fatalf("a peer that opened, served and refused was reported as silent, which sends an operator after a network fault: %+v", refused)
	}
	if refused.Refusal == "" {
		t.Fatalf("no refusal recorded, so nothing tells the operator this is a membership answer: %+v", refused)
	}
	if refused.Error != "" {
		t.Fatalf("a refusal must not also be reported as a transport error: %+v", refused)
	}
}

// TestRefusalTextIsBounded pins that a member cannot decide how large this
// daemon's answer is. The refusal field is the peer's own text and the
// membership response allows megabytes, so a handful of hostile rows would
// push the probe answer past the control socket's frame limit and destroy it
// entirely - the one part of the report an operator most needs when peers are
// refusing them.
func TestRefusalTextIsBounded(t *testing.T) {
	hostile := strings.Repeat("A", 3<<20)
	got := boundRefusal(hostile)
	if len(got) > maxProbeRefusalBytes+8 {
		t.Fatalf("a %d-byte refusal became %d bytes; the cap is %d", len(hostile), len(got), maxProbeRefusalBytes)
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("a truncated refusal must say it was truncated: %q", got)
	}
	if got := boundRefusal("not_member"); got != "not_member" {
		t.Fatalf("a real code must survive intact: %q", got)
	}
	if got := boundRefusal("unauthorized\n  serving\tnode"); got != "unauthorized serving node" {
		t.Fatalf("whitespace must collapse so one peer stays one line: %q", got)
	}
}

// TestProbeGivesEachPeerTheFloor pins the budget floor, which nothing else
// can: a loopback peer answers inside a millisecond, so a fast peer cannot
// tell a 500ms deadline from a 1ms one. A silent listener can. It accepts the
// connection and never writes, so the probe can only end at the deadline it
// handed out, and the elapsed time IS the deadline. Without the floor a peer
// gets a sub-millisecond deadline and is called unreachable for arithmetic
// reasons rather than network ones - and because `doctor -timeout`
// reaches probePeers unfiltered, the caller was able to ask for exactly that.
func TestProbeGivesEachPeerTheFloor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
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

	// Four silent members, not one: with a single peer only the
	// remaining-time clamp is reached, while several peers divide the budget
	// and exercise the per-slice floor as well.
	const silentMembers = 4
	var bindings []libp2ptransport.Binding
	for i := 0; i < silentMembers; i++ {
		silent, silentInfo := mustDaemonIdentity(t)
		if _, err := session.group.SignRecord(silent, membership.Record{Kind: membership.KindJoin}); err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
		binding, err := libp2ptransport.BindingFromPublicKey(silentInfo.EntmootPubKey)
		if err != nil {
			t.Fatal(err)
		}
		bindings = append(bindings, binding)
	}

	// A listener that accepts and never speaks: the libp2p handshake cannot
	// complete, so only the deadline ends the attempt.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Hold it open and write nothing.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	addr := listener.Addr().(*net.TCPAddr)
	for _, binding := range bindings {
		if err := persistGroupPeer(root, gid, peer.AddrInfo{
			ID:    binding.PeerID,
			Addrs: []multiaddr.Multiaddr{mustMultiaddr(t, fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", addr.Port))},
		}); err != nil {
			t.Fatalf("persistGroupPeer: %v", err)
		}
	}

	// A millisecond of budget across four peers. All three floors must lift
	// it: the budget, the per-peer slice, and the remaining-time clamp.
	started := time.Now()
	results, _, err := runtime.probePeers(ctx, gid, time.Millisecond)
	if err != nil {
		t.Fatalf("probePeers: %v", err)
	}
	elapsed := time.Since(started)
	if len(results) != silentMembers {
		t.Fatalf("got %d rows, want %d: %+v", len(results), silentMembers, results)
	}
	for _, result := range results {
		if result.Answered {
			t.Fatalf("a listener that never writes was reported as answering: %+v", result)
		}
	}
	// The whole floor, not half of it. Once the budget itself is clamped the
	// property is exact rather than best-effort: the earliest a worker can
	// finish is one floor after it starts, and a worker that is descheduled
	// past the deadline and reports "not attempted" can only do so once the
	// floor has already passed. So probePeers cannot return sooner than
	// minProbeSlice however starved the machine is - which is the point of
	// calling it a floor, and what a half-floor assertion could not say.
	if elapsed < minProbeSlice {
		t.Fatalf("the probe gave up after %s; a %s floor on the budget means it must wait at least that long before calling a peer unreachable",
			elapsed, minProbeSlice)
	}
}

// TestHostileRefusalCannotInflateTheAnswer drives the cap through the real
// path with a peer that answers a well-formed refusal whose error text is
// megabytes. Without the cap at the point the refusal is recorded, a few such
// members push the probe answer past the control socket's frame limit, and the
// operator gets nothing at all.
func TestHostileRefusalCannotInflateTheAnswer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
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

	hostile, hostileInfo := mustDaemonIdentity(t)
	if _, err := session.group.SignRecord(hostile, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatalf("join: %v", err)
	}
	hostileHost, _, err := libp2ptransport.NewHost(ctx, hostile, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer hostileHost.Close()

	// A member that speaks the protocol and answers every read with a refusal
	// whose text it chooses.
	shout := strings.Repeat("A", 2<<20)
	hostileHost.SetStreamHandler(libp2ptransport.MembershipProtocol, func(stream network.Stream) {
		defer stream.Close()
		var request libp2ptransport.MembershipSyncRequest
		if err := json.NewDecoder(io.LimitReader(stream, 1<<20)).Decode(&request); err != nil {
			return
		}
		payload, err := json.Marshal(libp2ptransport.MembershipSyncResponse{
			Version:   1,
			RequestID: request.RequestID,
			GroupID:   request.GroupID,
			Error:     libp2ptransport.SyncErrorCode(shout),
		})
		if err != nil {
			return
		}
		_, _ = stream.Write(append(payload, '\n'))
	})

	if err := persistGroupPeer(root, gid, peer.AddrInfo{ID: hostileHost.ID(), Addrs: hostileHost.Addrs()}); err != nil {
		t.Fatalf("persistGroupPeer: %v", err)
	}

	results, _, err := runtime.probePeers(ctx, gid, 3*time.Second)
	if err != nil {
		t.Fatalf("probePeers: %v", err)
	}
	var row *ipc.PeerProbeResult
	for i := range results {
		if results[i].MemberID == *hostileInfo.MemberID {
			row = &results[i]
		}
	}
	if row == nil {
		t.Fatalf("probe omitted the hostile member: %+v", results)
	}
	if !row.Answered {
		t.Fatalf("a peer that served a refusal was reported silent: %+v", row)
	}
	if len(row.Refusal) > maxProbeRefusalBytes+8 {
		t.Fatalf("the peer put %d bytes into this daemon's answer, cap is %d", len(row.Refusal), maxProbeRefusalBytes)
	}
	if len(row.Error) > maxProbeErrorBytes+64 {
		t.Fatalf("the error field carries %d bytes", len(row.Error))
	}

	// And the whole answer must still fit the control socket's frame.
	encoded, err := json.Marshal(&ipc.PeerProbeResp{Status: "probed", GroupID: gid, Peers: results})
	if err != nil {
		t.Fatalf("marshal probe response: %v", err)
	}
	if len(encoded) > ipc.MaxFrameSize {
		t.Fatalf("probe answer is %d bytes, over the %d-byte ipc frame", len(encoded), ipc.MaxFrameSize)
	}
}
