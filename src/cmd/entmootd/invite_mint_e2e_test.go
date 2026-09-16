package main

import (
	"context"
	"crypto/rand"
	"net"
	"testing"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"

	"github.com/libp2p/go-libp2p"
)

// TestDaemonMintWithNoNamedAddressesStaysRedeemable drives the real mint over
// a real IPC connection with an EMPTY bootstrap list — the exact request the
// ESP's group_create open-invite path produces — and asserts the capability it
// returns is one a joiner can actually send.
//
// This is the assertion the review asked for. Until it existed, the whole
// suite passed with the fill bound reverted: nothing called
// (*ipcServer).handleInviteCreate, so the bound that keeps a multi-homed
// host's 30 addresses out of a capability was untested, and the only net was
// the mint's own size refusal — which turns the bug into a refused invite
// rather than a working one.
func TestDaemonMintWithNoNamedAddressesStaysRedeemable(t *testing.T) {
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

	// A multi-homed host: the defect only shows when the daemon has more
	// addresses than a capability may carry, which is the ordinary case on a
	// real machine (the review measured 30) and never the case for a test
	// host listening once.
	listen := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		listen = append(listen, "/ip4/127.0.0.1/tcp/0")
	}
	host, binding0, err := libp2ptransport.NewHost(ctx, founder, libp2p.ListenAddrStrings(listen...))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close()
	if len(host.Addrs()) <= maxInviteFallbackAddrs {
		t.Fatalf("host reports %d addresses with %d listeners, want more than the %d an invite carries: "+
			"the fixture must outgrow the bound or it proves nothing", len(host.Addrs()), len(listen), maxInviteFallbackAddrs)
	}
	messages, err := store.OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer messages.Close()
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: founder, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: host, Binding: binding0, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer runtime.Close()
	if _, _, err := runtime.AddLocalGroup(ctx, gid); err != nil {
		t.Fatalf("AddLocalGroup: %v", err)
	}

	binding, err := libp2ptransport.BindingFromPublicKey(founderInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	_ = binding0
	server := &ipcServer{
		memberID: binding.MemberID,
		peerID:   binding.PeerID.String(),
		identity: founder,
		dataDir:  root,
		runtime:  runtime,
	}

	client, daemon := net.Pipe()
	defer client.Close()
	go func() {
		defer daemon.Close()
		server.handleInviteCreate(ctx, daemon, &ipc.InviteCreateReq{
			GroupID: gid,
			Open:    true,
			MaxUses: 1,
			// No BootstrapMultiaddrs: the daemon fills its own addresses,
			// which is the path that had no bound.
		})
	}()

	msgType, decoded, err := ipc.ReadAndDecode(client)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if frame, ok := decoded.(*ipc.ErrorFrame); ok {
		t.Fatalf("the mint refused its own invite: %s: %s", frame.Code, frame.Message)
	}
	resp, ok := decoded.(*ipc.InviteCreateResp)
	if !ok {
		t.Fatalf("response type %v is %T, want an invite", msgType, decoded)
	}
	if len(resp.Capability.AllowedMultiaddrs) == 0 {
		t.Fatal("the daemon named no address, so the invite cannot be redeemed at all")
	}
	if len(resp.Capability.AllowedMultiaddrs) > maxInviteFallbackAddrs {
		t.Fatalf("the daemon attached %d of its %d addresses, over the %d a capability may carry",
			len(resp.Capability.AllowedMultiaddrs), len(host.Addrs()), maxInviteFallbackAddrs)
	}
	if size, tooLarge := libp2ptransport.CapabilityTooLarge(resp.Capability); tooLarge {
		t.Fatalf("the minted capability is %d bytes, over the %d a joiner can send: the daemon's own address fill is unbounded",
			size, libp2ptransport.MaxCapabilityBytes)
	}
}
