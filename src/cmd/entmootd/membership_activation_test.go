package main

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"

	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// A founder upgrades a legacy group while its daemon runs. The daemon only
// retries groups still awaiting a checkpoint, so the upgrade itself starts the
// group there; otherwise it would stay dark until serve restarted.
func TestMembershipUpgradeStartsGroupInRunningDaemon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dataDir := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	_, memberInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0x2e)
	seedLegacyChain(t, dataDir, gid, founder, founderInfo, memberInfo)

	host, binding, err := libp2ptransport.NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })
	messages, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = messages.Close() })
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: founder, DataDir: dataDir, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: host, Binding: binding, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	t.Cleanup(runtime.Close)
	sockPath := controlSocketPath(dataDir)
	daemon := &ipcServer{
		memberID:          binding.MemberID,
		peerID:            binding.PeerID.String(),
		identity:          founder,
		dataDir:           dataDir,
		controlSocketPath: sockPath,
		runtime:           runtime,
	}
	listener, err := ipc.Listen(sockPath, "unix")
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go daemon.acceptLoop(ctx, listener)

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdMembership(daemonFlags(t, dataDir, founder), []string{"upgrade", "-group", gid.String()})
	})
	if code != exitOK {
		t.Fatalf("membership upgrade code = %d (%s)", code, stderr)
	}
	var upgraded map[string]any
	if err := json.Unmarshal([]byte(stdout), &upgraded); err != nil {
		t.Fatalf("membership upgrade stdout: %v\n%s", err, stdout)
	}
	if upgraded["status"] != "upgraded" || upgraded["daemon_activation"] != "activated" {
		t.Fatalf("membership upgrade output = %v, want upgraded and activated", upgraded)
	}
	if !slices.Contains(runtime.ActiveGroupIDs(), gid) {
		t.Fatalf("running daemon serves %v after the upgrade, want %s", runtime.ActiveGroupIDs(), gid)
	}
}
