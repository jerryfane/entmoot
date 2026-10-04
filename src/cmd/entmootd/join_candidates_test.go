package main

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// A stalled connection accepts TCP and never answers, so each dial of it
// lasts libp2p's whole per-dial timeout (5s on loopback, 15s for public
// addresses) however long the join's own budget is. A join has to survive one
// within its budget: through a later address when the invite lists several,
// as it does with other members' fallbacks, and through a fresh connection
// when the only address recovers, as serve does on its next round.
func TestAddCapabilityJoinsPastStalledConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		// addresses returns the invite's addresses for a founder listening
		// at founderTCP, and counts the stalled connections it hands out.
		addresses func(t *testing.T, founderTCP string, stalls *atomic.Int32) []string
	}{
		{
			name: "stalled_first_address_healthy_second",
			addresses: func(t *testing.T, founderTCP string, stalls *atomic.Int32) []string {
				return []string{tcpMultiaddr(t, stallingListener(t, "", stalls)), tcpMultiaddr(t, founderTCP)}
			},
		},
		{
			name: "only_address_stalls_once",
			addresses: func(t *testing.T, founderTCP string, stalls *atomic.Int32) []string {
				return []string{tcpMultiaddr(t, stallingListener(t, founderTCP, stalls))}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			gid := daemonTestGroupID(0x5d)
			founder, _ := mustDaemonIdentity(t)
			joiner, joinerInfo := mustDaemonIdentity(t)
			founderRoot := t.TempDir()
			mustCreateGroup(t, founderRoot, gid, founder, membership.DefaultPolicy())
			founderRuntime, founderSession, founderHost := startTestRuntime(t, ctx, founderRoot, founder, gid)
			defer founderRuntime.Close()
			defer founderHost.Close()
			founderTCP, err := founderHost.Addrs()[0].ValueForProtocol(multiaddr.P_TCP)
			if err != nil {
				t.Fatal(err)
			}

			var stalls atomic.Int32
			capability := mustBootstrapInvite(t, founderSession.group, founder, joinerInfo, founderHost.ID().String())
			capability.AllowedMultiaddrs = nil
			for _, address := range tc.addresses(t, "127.0.0.1:"+founderTCP, &stalls) {
				capability.AllowedMultiaddrs = append(capability.AllowedMultiaddrs, address+"/p2p/"+founderHost.ID().String())
			}
			if err := membership.SignInvite(founder, &capability); err != nil {
				t.Fatalf("SignInvite: %v", err)
			}

			joinerHost, joinerBinding, err := libp2ptransport.NewHost(ctx, joiner, libp2p.NoListenAddrs)
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer joinerHost.Close()
			joinerRoot := t.TempDir()
			messages, err := store.OpenSQLite(joinerRoot)
			if err != nil {
				t.Fatalf("OpenSQLite: %v", err)
			}
			defer messages.Close()
			joinerRuntime, err := newGroupRuntime(groupRuntimeConfig{
				Identity: joiner, DataDir: joinerRoot, Store: messages, Notify: newNotifyingStore(messages, nil),
				Host: joinerHost, Binding: joinerBinding, Mode: libp2ptransport.DirectConnectivity,
			})
			if err != nil {
				t.Fatalf("newGroupRuntime: %v", err)
			}
			defer joinerRuntime.Close()

			joinCtx, joinCancel := context.WithTimeout(ctx, 25*time.Second)
			defer joinCancel()
			if _, _, err := joinerRuntime.AddCapability(joinCtx, capability); err != nil {
				t.Fatalf("AddCapability: %v", err)
			}
			if stalls.Load() == 0 {
				t.Fatal("no connection stalled; the fixture did not exercise a stall")
			}
			if !founderSession.group.IsMemberID(*joinerInfo.MemberID) {
				t.Fatal("the founder did not admit the joiner")
			}
		})
	}
}

// stallingListener returns a loopback address whose connections are held
// open without a byte in reply. With a forward target, only the first one is;
// later connections are relayed to forward untouched.
func stallingListener(t *testing.T, forward string, stalls *atomic.Int32) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		var held []net.Conn
		defer func() {
			for _, conn := range held {
				_ = conn.Close()
			}
		}()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if forward == "" || stalls.Load() == 0 {
				stalls.Add(1)
				held = append(held, conn)
				continue
			}
			go func() {
				defer conn.Close()
				upstream, err := net.Dial("tcp", forward)
				if err != nil {
					return
				}
				defer upstream.Close()
				go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close() }()
				_, _ = io.Copy(conn, upstream)
			}()
		}
	}()
	return listener.Addr().String()
}

func tcpMultiaddr(t *testing.T, hostPort string) string {
	t.Helper()
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		t.Fatal(err)
	}
	return "/ip4/" + host + "/tcp/" + port
}
