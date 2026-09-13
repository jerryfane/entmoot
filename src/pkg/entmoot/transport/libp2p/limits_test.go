package libp2ptransport

import (
	"context"
	"io"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestConfiguredHostHardAdmissionBounds(t *testing.T) {
	for _, mode := range []string{"total connections", "peer connections", "peer streams"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			target, _, err := NewConfiguredHost(ctx, mustIdentity(t), HostConfig{Mode: DirectConnectivity, ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			target.SetStreamHandler("/entmoot/admission-regression/1", func(s network.Stream) {
				defer s.Close()
				var request [1]byte
				if _, err := io.ReadFull(s, request[:]); err != nil {
					return
				}
				if _, err := s.Write(request[:]); err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, s)
			})
			// Loopback deliberately avoids upstream per-IP protection masking the
			// configured global/per-peer limit. The target's limits are not overridden.
			remote := peer.AddrInfo{ID: target.ID(), Addrs: target.Addrs()}
			clients := make([]host.Host, 0, 65)
			defer func() {
				for _, client := range clients {
					_ = client.Close()
				}
			}()
			shared := mustIdentity(t)
			expected := 64
			if mode == "peer connections" {
				expected = 8
			}
			accepted := 0
			for attempt := 0; attempt <= expected; attempt++ {
				var client host.Host
				if mode == "peer streams" && len(clients) > 0 {
					client = clients[0]
				} else {
					identity := shared
					if mode == "total connections" {
						identity = mustIdentity(t)
					}
					client, _, err = NewHost(ctx, identity, libp2p.NoListenAddrs)
					if err != nil {
						t.Fatal(err)
					}
					clients = append(clients, client)
				}
				attemptCtx, stop := context.WithTimeout(ctx, 2*time.Second)
				err := client.Connect(attemptCtx, remote)
				if err == nil {
					var stream network.Stream
					stream, err = client.NewStream(attemptCtx, target.ID(), "/entmoot/admission-regression/1")
					if err == nil {
						_ = stream.SetDeadline(time.Now().Add(2 * time.Second))
						_, err = stream.Write([]byte{1})
						var ack [1]byte
						if err == nil {
							_, err = io.ReadFull(stream, ack[:])
						}
						if err == nil && ack[0] == 1 {
							accepted++
							_ = stream.SetDeadline(time.Time{})
						} else {
							_ = stream.Reset()
						}
					}
				}
				stop()
			}
			if accepted != expected {
				t.Fatalf("acknowledged %d %s; want exactly %d and denial above the bound", accepted, mode, expected)
			}
			if connections := len(target.Network().Conns()); connections > 64 {
				t.Fatalf("target retained %d connections", connections)
			}
			for _, id := range target.Network().Peers() {
				if connections := len(target.Network().ConnsToPeer(id)); connections > 8 {
					t.Fatalf("target retained %d connections to %s", connections, id)
				}
			}
		})
	}
}
