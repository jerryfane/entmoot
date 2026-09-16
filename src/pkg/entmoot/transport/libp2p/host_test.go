package libp2ptransport

import (
	"context"
	"io"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot/keystore"
)

func TestExistingIdentityHasStablePeerIDAcrossRestart(t *testing.T) {
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h1, binding1, err := NewHost(ctx, identity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	if err := h1.Close(); err != nil {
		t.Fatal(err)
	}
	h2, binding2, err := NewHost(ctx, identity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	if binding1 != binding2 || h2.ID() != binding1.PeerID {
		t.Fatalf("identity changed across restart: first=%+v second=%+v", binding1, binding2)
	}
	wrong := binding1.MemberID
	wrong[0] ^= 1
	if err := VerifyBinding(identity.PublicKey, wrong, binding1.PeerID); err == nil {
		t.Fatal("invalid roster-key/MemberID association accepted")
	}
}

func TestFreshHostsConnectWithoutPilot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	firstIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	secondIdentity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	first, firstBinding, err := NewHost(ctx, firstIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, secondBinding, err := NewHost(ctx, secondIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := VerifyBinding(firstIdentity.PublicKey, firstBinding.MemberID, first.ID()); err != nil {
		t.Fatal(err)
	}
	if err := VerifyBinding(secondIdentity.PublicKey, secondBinding.MemberID, second.ID()); err != nil {
		t.Fatal(err)
	}
	// An arbitrary protocol id: this test only needs two hosts to speak
	// something. It deliberately does not name a real Entmoot protocol.
	const probeProtocol = protocol.ID("/entmoot/host-test-probe/1")
	accepted := make(chan string, 1)
	first.SetStreamHandler(probeProtocol, func(stream network.Stream) {
		defer stream.Close()
		data, _ := io.ReadAll(io.LimitReader(stream, 64))
		accepted <- string(data)
	})
	if err := second.Connect(ctx, peer.AddrInfo{ID: first.ID(), Addrs: first.Addrs()}); err != nil {
		t.Fatalf("connect fresh hosts: %v", err)
	}
	stream, err := second.NewStream(ctx, first.ID(), probeProtocol)
	if err != nil {
		t.Fatalf("open probe stream: %v", err)
	}
	if _, err := stream.Write([]byte("bounded invite bootstrap")); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-accepted:
		if got != "bounded invite bootstrap" {
			t.Fatalf("bootstrap payload = %q", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
