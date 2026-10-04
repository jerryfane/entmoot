package libp2ptransport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store/storetest"
)

// ProxyFromEnvironment and system certificate roots are process-cached. Each
// client runs in a fresh process, just as a cloud job with a new proxy port does.
// The public destination is TEST-NET, never a routable peer: CONNECT is the only
// successful path to the TLS reverse proxy and the real libp2p WS listener.
func TestWSSJoinThroughEnvironmentProxy(t *testing.T) {
	if fixture := os.Getenv("ENTMOOT_WSS_TEST_FIXTURE"); fixture != "" {
		runWSSProxyClient(t, fixture)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	founder := mustIdentity(t)
	h, _, err := NewConfiguredHost(ctx, founder, HostConfig{ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0/ws"}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	gid, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	messages := storetest.New(t)
	defer messages.Close()
	server := SyncServer{Host: h, Store: messages, Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == gid }}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	received := make(chan entmoot.Message, 4)
	live, err := NewLiveGroup(ctx, LiveConfig{Host: h, GroupID: gid, Group: group, Store: messages, OnIngest: func(m entmoot.Message) { received <- m }})
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	var port string
	for _, address := range h.Network().ListenAddresses() {
		if value, err := address.ValueForProtocol(ma.P_TCP); err == nil {
			port = value
			break
		}
	}
	if port == "" {
		t.Fatal("WS TCP listener missing")
	}
	target, _ := url.Parse("http://127.0.0.1:" + port)
	direct := httputil.NewSingleHostReverseProxy(target)
	stalled := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: stallingRelay(t, target.Host)})
	var stallMode atomic.Bool
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stallMode.Load() {
			stalled.ServeHTTP(w, r)
			return
		}
		direct.ServeHTTP(w, r)
	}))
	defer tlsServer.Close()
	certFile := filepath.Join(t.TempDir(), "root.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tlsServer.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"HTTPS_PROXY", "https_proxy", "no_proxy", "untrusted_tls", "wrong_peer", "proxy_refused", "stalled_first_handshake"} {
		t.Run(mode, func(t *testing.T) {
			stallMode.Store(mode == "stalled_first_handshake")
			var tunnels atomic.Int32
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodConnect || r.Host != "example.com:443" {
					http.Error(w, "unexpected CONNECT destination", 400)
					return
				}
				if mode == "proxy_refused" {
					http.Error(w, "refused", http.StatusForbidden)
					return
				}
				upstream, err := net.DialTimeout("tcp", tlsServer.Listener.Addr().String(), 3*time.Second)
				if err != nil {
					http.Error(w, err.Error(), 502)
					return
				}
				downstream, rw, err := w.(http.Hijacker).Hijack()
				if err != nil {
					upstream.Close()
					return
				}
				defer downstream.Close()
				defer upstream.Close()
				if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
					return
				}
				if err := rw.Flush(); err != nil {
					return
				}
				tunnels.Add(1)
				go func() { _, _ = io.Copy(upstream, rw); upstream.Close() }()
				_, _ = io.Copy(downstream, upstream)
			}))
			defer proxy.Close()
			dir := t.TempDir()
			joiner := mustIdentity(t)
			if err := joiner.Save(filepath.Join(dir, "identity.json")); err != nil {
				t.Fatal(err)
			}
			remote := h.ID()
			if mode == "wrong_peer" {
				b, err := BindingFromPublicKey(mustIdentity(t).PublicKey)
				if err != nil {
					t.Fatal(err)
				}
				remote = b.PeerID
			}
			f := wssProxyFixture{Remote: remote.String(), Mode: mode, Invite: mustInvite(t, group, founder, joiner.PublicKey, 0, []string{h.ID().String()})}
			raw, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			fixture := filepath.Join(dir, "fixture.json")
			if err := os.WriteFile(fixture, raw, 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWSSJoinThroughEnvironmentProxy$", "-test.v")
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				switch strings.ToLower(key) {
				case "http_proxy", "https_proxy", "no_proxy", "all_proxy", "ssl_cert_file", "ssl_cert_dir", "entmoot_wss_test_fixture":
					continue
				}
				command.Env = append(command.Env, entry)
			}
			proxyKey := "HTTPS_PROXY"
			if mode == "https_proxy" {
				proxyKey = mode
			}
			command.Env = append(command.Env, "ENTMOOT_WSS_TEST_FIXTURE="+fixture, proxyKey+"="+proxy.URL, "SSL_CERT_DIR="+dir)
			if mode != "untrusted_tls" {
				command.Env = append(command.Env, "SSL_CERT_FILE="+certFile)
			}
			if mode == "no_proxy" {
				command.Env = append(command.Env, "NO_PROXY=example.com")
			}
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("client: %v\n%s", err, output)
			}
			if wssProxyJoinSucceeds(mode) {
				if tunnels.Load() == 0 {
					t.Fatal("join did not traverse CONNECT proxy")
				}
				// The first tunnel stalled until the libp2p dial gave up; only a
				// second connection inside the join's own deadline can have joined.
				if mode == "stalled_first_handshake" && tunnels.Load() < 2 {
					t.Fatalf("join used %d tunnel(s); want a fresh one after the stall", tunnels.Load())
				}
				select {
				case m := <-received:
					if string(m.Content) != "signed through CONNECT" || !bytes.Equal(m.Author.EntmootPubKey, joiner.PublicKey) {
						t.Fatalf("wrong author/content: %+v", m)
					}
					if err := signing.VerifyMessage(m, m.Author); err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("signed message never reached remote group")
				}
			} else if mode == "no_proxy" && tunnels.Load() != 0 {
				t.Fatal("NO_PROXY was ignored")
			}
		})
	}
}

func wssProxyJoinSucceeds(mode string) bool {
	return mode == "HTTPS_PROXY" || mode == "https_proxy" || mode == "stalled_first_handshake"
}

// stallingRelay forwards TCP to target. Its first connection delivers the
// founder's WebSocket upgrade response and then withholds every later founder
// byte until the client gives up, which is what the actual cloud's failed
// official join looked like: the client got its 101 and failed in "negotiate
// security protocol" exactly at libp2p's 15s per-dial timeout, while the
// ingress logged the founder's reply flight on an upgrade that never completed.
// Later connections are relayed untouched.
func stallingRelay(t *testing.T, target string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var accepted atomic.Int32
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			stall := accepted.Add(1) == 1
			go func() {
				defer client.Close()
				server, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer server.Close()
				clientDone := make(chan struct{})
				go func() {
					_, _ = io.Copy(server, client)
					_ = server.Close()
					close(clientDone)
				}()
				if !stall {
					_, _ = io.Copy(client, server)
					return
				}
				reader := bufio.NewReader(server)
				var header []byte
				for !bytes.HasSuffix(header, []byte("\r\n\r\n")) {
					b, err := reader.ReadByte()
					if err != nil {
						return
					}
					header = append(header, b)
				}
				if _, err := client.Write(header); err != nil {
					return
				}
				<-clientDone
			}()
		}
	}()
	return listener.Addr().String()
}

type wssProxyFixture struct {
	Remote string
	Mode   string
	Invite entmoot.BootstrapCapability
}

func runWSSProxyClient(t *testing.T, fixture string) {
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var f wssProxyFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(fixture)
	identity, err := keystore.Load(filepath.Join(dir, "identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	// A stalled first connection needs the join's own budget, not one dial's.
	timeout := 8 * time.Second
	if f.Mode == "stalled_first_handshake" {
		timeout = 45 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	h, binding, err := NewConfiguredHost(ctx, identity, HostConfig{ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	remote, err := peer.Decode(f.Remote)
	if err != nil {
		t.Fatal(err)
	}
	address, err := ma.NewMultiaddr("/ip4/192.0.2.1/tcp/443/tls/sni/example.com/ws")
	if err != nil {
		t.Fatal(err)
	}
	group, _, err := JoinGroupVia(ctx, h, []peer.AddrInfo{{ID: remote, Addrs: []ma.Multiaddr{address}}}, dir, identity, f.Invite, mustNode(t, identity))
	if !wssProxyJoinSucceeds(f.Mode) {
		if err == nil {
			group.Close()
			t.Fatal("unsafe connection unexpectedly admitted")
		}
		if membership.Exists(dir, f.Invite.GroupID) {
			t.Fatal("failed transport wrote membership")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	if !group.IsMemberID(binding.MemberID) {
		t.Fatal("own signed join did not establish membership")
	}
	messages := storetest.New(t)
	defer messages.Close()
	live, err := NewLiveGroup(ctx, LiveConfig{Host: h, GroupID: f.Invite.GroupID, Group: group, Store: messages})
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	// Wait for actual topic subscription propagation, not an arbitrary sleep.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for len(live.topic.ListPeers()) == 0 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("remote live topic unavailable")
		}
	}
	// Match the existing live transport fixtures' GossipSub heartbeat settle.
	time.Sleep(1500 * time.Millisecond)
	msg := signedLiveMessage(t, identity, group, f.Invite.GroupID, time.Now().UnixMilli(), "signed through CONNECT")
	if state, err := live.Publish(ctx, msg); err != nil || state != DeliveryPublished {
		t.Fatalf("publish: %s %v", state, err)
	}
	// Confirm remote persistence over the real history protocol before closing.
	for {
		page, err := RequestHistoryPage(ctx, h, peer.AddrInfo{ID: remote}, HistorySyncRequest{Version: 2, RequestID: "wss-delivery-check", GroupID: f.Invite.GroupID, Mode: "bodies", IDs: []entmoot.MessageID{msg.ID}})
		if err != nil {
			t.Fatal(err)
		}
		for _, stored := range page.Messages {
			if stored.ID == msg.ID {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("remote did not persist signed message")
		}
	}
}
