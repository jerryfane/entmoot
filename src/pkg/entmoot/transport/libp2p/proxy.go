package libp2ptransport

import (
	"context"
	"net"
	"net/http"
	"net/url"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	multiaddr "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
)

// proxiedDNSResolver is libp2p's default DNS resolver, except that it leaves a
// WebSocket address named by /dns, /dns4 or /dns6 unresolved when the
// environment proxy (HTTPS_PROXY for /tls/ws, HTTP_PROXY for /ws, minus
// NO_PROXY) will carry it. The WebSocket transport then sends the name in its
// CONNECT, so the proxy resolves it, which is the only resolution a host whose
// sole egress is that proxy can get: resolving locally first fails there and
// the swarm drops the address as "no good addresses". The swarm has already
// let the transport add /sni from the name, so TLS and the Host header still
// carry it; the libp2p security handshake verifies the peer ID as for any
// other address. Without an applicable proxy, resolution is unchanged.
type proxiedDNSResolver struct {
	swarm.ResolverFromMaDNS
	// proxy is the WebSocket dialer's own proxy choice, so the two agree.
	proxy func(*http.Request) (*url.URL, error)
}

var _ network.MultiaddrDNSResolver = proxiedDNSResolver{}

func newProxiedDNSResolver() proxiedDNSResolver {
	return proxiedDNSResolver{ResolverFromMaDNS: swarm.ResolverFromMaDNS{Resolver: madns.DefaultResolver}, proxy: http.ProxyFromEnvironment}
}

// ResolveDNSComponent implements network.MultiaddrDNSResolver.
func (r proxiedDNSResolver) ResolveDNSComponent(ctx context.Context, maddr multiaddr.Multiaddr, outputLimit int) ([]multiaddr.Multiaddr, error) {
	if target, ok := webSocketDialURL(maddr); ok {
		if proxyURL, err := r.proxy(&http.Request{URL: target}); err == nil && proxyURL != nil {
			return []multiaddr.Multiaddr{maddr}, nil
		}
	}
	return r.ResolverFromMaDNS.ResolveDNSComponent(ctx, maddr, outputLimit)
}

// webSocketDialURL returns the URL go-libp2p's WebSocket transport hands its
// dialer's proxy function for a /dns* WebSocket address: scheme https for
// /tls/ws and /wss, http for /ws, and the /sni name in place of the DNS name
// when one is present.
func webSocketDialURL(maddr multiaddr.Multiaddr) (*url.URL, bool) {
	if len(maddr) < 3 || maddr[1].Code() != multiaddr.P_TCP {
		return nil, false
	}
	switch maddr[0].Code() {
	case multiaddr.P_DNS, multiaddr.P_DNS4, multiaddr.P_DNS6:
	default:
		return nil, false
	}
	host := maddr[0].Value()
	rest := maddr[2:]
	if last := len(rest) - 1; rest[last].Code() == multiaddr.P_P2P {
		rest = rest[:last]
	}
	scheme := "https"
	switch {
	case len(rest) == 1 && rest[0].Code() == multiaddr.P_WS:
		scheme = "http"
	case len(rest) == 1 && rest[0].Code() == multiaddr.P_WSS:
	case len(rest) == 2 && rest[0].Code() == multiaddr.P_TLS && rest[1].Code() == multiaddr.P_WS:
	case len(rest) == 3 && rest[0].Code() == multiaddr.P_TLS && rest[1].Code() == multiaddr.P_SNI && rest[2].Code() == multiaddr.P_WS:
		host = rest[1].Value()
	default:
		return nil, false
	}
	return &url.URL{Scheme: scheme, Host: net.JoinHostPort(host, maddr[1].Value())}, true
}
